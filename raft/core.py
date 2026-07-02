import threading
import time
import random
import logging
import queue
from enum import Enum, auto

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RaftState(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()
    SHUTDOWN = auto()

class RaftNode:
    """
    The core Consensus Module. 
    Implements Leader Election and Log Replication.
    """
    def __init__(self, node_id, peers, transport, storage):
        self.node_id = node_id
        self.peers = peers  # List of node IDs
        self.transport = transport
        self.storage = storage

        # Persistent state
        self.current_term = self.storage.get_term()
        self.voted_for = self.storage.get_voted_for()
        self.log = self.storage.get_log() # List of dicts: {'term': T, 'command': C}

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = RaftState.FOLLOWER
        
        # Concurrency: Mutex to protect state transitions and log access
        self.lock = threading.Lock()
        
        # Election Timer logic
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(0.5, 0.7)
        
        # Leader-specific volatile state (reinitialized after election)
        self.next_index = {}
        self.match_index = {}

        # Background threads
        self.commit_queue = queue.Queue()
        self.running = True
        threading.Thread(target=self._run_election_timer, daemon=True).start()
        threading.Thread(target=self._run_apply_worker, daemon=True).start()

    def _run_election_timer(self):
        while self.running:
            time.sleep(0.01)
            with self.lock:
                if self.state == RaftState.LEADER or self.state == RaftState.SHUTDOWN:
                    continue
                
                elapsed = time.time() - self.last_heartbeat
                if elapsed >= self.election_timeout:
                    self._start_election()

    def _start_election(self):
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.storage.save_state(self.current_term, self.voted_for)
        self.last_heartbeat = time.time()
        
        logging.info(f"Node {self.node_id} starting election for term {self.current_term}")
        
        self.votes_received = {self.node_id}
        
        for peer in self.peers:
            threading.Thread(target=self._request_vote, args=(peer,), daemon=True).start()

    def _request_vote(self, peer_id):
        with self.lock:
            if self.state != RaftState.CANDIDATE: return
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0
            term_at_call = self.current_term

        args = {
            'term': term_at_call,
            'candidateId': self.node_id,
            'lastLogIndex': last_log_index,
            'lastLogTerm': last_log_term
        }
        
        response = self.transport.send_request_vote(peer_id, args)
        
        if response:
            with self.lock:
                if response['term'] > self.current_term:
                    self._step_down(response['term'])
                    return
                
                if self.state == RaftState.CANDIDATE and response['voteGranted'] and response['term'] == self.current_term:
                    self.votes_received.add(peer_id)
                    if len(self.votes_received) > (len(self.peers) + 1) // 2:
                        self._become_leader()

    def handle_request_vote(self, args):
        """
        RECEIVER: Handles RequestVote RPCs.
        Implements the Election Restriction safety check.
        """
        with self.lock:
            # 1. Reply false if term < currentTerm
            if args['term'] < self.current_term:
                return {'term': self.current_term, 'voteGranted': False}

            if args['term'] > self.current_term:
                self._step_down(args['term'])

            # 2. Check if already voted for someone else in this term
            if self.voted_for is not None and self.voted_for != args['candidateId']:
                return {'term': self.current_term, 'voteGranted': False}

            # 3. Election Restriction: Is the candidate's log at least as up-to-date as mine?
            my_last_index = len(self.log) - 1
            my_last_term = self.log[my_last_index]['term'] if my_last_index >= 0 else 0
            
            up_to_date = (args['lastLogTerm'] > my_last_term) or \
                         (args['lastLogTerm'] == my_last_term and args['lastLogIndex'] >= my_last_index)

            if up_to_date:
                self.voted_for = args['candidateId']
                self.storage.save_state(self.current_term, self.voted_for)
                self.last_heartbeat = time.time() # Reset election timer on voting
                return {'term': self.current_term, 'voteGranted': True}
            else:
                return {'term': self.current_term, 'voteGranted': False}

    def _become_leader(self):
        logging.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
        self.state = RaftState.LEADER
        
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1
            # Start a heartbeater thread for each peer
            threading.Thread(target=self._append_entries_loop, args=(peer,), daemon=True).start()

    def _append_entries_loop(self, peer_id):
        while self.running:
            with self.lock:
                if self.state != RaftState.LEADER: break
                
                prev_idx = self.next_index[peer_id] - 1
                prev_term = self.log[prev_idx]['term'] if prev_idx >= 0 else 0
                entries = self.log[self.next_index[peer_id]:]
                term_at_call = self.current_term
                commit_idx = self.commit_index

            args = {
                'term': term_at_call,
                'leaderId': self.node_id,
                'prevLogIndex': prev_idx,
                'prevLogTerm': prev_term,
                'entries': entries,
                'leaderCommit': commit_idx
            }

            response = self.transport.send_append_entries(peer_id, args)
            
            with self.lock:
                if not response or self.state != RaftState.LEADER or self.current_term != term_at_call:
                    time.sleep(0.05)
                    continue

                if response['term'] > self.current_term:
                    self._step_down(response['term'])
                    break

                if response['success']:
                    self.next_index[peer_id] = prev_idx + len(entries) + 1
                    self.match_index[peer_id] = prev_idx + len(entries)
                    self._update_commit_index()
                else:
                    # Log consistency check failed, decrement and retry
                    self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)

            time.sleep(0.05)

    def handle_append_entries(self, args):
        with self.lock:
            if args['term'] < self.current_term:
                return {'term': self.current_term, 'success': False}
            
            self.last_heartbeat = time.time()
            if args['term'] > self.current_term:
                self._step_down(args['term'])
            
            self.state = RaftState.FOLLOWER

            log_len = len(self.log)
            if args['prevLogIndex'] >= log_len:
                return {'term': self.current_term, 'success': False}
            
            if args['prevLogIndex'] >= 0 and self.log[args['prevLogIndex']]['term'] != args['prevLogTerm']:
                return {'term': self.current_term, 'success': False}

            new_idx = args['prevLogIndex'] + 1
            for i, entry in enumerate(args['entries']):
                if new_idx + i < len(self.log):
                    if self.log[new_idx + i]['term'] != entry['term']:
                        self.log = self.log[:new_idx + i]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            
            if args['entries']:
                self.storage.save_state(self.current_term, self.voted_for, self.log)

            if args['leaderCommit'] > self.commit_index:
                self.commit_index = min(args['leaderCommit'], len(self.log) - 1)

            return {'term': self.current_term, 'success': True}

    def _update_commit_index(self):
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n]['term'] != self.current_term: continue
            
            count = 1
            for peer in self.peers:
                if self.match_index[peer] >= n:
                    count += 1
            
            if count > (len(self.peers) + 1) // 2:
                self.commit_index = n
                logging.info(f"Leader committed up to index {n}")
                break

    def _step_down(self, new_term):
        self.current_term = new_term
        self.state = RaftState.FOLLOWER
        self.voted_for = None
        self.storage.save_state(self.current_term, self.voted_for)

    def propose(self, command):
        """
        Submits a command to the log.
        Returns: (is_leader, index, term)
        """
        with self.lock:
            if self.state != RaftState.LEADER:
                return False, -1, self.current_term
            
            entry = {'term': self.current_term, 'command': command}
            self.log.append(entry)
            index = len(self.log) - 1
            self.storage.save_state(self.current_term, self.voted_for, self.log)
            logging.info(f"Leader proposed index {index} in term {self.current_term}")
            return True, index, self.current_term

    def _run_apply_worker(self):
        """
        Monitors commit_index and pushes committed entries to the commit_queue.
        """
        while self.running:
            time.sleep(0.01)
            to_apply = []
            with self.lock:
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    entry = self.log[self.last_applied].copy()
                    entry['index'] = self.last_applied
                    to_apply.append(entry)
            
            for entry in to_apply:
                self.commit_queue.put(entry)
