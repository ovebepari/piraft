import threading
import time
import random
import logging
from enum import Enum, auto

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RaftState(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()
    SHUTDOWN = auto()

class RaftNode:
    """
    The Core Consensus Module.
    Implements Leader Election and Log Replication.
    """
    def __init__(self, node_id, peers, transport, storage):
        self.node_id = node_id
        self.peers = peers # List of node IDs
        self.transport = transport
        self.storage = storage

        # Persistent State
        self.current_term = self.storage.get_term()
        self.voted_for = self.storage.get_voted_for()
        self.log = self.storage.get_log() # List of dicts: {'term': T, 'command': C}

        # Volatile State
        self.commit_index = 0
        self.last_applied = 0
        self.state = RaftState.FOLLOWER

        # Concurrency: Mutex to protect state transitions and log access
        self.lock = threading.Lock()

        # Election Timer Logic
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(0.15, 0.3)

        # Leader-specific volatile state (reinitialized afer election)
        self.next_index = {}
        self.match_index = {}

        # Background Threads
        self.running = True
        threading.Thread(target=self._run_election_timer, daemon=True).start()

    def _run_election_timer(self):
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.storage.save_state(self.current_term, self.voted_for)
        self.last_hearbeat = time.time()

        logging.info(f"Node {self.node_id} starting election for term {self.current_term}")

        self.votes_received = {self.node_id}

        for peer in self.peers:
            threading.Thread(target=slef._request_vote, args=(peer,), daemon=True).start()


    def _request_vote(self, peer_id):
        with self.lock:
            if self.state != RaftState.CANDIDATE: return
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0
            term_at_call = self.current_term

            args =  {
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
                    if self.state == RaftState.CANDIDATE \
                    and response['voteGranted'] \
                    and response['term'] == self.current_term:
                        self.votes_received.add(peer_id)
                        if len(self.votes_received) > (len(self.peers)+1) // 2:
                            self._become_leader()

    def _become_leader(self):
        logging.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
        self.state = RaftState.LEADER

        # Initialize Leader State
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1
            # Start heartbeat thread for each peer
            threading.Thread(target=self._append_entries_loop, args=(peer,), daemon=True).start()

    def _append_entries_loop(self, peer_id):
        """
        Leader loop for a specific peer. Handles heartbeats and replication.
        Uses a simple back-off or interval (e.g., 50ms).
        """
        while self.running:
            with self.lock:
                if self.state != RaftState.LEADER: break

                prev_idx = self.next_index[peer_id] -1
                prev_term = self.log[prev_idx]['term'] if prev_idx >= 0 else 0
                entries = self.log[self.next_index[peer_id]:]
                term_at_call = self.current_term
                commit_idx = self.commit_index

                args = {
                    'term': term_at_call,
                    'leaderID': self.node_id,
                    'prevLogIndex': prev_idx,
                    'entries': entries,
                    'leaderCommit': commit_idx
                }

                response = self.transport.send_append_entries(peer_id, args)

                with self.lock:
                    if not response or self.state != RaftState.LEADER \
                        or self.current_term != term_at_call:
                            time.sleep(0.05) # Backoff/wait for next heartbeat
                            continue

                    if respnse['term'] > self.current_term:
                        self._step_down(response['term'])
                        break
                    
                    if response['success']:
                        self.next_index[peer_id] = prev_idx + len(entries) + 1
                        self.match_index[peer_id] = prev_idx + len(entries)
                        self._update_commit_index()

                    else:
                        # Log consistency check failed, decrement and retry
                        self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)

                    time.sleep(0.05) # Heartbeat Interval

    def handle_append_entries(self, args):
        """
        RECEIVER: Handles incoming log entries and heatbeats
        """
        with self.lock:
            # 1. Reply false if term < currentTerm
            if args['term'] < self.current_term:
                return {'term': self.current_term, 'success': False}

            self.last_heartbeat = time.time()

            if args['term'] > self.current_term:
                self._step_down(args['term'])

            self.state = RaftState.FOLLOWER

            # 2. Consistency Check: prevLogIndex/prevLogTerm
            log_len = len(self.log)
            if args['prevLogIndex'] >= log_len:
                return {'term': self.current_term, 'success': False}

            if args['prevLogIndex'] >= 0 \
                and self.log[args['prevLogIndex']['term'] != args['prevLogTerm']:
                return {'term':self.current_term, 'success': False}

            # 3. Append entries, resolve conflicts
            new_idx = args['prevILogIndex'] + 1
            for i, entry in enumerate(args['entries']):
                if nex_idx + i < len(self.log):
                    if self.log[new_idx+i]['term'] != entry['term']:
                        self.log = slef.log[:new_idx+i] # truncate conflict
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            if args['entries']:
                self.storage.save_state(self.currnet_term, self.voted_for, self.log)

            # 4. Update Commit Index
            if args['leaderCommit'] > self.commit_index:
                self.commit_index = min(args['leaderCommit'], len(self.log) -1)

            return {'term': self.current_term, 'success': True}

    def _update_commit_index(self):
        """
        Leader only: Find largest N such that a majority of match_index[i] >= N
        """
        for n in range(len(self.log)-1, self.commit_index, -1):
            if self.log[n]['term'] != self.current_term: continue

            count = 1 #self
            for peer in self.peers:
                if self.match_index[peer] >= n:
                    count += 1

            if count > (len(self.peers)+1) // 2:
                self.commit_index = n
                logging.info(f"Leader commited up to index {n}")
                break

    def _step_down(self, new_term):
        self.current_term = new_term
        self.state = RaftState.FOLLOWER
        self.voted_for = None
        self.storage.save_state(self.current_term, self.voted_for)
