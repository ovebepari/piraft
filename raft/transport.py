import json
import socket
import threading

class Transport:
    """
    Modular Networking Layer. 
    Can be swapped with gRPC or ZeroMQ.
    This version uses simple TCP sockets.
    """
    def __init__(self, port_map):
        self.port_map = port_map # {node_id: port}
        
    def send_request_vote(self, target_id, args):
        """
        Synchronous wrapper for an asynchronous RPC call.
        """
        try:
            return self._send_payload(target_id, "RequestVote", args)
        except Exception:
            return None

    def send_append_entries(self, target_id, args):
        try:
            return self._send_payload(target_id, "AppendEntries", args)
        except Exception:
            return None

    def _send_payload(self, target_id, method, args):
        port = self.port_map.get(target_id)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.1) # Don't block the consensus module forever
            s.connect(('localhost', port))
            payload = json.dumps({"method": method, "args": args})
            s.sendall(payload.encode())
            data = s.recv(1024)
            return json.loads(data.decode())

class TransportServer:
    """
    Listens for incoming RPCs and routes them to the RaftNode methods.
    """
    def __init__(self, raft_node, port):
        self.raft_node = raft_node
        self.port = port
        self.server_thread = threading.Thread(target=self._listen, daemon=True)

    def start(self):
        self.server_thread.start()

    def _listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self._handle_conn, args=(conn,)).start()

    def _handle_conn(self, conn):
        with conn:
            data = conn.recv(4096)
            if not data: return
            msg = json.loads(data.decode())
            
            response = {}
            if msg['method'] == "RequestVote":
                # RaftNode would have a handle_request_vote method
                response = self.raft_node.handle_request_vote(msg['args'])
            elif msg['method'] == "AppendEntries":
                response = self.raft_node.handle_append_entries(msg['args'])
                
            conn.sendall(json.dumps(response).encode())
