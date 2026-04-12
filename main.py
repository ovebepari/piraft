import sys
import time
from raft.core import RaftNode
from raft.transport import Transport, TransportServer
from raft.storage import Storage

def run_node(node_id, all_nodes_ports):
    # 1. Setup Storage
    storage = Storage(node_id)
    
    # 2. Setup Transport
    peers = [id for id in all_nodes_ports.keys() if id != node_id]
    transport = Transport(all_nodes_ports)
    
    # 3. Initialize Node
    node = RaftNode(node_id, peers, transport, storage)
    
    # 4. Start RPC Server
    server = TransportServer(node, all_nodes_ports[node_id])
    server.start()
    
    print(f"Node {node_id} is online on port {all_nodes_ports[node_id]}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.running = False

if __name__ == "__main__":
    # Example cluster config
    config = {
        0: 5000,
        1: 5001,
        2: 5002
    }
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <node_id>")
    else:
        run_node(int(sys.argv[1]), config)
