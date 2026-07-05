# PiRaft: Raft Implementation in Python3

A lightweight and modular implementation of the Raft consensus algorithm. This project implements leader election, log replication, and persistence as described in the [Raft paper](https://raft.github.io/raft.pdf).

## Architecture

```text
                                  +-----------------------+
                                  |        Client         |
                                  +-----------------------+
                                              |
                                              | [Propose Command]
                                              v
      +---------------------------------------+---------------------------------------+
      |                                    Node (Leader)                              |
      |  +------------------+          +------------------+          +--------------+ |
      |  | TransportServer  | -------> |    RaftNode      | -------> |   Storage    | |
      |  +------------------+          | (Consensus Mod)  |          +--------------+ |
      |          ^                     +------------------+                 |         |
      |          |                            |        |                    |         |
      +----------|----------------------------|--------|--------------------|---------+
                 |                            |        |                    v
                 |      [AppendEntries RPC]   |        |              [raft_node_n.json]
                 | <--------------------------+        |
                 |                                     | [Commit Queue]
                 |                                     v
      +----------|----------+                +------------------+
      |  Follower Nodes     |                |  State Machine   |
      +---------------------+                +------------------+
```

## Running 

To start a cluster, run multiple instances of `main.py` with different node IDs:

```bash
# Node 0
python3 main.py 0

# Node 1
python3 main.py 1

# Node 2
python3 main.py 2
```

The nodes will automatically discover each other (based on the configuration in `main.py`) and initiate leader election.

## Client API

### Proposing Commands
To submit a command to the cluster, use the `propose` method on the `RaftNode`. Note that commands should only be proposed to the current leader.

```python
# Submitting a command
is_leader, index, term = node.propose("your-command-data")

if is_leader:
    print(f"Command accepted at log index {index}")
else:
    print("This node is not the leader; redirect your request.")
```

### Applying Committed Entries
Applications should monitor the `commit_queue` to apply committed log entries to their local state machine.

```python
while True:
    # This blocks until a new entry is committed
    entry = node.commit_queue.get()
    
    command = entry['command']
    index = entry['index']
    term = entry['term']
    
    print(f"Applying command {command} at index {index}")
```

## Directory Structure
```text
piraft/
├── raft/
│   ├── core.py       # Consensus Module (Election & Replication)
│   ├── transport.py  # Network Layer (TCP RPC)
│   ├── storage.py    # Persistence Layer (JSON Disk Write)
│   └── __init__.py
├── main.py           # Cluster Entry Point
└── README.md
```
