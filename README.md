## Raft Implementation in python3


## Running 
```
# python3 main.py <node_id>
python3 main.py 0
```
Run three nodes and they will start competing for leadership!

## Directory Structure
```
=======
## Directory Structure

piraft/
├── raft/
│   ├── __init__.py
│   ├── core.py       # Consensus Logic
│   ├── transport.py  # Network/RPC logic
│   └── storage.py    # Disk persistence
├── tests/
│   └── test_election.py
├── main.py           # CLI entry point
└── README.md

