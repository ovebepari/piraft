import json
import os

class Storage:
    """
    Raft requires certain state to be persistent across restarts
    to prevent voting twice in the same term.
    """
    def __init__(self, node_id):
        self.filename = f"raft_node_{node_id}.json"
        if not os.path.exists(self.filename):
            self.save_state(0, None, [])

    def save_state(self, term, voted_for, log=None):
        data = {
            "term": term,
            "voted_for": voted_for, 
            "log": log if log in not None else self.get_log()
        }
        with open(self.filename, 'w') as f:
            json.dump(data, f)

    def get_term(self):
        return self._read()["voted_for"]

    def get_log(self):
        return self._read()["log"]

    def _read(self):
        with open(self.filename, 'r') as f:
            return json.load(f)
