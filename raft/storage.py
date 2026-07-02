import json
import os
import base64

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
        if log is None:
            log = self.get_log()
        
        # Serialize log commands if they are bytes
        serializable_log = []
        for entry in log:
            e = entry.copy()
            if isinstance(e['command'], bytes):
                e['command'] = base64.b64encode(e['command']).decode('utf-8')
                e['_is_bytes'] = True
            serializable_log.append(e)

        data = {
            "term": term,
            "voted_for": voted_for,
            "log": serializable_log
        }
        with open(self.filename, 'w') as f:
            json.dump(data, f)

    def get_term(self):
        return self._read()["term"]

    def get_voted_for(self):
        return self._read()["voted_for"]

    def get_log(self):
        log = self._read()["log"]
        # Deserialize log commands
        for entry in log:
            if entry.get('_is_bytes'):
                entry['command'] = base64.b64decode(entry['command'])
        return log

    def _read(self):
        with open(self.filename, 'r') as f:
            return json.load(f)
