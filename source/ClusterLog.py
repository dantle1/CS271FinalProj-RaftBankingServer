import os
import json

class ClusterLogger:
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        self.commitIndex = 0  # Track committed transactions
        self.term = 0  # Current term
        self.filename = f"../logs/cluster_{cluster_id}_log.json"
        # Initialize the log file
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as f:
                json.dump({"transactions": [], "commitIndex": 0, "term": 0}, f)

    def append_transaction(self, transaction):
        """
        Appends a transaction (a dictionary) to the cluster's log file.
        """
        with open(self.filename, 'r') as f:
            data = json.load(f)
        
        data["transactions"].append(transaction)
        with open(self.filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"[Cluster {self.cluster_id}] Committed transaction: {transaction}")

    def read_log(self):
        """
        Reads the entire log file and returns a list of committed transactions.
        """
        with open(self.filename, 'r') as f:
            data = json.load(f)
        return data["transactions"]
    
    def update_term(self, new_term):
        """Updates the term in the log."""
        with open(self.filename, 'r') as f:
            data = json.load(f)
        
        data["term"] = new_term
        with open(self.filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"[Cluster {self.cluster_id}] Updated term to {new_term}")

    def commit_next(self):
        """Increments the commit index."""
        with open(self.filename, 'r') as f:
            data = json.load(f)
        
        data["commitIndex"] += 1
        with open(self.filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"[Cluster {self.cluster_id}] Commit index updated to {data['commitIndex']}")

# Example usage for three separate clusters:
logger1 = ClusterLogger(1)
logger2 = ClusterLogger(2)
logger3 = ClusterLogger(3)

# Append transactions to cluster logs
logger1.append_transaction({"from": "Alice", "to": "Bob", "amount": 50})
logger2.append_transaction({"from": "Charlie", "to": "Dana", "amount": 75})
logger3.append_transaction({"from": "Eve", "to": "Frank", "amount": 100})

# Update term
logger1.update_term(1)
logger2.update_term(1)
logger3.update_term(1)

# Commit next transaction
logger1.commit_next()
logger2.commit_next()
logger3.commit_next()

# Inspect logs
#print("Cluster 1 Log:", logger1.read_log())
# print("Cluster 2 Log:", logger2.read_log())
#print("Cluster 3 Log:", logger3.read_log())
