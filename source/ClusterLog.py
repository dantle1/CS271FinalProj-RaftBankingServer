import os

class ClusterLogger:
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        # lock table 
        self.lock_table = {}
        self.filename = f"../logs/cluster_{cluster_id}_log.txt"
        # Initialize the log file by clearing any existing content.
        with open(self.filename, 'w') as f:
            f.write("")

    def append_transaction(self, transaction):
        """
        Appends a transaction (a string) to the cluster's log file.
        """
        with open(self.filename, 'a') as f:
            f.write(transaction + "\n")
        print(f"[Cluster {self.cluster_id}] Committed transaction: {transaction}")

    def read_log(self):
        """
        Reads the entire log file and returns a list of committed transactions.
        """
        with open(self.filename, 'r') as f:
            return f.read().splitlines()


# Example usage for three separate clusters:
logger1 = ClusterLogger(1)
logger2 = ClusterLogger(2)
logger3 = ClusterLogger(3)

# Instead of adding blocks to a blockchain, you append transactions to the corresponding file.
logger1.append_transaction("Alice Bob 50")     # Transaction: Alice sent 50 to Bob.
logger2.append_transaction("Charlie Dana 75")    # Transaction: Charlie sent 75 to Dana.
logger3.append_transaction("Eve Frank 100")      # Transaction: Eve sent 100 to Frank.

# If you need to inspect a log:
print("Cluster 1 Log:", logger1.read_log())
print("Cluster 2 Log:", logger2.read_log())
print("Cluster 3 Log:", logger3.read_log())