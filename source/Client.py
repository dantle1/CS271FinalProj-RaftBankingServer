import queue
import sys
import threading
import json
import re

from network_config import *
from ServerNode import *
from utils import *

class Client():
    def __init__(self, client_name, server_configs, client_configs):
        config = client_configs[client_name]
        self.name = config["name"]
        self.config = config
        self.current_txn = None
        self.server_names = list(server_configs.keys())
        all_configs = dict(server_configs)
        all_configs.update(client_configs)
        self.tcpServer = RaftTCPServer(self.name, all_configs)
        self.rpc_queue = queue.Queue()
        self.txn_queue = queue.Queue()
        self.txn_id = 0
        self.cluster_leaders = {1: None, 2: None, 3: None}
        self.estimate_leader = None
        self.estimate_leader_2 = None
        self.id2amount = {}
        self.cmd_thread = threading.Thread(
            target=self.handle_cmds, daemon=True)
        
    def commandList(self):
        print('1. transfer/t <x> <y> <amt> : Transfer money from X to Y.')
        print('2. balance/b <x> : Print current balance of X.')
        print('3. commitlist/c <x>: Print all committed transactions for Xth cluster.')
        print('4. help/h: Show command list again.')
    
    def userInput(self):
        print('Welcome to the Bank Client!')
        print('Below are the list of commands: ')
        self.commandList()
        while True:
            input_txn = input('>>>')
            # print balance
            if re.match(r"^(?:balance|b) (\d+)$", input_txn):
                parsed_text = input_txn.split()
                item_id = int(parsed_text[1])
                if getClusterofItem(item_id) == -1:
                    print("ERROR: Item ID is not in datastore")
                else:
                    self.request_balance(item_id)
            # upload transaction
            elif re.match(r"^(?:transfer|t) (\d+) (\d+) (\d+)$", input_txn):
                self.upload_transaction(input_txn)
            # help
            elif re.match(r"^(?:help|h)$", input_txn):
                self.commandList()
            # datastore
            elif re.match(r"^(?:commitlist|c) (\d+)$", input_txn):
                parsed_text = input_txn.split()
                cluster_id = int(parsed_text[1])
                self.print_commit_list(cluster_id)
            else:
                print('No Valid Command, Select from one below:')
                self.commandList()

    def print_commit_list(self, cluster_id):
        filename = f"../logs/cluster_{cluster_id}_log.txt"
        try:
            with open(filename, "r") as myfile:
                for line in myfile:
                    print(line.strip())
        except FileNotFoundError:
            print(f"ERROR: Invalid Cluster id {cluster_id}.")
            
    def handle_printBalance(self, req):
        print(f"User {req['item_id']} has a balance of {req['balance']} units")
    
    def start(self):
        self.tcpServer.start_server(self.rpc_queue)
        self.cmd_thread.start()
        self.userInput()

    def handle_complete_txn(self, req):
        print("---complete")
        txn_id = req['txn_id']
        if txn_id in self.txns:
            self.txns.pop(txn_id)
        else:
            return
        self.sender_balance = int(req['new_sender_balance'])
        self.receiver_balance = int(req['new_receiver_balance'])
        self.estimate_leader = req['leader_hint']
        # print("Leader: ", self.estimate_leader)
        # print("updated sender balance:", self.sender_balance)
        # print("updated receiver balance:", self.receiver_balance)

    def handle_cmds(self):
        self.txns = {}
        last_upload_time = time.time()
        interval = 1
        while True:
            # response
            while not self.rpc_queue.empty():
                req = self.rpc_queue.get()
                req_type = req['type']
                if req_type == txnCommitType:
                    self.handle_complete_txn(req)
                elif req_type == printBalanceType:
                    self.handle_printBalance(req)
                elif req['type'] == "leaderRedirect":
                    new_leader = req['leader']
                    cluster_id = req['cluster_id']
                    print(f"Redirected: Updating leader to {new_leader}")
                    self.estimate_leader = new_leader
                    self.cluster_leaders[cluster_id] = new_leader
                    print("Cluster Leaders: ", self.cluster_leaders)

            # transaction
            if time.time() - last_upload_time > interval:
                while not self.txn_queue.empty():
                    ta, tb, amount, txn_id = self.txn_queue.get()
                    # print(ta, tb, amount, txn_id)
                    assert not (txn_id in self.txns)
                    self.txns[txn_id] = (ta, tb, amount)
                for txn_id in self.txns:
                    ta, tb, amount = self.txns[txn_id]
                    print("txn_id: ", txn_id)
                    self.send_clientCommand(ta, tb, amount, txn_id)
                last_upload_time = time.time()

    def send_clientCommand(self, ta, tb, amount, txn_id):
        
        cluster_a = getClusterofItem(int(ta))
        cluster_b = getClusterofItem(int(tb))
        # intra shard transaction
        
        if cluster_a == cluster_b:
            data = {
            'type': clientCommandType,
            'isCross' : False,
            'send_client': ta,
            'recv_client': tb,
            'amount': amount,
            'txn_id' : txn_id,
            'source' : self.name
             }
            leader = self.cluster_leaders[cluster_a]
            servers_in_cluster = [name for name in self.server_names if server_configs[name]['cluster'] == cluster_a]
            if self.estimate_leader == None:
                leader = random.choice(servers_in_cluster)
            print(f"sending txn to server {leader}, who is the leader of cluster {cluster_a}")
            data = json.dumps(data)
            # print(data)
            self.tcpServer.send(leader, data)
        # cross shard interaction
        else:
            print("2PC needed for cross cluster transactions.")
            data = {
            'type': clientCommandType,
            'isCross' : True,
            'send_client': ta,
            'recv_client': tb,
            'amount': amount,
            'txn_id' : txn_id,
            'source' : self.name
             }
            leader_1 = self.estimate_leader
            leader_2 = self.estimate_leader_2
            servers_in_cluster1 = [name for name in self.server_names if server_configs[name]['cluster'] == cluster_a]
            servers_in_cluster2 = [name for name in self.server_names if server_configs[name]['cluster'] == cluster_b]
            if self.estimate_leader == None:
                leader1 = random.choice(servers_in_cluster1)
            if self.estimate_leader_2 == None:
                leader2 = random.choice(servers_in_cluster2)
            print(f"sending txn to server {leader_1}, who is the leader of cluster {cluster_a}")
            print(f"sending txn to server {leader_2}, who is the leader of cluster {cluster_b}")
            data = json.dumps(data)
            self.tcpServer.send(leader_1, data)
            self.tcpServer.send(leader_2, data)


    def request_balance(self, item_id):
        data = {
        'type': printBalanceType,
        'source': self.name,
        'item_id': item_id  # Request balance for a specific client ID
        }
        # print("cluster: ", getClusterofItem(item_id))
        servers_in_cluster = [name for name in self.server_names if server_configs[name]['cluster'] == getClusterofItem(item_id)]
        for server in servers_in_cluster:
            # print(f"Requesting balance for Client {item_id} from {server}")
            str_data = json.dumps(data)
            self.tcpServer.send(server, str_data)

    def upload_transaction(self, input_txn):
        parsed_txn = input_txn.split()
        if len(parsed_txn) != 4:
            print("wrong format. enter ta, tb, tc")
            return
        ta = int(parsed_txn[1])
        tb = int(parsed_txn[2])
        if getClusterofItem(ta) == -1 or getClusterofItem(tb) == -1:
            print("ERROR: Item ID is not in datastore")
            return
        amount = parsed_txn[3]
        self.txn_queue.put((ta, tb, amount, self.txn_id))
        print(list(self.txn_queue.queue))
        self.id2amount[self.txn_id] = amount
        self.txn_id += 1

def client_main():
    client_name = sys.argv[1]
    client = Client(client_name, server_configs, client_configs)
    client.start()

if __name__ == "__main__":
    client_main()
