import queue
import sys
import os
import threading
import socket
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
        self.estimate_leader = None
        self.estimate_leader_2 = None
        self.id2amount = {}
        self.cmd_thread = threading.Thread(
            target=self.handle_cmds, daemon=True)
        
    def commandList(self):
        print('1. transfer/t <x> <y> <amt> : Transfer money from X to Y.')
        print('2. balance/b <x> : Print current balance of X.')
        print('3. datastore/d <x>: Print all committed transactions for Xth cluster.')
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
            elif re.match(r"^(?:datastore|d)$", input_txn):
                self.request_datastore()
            else:
                print('No Valid Command, Select from one below:')
                self.commandList()

    def handle_printBalance(self, req):
        print(f"User {req['item_id']} has a balance of {req['balance']} units")
    
    def start(self):
        self.tcpServer.start_server(self.rpc_queue)
        self.cmd_thread.start()
        self.userInput()

    def add_transaction(self):
        '''
        prompt user for standard input of transactions
        '''
        pass

    def handle_complete_txn(self, req):
        print("---complete")
        txn_id = req['txn_id']
        if txn_id in self.txns:
            self.txns.pop(txn_id)
        else:
            return

        self.balance = float(req['balance'])
        print("updated balance:", self.balance)

    

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
            # transaction
            if time.time() - last_upload_time > interval:
                while not self.txn_queue.empty():
                    ta, tb, amount, txn_id = self.txn_queue.get()
                    assert not (txn_id in self.txns)
                    self.txns[txn_id] = (ta, tb, amount)
                last_upload_time = time.time()

                for txn_id in self.txns:
                    ta, tb, amount = self.txns[txn_id]
                    self.send_clientCommand(ta, tb, amount, txn_id)

    def send_clientCommand(self, ta, tb, amount, txn_id):
        data = {
            'type': clientCommandType,
            'send_client': ta,
            'recv_client': tb,
            'amount': amount,
            'txn_id' : txn_id,
            'source' : self.name
        }
        if getClusterofItem(int(ta)) == getClusterofItem(int(tb)):
            leader = self.estimate_leader
            if self.estimate_leader == None:
                leader = random.choice(self.server_names)
        print("sending txn to esitmated leader", leader)
        data = json.dumps(data)
        # print(data)
        self.tcpServer.send(leader, data)

    def update_client(self):
        pass

    def request_balance(self, item_id):
        data = {
        'type': printBalanceType,
        'source': self.name,
        'item_id': item_id  # Request balance for a specific client ID
        }
        # print("cluster: ", getClusterofItem(item_id))
        servers_in_cluster = [name for name in self.server_names if server_configs[name]['cluster'] == getClusterofItem(item_id)]
        leader = self.estimate_leader or random.choice(servers_in_cluster)

        for server in servers_in_cluster:
            print(f"Requesting balance for Client {item_id} from {server}")
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
        self.id2amount[self.txn_id] = amount
        self.txn_id += 1


def client_main():
    client_name = sys.argv[1]
    client = Client(client_name, server_configs, client_configs)
    client.start()

if __name__ == "__main__":
    client_main()
