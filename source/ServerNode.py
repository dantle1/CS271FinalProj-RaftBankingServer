import sys
import time
from utils import *
import queue
import threading
from Log import *
from RaftTCPServer import *
from network_config import *
follower_role = 0
candidate_role = 1
leader_role = 2
requestVoteType = "requestVote"
appendEntriesType = "appendEntriesType"
voteResponseType = "voteResponse"
appendEntriesResponseType = "appendEntriesResponseType"
startElectionType = "startElection"
clientCommandType = "clientCommand"
txnCommitType = "txnCommit"
printBalanceType = "printBalance"


def start_all_servers():
    servers = []
    for name in server_configs:
        servers.append(ServerNode(name, server_configs, client_configs))
        servers[-1].start()
    return servers


class ServerNode():
    def __init__(self, config_name, server_configs, client_configs, avg_election_timeout=10,
                 init_txns=None):

        # server configuration related
        config = server_configs[config_name]
        self.name = config_name
        self.config = config
        
        # cluster-related
        self.cluster = config['cluster']
        self.dataStore = dataStore(self.cluster)
        self.cluster_file = f"../logs/cluster_{self.cluster}_log.txt"
        print("Writing commits to ", self.cluster_file)
        self.log = Log()
        
        # raft election
        self.role = follower_role
        self.avg_election_timeout = avg_election_timeout
        self.election_timeout = gen_timeout(self.avg_election_timeout)
        self.vote_for = None
        self.received_vote = 0
        self.term = 0
        self.other_names = [name for name in server_configs if server_configs[name]['cluster'] == self.cluster]
        self.other_names.remove(self.name)
        self.last_refresh_time = time.time()
        self.crash_list = []
        #  RPC queue
        self.rpc_queue = queue.Queue()
        all_configs = dict(server_configs)
        all_configs.update(client_configs)

        # threading
        self.tcpServer = RaftTCPServer(self.name, all_configs)
        self.handler_thread = threading.Thread(
            target=self.handle_rpc_queue, daemon=True)
        self.timeout_thread = threading.Thread(
            target=self.check_time_out, daemon=True)
        self.notify_timeout_thread_refresh_time_event = threading.Event()
        self.notify_timeout_thread_refresh_time_event.clear()

        # heart beat for leader
        self.heartbeat_thread = None
        self.heartbeat_end_event = threading.Event()
        self.heartbeat_end_event.clear()
        self.leader = None

        # for store client txn
        self.txn_buffer = []

        # log commit info
        self.name2loggedIndex = None
        self.name2lastReqTime = {}
        for name in self.other_names:
            self.name2lastReqTime[name] = time.time()
        self.init_txns_list(init_txns)

    def calculate_balance(self):
        # Calculate balances based on committed transactions
        commit_init_transactions = []
        commitIdx = 0
        for block in self.log.chain[:self.log.commitIndex]:
            transaction = block.ta
            t = transaction.split()
            if len(t) != 3:
                continue
            sender, receiver, amount = t[0], t[1], int(t[2])
            # print(f"sender {sender} balance before: {self.dataStore[sender]}")
            # print(f"receiver {receiver} balance before: {self.dataStore[receiver]}")
            # server not involved with transactions for different clusters
            if getClusterofItem(int(sender)) != self.cluster and getClusterofItem(int(receiver)) != self.cluster:
                continue
            if self.dataStore[sender] >= amount:
                with open(self.cluster_file, "a") as myfile:
                    myfile.write(f"{commitIdx} {block.term} {sender} {receiver} {amount} \n")
                    self.dataStore[sender] -= amount
                    self.dataStore[receiver] += amount
                    commitIdx += 1
            else: 
                print(f"ERROR: Aborted transaction {t} due to insufficient funds")
                continue
            # print(f"sender {sender} balance after: {self.dataStore[sender]}")
            # print(f"receiver {receiver} balance after: {self.dataStore[receiver]}")
            commit_init_transactions.append((sender, receiver))
        return commit_init_transactions
            
    def init_txns_list(self, txns):
        for i in range(len(txns)):
            new_block = Block(txns[i], self.term)
            self.log.append(new_block)
        if i == len(txns)-1:
            self.txn_buffer = [(txns[i], (None, None))]
        self.log.commitIndex = len(self.log) - 1
        # self.log.print_chain()

    def start(self):
        self.last_refresh_time = time.time()
        # for testing
        if "is_leader" in self.config:
            self.trans_leader()

        self.tcpServer.start_server(self.rpc_queue)
        self.handler_thread.start()
        self.timeout_thread.start()
        while True:
            # handle user input?
            pass

    def heartbeat_leader_thread(self):
        last_time = time.time()
        interval = self.election_timeout/3.0
        while True:
            if self.heartbeat_end_event.is_set():
                self.heartbeat_end_event.clear() # no need to clear, but harmless
                return

            if time.time() - last_time > interval:
                self.send_heartbeats()
                last_time = time.time()


    def add_election_req(self, role):
        req = {
            "type": startElectionType
        }
        self.rpc_queue.put(req)

    def update_refresh_time(self):
        self.last_refresh_time = time.time()
        self.notify_timeout_thread_refresh_time_event.set()

    def check_time_out(self):
        self.last_refresh_time = time.time()
        while True:
            if self.notify_timeout_thread_refresh_time_event.is_set():
                # print("timeout thread refreshing....")
                self.last_refresh_time = time.time()
                self.notify_timeout_thread_refresh_time_event.clear()

            is_timeout = (
                time.time() - self.last_refresh_time) > self.election_timeout
            if is_timeout:
                # print(self.name, "timeout")
                self.add_election_req(candidate_role)
                self.last_refresh_time = time.time()

    def trans_candidate(self):
        self.update_refresh_time()
        self.role = candidate_role
        self.term += 1
        # candidate vote for itself
        self.vote_for = self.name
        self.received_vote = 1
        self.send_requestVotes()
        # print(self.crash_list)
        if self.received_vote >= 2:
            self.trans_leader()

    def trans_follower(self):
        # step down
        assert self.role != follower_role
        if self.role == leader_role:
            self.heartbeat_end_event.set()
            # self.heartbeat_thread.join()
            # print("heartbeat ends")
            self.heartbeat_thread = None
        assert self.heartbeat_thread == None
        self.role = follower_role
        self.vote_for = None
        self.leader = None

    def trans_leader(self):
        print("========================")
        print(self.name, "become leader, term", self.term, "votes", self.received_vote)
        print("========================")
        self.role = leader_role
        self.name2nextIndex = {}
        self.name2lastContactTime = {}
        self.name2lastReqTime = {}
        self.name2loggedIndex = {}

        # self.txn_buffer = []
        for name in self.other_names:
            self.name2nextIndex[name] = self.log.lastLogIndex() + 1
            self.name2lastContactTime[name] = time.time()
            self.name2loggedIndex[name] = set()
            self.name2lastReqTime[name] = time.time()
        self.send_heartbeats() # send heartbeats immediately
        self.heartbeat_end_event.clear()
        # self.heartbeat_thread = threading.Thread(
        #     target=self.heartbeat_leader_thread, daemon=True)
        # self.heartbeat_thread.start()        
        '''
        Todo: if leader has been elected
        1. send heartbeat (empty AppendEntries)
        2. receive client commd
        3. logIndex >= nextIndex for a follower, send AppendEntries with log entries
        4. N > commitIndex ?
        '''

    def response_vote(self, candidate, granted):
        data = {
            'type': voteResponseType,
            'term': self.term,
            'voteGranted': granted
        }
        data = json.dumps(data)
        self.tcpServer.send(candidate, data)

    def is_completer_log(self, lastLogIndex, lastLogTerm):
        '''
        todo
        '''
        not_completer = self.log.lastLogTerm() > lastLogTerm or\
                        (self.log.lastLogTerm() == lastLogTerm and\
                         self.log.lastLogIndex() > lastLogIndex)
        return not not_completer

    def handle_startElection_req(self, req):
        if self.role == leader_role:
            return
        print(self.name, "start election at term", self.term + 1)
        self.trans_candidate()

    def has_matched_block(self, index, term):
        if index <0 or index >= len(self.log):
            return False
        block = self.log.get(index)
        return block.term == term

    def handle_appendEntries_req(self, req):
        '''
        '''
        self.update_refresh_time()
        is_heartbeat = req['is_heartbeat']
        term = req['term']
        leader = req['leaderId']
        prevLogIndex = req['prevLogIndex']
        prevLogTerm = req['prevLogTerm']
        commitIndex = req['commitIndex']
        block_list = req['entries']
        block_list = [Block.from_dict(d) for d in block_list]
        next_id = prevLogIndex + len(block_list) + 1
        # not valid leader term case
        if self.term > term:
            self.response_appendEntries(leader, False, prevLogIndex, next_id, term)
            return

        if term > self.term:
            self.term = term

        self.leader = leader
        # stepdown
        if self.role == candidate_role or self.role == leader_role:
            self.trans_follower()
        
        # is heartbeat
        if is_heartbeat:
            self.response_appendEntries(leader, True, prevLogIndex, next_id, term, is_heartbeat=True)
            return
        
        assert prevLogIndex >= 0
        if not self.has_matched_block(prevLogIndex, prevLogTerm):
            self.response_appendEntries(leader, False, prevLogIndex, next_id, term)
            return
        
        # append new entries
        self.log.update_chain_at(prevLogIndex+1, block_list)
        self.log.commitIndex = commitIndex
        # advance state machine - balance

        # respond
        self.response_appendEntries(leader, True, prevLogIndex, next_id, term)
        
    def handle_requestVote_req(self, req):
        candidate_term = req['term']
        candidate = req['source']
        lastLogIndex = req['lastLogIndex']
        lastLogTerm = req['lastLogTerm']
        if candidate_term > self.term:
            self.term = candidate_term
            # stepdown
            if self.role == leader_role or self.role == candidate_role:
                self.trans_follower()

        if candidate_term == self.term and\
           (self.vote_for == None or self.vote_for == candidate) and\
           self.is_completer_log(lastLogIndex, lastLogTerm):
            print("%s grant vote to %s at term %d" % (self.name, candidate, self.term))
            self.vote_for = req['source']
            self.response_vote(candidate, True)
            self.election_timeout = gen_timeout(self.avg_election_timeout)
            self.update_refresh_time()
        else:
            self.response_vote(candidate, False)

    def handle_voteResponse_req(self, req):
        '''
        only candidate cares about it
        data = {
            'type': voteResponseType,
            'term': self.term,
            'voteGranted': granted
        }

        if not vote granted:
            => 'follower'
        if vote granted:
            add voted number, compare to majority
        '''
        if self.role != candidate_role:
            return

        majority = 2
        if req['voteGranted']:
            assert req['term'] <= self.term
            if req['term'] == self.term:
                self.received_vote += 1
                if self.received_vote >= majority:
                    self.trans_leader()
        if req['term'] > self.term:
            self.term = req['term']
            self.trans_follower()

    def update_success_follower_log(self, name, next_index):
        self.name2loggedIndex[name].add(next_index-1)
        self.name2nextIndex[name] = next_index
        
    # def majority(self):
    #     servers_in_cluster = [server for server in server_configs.values() if server['cluster'] == self.cluster]
    #     return int((len(servers_in_cluster)-len(self.crash_list))/2)+1
        # return int(len(configs)/2)+1

    def count_logged_nodes_num(self, i):
        # leader self logged
        count = 1
        for name in self.name2loggedIndex:
            for index in self.name2loggedIndex[name]:
                if i <= index:
                    count += 1
                    break
        return count

    def check_update_commit(self):
        start = self.log.get_commitIndex() + 1
        end = len(self.log)
        for i in range(start, end):
            if self.count_logged_nodes_num(i) >= 2:
                self.log.commit_next()
            else:
                break
            
    def handle_appendEntriesResponse_req(self, req):
        '''
        only leader considers this message
        when in other state (role), slow network may deliver previous
        sent response, so need to check role
        data = {
            "type" : appendEntriesResponseType,
            "term" : self.term,
            "success" : success,
            "source" : self.name,
            "prevLogIndex" : prevLogIndex,
            "is_heartbeat": is_heartbeat
        }
        '''
        if self.role != leader_role:
            return

        term = req['term']
        success = req['success']
        name = req['source']
        prevLogIndex = req['prevLogIndex']
        is_heartbeat = req['is_heartbeat']
        next_index = req['next_index']
        sent_term = req['sent_term']
        # stepdown
        if term > self.term:
            assert not req['success']
            self.trans_follower()
            return

        # corner case: append sent in previous terms
        # just returned, meaningless
        if sent_term < self.term:
            # assert not req['success']
            return
        
        if is_heartbeat:
            return
        
        if success:
            self.update_success_follower_log(name, next_index)
            self.check_update_commit()
        else:
            self.name2nextIndex[name] -= 1
            assert self.name2nextIndex[name] >= 0

    def handle_printBalance_req(self, req):
        print("----commit index:", self.log.get_commitIndex())
        if self.role != leader_role:
            return
        print("-----print balance")
        self.log.print_chain()
        client = req['source']
        item_id = req['item_id']
        item_balance = self.dataStore[str(item_id)]
        data = {
            "type" : printBalanceType,
            "source" : self.name,
            "item_id" : item_id,
            "balance" :  item_balance
        }
        data = json.dumps(data)
        self.tcpServer.send(client, data)

    def notify_client_leader(self, client_name, leader_name):
        response = {
            "type": "leaderRedirect",
            "leader": leader_name,
            "cluster_id": self.cluster
        }
        self.tcpServer.send(client_name, json.dumps(response))
        print(f"Redirecting client {client_name} to leader {leader_name}")

    def handle_clientCommand_req(self, req):
        '''
        data = {
            type: clientCommandType
            send_client:
            recv_client:
            amount:
        }
        '''
        if self.role == candidate_role:
            # print('CANDIDATE')
            # discard, let client reissue
            return
        elif self.role == follower_role:
            #print('FOLLOWER')
            # redirect
            # Redirect client with leader info
            if self.leader:
                leader_name = self.leader
            else:
                # If the leader is unknown, pick a random server (may not be accurate)
                leader_name = self.other_names[0] if self.other_names else None
            if leader_name:
                self.notify_client_leader(req['source'], leader_name)
            else:
                print("No known leader to redirect to. Dropping request.")
        elif self.role == leader_role:
            #print('LEADER')
            send_client = req['send_client']
            recv_client = req['recv_client']
            amount = req['amount']
            txn_id = req['txn_id']
            client_name = req['source']

            temp = str(send_client) + " " + str(recv_client) + " " + str(amount)
            txn_info = (client_name, txn_id)
            if self.log.txn_committed(txn_info):
                self.response_client_txn(txn_info)
                return
            if self.log.has_txn(txn_info):
                # do nothing, wait for commit
                return
            if len(self.txn_buffer)==1:
                if self.txn_buffer[0][1] == txn_info:
                    return

            self.txn_buffer.append((temp, txn_info))
            if len(self.txn_buffer) == 2:
                new_block = Block(self.txn_buffer[0][0], self.term)
                # client_name = txn_buffer[1][0]
                # txn_id = txn_buffer[1][1]
                txn_info_a = self.txn_buffer[0][1]
                txn_info_b = self.txn_buffer[1][1]
                new_block.add_info(txn_info_a)
                new_block.add_info(txn_info_b)
                self.log.append(new_block)
                self.log.print_chain()
                self.txn_buffer = []

    def  update_last_req_time(self, req):
        if not ("source" in req):
            return
        name = req['source']
        # only considers server name
        if name in self.other_names:
            self.name2lastReqTime[name] = time.time()

    def update_crash_list(self):
        if self.role == follower_role:
            return
        self.crash_list = []
        for name in self.name2lastReqTime:
            last_time = self.name2lastReqTime[name]
            if time.time() - last_time > self.election_timeout:
                self.crash_list.append(name)

    def handle_req(self, req):
        # print("req:", req, type(req))
        req_type = req["type"]
        self.update_last_req_time(req)
        if req_type == startElectionType:
            self.update_refresh_time()
            self.handle_startElection_req(req)
        elif req_type == requestVoteType:
            self.handle_requestVote_req(req)
        elif req_type == voteResponseType:
            self.handle_voteResponse_req(req)
        elif req_type == appendEntriesType:
            self.handle_appendEntries_req(req)
        elif req_type == appendEntriesResponseType:
            self.handle_appendEntriesResponse_req(req)
        elif req_type == clientCommandType:
            self.handle_clientCommand_req(req)
        elif req_type == printBalanceType:
            self.handle_printBalance_req(req)
        else:
            print("not implemented:", req)

    def handle_rpc_queue(self):
        '''
        todo: dispatch all reqs here
        '''
        while True:
            self.update_crash_list()
            if self.role == leader_role:
                self.check_update_commit()
                self.leader_update_followers()
            while not self.rpc_queue.empty():
                req = self.rpc_queue.get()
                if (req["type"] == appendEntriesType and req['is_heartbeat'] == False):
                    print(self.name, "term", self.term, ":", req)
                self.handle_req(req)
        return

    def send_requestVote(self, name):
        # print("sending vote request to", name)
        data = {
            "type": requestVoteType,
            "term": self.term,
            "lastLogIndex": len(self.log) - 1,
            "lastLogTerm": self.log.get(-1).term,
            "source": self.name
        }
        data = json.dumps(data)
        self.tcpServer.send(name, data)

    def send_requestVotes(self):
        for name in self.other_names:
            self.send_requestVote(name)

    def send_appendEntries(self, name, is_heartbeat=False):
        next_index = self.name2nextIndex[name]
        prevLogIndex = next_index-1
        data = {
            "type": appendEntriesType,
            "term": self.term,
            "prevLogIndex": prevLogIndex,
            "prevLogTerm": self.log.get(prevLogIndex).term,
            "source": self.name,
            "leaderId": self.name,
            "entries": self.log.get_entries_start_at_list(next_index),
            "commitIndex": self.log.get_commitIndex(),
            "is_heartbeat": is_heartbeat
        }
        data = json.dumps(data)
        self.tcpServer.send(name, data)

    def send_heartbeats(self):
        for name in self.name2nextIndex:
            self.send_appendEntries(name, is_heartbeat=True)

    def response_appendEntries(self, leader, success, prevLogIndex,
                               next_index, sent_term, is_heartbeat=False):
        data = {
            "type" : appendEntriesResponseType,
            "term" : self.term,
            "success" : success,
            "source" : self.name,
            "prevLogIndex" : prevLogIndex,
            "is_heartbeat": is_heartbeat,
            "next_index": next_index,
            "sent_term" : sent_term
        }
        data = json.dumps(data)
        self.tcpServer.send(leader, data)

    def response_client_txn(self, txn_info):
        commit_init_txns = self.calculate_balance()
        for commit_txn in commit_init_txns:
            data = {
                "type" : txnCommitType,
                "term" : self.term,
                "source" : self.name,
                "txn_id" : txn_info[1],
                "new_sender_balance" :  self.dataStore[commit_txn[0]],
                "new_receiver_balance" : self.dataStore[commit_txn[1]],
                "leader_hint" : self.leader
            }
            print("-----response txn")
            data = json.dumps(data)
            client = txn_info[0]
            self.tcpServer.send(client, data)

    def redirect_clientCommand(self, name, req):
        data = json.dumps(req)
        self.tcpServer.send(name, data)

    def leader_update_followers(self):
        assert self.role == leader_role
        interval = self.avg_election_timeout/5
        for name in self.name2nextIndex:
            nextIndex = self.name2nextIndex[name]
            # print("--------", self.log.lastLogIndex(), nextIndex)
            if self.log.lastLogIndex() < nextIndex:
                # heartbeat
                if time.time() -  self.name2lastContactTime[name] > interval:
                    self.send_appendEntries(name, is_heartbeat=True)
                    self.name2lastContactTime[name] = time.time()
                continue
            
            if time.time() - self.name2lastContactTime[name] > interval:
                self.send_appendEntries(name, False)
                self.name2lastContactTime[name] = time.time()
        

def main():
    server_name = "server" + sys.argv[1]
    print(server_configs[server_name]['cluster'])
    if server_configs[server_name]['cluster'] == 1:
        transaction_list = transaction_list_1
    elif server_configs[server_name]['cluster'] == 2:
        transaction_list = transaction_list_2
    elif server_configs[server_name]['cluster'] == 3:
        transaction_list = transaction_list_3
    server = ServerNode(server_name, server_configs, client_configs, init_txns=transaction_list)
    server.start()


if __name__ == "__main__":
    main()
