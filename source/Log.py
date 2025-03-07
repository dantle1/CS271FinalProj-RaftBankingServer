from utils import *
import json

class Block:
    def __init__(self, ta, term):
        self.ta = ta
        self.term = term
        self.infos = []

    def add_info(self, info):
        self.infos.append(info)
        
    def __str__(self):
        return "term: %d, ta: %s" % (self.term, self.ta)

    def to_dict(self):
        return {
            "txn": self.ta,
            "term": self.term
        }

    def from_dict(d):
        ta = d["txn"] 
        term = d["term"]
        return Block(ta, term)

    def print_block(self):
        data = self.to_dict()
        for key in data:
            print(key, ":", data[key])


class Log():
    def __init__(self):
        self.chain = []
        self.commitIndex = 0
        
    def append(self, block):
        self.chain.append(block)

    def get(self, idx):
        return self.chain[idx]

    def pop(self, idx):
        self.chain.pop(idx)

    def __len__(self):
        return len(self.chain)

    def get_commitIndex(self):
        return self.commitIndex

    def get_entries_start_at_list(self, pos):
        res = []
        for i in range(pos, len(self.chain)):
            block = self.chain[i]
            res.append(block.to_dict())
        return res

    def lastLogIndex(self):
        return len(self.chain) - 1

    def lastLogTerm(self):
        return self.chain[-1].term

    def update_chain_at(self, start, block_list):
        # no need to calculate previous block's hash again
        self.chain = self.chain[:start]
        for i in range(len(block_list)):
            self.chain.append(block_list[i])

    def commit_next(self):
        print("----commit")
        self.commitIndex += 1

    def print_chain(self):
        print("===========================================")
        print("===========================================")
        for i in range(len(self.chain)):
            block = self.chain[i]
            print("Block index", i)
            block.print_block()
            print("+++++++++++++++++++++++++++++++")
        print("===========================================")
        print("===========================================")

    def txn_committed(self, info):
        for i in range(self.get_commitIndex()+1):
            if info in self.chain[i].infos:
                return True
        return False

    def has_txn(self, info):
        for i in range(len(self.chain)):
            if info in self.chain[i].infos:
                return True
        return False
