import hashlib
import os
import random
import base64
import json

def dataStore(cluster):
    with open("../logs/datastore.json", "r") as file:
        data_store = json.load(file)
    key = "cluster_" + str(cluster)
    return data_store[key]

def getClusterofItem(x):
    # invalid cluster
    if x <= 0 or x > 3000:
        return -1
    # cluster 1
    elif x >= 1 and x <= 1000:
        return 1
    # cluster 2
    elif x >= 1001 and x <= 2000:
        return 2
    # cluster 3
    else:
        return 3

def gen_timeout(t=4):
    '''
    t : timeout
    return timeout in [t, 2t]
    '''
    return random.random() * t + t

