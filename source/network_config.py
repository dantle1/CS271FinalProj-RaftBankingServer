import json
from PreProcess import *

start_port = 10000
delay = 5
end_symbol = "###"
server1 = {
    "send_port" :start_port+1,
    "recv_port": start_port+2,
    "name" : "server1",
    "id" : 1,
    "cluster" : 1
}

server2 = {
    "send_port" :start_port+3,
    "recv_port": start_port+4,
    "name" : "server2",
    "id" : 2,
    "cluster" : 1
}

server3 = {
    "send_port" :start_port+5,
    "recv_port": start_port+6,
    "name" : "server3",
    "id" : 3,
    "cluster" : 1
}

server4 = {
    "send_port" :start_port+7,
    "recv_port": start_port+8,
    "name" : "server4",
    "id" : 4,
    "cluster" : 2
}

server5 = {
    "send_port" :start_port+9,
    "recv_port": start_port+10,
    "name" : "server5",
    "id" : 5,
    "cluster" : 2
}

server6 = {
    "send_port" :start_port+11,
    "recv_port": start_port+12,
    "name" : "server6",
    "id" : 6,
    "cluster" : 2
}

server7 = {
    "send_port" :start_port+13,
    "recv_port": start_port+14,
    "name" : "server7",
    "id" : 7,
    "cluster" : 3
}

server8 = {
    "send_port" :start_port+15,
    "recv_port": start_port+16,
    "name" : "server8",
    "id" : 8,
    "cluster" : 3
}

server9 = {
    "send_port" :start_port+17,
    "recv_port": start_port+18,
    "name" : "server9",
    "id" : 9,
    "cluster" : 3
}

server_configs = {
    "server1" : server1,
    "server2" : server2,
    "server3" : server3,
    "server4" : server4,
    "server5" : server5,
    "server6" : server6,
    "server7" : server7,
    "server8" : server8,
    "server9" : server9
}

clientA = {
    "send_port" :start_port+18,
    "recv_port": start_port+19,
    "name" : "A"
}

clientB = {
    "send_port" :start_port+20,
    "recv_port": start_port+21,
    "name" : "B"
}

client_configs = {
    'A': clientA,
    'B': clientB
}

process_file("input_file.txt")

with open('input_file_1.txt', 'r') as myfile:
    content = myfile.readlines()
transaction_list_1 = [x.strip() for x in content]
#print(transaction_list_1)

with open('input_file_2.txt', 'r') as myfile:
    content = myfile.readlines()
transaction_list_2 = [x.strip() for x in content]
#print(transaction_list_2)

with open('input_file_3.txt', 'r') as myfile:
    content = myfile.readlines()
transaction_list_3 = [x.strip() for x in content]
#print(transaction_list_3)

with open('../logs/datastore.json', 'r') as myfile:
    datastore = json.load(myfile)
# print(datastore)