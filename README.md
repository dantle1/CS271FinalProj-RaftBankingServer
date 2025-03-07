USAGE

This project is designed to simulate a distributed banking system.  The banking system consists of 3000 different accounts, evenly split into 3 clusters.  Each cluster also has 3 individual servers, where one of the servers acts as a leader for committing transfer transactions and updating the system.  To get started:

1. Open 11 terminals & run ```cd source``` for all of them.
2. For two terminals, open a client.  To start a client:
   ```python Client.py <A/B>```
3. For the rest of the terminals, open a different server.  To start a server:
    ``` python ServerNode.py <1-9>```
4. To print balance for certain item, run on one of client terminals:
   ```balance <item_id>```
5. To print committed transactions for a cluster, run on client:
   ```commitlist <cluster_id>```
6. To start a transaction, run on client:
   ```transfer <item_x> <item_y> <amt>```
7. To partition a certain server (for demo purposes), run on client:
   ```partition <server(1-9)>```