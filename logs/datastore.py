import json
import random

def generate_clusters():
    clusters = {
        "cluster_1": {str(i): 10 for i in range(1, 1001)},
        "cluster_2": {str(i): 10 for i in range(1001, 2001)},
        "cluster_3": {str(i): 10 for i in range(2001, 3001)}
    }
    
    with open("datastore.json", "w") as f:
        json.dump(clusters, f, indent=4)
    
    print("datastore.json file has been generated.")

if __name__ == "__main__":
    generate_clusters()
