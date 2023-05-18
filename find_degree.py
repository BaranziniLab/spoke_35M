import pandas as pd
import networkx as nx
import sys
import boto3
import time
import pickle

# node_type = sys.argv[1]
node_type = "Compound"
GRAPH_PATH = sys.argv[1]
bucket_name = sys.argv[2]
file_name = sys.argv[3]


def main():
    start_time = time.time()
    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)
    node_prefix = "{}:".format(node_type)
    s3_client = boto3.client('s3')    
    node_degree_list = []
    for node in G.nodes():
        if node.startswith(node_prefix):
            degree = G.degree(node)
            node_degree_list.append({'node_id': node, 'degree': degree})
    df = pd.DataFrame(node_degree_list)
    csv_data = df.to_csv(index=False)
    s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=file_name)
    s3_client.close()
    completion_time = round((time.time()-start_time)/(60),2)
    print("Completed in {} min".format(completion_time))


if __name__ == "__main__":
    main()



