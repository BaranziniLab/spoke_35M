import networkx as nx
import sys
import pickle
import time
import multiprocessing as mp
import pandas as pd
import numpy as np
import boto3
import os

GRAPH_PATH = sys.argv[1]
MAPPING_FILE = sys.argv[2]
IDENTIFIER_COLUMN = sys.argv[3]
NCORES = int(sys.argv[4])
bucket_name = sys.argv[5]
sublocation = sys.argv[6]
check_existing_compounds = int(sys.argv[7])


def main():
    start_time = time.time()
    global G
    if check_existing_compounds == 1:
        bucket_location = bucket_name + "/" + sublocation + "/"
        node_list = compare_saved_ppr(MAPPING_FILE, IDENTIFIER_COLUMN, bucket_location)
    else:
        mapping_file_df = pd.read_csv(MAPPING_FILE)
        mapping_file_df.dropna(subset=[IDENTIFIER_COLUMN], inplace=True)
        node_list = list(mapping_file_df[IDENTIFIER_COLUMN].unique())

    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)
    p = mp.Pool(NCORES)
    p.map(personalized_pagerank, node_list)
    p.close()
    p.join()
    print("Files are created and transferred to S3 in {} hrs".format(round((time.time() - start_time) / (60*60), 2)))



def personalized_pagerank(node_id):
    personalization = {node_id: 1}
    try:
        pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=200, tol=1e-12)
        out_dict = {"node_id": node_id,
                    "embedding": np.array(list(pagerank.values()))
                    }
        del pagerank
    except:
        out_dict = {
            "node_id": node_id,
            "embedding": np.array([])
        }
    binary_data = pickle.dumps(out_dict)
    del out_dict
    s3_client = boto3.client('s3')
    object_key = '{}/{}_dict.pickle'.format(sublocation, node_id)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
    s3_client.close()
    del binary_data


if __name__ == "__main__":
    main()
