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
NCORES = int(sys.argv[3])
bucket_name = sys.argv[4]
sublocation = sys.argv[5]


def main():
    start_time = time.time()
    global G
    mapping_file_df = pd.read_csv(MAPPING_FILE)
    mapping_file_df["spoke_identifer"] = "Compound:" + mapping_file_df["spoke_identifer"]
    cmd = "aws s3 ls s3://{}/{}/".format(bucket_name, sublocation)
    out = os.popen(cmd)
    out_list = out.read().split("\n")
    saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
    saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]
    node_list = mapping_file_df["spoke_identifer"].unique()
    node_list = list(set(node_list) - set(saved_compound_list_))

    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)
    p = mp.Pool(NCORES)
    p.map(personalized_pagerank, node_list)
    p.close()
    p.join()
    print("Files are created and transferred to S3 in {} hrs".format(round((time.time() - start_time) / (60*60), 2)))



# def personalized_pagerank(node_id):
# 	personalization = {node_id: 1}
# 	pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=100, tol=1e-06)
# 	out_dict = {"node_id": node_id,
# 				"embedding": np.array(list(pagerank.values())),
# 				"feature_ids": np.array(list(pagerank.keys()))
# 				}
# 	del pagerank
# 	filename = '{}_dict.pickle'.format(node_id)
# 	with open(filename, 'wb') as f:
# 		pickle.dump(out_dict, f)
# 	del out_dict


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
