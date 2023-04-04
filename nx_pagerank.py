import networkx as nx
import sys
import pickle
import time
import multiprocessing as mp
import pandas as pd
import numpy as np
import boto3

GRAPH_PATH = sys.argv[1]
MAPPING_FILE = sys.argv[2]
NCORES = int(sys.argv[3])

def main():
	start_time = time.time()
	global G
	mapping_file_df = pd.read_csv(MAPPING_FILE)
	mapping_file_df["spoke_identifer"] = "Compound:" + mapping_file_df["spoke_identifer"]
	node_list = mapping_file_df["spoke_identifer"].unique()
	node_list = node_list[0:2]
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)
	p = mp.Pool(NCORES)
	p.map(personalized_pagerank, node_list)
	print("Files are created and transferred to S3 in {} hrs".format(round((time.time() - start_time) / (60*60), 2)))



def personalized_pagerank(node_id):
	personalization = {node_id: 1}
	pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=100, tol=1e-06)
	out_dict = {"node_id": node_id,
				"embedding": np.array(list(pagerank.values())),
				"feature_ids": np.array(list(pagerank.keys()))
				}
	del pagerank
	filename = 'spoke35M/{}_dict.pickle'.format(node_id)
	with open(filename, 'wb') as f:
		pickle.dump(out_dict, f)
	del out_dict

# def personalized_pagerank(node_id):
# 	personalization = {node_id: 1}
# 	pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=100, tol=1e-06)
# 	out_dict = {"node_id": node_id,
# 				"embedding": np.array(list(pagerank.values())),
# 				"feature_ids": np.array(list(pagerank.keys()))
# 				}
# 	del pagerank
# 	binary_data = pickle.dumps(out_dict)
# 	del out_dict
# 	s3_client = boto3.client('s3')
# 	bucket_name = 'ic-spoke'
# 	object_key = 'spoke35M/{}_dict.pickle'.format(node_id)
# 	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
# 	s3_client.close()
# 	del binary_data


if __name__ == "__main__":
	main()
