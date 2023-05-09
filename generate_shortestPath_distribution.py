import networkx as nx
import sys
import time
import pickle
import random
import boto3
import pandas as pd
import multiprocessing as mp


GRAPH_PATH = sys.argv[1]
TARGET_NODE = sys.argv[2]
N_RANDOM_SOURCE_NODES = int(sys.argv[3])
NCORES = int(sys.argv[4])


def main():
	global G
	start_time = time.time()
	print("Reading PPR feature map from S3 ...")
	s3_client = boto3.client('s3')
	bucket_name = 'ic-spoke'
	object_key = "spoke35M/spoke35M_converged_ppr/spoke35M_ppr_features.csv"
	s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	feature_df = pd.read_csv(s3_object["Body"])
	unique_nodetypes = feature_df.node_type.unique()
	print("Extracted unique nodetypes from the feature file!")
	print("Loading the graph ...")
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)
	print("Graph is loaded!")
	print("Computing distribution of shortest path lengths for each node type")
	p = mp.Pool(NCORES)
	shortest_pathlength_list = p.map(get_shortest_pathlength_distribution, unique_nodetypes)
	p.close()
	p.join()
	object_key = "spoke35M/spoke35M_iMSMS_embedding_analysis/shortest_pathLength_distributions_of_all_nodetypes_to_MS_node.pickle"
	binary_data = pickle.dumps(shortest_pathlength_list)
	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed in {} min".format(completion_time))


def get_shortest_pathlength_distribution(source_nodetype):
	source_nodes = [node for node in G.nodes if node.startswith(source_nodetype+":")]
	if len(source_nodes) >= N_RANDOM_SOURCE_NODES:
		random_source_nodes = random.sample(source_nodes, N_RANDOM_SOURCE_NODES)
	else:
		random_source_nodes = source_nodes
	node_type_specific_shortest_path_length_list = []
	for source_node in random_source_nodes:
		node_type_specific_shortest_path_length_list.append(nx.shortest_path_length(G, source=source_node, target=TARGET_NODE))
	node_type_specific_shortest_path_length_dict = {}
	node_type_specific_shortest_path_length_dict["node_type"] = source_nodetype
	node_type_specific_shortest_path_length_dict["shortest_pathLength_distribution"] = node_type_specific_shortest_path_length_list
	return node_type_specific_shortest_path_length_dict


if __name__ == "__main__":
	main()
