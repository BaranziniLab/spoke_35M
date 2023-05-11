import pandas as pd
import numpy as np
from scipy.stats import norm
import sys
import boto3
import pickle
import multiprocessing as mp
import time



compound_type = sys.argv[1]
sample = sys.argv[2]
sheet_index = sys.argv[3]
top_node_count = int(sys.argv[4])
NCORES = int(sys.argv[5])
GRAPH_PATH = sys.argv[6]

destination_disease_node = "Disease:DOID:2377"


def main():
	global G, feature_df, embedding, shortest_pathlength_distribution_dict
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)
	print("Reading PPR feature map from S3 ...")
	s3_client = boto3.client('s3')
	bucket_name = 'ic-spoke'
	object_key = "spoke35M/spoke35M_converged_ppr/spoke35M_ppr_features.csv"
	s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	feature_df = pd.read_csv(s3_object["Body"])
	unique_nodetypes = feature_df.node_type.unique()
	filename = "spoke_embedding_for_IMSMS_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + sheet_index + "_dict.pickle"
	object_key = 'spoke35M/spoke35M_iMSMS_embedding/{}'.format(filename)
	response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	embedding_dict = pickle.loads(response['Body'].read())
	embedding = embedding_dict["embedding"]
	object_key = "spoke35M/spoke35M_iMSMS_embedding_analysis/shortest_pathLength_distributions_of_all_nodetypes_to_MS_node.pickle"
	response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	shortest_pathlength_distribution_list = pickle.loads(response['Body'].read())
	shortest_pathlength_distribution_dict = {}
	for item in shortest_pathlength_distribution_list:
		shortest_pathlength_distribution_dict[item["node_type"]] = item["shortest_pathLength_distribution"]
	p = mp.Pool(NCORES)
	top_nodes_list_of_dict = p.map(get_top_nodes_for_the_nodetype, unique_nodetypes)
	p.close()
	p.join()
	binary_data = pickle.dumps(top_nodes_list_of_dict)
	filename = "top_nodes_for_each_nodetype_for_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + sheet_index + "_list.pickle"
	object_key = "spoke35M/spoke35M_iMSMS_embedding_analysis/{}".format(filename)
	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed in {} min".format(completion_time))


def get_top_nodes_for_the_nodetype(sel_nodetype):
	nodetype_specific_shortest_pathlength_distribution = shortest_pathlength_distribution_dict[sel_nodetype]
	nodetype_specific_shortest_pathlength_distribution_none_removed = [x for x in nodetype_specific_shortest_pathlength_distribution if x is not None]
	nodetype_specific_shortest_pathlength_distribution_none_removed_mean = np.mean(nodetype_specific_shortest_pathlength_distribution_none_removed)
	nodetype_specific_shortest_pathlength_distribution_none_removed_std = np.std(nodetype_specific_shortest_pathlength_distribution_none_removed)
	nodetype_specific_feature_df = feature_df[feature_df["node_type"]==sel_nodetype]
	nodetype_index = nodetype_specific_feature_df.index.values
	nodetype_specific_embedding = embedding[nodetype_index]
	nodetype_specific_feature_df["embedding_values"] = nodetype_specific_embedding
	nodetype_specific_feature_df_positive = nodetype_specific_feature_df[nodetype_specific_feature_df["embedding_values"] >= 0]
	nodetype_specific_feature_df_negative = nodetype_specific_feature_df[nodetype_specific_feature_df["embedding_values"] < 0]
	if nodetype_specific_feature_df_negative.shape[0] != 0:
		nodetype_specific_feature_df_negative.sort_values(by="embedding_values", inplace=True)
		top_negative_nodes = nodetype_specific_feature_df_negative.head(top_node_count)
		top_negative_nodes = get_shortest_path_to_disease_node(top_negative_nodes, nodetype_specific_shortest_pathlength_distribution_none_removed_mean, nodetype_specific_shortest_pathlength_distribution_none_removed_std)
	else:
		top_negative_nodes = None
	if nodetype_specific_feature_df_positive.shape[0] != 0:
		nodetype_specific_feature_df_positive.sort_values(by="embedding_values", inplace=True)
		top_positive_nodes = nodetype_specific_feature_df_positive.tail(top_node_count)
		top_positive_nodes = get_shortest_path_to_disease_node(top_positive_nodes, nodetype_specific_shortest_pathlength_distribution_none_removed_mean, nodetype_specific_shortest_pathlength_distribution_none_removed_std)
	else:
		top_positive_nodes = None
	top_nodes_dict = {}
	top_nodes_dict["nodetype"] = sel_nodetype
	top_nodes_dict["top_negative_nodes"] = top_negative_nodes
	top_nodes_dict["top_positive_nodes"] = top_positive_nodes
	return top_nodes_dict


def get_shortest_path_to_disease_node(df_, mean_, std_):
	shortest_pathlength_list = []
	shortest_pathlength_p_value_list = []
	for index, row in df_.iterrows():
		source_node = row["node_type"] + ":" + row["node_id"]
		try:
			shortest_pathlength = nx.shortest_path_length(G, source=source_node, target=destination_disease_node)
			z_score = (shortest_pathlength - mean_) / std_
			p_value = norm.cdf(z_score)
		except:
			shortest_pathlength = None
			p_value = None
		shortest_pathlength_list.append(shortest_pathlength)
		shortest_pathlength_p_value_list.append(p_value)
	df_["shortest_pathlength_to_MS_disease_node"] = shortest_pathlength_list
	df_["p_value"] = shortest_pathlength_p_value_list
	return df_


if __name__ == "__main__":
	main()

