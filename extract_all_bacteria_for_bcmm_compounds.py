import sys
import boto3
import pandas as pd
import numpy as np
from scipy import stats
import multiprocessing as mp
import time
import json
import pickle
import networkx as nx
from s3_utility import read_pickle_file_from_s3

GRAPH_PATH = sys.argv[1]
MAPPING_FILE = sys.argv[2]
BACTERIA_FILE = sys.argv[3]
BUCKET_NAME = sys.argv[4]
PPR_FILE_LOCATION = sys.argv[5]
SAVE_LOCATION = sys.argv[6]
NCORES = int(sys.argv[7])


mapping_file_df = pd.read_csv(MAPPING_FILE)
compound_names = mapping_file_df["compound_name"].unique()
bacteria_df = pd.read_csv(BACTERIA_FILE, sep="\t")
bacteria_df["type_id"] = "Organism:" + bacteria_df["spoke_identifier"].astype(str)

def main():
	global bacteria_feature_df_with_names, bacteria_feature_indices, G
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)
	feature_df = get_feature_map()
	feature_df["type_id"] = feature_df["node_type"] + ":" + feature_df["node_id"]
	bacteria_feature_df = feature_df[feature_df["type_id"].isin(bacteria_df.type_id.unique())]
	bacteria_feature_df_with_names = pd.merge(bacteria_feature_df, bacteria_df, on="type_id")
	bacteria_feature_df_with_names = bacteria_feature_df_with_names[["spoke_identifier", "spoke_name"]]
	bacteria_feature_indices = bacteria_feature_df.index.values
	p = mp.Pool(NCORES)
	out_list_of_df = p.map(get_all_bacteria_for_the_compound, compound_names)
	p.close()
	p.join()
	merged_out_df = pd.DataFrame(columns=["ncbi_id", "name"])
	for df in out_list_of_df:
	    merged_df = pd.merge(merged_out_df, df, on=["ncbi_id", "name"], how="outer")

	s3_client = boto3.client('s3')
	file_name = SAVE_LOCATION + "/bcmm_compounds_all_bacteria.csv"
	csv_data = merged_df.to_csv(index=False)
	s3_client.put_object(Body=csv_data, Bucket=BUCKET_NAME, Key=file_name)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed in {} min!".format(completion_time))


def get_all_bacteria_for_the_compound(item):
	bacteria_feature_df_with_names_ = bacteria_feature_df_with_names.copy()
	bacteria_feature_df_with_names_["type_id"] = "Organism:" + bacteria_feature_df_with_names_["spoke_identifier"].astype(str)
	bacteria_list = list(bacteria_feature_df_with_names_["type_id"])
	bacteria_feature_df_with_names_.drop("type_id", axis=1, inplace=True)
	spoke_compound_nodes_ids = list(mapping_file_df[mapping_file_df["compound_name"]==item].spoke_identifier.unique())
	spoke_compound_nodes_ids = list(map(lambda x:"Compound:"+x, spoke_compound_nodes_ids))
	spoke_vector = 0
	shortest_pathlength_list = []
	for compound_id in spoke_compound_nodes_ids:
		object_key = PPR_FILE_LOCATION + "/" + compound_id + "_dict.pickle"
		spoke_embedding_data = read_pickle_file_from_s3(BUCKET_NAME, object_key)
		if spoke_embedding_data["embedding"].shape[0] != 0:
			spoke_vector += spoke_embedding_data["embedding"]
			shortest_pathlength_list_ = []
			for bacteria in bacteria_list:
				shortest_pathlength_list_.append(get_shortest_pathlength(bacteria, compound_id))
			shortest_pathlength_list.append(shortest_pathlength_list_)			
	try:
		shortest_pathlength_list = [list(pair) for pair in zip(*shortest_pathlength_list)]
		spoke_bacteria_vector = spoke_vector[bacteria_feature_indices]
		bacteria_feature_df_with_names_["ppr_values"] = spoke_bacteria_vector
		bacteria_feature_df_with_names_["ppr_percentile"] = bacteria_feature_df_with_names_.ppr_values.apply(lambda x:stats.percentileofscore(bacteria_feature_df_with_names_.ppr_values, x))/100
		bacteria_feature_df_with_names_.drop("ppr_values", axis=1, inplace=True)
		bacteria_feature_df_with_names_.rename(columns={"spoke_identifier":"ncbi_id", "spoke_name": "name", "ppr_percentile":item}, inplace=True)
		bacteria_feature_df_with_names_["shortest_pathlength_to_{}".format(item)] = shortest_pathlength_list
		return bacteria_feature_df_with_names_
	except:
		return pd.DataFrame(columns=["ncbi_id", "name", item, "shortest_pathlength_to_{}".format(item)])


def get_shortest_pathlength(source, target):
	try:
		shortest_pathlength = nx.shortest_path_length(G, source=source, target=target)
	except:
		shortest_pathlength = None
	return shortest_pathlength


def get_feature_map():
	s3_client = boto3.client('s3')
	object_key = PPR_FILE_LOCATION + "/spoke35M_ppr_features.csv"
	s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
	feature_df = pd.read_csv(s3_object["Body"])
	return feature_df


if __name__ == "__main__":
	main()

