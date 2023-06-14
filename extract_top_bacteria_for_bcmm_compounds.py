import sys
import boto3
import pandas as pd
import numpy as np
from scipy import stats
import multiprocessing as mp
import time
import json
from s3_utility import read_pickle_file_from_s3


MAPPING_FILE = sys.argv[1]
BACTERIA_FILE = sys.argv[2]
BUCKET_NAME = sys.argv[3]
PPR_FILE_LOCATION = sys.argv[4]
SAVE_LOCATION = sys.argv[5]
NCORES = int(sys.argv[6])

N = 25

mapping_file_df = pd.read_csv(MAPPING_FILE)
compound_names = mapping_file_df["compound_name"].unique()
bacteria_df = pd.read_csv(BACTERIA_FILE, sep="\t")
bacteria_df["type_id"] = "Organism:" + bacteria_df["spoke_identifier"].astype(str)


def main():
	global bacteria_feature_df_with_names, bacteria_feature_indices
	start_time = time.time()
	feature_df = get_feature_map()
	feature_df["type_id"] = feature_df["node_type"] + ":" + feature_df["node_id"]
	bacteria_feature_df = feature_df[feature_df["type_id"].isin(bacteria_df.type_id.unique())]
	bacteria_feature_df_with_names = pd.merge(bacteria_feature_df, bacteria_df, on="type_id")
	bacteria_feature_df_with_names = bacteria_feature_df_with_names[["spoke_identifier", "spoke_name"]]
	bacteria_feature_indices = bacteria_feature_df.index.values
	p = mp.Pool(NCORES)
	out_list_of_dict = p.map(get_top_N_bacteria_for_the_compound, compound_names)
	p.close()
	p.join()
	concatenated_dict = {}
	for dictionary in out_list_of_dict:
		concatenated_dict.update(dictionary)
	json_data = json.dumps(concatenated_dict)
	s3_client = boto3.client('s3')
	file_name = SAVE_LOCATION + "/bcmm_compounds_top_bacteria.json"
	s3_client.put_object(Body=json_data, Bucket=BUCKET_NAME, Key=file_name)
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed and saved in {} min".format(completion_time))


def get_top_N_bacteria_for_the_compound(item):
	out = {}
	bacteria_feature_df_with_names_ = bacteria_feature_df_with_names.copy()
	spoke_compound_nodes_ids = list(mapping_file_df[mapping_file_df["compound_name"]==item].spoke_identifier.unique())
	spoke_compound_nodes_ids = list(map(lambda x:"Compound:"+x, spoke_compound_nodes_ids))
	spoke_vector = 0
	for compound_id in spoke_compound_nodes_ids:
		object_key = PPR_FILE_LOCATION + "/" + compound_id + "_dict.pickle"
		spoke_embedding_data = read_pickle_file_from_s3(BUCKET_NAME, object_key)
		spoke_vector += spoke_embedding_data["embedding"]
	try:
		spoke_bacteria_vector = spoke_vector[bacteria_feature_indices]
		bacteria_feature_df_with_names_["ppr_values"] = spoke_bacteria_vector
		bacteria_feature_df_with_names_["ppr_percentile"] = bacteria_feature_df_with_names_.ppr_values.apply(lambda x:stats.percentileofscore(bacteria_feature_df_with_names_.ppr_values, x))/100
		bacteria_feature_df_with_names_top_N = bacteria_feature_df_with_names_.sort_values(by="ppr_percentile", ascending=False).head(N)
		bacteria_feature_df_with_names_top_N.drop("ppr_values", axis=1, inplace=True)
		bacteria_feature_df_with_names_top_N.rename(columns={"spoke_identifier":"ncbi_id", "spoke_name": "name", "ppr_percentile":"percentile_score"}, inplace=True)
		out[item] = bacteria_feature_df_with_names_top_N.to_dict('records')		
	except:
		out[item] = []
	return out

def get_feature_map():
	s3_client = boto3.client('s3')
	object_key = PPR_FILE_LOCATION + "/spoke35M_ppr_features.csv"
	s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
	feature_df = pd.read_csv(s3_object["Body"])
	return feature_df

if __name__ == "__main__":
	main()

