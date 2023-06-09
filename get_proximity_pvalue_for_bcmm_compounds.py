import sys
import json
import boto3
import pickle
import pandas as pd
import numpy as np
import random
import networkx as nx
from scipy.stats import norm
import multiprocessing as mp
import time
from s3_utility import read_pickle_file_from_s3


GRAPH_PATH = sys.argv[1]
MAPPING_FILE = sys.argv[2]
BACTERIA_FILE = sys.argv[3]
BUCKET_NAME = sys.argv[4]
SAVE_LOCATION = sys.argv[5]
JSON_FILE_LOCATION = sys.argv[6]
PPR_FILE_LOCATION = sys.argv[7]
NCORES = int(sys.argv[8])

N_RANDOM_BACTERIA_NODES = 3000

s3_client = boto3.client("s3")
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=JSON_FILE_LOCATION)
content = response['Body'].read().decode('utf-8')
top_bacteria_data = json.loads(content)

compound_names = list(top_bacteria_data.keys())
mapping_file_df = pd.read_csv(MAPPING_FILE)
bacteria_df = pd.read_csv(BACTERIA_FILE, sep="\t")
bacteria_df["type_id"] = "Organism:" + bacteria_df["spoke_identifier"].astype(str)

# saved_compounds_with_pagerank = get_saved_compounds_with_pagerank(BUCKET_NAME, PPR_FILE_LOCATION)
random_bacteria_nodes = random.sample(list(bacteria_df.type_id.unique()), N_RANDOM_BACTERIA_NODES)

def main():
	global G
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)
	out_list_of_dict = []
	for compound_name in compound_names:
		out_list_of_dict.append(get_proximity_pvalue(compound_name))
	concatenated_dict = {}
	for dictionary in out_list_of_dict:
		concatenated_dict.update(dictionary)
	json_data = json.dumps(concatenated_dict)
	s3_client = boto3.client('s3')
	file_name = SAVE_LOCATION + "/bcmm_compounds_top_bacteria_with_proximity_score.json"
	s3_client.put_object(Body=json_data, Bucket=BUCKET_NAME, Key=file_name)
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed and saved in {} min".format(completion_time))


def get_shortest_pathlength(source, target):
	try:
		shortest_pathlength = nx.shortest_path_length(G, source=source, target=target)
	except:
		shortest_pathlength = None
	return shortest_pathlength


def get_proximity_pvalue(item):
	mapping_file_df_sub = mapping_file_df[mapping_file_df["compound_name"]==item]
	spoke_ids = mapping_file_df_sub.spoke_identifier.unique()
	spoke_ids = list(map(lambda x:"Compound:"+x, spoke_ids))
	top_bacteria_list = []
	for item_ in top_bacteria_data[item]:
		top_bacteria_list.append("Organism:" + str(item_["ncbi_id"]))
	p_value_list = []
	for spoke_id in spoke_ids:
		object_key = PPR_FILE_LOCATION + "/" + spoke_id + "_dict.pickle"
		spoke_embedding_data = read_pickle_file_from_s3(BUCKET_NAME, object_key)
		if spoke_embedding_data["embedding"].shape[0] != 0:
			p = mp.Pool(NCORES)
			args_list_of_tuples = list(zip(random_bacteria_nodes, [spoke_id]*len(random_bacteria_nodes)))										
			bacteria_shortest_path_length_list = p.starmap(get_shortest_pathlength, args_list_of_tuples)			
			p.close()
			p.join()			
			bacteria_shortest_path_length_distribution_none_removed = [x for x in bacteria_shortest_path_length_list if x is not None]
			bacteria_shortest_path_length_distribution_none_removed_mean = np.mean(bacteria_shortest_path_length_distribution_none_removed)
			bacteria_shortest_path_length_distribution_none_removed_std = np.std(bacteria_shortest_path_length_distribution_none_removed)
			p_value_list_ = []
			for top_bacteria in top_bacteria_list:
				try:
					shortest_pathlength = nx.shortest_path_length(G, source=top_bacteria, target=spoke_id)
					z_score = (shortest_pathlength - bacteria_shortest_path_length_distribution_none_removed_mean) / bacteria_shortest_path_length_distribution_none_removed_std
					p_value = norm.cdf(z_score)
				except:
					p_value = None
				p_value_list_.append(p_value)
			p_value_list.append(p_value_list_)
	p_value_df = pd.DataFrame(p_value_list)
	out = {}
	out[item] = top_bacteria_data[item]
	for index, p_value_item in enumerate(np.array(p_value_df.min())):
		out[item][index]["proximity_pvalue"] = p_value_item
	return out


if __name__ == "__main__":
	main()



