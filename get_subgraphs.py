import sys
import pickle
import boto3
import networkx as nx
import multiprocessing as mp
import pandas as pd
import numpy as np
import time
from utility import get_significant_compounds_with_disease_association


compound_type = sys.argv[1]
sample = sys.argv[2]
sel_sheet_index = int(sys.argv[3])
data_path = sys.argv[4]
GRAPH_PATH = sys.argv[5]
NCORES = int(sys.argv[6])
BUCKET_NAME = sys.argv[7]
EMBEDDING_ANALYSIS_FILE_LOCATION = sys.argv[8]

pvalue_thresh = 0.05
destination_disease_node = "Disease:DOID:2377"
bucket_name = BUCKET_NAME


def main():
	global G, s3_client
	s3_client = boto3.client('s3')
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)
	salient_intermediate_nodes_proximal_to_MS = get_salient_intermediate_nodes_proximal_to_MS()    
	intermediate_nodes_to_MS_node_paths = get_paths_from_intermediate_nodes_to_MS_node(salient_intermediate_nodes_proximal_to_MS, destination_disease_node)
	compound_to_intermediate_paths = []
	if compound_type == "targeted":
		starting_compound_nodes = get_starting_compound_nodes()	
		compound_to_intermediate_paths = get_paths_from_compound_to_intermediate_nodes(starting_compound_nodes, salient_intermediate_nodes_proximal_to_MS)	
	extracted_path = {}
	extracted_path["compound_to_intermediate"] = compound_to_intermediate_paths
	extracted_path["intermediate_to_MS"] = intermediate_nodes_to_MS_node_paths
	binary_data = pickle.dumps(extracted_path)
	filename = "extracted_paths_for_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + str(sel_sheet_index) + "_dict.pickle"
	object_key = "{}/{}".format(EMBEDDING_ANALYSIS_FILE_LOCATION, filename)
	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed in {} min".format(completion_time))


def get_paths_from_intermediate_nodes_to_MS_node(salient_intermediate_nodes_proximal_to_MS, destination_disease_node):
	intermediate_nodes_to_MS_node_paths = {}
	for salient_intermediate_nodes_proximal_to_MS_ in salient_intermediate_nodes_proximal_to_MS:
		intermediate_nodes_to_MS_node_paths[salient_intermediate_nodes_proximal_to_MS_] = get_shortest_path(salient_intermediate_nodes_proximal_to_MS_, destination_disease_node)
	# p = mp.Pool(NCORES)
	# args = zip(list(salient_intermediate_nodes_proximal_to_MS), [destination_disease_node]*len(salient_intermediate_nodes_proximal_to_MS))
	# intermediate_nodes_to_MS_node_paths_list = p.starmap(get_shortest_path, args)
	# p.close()
	# p.join()
	# intermediate_nodes_to_MS_node_paths = pd.concat(intermediate_nodes_to_MS_node_paths_list, ignore_index=True).drop_duplicates()
	return intermediate_nodes_to_MS_node_paths


def get_paths_from_compound_to_intermediate_nodes(starting_compound_nodes, salient_intermediate_nodes_proximal_to_MS):
	compound_to_intermediate_path_dict = {}
	p = mp.Pool(NCORES)
	for starting_compound_node in starting_compound_nodes:		
		args = list(zip([starting_compound_node]*len(salient_intermediate_nodes_proximal_to_MS), list(salient_intermediate_nodes_proximal_to_MS)))
		compound_to_intermediate_path_list = p.starmap(get_shortest_path, args)
		compound_to_intermediate_path_dict[starting_compound_node] = compound_to_intermediate_path_list
	p.close()
	p.join()
	return compound_to_intermediate_path_dict


def get_shortest_path(source, target):
    try:
        path_list = nx.shortest_path(G, source=source, target=target)
        path_list_tuples = list(map(lambda x:(path_list[x], path_list[x+1]), range(len(path_list)-1)))
    except:
        path_list_tuples = []        
    return pd.DataFrame(path_list_tuples, columns=['source', 'target'])


def get_salient_intermediate_nodes_proximal_to_MS():
	filename = "top_nodes_for_each_nodetype_for_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + str(sel_sheet_index) + "_list.pickle"
	object_key = "{}/{}".format(EMBEDDING_ANALYSIS_FILE_LOCATION, filename)
	response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	top_nodes_list_of_dict = pickle.loads(response['Body'].read())
	salient_intermediate_nodes_proximal_to_MS = []
	for item in top_nodes_list_of_dict:
		top_negative_nodes = item["top_negative_nodes"]
		top_positive_nodes = item["top_positive_nodes"]
		top_nodes = pd.concat([top_negative_nodes, top_positive_nodes], ignore_index=True).dropna()
		top_nodes_proximal_to_MS = top_nodes[top_nodes.p_value < pvalue_thresh]
		top_nodes_proximal_to_MS["composite_id"] = top_nodes_proximal_to_MS["node_type"] + ":" + top_nodes_proximal_to_MS["node_id"]
		salient_intermediate_nodes_proximal_to_MS.append(top_nodes_proximal_to_MS["composite_id"].values)
	salient_intermediate_nodes_proximal_to_MS = np.concatenate(salient_intermediate_nodes_proximal_to_MS)
	return salient_intermediate_nodes_proximal_to_MS


def get_starting_compound_nodes():
	GLM_significant_compounds_mapped_to_SPOKE = get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
	GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer = "Compound:" + GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer
	starting_compound_nodes = GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer.unique()
	return starting_compound_nodes


if __name__ == "__main__":
	main()



