import boto3
import networkx as nx
import pickle
import sys
from utility import get_significant_compounds_with_disease_association
import numpy as np


compound_type = sys.argv[1]
sample = sys.argv[2]
sel_sheet_index = int(sys.argv[3])
data_path = sys.argv[4]
destination_disease_node = sys.argv[5]
GRAPH_PATH = sys.argv[6]


with open(GRAPH_PATH, "rb") as f:
    G = pickle.load(f)

pvalue_thresh = 0.05
GLM_significant_compounds_mapped_to_SPOKE = get_significant_compounds_with_disease_association(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer = "Compound:" + GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer
starting_compound_nodes = GLM_significant_compounds_mapped_to_SPOKE.spoke_identifer.unique()

filename = "top_nodes_for_each_nodetype_for_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + str(sel_sheet_index) + "_list.pickle"
object_key = "spoke35M/spoke35M_iMSMS_embedding_analysis/{}".format(filename)
s3_client = boto3.client('s3')
bucket_name = 'ic-spoke'
response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
top_nodes = pickle.loads(response['Body'].read())
intermediate_nodes = []
for item in top_nodes:
	if item["top_negative_nodes"].shape[0] != 0:		
		item["top_negative_nodes"]["commposed_id"] = item["top_negative_nodes"]["node_type"] + ":" + item["top_negative_nodes"]["node_id"]
		intermediate_node_id = item["top_negative_nodes"]["commposed_id"].head(5).unique()
		intermediate_nodes.append(intermediate_node_id)
	if item["top_positive_nodes"].shape[0] != 0:
		item["top_positive_nodes"]["commposed_id"] = item["top_positive_nodes"]["node_type"] + ":" + item["top_positive_nodes"]["node_id"]
		intermediate_node_id = item["top_positive_nodes"]["commposed_id"].tail(5).unique()
		intermediate_nodes.append(intermediate_node_id)

intermediate_nodes = np.concatenate(intermediate_nodes)
paths = []
for starting_node in starting_compound_nodes:
	for path in nx.all_simple_paths(G, starting_node, destination_disease_node, cutoff=5): 
		if len(path) == 2:
			paths.append(path)
		elif len(set(intermediate_nodes).intersection(set(path))) > 1:
			paths.append(path)











