import networkx as nx
import sys
import pickle
import boto3
import os
import numpy as np
import time


GRAPH_PATH = sys.argv[1]

node_list = ["Compound:inchikey:AAOVKJBEBIDNHE-UHFFFAOYSA-N"]


def main():
	start_time = time.time()
    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)

    for item in node_list:
    	personalization = {item:1}
    	personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=200, tol=1e-12)
	features = np.array(list(personalized_pagerank.keys()))
	binary_data = pickle.dumps(features)
	s3_client = boto3.client('s3')
	bucket_name = 'ic-spoke'
	object_key = 'spoke35M/spoke35M_converged_ppr/ppr_features_array.pickle'
	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Features are extracted from PPR and then stored in S3 in {} min!".format(completion_time))



if __name__ == "__main__":
    main()
