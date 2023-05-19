import networkx as nx
import sys
import pickle
import boto3
import os
import numpy as np
import time
import gzip


GRAPH_PATH = sys.argv[1]
BUCKET_NAME = sys.argv[2]
FILE_LOCATION = sys.argv[3]

node_list = ["Compound:inchikey:AAOVKJBEBIDNHE-UHFFFAOYSA-N"]


def main():
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)

	for item in node_list:
		personalization = {item:1}
		personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=200, tol=1e-12)
	features = {}
	features["features"] = np.array(list(personalized_pagerank.keys()))
	binary_data = pickle.dumps(features)
	compressed_data = gzip.compress(binary_data)
	s3_client = boto3.client('s3')
	bucket_name = BUCKET_NAME
	object_key = '{}/ppr_features_dict_gzip_compressed'.format(FILE_LOCATION)
	s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=compressed_data)
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Features are extracted from PPR and then stored in S3 in {} min!".format(completion_time))



if __name__ == "__main__":
	main()
