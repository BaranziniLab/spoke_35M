import networkx as nx
import sys
import pickle
import boto3
import os
import numpy as np
import time
import gzip
import pandas as pd


GRAPH_PATH = sys.argv[1]
BUCKET_NAME = sys.argv[2]
FILE_LOCATION = sys.argv[3]
SAVE_NAME = sys.argv[4]
NODE_TYPE_SEPERATOR = sys.argv[5]

node_list = ["Compound" + NODE_TYPE_SEPERATOR + "inchikey:AAOVKJBEBIDNHE-UHFFFAOYSA-N"]


def main():
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)

	for item in node_list:
		personalization = {item:1}
		personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=200, tol=1e-12)
	features = {}
	features["features"] = np.array(list(personalized_pagerank.keys()))

	features = pd.DataFrame(np.array(list(personalized_pagerank.keys())), columns=["node_id"])
	features.loc[:, "node_type"] = features.node_id.apply(lambda x:x.split(":")[0])
	features.node_id = features.node_id.apply(lambda x:":".join(x.split(":")[1:]))
	csv_data = features.to_csv(index=False)
	file_name = "{}/{}".format(FILE_LOCATION, SAVE_NAME)
	s3_client = boto3.client('s3')
	s3_client.put_object(Body=csv_data, Bucket=BUCKET_NAME, Key=file_name)

	# binary_data = pickle.dumps(features)
	# compressed_data = gzip.compress(binary_data)
	# s3_client = boto3.client('s3')
	# bucket_name = BUCKET_NAME
	# object_key = '{}/ppr_features_dict_gzip_compressed'.format(FILE_LOCATION)
	# s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=compressed_data)
	
	s3_client.close()
	completion_time = round((time.time()-start_time)/(60),2)
	print("Features are extracted from PPR and then stored in S3 in {} min!".format(completion_time))



if __name__ == "__main__":
	main()
