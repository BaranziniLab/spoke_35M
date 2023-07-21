import boto3
import sys
import pandas as pd
import pickle
import time
from s3_utility import read_pickle_file_from_s3

MAPPING_FILE = sys.argv[1]
IDENTIFIER_COLUMN = sys.argv[2]
DRUG_FILE = sys.argv[3]
BUCKET_NAME = sys.argv[4]
PPR_LOCATION = sys.argv[5]
SAVE_LOCATION = sys.argv[6]
SAVE_NAME = sys.argv[7]


mapping_file_df = pd.read_csv(MAPPING_FILE)
mapping_file_df.dropna(subset=[IDENTIFIER_COLUMN], inplace=True)
node_list = list(mapping_file_df[IDENTIFIER_COLUMN].unique())

drug_df = pd.read_csv(DRUG_FILE)
drug_list = list(drug_df.identifier.values)

s3_client = boto3.client('s3')
object_key = PPR_LOCATION + "/spoke35M_ppr_features.csv"
s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
feature_df = pd.read_csv(s3_object["Body"])
s3_client.close()

def main():
	start_time = time.time()
	node_drug_dictionary = {}
	for item in node_list:
		object_key = PPR_LOCATION + "/" + item + "_dict.pickle"
		embedding_data = read_pickle_file_from_s3(BUCKET_NAME, object_key)
		if len(embedding_data["embedding"]) > 0:
			feature_df_ = feature_df.copy()
			feature_df_["embedding_score"] = embedding_data["embedding"]
			feature_df_drug = feature_df_[feature_df_.node_id.isin(drug_list)]
			feature_df_drug = feature_df_drug.sort_values(by="embedding_score", ascending=False)
			feature_df_drug_with_name = pd.merge(feature_df_drug, drug_df, left_on = "node_id", right_on="identifier").drop("identifier", axis=1)
			node_drug_dictionary[item] = feature_df_drug_with_name
	
	binary_data = pickle.dumps(node_drug_dictionary)
	s3_client = boto3.client('s3')
	object_key = SAVE_LOCATION + "/" + SAVE_NAME
	s3_client.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=binary_data)
	s3_client.close()
	print("Completed in {} hrs".format(round((time.time() - start_time)/(60*60), 2)))


if __name__ == "__main__":
	main()

