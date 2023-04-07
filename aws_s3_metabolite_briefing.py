import pandas as pd
import numpy as np
import os
import sys
import boto3

GLOBAL_MAPPING_FILE = sys.argv[1]
SHORTCHAIN_MAPPING_FILE = sys.argv[2]

cmd = "aws s3 ls s3://ic-spoke/spoke35M/"
out = os.popen(cmd)
out_list = out.read().split("\n")
saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]

print("There are {} compounds saved in S3 bucket".format(len(saved_compound_list_)))

global_file = pd.read_csv(GLOBAL_MAPPING_FILE)
shortchain_file = pd.read_csv(SHORTCHAIN_MAPPING_FILE)

global_short_chain_shared = global_file[global_file.spoke_identifer.isin(shortchain_file.spoke_identifer.values)]


global_short_chain_shared_merge = pd.merge(global_short_chain_shared, shortchain_file, on="spoke_identifer")
global_short_chain_shared_merge = global_short_chain_shared_merge.rename(columns={"name_x":"global_compound_name", "name_y":"short_chain_fatty_acid_name"})

print("There are {} short chain fatty acids shared in global file".format(global_short_chain_shared_merge.short_chain_fatty_acid_name.unique().shape[0]))
print("They are:")
print(global_short_chain_shared_merge)


cmd = "aws s3 ls s3://ic-spoke/spoke35M/"
out = os.popen(cmd)
out_list = out.read().split("\n")
saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]

s3_client = boto3.client('s3')
bucket_name = 'ic-spoke'
compounds_with_no_pagerank = []
for node_id in saved_compound_list_:
  object_key = 'spoke35M/{}_dict.pickle'.format(node_id)
  response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
  if response['ContentLength'] < 222:
    compounds_with_no_pagerank.append(node_id)
print("There are {} Compounds with no pagerank".format(len(compounds_with_no_pagerank)))
compounds_with_no_pagerank_df = pd.DataFrame(compounds_with_no_pagerank, columns=["node_id"])
csv_string = compounds_with_no_pagerank_df.to_csv(index=False)
s3_client.put_object(Bucket=bucket_name, Key="spoke35M/compounds_with_no_pagerank.csv", Body=csv_string.encode('utf-8'))


