import pandas as pd
import numpy as np
import os
import sys
import boto3
from s3_utility import get_saved_compounds_with_no_pagerank 

GLOBAL_MAPPING_FILE = sys.argv[1]
SHORTCHAIN_MAPPING_FILE = sys.argv[2]
BUCKET_NAME = sys.argv[3]
FILE_LOCATION = sys.argv[4]

bucket_location = BUCKET_NAME + "/" + FILE_LOCATION + "/"
cmd = "aws s3 ls s3://{}".format(bucket_location)
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

print("Checking for saved compounds with no pagerank ...")
compounds_with_no_pagerank = get_saved_compounds_with_no_pagerank(BUCKET_NAME, FILE_LOCATION)
print("There are {} Compounds with no pagerank".format(len(compounds_with_no_pagerank)))


