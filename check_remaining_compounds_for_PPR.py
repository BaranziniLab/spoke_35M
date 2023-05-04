import os
import numpy as np
import sys

MAPPING_FILE = sys.argv[1]

mapping_file_df = pd.read_csv(MAPPING_FILE)
mapping_file_df["spoke_identifer"] = "Compound:" + mapping_file_df["spoke_identifer"]
cmd = "aws s3 ls s3://ic-spoke/spoke35M/spoke35M_converged_ppr/"
out = os.popen(cmd)
out_list = out.read().split("\n")
saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]
node_list = mapping_file_df["spoke_identifer"].unique()
node_list = list(set(node_list) - set(saved_compound_list_))

print("There are {} compounds left for the PPR to get computed".format(len(node_list)))