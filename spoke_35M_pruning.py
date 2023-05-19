import sys
import os
import time
import pickle
import networkx as nx

GRAPH_PATH = sys.argv[1]
data_path = sys.argv[2]


compounds_to_remove = ["Compound:inchikey:MYMOFIZGZYHOMD-UHFFFAOYSA-N",
"Compound:inchikey:XLYOFNOQVPJJNP-UHFFFAOYSA-N",
"Compound:inchikey:UFHFLCQGNIYNRP-UHFFFAOYSA-N",
"Compound:inchikey:GPRLSGONYQIRFK-UHFFFAOYSA-N",
"Compound:inchikey:XLYOFNOQVPJJNP-BJUDXGSMSA-N"]

def main():
	start_time = time.time()
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)
	G_new = G.copy()
	G_new.remove_nodes_from(compounds_to_remove)
	nx.write_gpickle(G_new, os.path.join(data_path, 'spoke_35M_compound_pruned_version.gpickle'))
	completion_time = round((time.time()-start_time)/(60),2)
	print("Completed in {} min".format(completion_time))

