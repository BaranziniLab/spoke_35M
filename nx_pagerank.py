import networkx as nx
import sys
import pickle
import time
import multiprocessing as mp
import pandas as pd
import numpy as np
import os

GRAPH_PATH = sys.argv[1]
MAPPING_FILE = sys.argv[2]
SAVE_PATH = sys.argv[3]
NCORES = int(sys.argv[4])


def main():
	start_time = time.time()
	global G
	mapping_file_df = pd.read_csv(MAPPING_FILE)
	mapping_file_df["spoke_identifer"] = "Compound:" + mapping_file_df["spoke_identifer"]
	node_list = mapping_file_df["spoke_identifer"].unique()
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)
	p = mp.Pool(NCORES)
	p.map(personalized_pagerank, node_list)



def personalized_pagerank(node_id):
	personalization = {node_id: 1}
	pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=100, tol=1e-06)
	out_dict = {}
	out_dict["node_id"] = node_id
	out_dict["embedding"] = np.array(list(pagerank.values()))
	out_dict["feature_ids"] = np.array(list(pagerank.keys()))
	del(pagerank)
	filename = os.path.join(SAVE_PATH, "{}_dict.pickle".format(node_id))
	with open(filename, 'wb') as f:
		pickle.dump(out_dict, f)
	del(out_dict)

if __name__ == "__main__":
    main()