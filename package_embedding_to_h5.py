from tables import *
import pandas as pd
import numpy as np
import pickle
import os
import sys
import time
import networkx as nx
from s3_utility import read_pickle_file_from_s3


BUCKET_NAME = sys.argv[1]
PPR_SUB_LOCATION = sys.argv[2]
MAPPING_FILE = sys.argv[3]
IDENTIFIER_COLUMN = sys.argv[4]
GRAPH_PATH = sys.argv[5]
SAVE_PATH = sys.argv[6]
SAVE_FILENAME = sys.argv[7]


mapping_file_df = pd.read_csv(MAPPING_FILE)
mapping_file_df.dropna(subset=[IDENTIFIER_COLUMN], inplace=True)
node_list = list(mapping_file_df[IDENTIFIER_COLUMN].unique())


def main():
	start_time = time.time()
	package_embedding_data()
	print("Completed in {} hrs".format(round((time.time() - start_time)/(60*60), 2)))


def package_embedding_data():
	ppr_array, row_df, col_df = consolidate_data()

	class row_index_table(IsDescription):
	    node_id = StringCol(50)
	    node_type = StringCol(25)
	    row_index = UInt16Col()

	class column_index_table(IsDescription):
	    node_id = StringCol(50)
	    node_type = StringCol(25)
	    column_index = UInt32Col()
	 
	h5file = open_file(os.path.join(SAVE_PATH, SAVE_FILENAME), mode="w", title="Embedding file")
	row_group = h5file.create_group("/", "row_index", "row index mapping")
	row_table = h5file.create_table(row_group, "row_index_table", row_index_table, "row index mapping table")
	column_group = h5file.create_group("/", "column_index", "column index mapping")
	column_table = h5file.create_table(column_group, "column_index_table", column_index_table, "column index mapping table")
	array_group = h5file.create_group("/", "embedding", "embedding array group")
	h5file.create_array(array_group, "embedding_array", ppr_array, "embedding array")

	row_table_pointer = row_table.row
	for index, row in row_df.iterrows():
		row_table_pointer["node_id"] = row["node_id"]
		row_table_pointer["node_type"] = row["node_type"]
		row_table_pointer["row_index"] = row["row_index"]
		row_table_pointer.append()
	row_table.flush()

	column_table_pointer = column_table.row
	for index, row in col_df.iterrows():
		column_table_pointer["node_id"] = row["node_id"]
		column_table_pointer["node_type"] = row["node_type"]
		column_table_pointer["column_index"] = row["column_index"]
		column_table_pointer.append()
	column_table.flush()
	h5file.close()


def consolidate_data():
	with open(GRAPH_PATH, "rb") as f:
		G = pickle.load(f)

	personalization = {node_list[0]:1}
	personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=200, tol=1e-12)
	col_df = pd.DataFrame(np.array(list(personalized_pagerank.keys())), columns=["node_id"])
	col_df.loc[:, "node_type"] = col_df.node_id.apply(lambda x:x.split("|")[0])
	col_df.node_id = col_df.node_id.apply(lambda x:":".join(x.split("|")[1:]))
	col_df = col_df.reset_index().rename(columns={"index": "column_index"})
	col_df = col_df[["node_id", "node_type", "column_index"]]
	del(G)

	row_index_dict = {}
	ppr_array = []
	row_index_count = 0
	for item in node_list:
		object_key = PPR_SUB_LOCATION + "/" + item + "_dict.pickle"
		embedding_data = read_pickle_file_from_s3(BUCKET_NAME, object_key)
		if len(embedding_data["embedding"]) > 0:
			row_index_dict[embedding_data["node_id"]] = row_index_count
			ppr_array.append(embedding_data["embedding"])
			row_index_count += 1

	row_df = pd.DataFrame(list(row_index_dict.items()), columns=["node_id", "row_index"])
	row_df.loc[:, "node_type"] = row_df.node_id.apply(lambda x:x.split("|")[0])
	row_df.node_id = row_df.node_id.apply(lambda x:":".join(x.split("|")[1:]))
	row_df = row_df[["node_id", "node_type", "row_index"]]
	ppr_array = np.array(ppr_array)
	return ppr_array, row_df, col_df




if __name__ == "__main__":
	main()




