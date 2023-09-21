import numpy as np
import pandas as pd
import os
import boto3
import pickle
import sys
from s3_utility import read_pickle_file_from_s3, read_csv_file_from_s3
from tables import *
import time


NUMBER_OF_FEATURES = sys.argv[1]
MAPPING_FILE = sys.argv[2]
BUCKET_NAME = sys.argv[3]
SUBLOCATION = sys.argv[4]
PPR_FEATURE_LOCATION = sys.argv[5]
SAVE_PATH = sys.argv[6]
SAVE_FILENAME = sys.argv[7]
CHECK_ANY_SAVED_NODES = int(sys.argv[8])
NODE_TYPE = sys.argv[9]
NODE_TYPE_SEPERATOR = sys.argv[10]


if CHECK_ANY_SAVED_NODES == 1:
    bucket_location = BUCKET_NAME + "/" + SUBLOCATION + "/"
    node_list = compare_saved_ppr(MAPPING_FILE, IDENTIFIER_COLUMN, bucket_location, NODE_TYPE, NODE_TYPE_SEPERATOR)
else:
    mapping_file_df = pd.read_csv(MAPPING_FILE)
    mapping_file_df.dropna(subset=[IDENTIFIER_COLUMN], inplace=True)
    node_list = list(mapping_file_df[IDENTIFIER_COLUMN].unique())

def main():
	start_time = time.time()
	mean_embedding = get_mean_of_rank_normalized_embeddings(node_list)
	feature_df = read_csv_file_from_s3(BUCKET_NAME, PPR_FEATURE_LOCATION)
	feature_indices, feature_sel_df = get_features(mean_embedding, feature_df)
	embedding_feature_reduced_dim, embedding_row_df = extract_features(node_list, feature_indices)
	package_embedding_data(embedding_feature_reduced_dim, embedding_row_df, feature_sel_df)
	print("Completed in {} hrs".format(round((time.time() - start_time)/(60*60), 2)))


def package_embedding_data(ppr_array, row_df, col_df):
	class row_index_table(IsDescription):
		row_index = UInt16Col()
		node_id = StringCol(50)
		node_type = StringCol(25)

	class column_index_table(IsDescription):
		column_index = UInt32Col()
		node_id = StringCol(50)
		node_type = StringCol(25)

	h5file = open_file(os.path.join(SAVE_PATH, SAVE_FILENAME), mode="w", title="Embedding file")
	row_group = h5file.create_group("/", "row_index", "row index mapping")
	row_table = h5file.create_table(row_group, "row_index_table", row_index_table, "row index mapping table")
	column_group = h5file.create_group("/", "column_index", "column index mapping")
	column_table = h5file.create_table(column_group, "column_index_table", column_index_table, "column index mapping table")
	array_group = h5file.create_group("/", "embedding", "embedding array group")
	h5file.create_array(array_group, "embedding_array", ppr_array, "embedding array")
	
	row_table_pointer = row_table.row
	for index, row in row_df.iterrows():
		row_table_pointer["row_index"] = row["row_index"]	
		row_table_pointer["node_id"] = row["node_id"]
		row_table_pointer["node_type"] = row["node_type"]
		row_table_pointer.append()
	row_table.flush()

	column_table_pointer = column_table.row
	for index, row in col_df.iterrows():
		column_table_pointer["column_index"] = row["column_index"]
		column_table_pointer["node_id"] = row["node_id"]
		column_table_pointer["node_type"] = row["node_type"]
		column_table_pointer.append()
	column_table.flush()
	h5file.close()



def extract_features(node_list_, sel_indices):
	embedding_reduced_dim = []
	row_info = []
	for row_index, node_id in enumerate(node_list_):
		Key = SUBLOCATION + "/{}_dict.pickle".format(node_id)
		embedding_dict = read_pickle_file_from_s3(BUCKET_NAME, Key)
		embedding_arr = embedding_dict["embedding"]
		embedding_reduced_dim.append(embedding_arr[sel_indices])
		row_info.append((node_id, row_index))
	row_df = pd.DataFrame(row_info, columns=["node_id", "row_index"])
	row_df.loc[:, "node_type"] = row_df.node_id.apply(lambda x:x.split(NODE_TYPE_SEPERATOR)[0])
	row_df.loc[:, "node_id"] = row_df.node_id.apply(lambda x:x.split(NODE_TYPE_SEPERATOR)[-1])
	return np.vstack(embedding_reduced_dim), row_df[["row_index", "node_id", "node_type"]]

def get_features(arr, feature_df_):
	sel_indices = np.argsort(arr)[-NUMBER_OF_FEATURES:]
	sel_indices_ = np.sort(sel_indices)		
	feature_df_sel = feature_df_.iloc[sel_indices_]
	return sel_indices_, feature_df_sel.reset_index().drop("index", axis=1).reset_index().rename(columns={"index":"column_index"})

def rank_normalization_of_embedding(arr):
	sorted_indices = np.argsort(arr)
	ranks = np.empty_like(sorted_indices)
	ranks[sorted_indices] = np.arange(1, len(arr) + 1)
	return ranks

def get_mean_of_rank_normalized_embeddings(node_list_):
	embedding_sum = 0
	for node_id in node_list_:
		Key = SUBLOCATION + "/{}_dict.pickle".format(node_id)
		embedding_dict = read_pickle_file_from_s3(BUCKET_NAME, Key)
		rank_normalized_embedding = rank_normalization_of_embedding(embedding_dict["embedding"])
		embedding_sum += rank_normalized_embedding
	return np.divide(embedding_sum, len(node_list_))


if __name__ == "__main__":
	main()

