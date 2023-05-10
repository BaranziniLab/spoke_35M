import boto3
import pickle
import sys
import time
import os
from utility import get_spoke_embedding
from s3_utility import get_saved_compounds_with_pagerank, read_pickle_file_from_s3


compound_type = sys.argv[1]
sample = sys.argv[2]
sel_sheet_index = int(sys.argv[3])
data_path = sys.argv[4]

pvalue_thresh = 0.05
bucket_name = 'ic-spoke'
sheet_name_list = ["Without outlier", "Without outlier-MS treated", "Without outlier-MS not treated",
              "With outlier", "With outlier-MStreated", "With outlier-MS not treated"]
print("Fetching compounds with pagerank in S3 bucket ...")
compounds_with_pagerank = get_saved_compounds_with_pagerank()
print("Fetched!")


def main():
    start_time = time.time()
    print("Starting to create SPOKE embedding vector ...")
    spoke_embedding_dict = get_spoke_embedding(compound_type, sample, sel_sheet_index, data_path, pvalue_thresh=pvalue_thresh)
    binary_data = pickle.dumps(spoke_embedding_dict)
    del(spoke_embedding_dict)
    filename = "spoke_embedding_for_IMSMS_" + compound_type + "_compounds_" + sample + "_sample_" + "sheet_index_" + str(sel_sheet_index) + "_dict.pickle" 
    s3_client = boto3.client('s3')    
    object_key = 'spoke35M/spoke35M_iMSMS_embedding/{}'.format(filename)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
    s3_client.close()
    del(binary_data)
    print("Completed in {} min".format(round((time.time() - start_time)/60, 2)))
    


if __name__ == "__main__":
    main()
