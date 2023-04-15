import boto3
import os
import numpy as np


def get_saved_compoundid_from_s3():
	cmd = "aws s3 ls s3://ic-spoke/spoke35M/"
	out = os.popen(cmd)
	out_list = out.read().split("\n")
	saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
	saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]
	return saved_compound_list_


def get_saved_compounds_with_no_pagerank():
	saved_compound_list_ = get_saved_compoundid_from_s3()
	s3_client = boto3.client('s3')
	bucket_name = 'ic-spoke'
	compounds_with_no_pagerank = []
	for node_id in saved_compound_list_:
	  object_key = 'spoke35M/{}_dict.pickle'.format(node_id)
	  response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
	  if response['ContentLength'] < 222:
	    compounds_with_no_pagerank.append(node_id)
	return compounds_with_no_pagerank

def get_saved_compounds_with_pagerank():
	saved_compound_list_ = get_saved_compoundid_from_s3()
	s3_client = boto3.client('s3')
	bucket_name = 'ic-spoke'
	compounds_with_pagerank = []
	for node_id in saved_compound_list_:
	  object_key = 'spoke35M/{}_dict.pickle'.format(node_id)
	  response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
	  if response['ContentLength'] > 222:
	    compounds_with_pagerank.append(node_id)
	return compounds_with_pagerank

