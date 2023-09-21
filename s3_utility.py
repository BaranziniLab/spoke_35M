import boto3
import os
import numpy as np
import pickle
import pandas as pd


def read_pickle_file_from_s3(bucket_name, object_key):
	s3 = boto3.client('s3')
	response = s3.get_object(Bucket=bucket_name, Key=object_key)
	content = response['Body'].read()
	data = pickle.loads(content)
	return data

def read_csv_file_from_s3(bucket_name, object_key):
	s3_client = boto3.client('s3')
	s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
	return pd.read_csv(s3_object["Body"])

def get_saved_compoundid_from_s3(bucket_name, file_location):
	bucket_location = bucket_name + "/" + file_location + "/"
	cmd = "aws s3 ls s3://{}".format(bucket_location)
	out = os.popen(cmd)
	out_list = out.read().split("\n")
	saved_compound_list = np.array([element for element in out_list if "Compound:inchikey:" in element])
	saved_compound_list_ = ['Compound:inchikey:' + element.split("Compound:inchikey:")[-1].replace('_dict.pickle', '') for element in saved_compound_list if "Compound:inchikey:" in element]
	return saved_compound_list_


def get_saved_compounds_with_no_pagerank(bucket_name, file_location):
	saved_compound_list_ = get_saved_compoundid_from_s3(bucket_name, file_location)
	s3_client = boto3.client('s3')
	bucket_name = bucket_name
	compounds_with_no_pagerank = []
	for node_id in saved_compound_list_:
	  object_key = '{}/{}_dict.pickle'.format(file_location, node_id)
	  response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
	  if response['ContentLength'] < 222:
	    compounds_with_no_pagerank.append(node_id)
	return compounds_with_no_pagerank

def get_saved_compounds_with_pagerank(bucket_name, file_location):
	saved_compound_list_ = get_saved_compoundid_from_s3(bucket_name, file_location)
	s3_client = boto3.client('s3')
	bucket_name = bucket_name
	compounds_with_pagerank = []
	for node_id in saved_compound_list_:
	  object_key = '{}/{}_dict.pickle'.format(file_location, node_id)
	  response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
	  if response['ContentLength'] > 222:
	    compounds_with_pagerank.append(node_id)
	return compounds_with_pagerank

