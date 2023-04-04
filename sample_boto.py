import boto3
import pickle
import numpy as np

# Assume you have the following dictionary
data = {
"node" : "node_1",
"embedding" : np.random.random((35000000,)).astype("float64"),
"feature_id" : np.random.random((35000000,)).astype("float64")
}

# Convert the dictionary to a binary format
binary_data = pickle.dumps(data)

# Set up S3 client
s3_client = boto3.client('s3')

# Set up S3 bucket and object key
bucket_name = 'ic-spoke'
object_key = 'sample_data.pickle'

# Upload the binary data to S3 bucket as an object
s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=binary_data)
