import boto3
import pickle
import time

# create an S3 client
s3 = boto3.client('s3')

# specify the S3 bucket name and the object key (i.e., the file name)
bucket_name = 'ic-spoke'
object_key = 'spoke35M/Compound:inchikey:FERIUCNNQQJTOY-UHFFFAOYSA-N_dict.pickle'

start_time = time.time()
# download the file as a stream
response = s3.get_object(Bucket=bucket_name, Key=object_key)

# read the stream and deserialize the pickle file
content = response['Body'].read()
data = pickle.loads(content)
print(data.keys())
print(data["embedding"].shape)
print("Data is read from S3 in {} min".format(round((time.time()-start_time)/(60), 2)))
