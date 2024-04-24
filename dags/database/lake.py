from constants import *
from google.cloud import storage
import os
import logging

# from fastavro import writer, reader, schema
# from rec_avro import to_rec_avro_destructive, from_rec_avro_destructive, rec_avro_schema


# For efficiency, to_rec_avro_destructive() destroys rec, and reuses it's
# data structures to construct avro_objects 


# store records in avro
# with open('json_in_avro.avro', 'wb') as x:
#    writer(x, schema.parse_schema(rec_avro_schema()), avroObjects)


def create_bucket(bucket_name : str):
    """Creates a new bucket. 
    
    Taken from : https://cloud.google.com/storage/docs/ 
    
    Args :
        - bucket_name : (str) the name of the dataset
    """
    client = storage.Client.from_service_account_json(
        CREDENTIALS
    )
    bucket = client.create_bucket(bucket_name)
    logging.info('Bucket {} created'.format(bucket.name))

def upload_string_blob(bucket_name, file_string, file_name) :
    """
        Uploads a dataframe directly into gcs bucket
    """

    client = storage.Client.from_service_account_json(
        CREDENTIALS
    )

    # The bucket on GCS in which to write the CSV file
    bucket = client.bucket(bucket_name)
    # The name assigned to the CSV file on GCS
    blob = bucket.blob(file_name)
    blob.upload_from_string(file_string)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket. 
    Taken from : https://cloud.google.com/storage/docs/ 
    
    Args :
        - bucket_name : (String) the name of the bucket
        - source_file_name : (String) the filename from local directory
        - destination_blob_mame : (String) the filename to be saved as        
    
    """
    client = storage.Client.from_service_account_json(
        CREDENTIALS
    )
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # Check if the blob already exists
    if blob.exists():
        logging.info('Blob {} already exists. Skipping upload.'.format(destination_blob_name))
        return
    
    # Upload the file
    blob.upload_from_filename(source_file_name)
    logging.info('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def get_blob(bucket_name,destination_directory,destination_blob_name):
    """
    Takes the data from your GCS Bucket and puts it into the working directory
    Taken from : https://cloud.google.com/storage/docs/ 
    
    Args :
        - bucket_name : (String) the name of the bucket to extract from
        - destination_directory : (String) the name of the directory to store the blob files
        - destination_blob_name : (String) the name of the file you want to retrieve
    """
    client = storage.Client.from_service_account_json(
        CREDENTIALS
    )
    os.makedirs(destination_directory, exist_ok = True) # create a local directory if it doesnt exist
    full_file_path = os.path.join(destination_directory, destination_blob_name)
    blobs = client.list_blobs(bucket_name) # get the list of blobs from the bucket

    for blob in blobs:
        if (blob.name == destination_blob_name) : # Checks that blob is what we are looking for
            blob.download_to_filename(full_file_path) # If it is, we will download to directory
            logging.info("Blob found and downloaded!")
            return
    
