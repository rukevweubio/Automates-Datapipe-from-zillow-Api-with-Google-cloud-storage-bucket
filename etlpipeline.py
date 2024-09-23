import requests
import os 
import json
import pandas as pd 
import requests
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()



def extraction_of_data():
    url = "https://zillow-com1.p.rapidapi.com/people/searchByAddress"

    querystring = {
    "address": "2246 Tennessee St",
    "location": "Lawrence, KS 66046",
    "format": "full"
    }

    headers = {
    "x-rapidapi-key": "89338c3b35mshef78a559ef744efp15f713jsn28765a5b0ba1",
    "x-rapidapi-host": "zillow-com1.p.rapidapi.com"
    }

    # Make the API request
    response = requests.get(url, headers=headers, params=querystring)

    # Convert the response to JSON
    data = response.json()
    data = pd.DataFrame(data)
    return  data
data=extraction_of_data()
data

google_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "GOOGLE_APPLICATION_CREDENTIALS "


def cleaning_data():
    data[['firstname','lastname']]= data['fullName'].str.split(' ',n=1 ,expand =True)
    data['phone'] = data['phone'].str.lstrip('(').str.rstrip(')').str.replace(')','')
    data['age']= data['age'].str.replace('Deceased (1918 - 2002','50').str.replace(')','')
    data.pop('fullName')
    
    
cleaning_data()  

#create a google bucket for strorage
def create_google_s3_bucket(bucket_name, location='europe-west1'):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, location=location)
    print(f"Bucket {new_bucket.name} created in {new_bucket.location}.")

create_google_s3_bucket(bucket_name = 'zillow-api-ubior-project-12345')

def load_data_into_csv(data):
    return  data.to_csv('zillowcsv', index=False)

load_data_into_csv(data)


def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    # Get the bucket object
    bucket = storage_client.bucket(bucket_name)
    # Create a blob object from the bucket
    blob = bucket.blob(destination_blob_name)
    # Upload the file to the bucket
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
bucket_name = 'your-unique-bucket-name'
source_file_name = "zillowcsv"  
destination_blob_name = 'zillowcsv_file' 

# Call the function to upload the file
upload_to_bucket(bucket_name, source_file_name, destination_blob_name)