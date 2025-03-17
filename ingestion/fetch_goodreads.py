import boto3
import json
import os
from datasets import load_dataset
from dotenv import load_dotenv

# Load .env file
load_dotenv("./.env")


# Get AWS credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

S3_RAW_DATA_PATH = "raw/goodreads-books-2024-03-14.jsonl"


# Initialize S3 client with credentials from environment variables
s3_client = boto3.client("s3", 
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  region_name=AWS_DEFAULT_REGION)


dataset = load_dataset("BrightData/Goodreads-Books", split="train", streaming=True)


# Temporary local file to write the data before uploading
with open("./temp.jsonl", "w") as f:
    for i, book in enumerate(dataset):
        f.write(json.dumps(book) + "\n")
        if i >= 100000:  
            break


def upload_to_s3(local_file_path, s3_bucket_name, s3_file_path):
    print(f"Uploading to s3 bucket - {s3_bucket_name}/{s3_file_path}")
    try:
        # Upload file to S3
        s3_client.upload_file(local_file_path, s3_bucket_name, s3_file_path)
        print(f"    File uploaded to s3://{s3_bucket_name}/{s3_file_path}")
    except FileNotFoundError:
        print(f"    The file {local_file_path} was not found.")
    except Exception as e:
        print(f"    Error uploading file: {e}")



local_file_path = './temp.jsonl'
upload_to_s3(local_file_path, S3_BUCKET_NAME, S3_RAW_DATA_PATH)