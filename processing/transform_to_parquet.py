import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
import json
from io import BytesIO
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# Load AWS credentials
load_dotenv("../ingestion/fetch_goodreads/.env")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

RAW_DATA_PATH = "raw/goodreads-books-2024-03-14.jsonl"  # Raw JSONL file
PROCESSED_DATA_PATH = "processed/goodreads-books-2024-03-14.parquet"

def load_jsonl_from_s3():
    """Load raw JSONL data from S3 into a DataFrame."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=RAW_DATA_PATH)
        jsonl_data = response["Body"].read().decode("utf-8").splitlines()
        data = [json.loads(line) for line in jsonl_data] #Convert list of dicts to dataframe
        return pd.DataFrame(data)
    except ClientError as e:
        print(f"Error loading JSONL from S3: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def save_parquet_to_s3(df):
    """Convert DataFrame to Parquet and upload to S3."""
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy")
        buffer.seek(0)
        
        s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, PROCESSED_DATA_PATH, ExtraArgs={"Metadata": {"status": "processed"}})
        print(f"Processed data uploaded to s3://{S3_BUCKET_NAME}/{PROCESSED_DATA_PATH}")
    except NoCredentialsError:
        print("AWS credentials not available.")
    except ClientError as e:
        print(f"Error uploading Parquet to S3: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def main():
    print("Loading raw data from S3...")
    df = load_jsonl_from_s3()

    if df is not None:
        print(f"Loaded {len(df)} records. Transforming to Parquet...")
        save_parquet_to_s3(df)
        print("Data transformation complete!")

if __name__ == "__main__":
    main()