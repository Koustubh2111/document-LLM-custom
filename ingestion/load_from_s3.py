import pandas as pd
import boto3
from io import BytesIO
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from dotenv import load_dotenv
import os




class S3DataFetcher:
    def __init__(self, env_path = './.env'):
        """
        Initialize S3DataFetcher with S3 client using environment variables.
        """
        # Load environment variables from .env file
        load_dotenv(env_path)


        # Load environment variables
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.file_key = os.getenv("S3_PARQUET_FILE_PATH")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION")
        
        # Create the S3 client with the provided AWS credentials and region
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region
        )

    def check_connection(self):
        """
        Check if the connection to AWS S3 is valid.
        """
        try:
            # Try to list the buckets to verify AWS credentials and connection
            response = self.s3_client.list_buckets()
            print("AWS S3 connection successful.")
            return True
        except NoCredentialsError:
            print("Error: AWS credentials are not provided or are invalid.")
        except PartialCredentialsError:
            print("Error: AWS credentials are partially incorrect or incomplete.")
        except ClientError as e:
            print(f"Error: Unable to connect to S3. {e}")
        return False

    def check_bucket_exists(self):
        """
        Check if the specified bucket exists.
        """
        try:
            # Try to list the objects in the bucket to verify its existence
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' exists.")
            return True
        except ClientError as e:
            print(f"Error: Bucket '{self.bucket_name}' does not exist or is inaccessible. {e}")
            return False

    def check_file_exists(self):
        """
        Check if the specified file exists in the given S3 bucket.
        """
        try:
            # Try to fetch the object to check if the file exists
            self.s3_client.head_object(Bucket=self.bucket_name, Key=self.file_key)
            print(f"File '{self.file_key}' exists in bucket '{self.bucket_name}'.")
            return True
        except ClientError as e:
            print(f"Error: File '{self.file_key}' does not exist in the bucket. {e}")
            return False

    def fetch_parquet_from_s3(self):
        """
        Fetch Parquet file from S3 and load it into a Pandas DataFrame.
        :return: Pandas DataFrame containing the data from the Parquet file
        """

        # Check AWS connection first
        if not self.check_connection():
            return None
        # Check if the bucket exists
        if not self.check_bucket_exists():
            return None
        # Check if the file exists in the bucket
        if not self.check_file_exists():
            return None
        
        try:
            print(f"Downloading {self.file_key} from S3 bucket {self.bucket_name}...")
            # Fetch the Parquet file from S3
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
            parquet_data = obj['Body'].read()
            
            # Use BytesIO to simulate file-like object and load it into a DataFrame
            data = BytesIO(parquet_data)
            df = pd.read_parquet(data)
            print("Data successfully loaded from S3 into DataFrame.")
            return df
        except Exception as e:
            print(f"Error fetching Parquet file from S3: {str(e)}")
            return None

# %%
