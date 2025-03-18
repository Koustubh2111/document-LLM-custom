import sys
import os
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError
from tqdm import tqdm
import logging
import torch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from indexing.preprocessing import DataPreprocessor
from indexing.embedding import EmbeddingModel

from ingestion.load_from_s3 import S3DataFetcher

from elasticsearch.helpers import bulk, BulkIndexError
import traceback


class GoodreadsIndexer():
    def __init__(self, es_client, index_name):
        self.es_client = es_client
        self.index_name = index_name
        self.preprocessor = DataPreprocessor()  # 512 chunks
        self.embeddings_obj = EmbeddingModel()

    def prepare_documents(self, df):
        """
        Prepare documents to be indexed in Elasticsearch.
        :param df: DataFrame to be indexed
        """
        documents = []  # List to store all the documents to be indexed
        try:

            df = df.head(1000)  # Limit to first 1k/100k rows for testing
            # Replace NaN values with 0 or another default value
            df['num_reviews'].fillna(0, inplace=True) #ES cannot parse Nan
            
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing Documents"):

                # Check for missing values in 'summary'
                summary = row.get('summary', '')  # Safer way to get column
                if not summary:
                    summary = "No summary available"

                # Split long summaries into chunks
                summary_chunks = self.preprocessor.split_text_into_chunks(
                    self.preprocessor.preprocess_text(summary)
                )


                # Generate embeddings for each chunk
                chunk_embeddings = []
                for chunk in summary_chunks:
                    if chunk:
                        embedding = self.embeddings_obj.get_embedding(chunk)

                        #Check if embedding has zero magnitude (all zeros)
                        if embedding.shape[0] == 0 or embedding.norm() == 0:  # Check for zero embedding magnitude
                            logging.warning(f"Empty embedding generated for text: {chunk}")
                            continue  # Skip this chunk if embedding is invalid
                        chunk_embeddings.append(embedding)
                    else:
                        continue  # Skip empty chunks


                # Prepare documents for each chunk
                for chunk_idx, (chunk, embedding) in tqdm(enumerate(zip(summary_chunks, chunk_embeddings)), \
                                                          total=len(summary_chunks), leave=False):
                    document = {
                        "_op_type": "index",
                        "_index": self.index_name,
                        "_id": f"{row['id']}_{chunk_idx}",  # Unique ID for each chunk
                        "_source": {
                            "url": row['url'],
                            "name": row['name'],
                            "author": row['author'],
                            "star_rating": row['star_rating'],
                            "num_ratings": row['num_ratings'],
                            "num_reviews": row['num_reviews'],
                            "summary_chunk": chunk,
                            "genres": row['genres'],
                            "first_published": row['first_published'],
                            "about_author": row['about_author'],
                            "community_reviews": row['community_reviews'],
                            "kindle_price": row['kindle_price'],
                            "embedding": embedding.tolist() if not isinstance(embedding, list) else embedding 
                        }
                    }
                    documents.append(document)

            print(f"Finished processing all {len(df)} rows.")
            return documents

        except Exception as e:
            print("Error in prepare_documents():", traceback.format_exc())
            return []


    def index_data(self, df):
        """
        Index the DataFrame data into Elasticsearch using the bulk helper.
        :param df: DataFrame to be indexed
        """
        try:
            # Prepare the documents for indexing
            print("Preparing documents for indexing...")
            documents = self.prepare_documents(df)
            print(f"Prepared {len(documents)} documents.")

            if not documents:
                print("No documents to index.")
                return

            # Using bulk to send the documents to Elasticsearch
            success, failed = bulk(self.es_client, documents, raise_on_error=False)  #AI COMMENT: Added raise_on_error=False to prevent exception and log failed docs
            print(f"Successfully indexed {success} documents.")
            if failed:
                print(f"Failed to index {len(failed)} documents.")  #AI COMMENT: Added logging for failed documents
                # Log the details of the failed documents
                for error in failed:
                    print(f"Failed document: {error}")  #AI COMMENT: Logs each failed document's error message

        except RequestError as e:
            print(f"Elasticsearch request error: {str(e)}")
        except ConnectionError as e:
            print(f"Elasticsearch connection error: {str(e)}")
        except BulkIndexError as e:  #AI COMMENT: Caught BulkIndexError to handle the bulk-specific error
            print(f"Bulk index error: {str(e)}")
        except Exception as e:
            print("Unexpected error:", traceback.format_exc())  # Logs full traceback
        

if __name__ == "__main__":

    # Get ES Client
    es_host = "http://localhost:9200"
    index_name = "goodreads"
    es_client = Elasticsearch(es_host, verify_certs=False)

    # Index the data
    indexer = GoodreadsIndexer(es_client=es_client, index_name=index_name)

    # Load processed data
    s3_data_fetch = S3DataFetcher(env_path="../ingestion/.env")
    processed_data = s3_data_fetch.fetch_parquet_from_s3()

    # Index the data into ES
    indexer.index_data(df=processed_data)
