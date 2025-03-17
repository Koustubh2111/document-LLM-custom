
from elasticsearch import Elasticsearch
import torch
import logging

class ElasticsearchVectorStore:
    """Handles storing and searching embeddings in Elasticsearch."""

    def __init__(self, es_host: str, index_name: str):
        """Initialize Elasticsearch client and ensure index is created."""
        self.es = Elasticsearch(es_host, verify_certs=False)
        self.index_name = index_name
        self.create_index()  # Ensure index exists on startup

    def create_index(self):
        """Creates an Elasticsearch index if it does not exist."""
        mapping = {
            "mappings": {
                "properties": {
                    "book_id": {"type": "keyword"},
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "genre": {"type": "keyword"},
                    "rating": {"type": "float"},
                    "num_reviews": {"type": "integer"},
                    "embedding": {"type": "dense_vector", "dims": 384}  
                }
            }
        }
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=mapping)
            print(f"Index '{self.index_name}' created.")
        else:
            print(f"Index '{self.index_name}' already exists.")



    def insert_document(self, book_id: str, title: str, description: str, genre: list, rating: float, num_reviews: int, embedding: list):
        """Insert a book document into Elasticsearch with metadata and vector embedding."""
        try:
            doc = {
                "book_id": book_id,
                "title": title,
                "description": description,
                "genre": genre,  # Store as list
                "rating": rating,
                "num_reviews": num_reviews,
                "embedding": embedding,  # Vector representation
            }
            self.es.index(index=self.index_name, id=book_id, body=doc)
            print(f"Inserted: {title} (ID: {book_id})")
        except Exception as e:
            logging.error(f"Failed to insert {title}: {e}")

    def delete_index(self):
        """Delete the Elasticsearch index."""
        self.es.indices.delete(index=self.index_name, ignore=[400, 404]) #ignore codes
        print(f"Index '{self.index_name}' deleted.")
