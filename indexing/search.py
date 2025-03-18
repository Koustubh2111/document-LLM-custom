import numpy as np
from elasticsearch import Elasticsearch
import sys
import os 


from indexing.embedding import EmbeddingModel

class ElasticsearchRetriever:
    def __init__(self, es_host='localhost', es_port=9200, es_scheme='http'):
        # Specify the scheme explicitly (http or https)
        self.es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

    def vector_search(self, query_text: str, index_name: str, top_k=5):
        # Get the embedding for the query text
        query_embedding = self.get_embedding(query_text)
        
        # Convert the embedding to a list for Elasticsearch query
        query_embedding_list = query_embedding.cpu().detach().numpy().tolist() #move to cpu, detach from comp grap

        # Create the query body for Elasticsearch vector search
        #Defining a custom cosine similarity script score as script_score for matching with all documents
        body = {
            "query": {
                "script_score": {
                    "query": {
                        "match_all": {}
                    },
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {
                            "query_vector": query_embedding_list
                        }
                    }
                }
            },
            "_source": ["name", "author", "url", "genres", "star_rating", "first_published", "kindle_price", "summary_chunk"],  # Specify which fields to return
            "size": top_k  # Number of top documents to return
        }

        # Perform the search
        response = self.es.search(index=index_name, body=body)
        
        # Extract and return the top-k documents
        documents = [hit["_source"] for hit in response["hits"]["hits"]]
        return documents

    def get_embedding(self, text: str):
        embed = EmbeddingModel()
        return embed.get_embedding(text)
    

if __name__ == "__main__":

    search = ElasticsearchRetriever()
    query_text = "Find me books that have a summary like a murder mystery in a small town. The protagonist must be female."
    docs = search.vector_search(query_text, 'goodreads', 10)