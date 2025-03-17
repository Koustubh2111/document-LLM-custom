from elasticsearch import Elasticsearch
import logging

class ESSearch:
    def __init__(self, es_host: str, index_name: str):
        """
        Initialize the Elasticsearch search class.
        
        :param es_host: Elasticsearch host URL
        :param index_name: Name of the Elasticsearch index
        """
        self.es = Elasticsearch(es_host)
        self.index_name = index_name

    def search(self, query: str, fields=None, top_n: int = 10):
        """
        Search for documents in Elasticsearch.

        :param query: Search query string.
        :param fields: List of fields to search in (default: all relevant fields).
        :param top_n: Number of results to return.
        :return: List of search results.
        """
        if fields is None:
            fields = ["summary", "title", "author", "genre", "rating"]

        search_query = {
            "size": top_n,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": fields,
                    "fuzziness": "AUTO"  # Allows typo tolerance in searches
                }
            }
        }

        try:
            response = self.es.search(index=self.index_name, body=search_query)
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            logging.error(f"Search failed: {e}")
            return []

    def semantic_search(self, embedding: list, top_n: int = 10):
        """
        Perform semantic search using vector similarity.

        :param embedding: Query embedding (list of floats).
        :param top_n: Number of top results to return.
        :return: List of search results.
        """
        try:
            search_query = {
                "size": top_n,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": embedding}
                        }
                    }
                }
            }
            response = self.es.search(index=self.index_name, body=search_query)
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            logging.error(f"Semantic search failed: {e}")
            return []
