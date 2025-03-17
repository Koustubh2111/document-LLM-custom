#%%
from preprocessing import DataPreprocessor
from embedding import EmbeddingModel
from elasticsearch_idx import ElasticsearchVectorStore
from search import ESSearch
import torch

sample_data = [
                {"title": "Book 1", "summary": "This is a great book. It talks about fantasy and adventure."},
                {"title": "Book 2", "summary": "A mystery thriller that will keep you on edge."}
              ]


#%%

preprocessor = DataPreprocessor(chunk_size=512)
print(f"Sample Data : {sample_data}")

preprocessed_summary = [preprocessor.preprocess_text(s['summary']) for s in sample_data]
print(f"Preprocessed summaries : {preprocessed_summary}")

chunks = [preprocessor.split_text_into_chunks(p) for p in preprocessed_summary]
print(f"Chunks : {chunks}")

#%%

for i, chunk in enumerate(chunks):
    print(f"    Preprocessing chunk : {chunk}")
    text = ' '.join(chunk)
    print(f"    Getting embeddings for {text}")
    e = EmbeddingModel()
    embedding = e.get_embedding(text=text)

    sample_data[i]['embedding'] = embedding

    
# %%
"""
docker run -d --name elasticsearch -e 
"discovery.type=single-node" -e "xpack.security.enabled=false" #make this true with password
-p 9200:9200 -p 9300:9300 
docker.elastic.co/elasticsearch/elasticsearch:8.17.2
"""
es_url = "http://localhost:9200"
index_name = "test_index"

elastic = ElasticsearchVectorStore(es_url, index_name)
# %%
#Elastic search takes only JSON serializable data, a pytorch tensor cannot be sent, converted to list
for i, doc in enumerate(sample_data):
    elastic.insert_document(
        book_id = i + 1, \
        title=doc['title'], \
        description=f"This is loading sample data {i + 1}",\
        genre='Fantasy',\
        rating=8.2 + i,\
        num_reviews=10,\
        embedding=doc['embedding'].tolist() if isinstance(doc['embedding'], torch.Tensor) else doc['embedding']
    )
# %%

search = ESSearch(es_url, index_name)

# %%
query = "i want a movie that is an expansive fantasy"
query_embeddings = e.get_embedding(query)

# %%
search.search(query=query) #failed on using fizzy logic, fix later
search.semantic_search(query_embeddings.tolist()) #worked

