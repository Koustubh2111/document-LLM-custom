
## 📌 Classes Overview

### 1️⃣ **Preprocessing Class (`DataPreprocessor`)**
   - Cleans and processes raw text data.
   - Splits text into smaller chunks for embedding.
   - Handles punctuation removal, lowercasing, and whitespace normalization.

### 2️⃣ **Embedding Class (`EmbeddingModel`)**
   - Converts text chunks into dense vector embeddings using a pre-trained model.
   - Uses an auto tokenizer and model to generate high-dimensional representations.

### 3️⃣ **Elasticsearch Class (`ElasticsearchVectorStore`)**
   - Manages the connection to Elasticsearch.
   - Creates an index with appropriate mappings (text, keywords, numbers, and embeddings).
   - Inserts documents into Elasticsearch.

### 4️⃣ **Search Class (`ESSearch`)**
   - Handles search queries using different strategies (fuzzy search, keyword match, vector similarity).
   - Queries Elasticsearch for relevant results.
   - Supports range queries for numeric fields like ratings.

---

## 🛠 Issues Resolved

1. **Elasticsearch Connection Issues**  
   - Fixed connection errors due to HTTP/HTTPS conflicts.  
   - Resolved authentication errors by configuring Elasticsearch correctly.

2. **Index Mapping and Query Issues**  
   - Fixed `search_phase_execution_exception` caused by fuzzy queries on numeric fields. (FIX)  
   - Adjusted queries to use range filters for `rating` instead of fuzzy matching.

3. **Serialization Errors**  
   - Resolved `Unable to serialize to JSON` issue by converting tensor embeddings to lists before inserting into Elasticsearch.

4. **Docker Configuration Fixes**  
   - Switched to Elasticsearch 8.x and ensured proper security configurations.
   - Updated the `docker-compose` setup to enable authentication.