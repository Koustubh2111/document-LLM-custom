from nltk.tokenize import sent_tokenize
import logging
import re

class DataPreprocessor:
    def __init__(self, chunk_size=512):
        self.chunk_size = chunk_size

    def preprocess_text(self, text: str) -> str:
        """Remove non-alphanumeric characters."""
        try:
            return re.sub(r'[^A-Za-z0-9\s]', '', text)
        except Exception as e:
            logging.error(f"Error during text preprocessing: {e}") #logging helps in debugging
            return ""

    def split_text_into_chunks(self, text: str) -> list[str]:
        """Split text into smaller chunks."""
        try:
            sentences = sent_tokenize(text)
            chunks = []
            current_chunk = ""
            for sentence in sentences:
                if len(current_chunk) + len(sentence) < self.chunk_size:
                    current_chunk += " " + sentence
                else:
                    chunks.append(current_chunk)
                    current_chunk = sentence
            if current_chunk:
                chunks.append(current_chunk)
            return chunks
        except Exception as e:
            logging.error(f"Error during text chunking: {e}")
            return []
        
    def test_methods(self):
        "Test the two preprocessing methods"

        sample_data = [
        {"title": "Book 1", "summary": "This is a great book. It talks about fantasy and adventure."},
        {"title": "Book 2", "summary": "A mystery thriller that will keep you on edge."}
        ]
        print(f"Sample Data : {sample_data}")
        #1. preprocess_text
        preprocessed_summary = [self.preprocess_text(s['summary']) for s in sample_data]
        print(f"Preprocessed summaries : {preprocessed_summary}")
        #2. split_text_into_chunks
        chunks = [self.split_text_into_chunks(p) for p in preprocessed_summary]
        print(f"Chunks : {chunks}")

        

        

# %%
