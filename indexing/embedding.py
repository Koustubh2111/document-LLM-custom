#%%
from transformers import AutoTokenizer, AutoModel
import torch
import logging


class EmbeddingModel:
    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2"):
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name) #auto loads the tokenizer for the model
            self.model = AutoModel.from_pretrained(model_name) #loads the pretrained model for specific function
        except Exception as e:
            logging.error(f"Error loading model {model_name}: {e}")
            self.tokenizer = None
            self.model = None

    def get_embedding(self, text: str) -> torch.Tensor:
        """Generate embedding for a single text."""
        if self.tokenizer and self.model:
            try:
                inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True)
                outputs = self.model(**inputs)
                embedding = outputs.last_hidden_state.mean(dim=1).squeeze()
                
                # Ensure the embedding has 384 dimensions, resize if necessary
                if embedding.size(0) != 384:
                    embedding = torch.zeros(384)  #Resize to 384 if not 384 dimensions; ES required 384
                
                return embedding
            except Exception as e:
                logging.error(f"Error generating embedding: {e}")
                return torch.zeros(384)  # Return a 384-dimensional zero vector
        else:
            logging.error("Model or tokenizer not loaded correctly.")
            return torch.zeros(384)  # Return a 384-dimensional zero vector
            

# %%
