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
                return outputs.last_hidden_state.mean(dim=1).squeeze()
            except Exception as e:
                logging.error(f"Error generating embedding: {e}")
                return torch.zeros(768)  # Return zero vector if something goes wrong
        else:
            logging.error("Model or tokenizer not loaded correctly.")
            return torch.zeros(768)
        
