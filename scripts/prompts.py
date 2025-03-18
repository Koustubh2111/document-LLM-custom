from transformers import LlamaTokenizer, LlamaForCausalLM
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from indexing.search import ElasticsearchRetriever
import ollama

class PromptGenerator:
    def __init__(self):
        pass

    def create_prompt(self, documents, query: str):
        """Creates the prompt the documets retrived from elastic search as 
        context amd uses the same search query used for getting docs from ES
        """
        prompt = "Based on the following documents, answer the question:\n\n"
        for doc in documents:
            prompt += f"Document: {doc['name']}\nSummary: {doc['summary_chunk']}\n\n"
        prompt += f"Answer the question: {query}"
        return prompt
    
class AnswerGenerator:
    def __init__(self, model_name="llama3.2:3b"):
        self.model_name = model_name

    def generate_answer(self, prompt: str):
        response = ollama.chat(model=self.model_name, messages=[{"role": "user", "content": prompt}])
        print(response['message']['content'])
    

if __name__ == "__main__":

    #1. Get docs from elastic search
    search = ElasticsearchRetriever()
    query_text = "Find me books that have a summary like a murder mystery in a small town. The protagonist must be female."
    retrived_docs = search.vector_search(query_text, 'goodreads', 10)

    #2. Generate Prompt
    p = PromptGenerator()
    prompt = p.create_prompt(documents=retrived_docs, query=query_text)

    #3. Generate answer
    a = AnswerGenerator()
    a.generate_answer(prompt=prompt)

    



# %%
