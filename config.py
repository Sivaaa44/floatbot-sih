import os
import yaml
from dotenv import load_dotenv

load_dotenv()

class Config:
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.database_path = 'argo_floats.db'
        self.semantic_model_path = 'argo_semantic_model.yaml'
        self.gemini_model = "gemini-2.0-flash-exp"
        
        if not self.google_api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment variables")
    
    def load_semantic_model(self):
        with open(self.semantic_model_path, 'r') as file:
            return yaml.safe_load(file)

config = Config()