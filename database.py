# database.py
import os
from dotenv import load_dotenv
from supabase import create_client

class DatabaseClient:
    def __init__(self):
        load_dotenv()
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("Credentials missing in .env")
            
        self.client = create_client(url, key)

    def get_client(self):
        return self.client