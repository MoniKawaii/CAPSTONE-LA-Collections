from supabase import create_client
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("Supabase URL and Service Role Key must be set in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_supabase_client():
    """Get the Supabase client instance"""
    return supabase


