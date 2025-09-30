"""
Supabase Client Configuration

This module initializes the Supabase client with proper error handling
and environment variable validation.
"""

import os
import sys
from typing import Optional

try:
    from supabase import create_client, Client
except ImportError as e:
    print(f"Error: Supabase package not found. Install with: pip install supabase")
    raise e

try:
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: python-dotenv package not found. Install with: pip install python-dotenv")
    raise e

# Load environment variables
load_dotenv()

# Get environment variables with validation
SUPABASE_URL: Optional[str] = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY: Optional[str] = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL must be set in .env file")

if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY must be set in .env file")

# Initialize Supabase client
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    print("SUCCESS: Supabase client initialized successfully")
except Exception as e:
    print(f"ERROR: Failed to initialize Supabase client: {e}")
    raise e

# Export for easy importing
__all__ = ['supabase', 'SUPABASE_URL']


