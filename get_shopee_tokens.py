#!/usr/bin/env python3
"""
Shopee Authorization URL Generator
This script generates a signed authorization URL for the Shopee API.
"""

import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
from dotenv import load_dotenv

# --- Configuration ---

# Load environment variables from .env file
load_dotenv()

# Your Partner ID and Partner Key from the .env file
PARTNER_ID = os.getenv('SHOPEE_PARTNER_ID')
PARTNER_KEY = os.getenv('SHOPEE_API_SECRET')

# The fixed API path for shop authorization
API_PATH = '/api/v2/shop/auth_partner'

# !!! IMPORTANT !!!
# Replace this with your actual callback URL where Shopee will redirect.
# This could be an ngrok URL for local testing.
REDIRECT_URL = 'https://oscitant-brody-pseudonationally.ngrok-free.dev'

# The base URL for the Sandbox/Test-Stable environment
HOST = 'https://partner.test-stable.shopeemobile.com'

# ----------------------------------------------------------------------

def generate_shopee_auth_link(partner_id, partner_key, api_path, redirect_url, host):
    """Generates the signed authorization URL for Shopee."""
    
    # 1. Check for missing credentials
    if not partner_id or not partner_key:
        print("Error: Missing required environment variables.")
        print("Please set SHOPEE_PARTNER_ID and SHOPEE_API_SECRET in your .env file.")
        return None

    # Convert partner_id to integer for consistency
    try:
        partner_id = int(partner_id)
    except (ValueError, TypeError):
        print(f"Error: Invalid SHOPEE_PARTNER_ID: '{partner_id}'. It must be a number.")
        return None

    # 2. Get the current Unix timestamp
    timestamp = int(time.time())
    
    # 3. Create the Base String for the signature
    base_string = f"{partner_id}{api_path}{timestamp}"
    
    # 4. Calculate the HMAC-SHA256 signature
    signature = hmac.new(
        partner_key.encode('utf-8'),
        base_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    # 5. Construct the final query parameters
    query_params = {
        'partner_id': partner_id,
        'timestamp': timestamp,
        'sign': signature,
        'redirect': redirect_url
    }
    
    # 6. Build the final authorization URL
    auth_link = f"{host.strip()}{api_path}?{urlencode(query_params)}"
    
    return auth_link, timestamp, base_string

def main():
    """Main function to generate and print the authorization link."""
    print("--- Shopee Authorization URL Generator ---")
    
    authorization_link, timestamp, base_string = generate_shopee_auth_link(
        PARTNER_ID,
        PARTNER_KEY,
        API_PATH,
        REDIRECT_URL,
        HOST
    )
    
    if authorization_link:
        print(f"\nGenerated Timestamp: {timestamp}")
        print(f"Base String for Signature: {base_string}")
        print("\n✅ Authorization Link (Sandbox):")
        print(authorization_link)
        print("\nVisit this URL in your browser to authorize the application.")
    else:
        print("\n❌ Failed to generate URL due to missing configuration.")

if __name__ == "__main__":
    main()
