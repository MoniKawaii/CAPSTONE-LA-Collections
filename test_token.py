#!/usr/bin/env python3
"""
Test Token Validity

This script tests if the access token is valid at all
"""

import json
import os
from lazop_sdk import LazopClient, LazopRequest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_tokens():
    """Load tokens from JSON file"""
    with open('lazada_tokens.json', 'r') as f:
        return json.load(f)

def test_token():
    """Test if token is valid"""
    print("🔍 Testing Token Validity...")
    print("=" * 40)
    
    try:
        # Load credentials and tokens
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        tokens = load_tokens()
        access_token = tokens['access_token']
        
        print(f"📱 App Key: {app_key}")
        print(f"🔑 Access Token: {access_token}")
        
        # Try different server URLs
        urls = [
            'https://api.lazada.com/rest',      # Production
            'https://api.lazada.sg/rest',       # Singapore
            'https://api.lazada.co.th/rest',    # Thailand  
            'https://api.lazada.vn/rest',       # Vietnam
            'https://api.lazada.com.my/rest',   # Malaysia
            'https://api.lazada.com.ph/rest'    # Philippines
        ]
        
        for url in urls:
            print(f"\n🌐 Testing with URL: {url}")
            
            # Initialize client
            client = LazopClient(
                server_url=url,
                app_key=app_key,
                app_secret=app_secret
            )
            
            # Try a simple API call
            request = LazopRequest('/seller/get', 'GET')
            response = client.execute(request, access_token)
            
            print(f"   Response Type: {response.type}")
            print(f"   Response Code: {response.code}")
            print(f"   Response Message: {response.message}")
            
            if response.type != 'ISV' or response.code != 'IllegalAccessToken':
                print("   ✅ This URL might work!")
                print(f"   Response Body: {response.body}")
                break
            else:
                print("   ❌ Invalid token for this URL")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_token()