#!/usr/bin/env python3
"""
Direct Lazada API Test

This script tests the Lazada API directly using the lazop SDK
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

def test_lazada_api():
    """Test Lazada API directly"""
    print("🔍 Testing Lazada API Directly...")
    print("=" * 40)
    
    try:
        # Load credentials and tokens
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        tokens = load_tokens()
        access_token = tokens['access_token']
        
        print(f"📱 App Key: {app_key}")
        print(f"🔑 Access Token: {access_token[:20]}...")
        
        # Initialize client (Philippines)
        client = LazopClient(
            server_url='https://api.lazada.com.ph/rest',
            app_key=app_key,
            app_secret=app_secret
        )
        
        # Test 1: Try orders/get
        print("\n📦 Testing /orders/get...")
        request = LazopRequest('/orders/get', 'GET')
        request.add_api_param('update_before', '2024-10-05T16:00:00+08:00')
        request.add_api_param('sort_direction', 'DESC')
        request.add_api_param('offset', '0')
        request.add_api_param('limit', '10')
        request.add_api_param('update_after', '2024-01-01T09:00:00+08:00')
        request.add_api_param('sort_by', 'updated_at')
        request.add_api_param('created_before', '2024-10-05T16:00:00+08:00')
        request.add_api_param('created_after', '2024-01-01T09:00:00+08:00')
        request.add_api_param('status', 'shipped')
        
        response = client.execute(request, access_token)
        print(f"Response Type: {response.type}")
        print(f"Response Code: {response.code}")
        print(f"Response Message: {response.message}")
        print(f"Response Body: {response.body}")
        
        # Test 2: Try seller/get (basic info)
        print("\n👤 Testing /seller/get...")
        request2 = LazopRequest('/seller/get', 'GET')
        
        response2 = client.execute(request2, access_token)
        print(f"Response Type: {response2.type}")
        print(f"Response Code: {response2.code}")
        print(f"Response Message: {response2.message}")
        print(f"Response Body: {response2.body}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lazada_api()