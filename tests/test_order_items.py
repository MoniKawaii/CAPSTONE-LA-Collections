#!/usr/bin/env python3
"""
Test Lazada Orders Items API

This script tests the /orders/items/get API to get detailed item information
"""

import json
import os
from lazop_sdk import LazopClient, LazopRequest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_tokens():
    """Load tokens from JSON file"""
    with open('app/lazada/lazada_tokens.json', 'r') as f:
        return json.load(f)

def test_order_items_api():
    """Test the order items API"""
    print("🔍 Testing Lazada Order Items API...")
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
        
        # Test order items API - need to get order IDs first
        print("\n📦 Step 1: Getting order IDs...")
        orders_request = LazopRequest('/orders/get', 'GET')
        orders_request.add_api_param('created_after', '2025-09-01T00:00:00+08:00')
        orders_request.add_api_param('limit', '5')  # Just get a few for testing
        
        orders_response = client.execute(orders_request, access_token)
        
        if orders_response.body and 'data' in orders_response.body:
            orders = orders_response.body['data'].get('orders', [])
            if orders:
                # Get order IDs
                order_ids = [str(order['order_id']) for order in orders[:3]]  # Test with first 3
                print(f"🔍 Using Order IDs: {order_ids}")
                
                # Now test order items API
                print("\n📦 Step 2: Getting order items...")
                request = LazopRequest('/orders/items/get', 'GET')
                request.add_api_param('order_ids', json.dumps(order_ids))  # API expects JSON array
                
                response = client.execute(request, access_token)
                print(f"Response Type: {response.type}")
                print(f"Response Code: {response.code}")
                print(f"Response Message: {response.message}")
                
                if response.body:
                    print("✅ Order Items API Success!")
                    # Pretty print first few items
                    body = response.body
                    if 'data' in body and body['data']:
                        print(f"\n📊 Found {len(body['data'])} order items")
                        print("\n📋 Sample Order Items:")
                        for i, item in enumerate(body['data'][:3]):
                            print(f"\nItem {i+1}:")
                            for key, value in item.items():
                                print(f"  {key}: {value}")
                                
                        # Save full response for analysis
                        with open('app/lazada/order_items_sample.json', 'w') as f:
                            json.dump(body, f, indent=2)
                        print(f"\n💾 Full response saved to: app/lazada/order_items_sample.json")
                    else:
                        print("📊 Response Body:", body)
            else:
                print("❌ No orders found to test items API")
        else:
            print("❌ Failed to get orders for testing items API")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_order_items_api()