import pandas as pd
import os
import requests
import hmac
import hashlib
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_signature(app_secret, api_path, parameters):
    """Generate HMAC-SHA256 signature for Lazada API"""
    # Sort parameters by key
    sorted_params = sorted(parameters.items())
    
    # Create query string
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    
    # Create string to sign
    string_to_sign = api_path + query_string
    
    # Generate signature
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    return signature

def get_valid_access_token():
    """Get a valid access token, refresh if needed"""
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([access_token, refresh_token, app_key, app_secret]):
        raise ValueError("Missing Lazada API credentials in environment variables")
    
    # For now, return the access token directly
    # TODO: Add token expiry check and refresh logic
    return access_token

def fetch_lazada_orders():
    """Fetch orders from Lazada API"""
    try:
        access_token = get_valid_access_token()
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # API endpoint
        api_path = '/orders/get'
        base_url = 'https://api.lazada.com.ph/rest'
        
        # Parameters
        timestamp = str(int(time.time() * 1000))
        parameters = {
            'app_key': app_key,
            'timestamp': timestamp,
            'sign_method': 'sha256',
            'access_token': access_token,
            'status': 'delivered',  # Get delivered orders
            'limit': '100'
        }
        
        # Generate signature
        signature = generate_signature(app_secret, api_path, parameters)
        parameters['sign'] = signature
        
        # Make API call
        response = requests.get(f"{base_url}{api_path}", params=parameters)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0':  # Success
                return data.get('data', {}).get('orders', [])
            else:
                print(f"Lazada API error: {data.get('message', 'Unknown error')}")
                return []
        else:
            print(f"HTTP error: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Error fetching Lazada orders: {e}")
        return []

def get_lazada_transactions():
    """Get Lazada transactions and transform to standard format"""
    print("Fetching Lazada transactions...")
    
    try:
        # Fetch orders from API
        orders = fetch_lazada_orders()
        
        if not orders:
            print("No Lazada orders found, returning empty DataFrame")
            return pd.DataFrame(columns=['transaction_id', 'product_name', 'quantity', 'price', 'customer_name', 'transaction_date', 'source'])
        
        # Transform to standard format
        transactions = []
        for order in orders:
            order_id = order.get('order_id', '')
            customer_name = f"{order.get('address_billing', {}).get('first_name', '')} {order.get('address_billing', {}).get('last_name', '')}".strip()
            order_date = order.get('created_at', '')
            
            # Process order items
            for item in order.get('order_items', []):
                transaction = {
                    'transaction_id': f"LZ_{order_id}_{item.get('order_item_id', '')}",
                    'product_name': item.get('name', ''),
                    'quantity': int(item.get('quantity', 0)),
                    'price': float(item.get('item_price', 0)),
                    'customer_name': customer_name or 'Unknown Customer',
                    'transaction_date': order_date,
                    'source': 'lazada'
                }
                transactions.append(transaction)
        
        df = pd.DataFrame(transactions)
        print(f"Retrieved {len(df)} Lazada transactions")
        return df
        
    except Exception as e:
        print(f"Error processing Lazada transactions: {e}")
        # Return empty DataFrame with correct columns if error occurs
        return pd.DataFrame(columns=['transaction_id', 'product_name', 'quantity', 'price', 'customer_name', 'transaction_date', 'source'])

if __name__ == "__main__":
    # Test the function
    df = get_lazada_transactions()
    print("\nLazada transactions:")
    print(df.head())
    print(f"\nTotal transactions: {len(df)}")