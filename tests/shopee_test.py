"""
Shopee API Integration Test

This file tests the Shopee API integration for the LA Collections project.
"""
import time
import hashlib
import hmac
import requests
import json
import os
import sys
from urllib.parse import urlencode
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path so we can import app modules if needed
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Load environment variables
load_dotenv()

# Shopee API credentials from environment
PARTNER_ID = os.getenv('SHOPEE_PARTNER_ID')
API_KEY = os.getenv('SHOPEE_API_KEY')
API_SECRET = os.getenv('SHOPEE_API_SECRET')
BASE_URL = os.getenv('SHOPEE_BASE_URL', 'https://partner.test-stable.shopeemobile.com')

# Token file path
TOKEN_DIRECTORY = Path('tokens')
TOKEN_FILE = TOKEN_DIRECTORY / 'shopee_tokens.json'

# Ensure token directory exists
TOKEN_DIRECTORY.mkdir(exist_ok=True)

if not all([PARTNER_ID, API_KEY, API_SECRET]):
    raise ValueError("SHOPEE_PARTNER_ID, SHOPEE_API_KEY, and SHOPEE_API_SECRET must be set in .env file")

def generate_shopee_signature(path, timestamp, access_token=None, shop_id=None):
    """
    Generate Shopee API signature using HMAC-SHA256
    
    Args:
        path (str): API endpoint path
        timestamp (int): Current timestamp in seconds
        access_token (str, optional): Access token for shop API calls
        shop_id (int or str, optional): Shop ID for shop API calls
        
    Returns:
        str: Generated signature
    """
    # Debug signature components
    print(f"DEBUG Signature Components:")
    print(f"- Path: {path}")
    print(f"- Timestamp: {timestamp}")
    print(f"- Partner ID: {PARTNER_ID}")
    if access_token:
        print(f"- Access Token: {access_token[:5]}...")
    if shop_id:
        print(f"- Shop ID: {shop_id}")
    
    # Ensure path has correct format with /api/v2/ prefix
    if not path.startswith('/'):
        path = '/' + path
    
    if not path.startswith('/api/v2/'):
        if path.startswith('/auth/') or path.startswith('/shop/'):
            path = f"/api/v2{path}"
        elif path.startswith('/api/'):
            path = path.replace('/api/', '/api/v2/')
    
    # Convert partner_id to int as required by Shopee
    partner_id = int(PARTNER_ID)
    
    # Get partner key (API SECRET, not API key)
    partner_key = API_SECRET.encode('utf-8')  # Must be bytes for hmac
    
    # Construct base string according to Shopee documentation
    # CRITICAL: Check exact format as required by Shopee
    if shop_id is not None and access_token is not None:
        # Shop API format
        base_string = f"{partner_id}{path}{timestamp}{access_token}{shop_id}"
    else:
        # Public API format (for authentication)
        base_string = f"{partner_id}{path}{timestamp}"
    
    print(f"- Base String: '{base_string}'")
    
    # Generate signature using HMAC-SHA256
    base_string_bytes = base_string.encode('utf-8')
    hmac_obj = hmac.new(partner_key, base_string_bytes, hashlib.sha256)
    signature = hmac_obj.hexdigest()  # Convert to lowercase hex string
    print(f"- Generated Signature: {signature}")
    
    return signature

def get_authorization_url(redirect_uri="https://oscitant-brody-pseudonationally.ngrok-free.dev"):
    """
    Generate authorization URL for Shopee OAuth
    
    Args:
        redirect_uri (str): Redirect URI after authorization
        
    Returns:
        str: Full authorization URL
    """
    path = "/api/v2/auth/token/get"
    timestamp = int(time.time())
    
    # Generate signature for Public API (no access token)
    signature = generate_shopee_signature(path, timestamp)
    
    # Prepare URL parameters
    # IMPORTANT: partner_id must be string in URL parameters according to Shopee docs
    params = {
        "partner_id": PARTNER_ID,       # Shopee expects this as string in URL params 
        "timestamp": timestamp,         # Integer timestamp
        "sign": signature.lower(),      # Lowercase hex signature
        "redirect": redirect_uri        # Redirect URI
    }
    
    print(f"Authorization URL Parameters:")
    for k, v in params.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    # Construct full URL
    base_url = BASE_URL.rstrip('/')
    auth_url = f"{base_url}{path}"
    
    # Build query string with URL encoding
    import urllib.parse
    query_parts = []
    for k, v in params.items():
        encoded_value = urllib.parse.quote(str(v))
        query_parts.append(f"{k}={encoded_value}")
    query_string = "&".join(query_parts)
    
    full_auth_url = f"{auth_url}?{query_string}"
    print(f"Authorization URL: {full_auth_url}")
    
    return full_auth_url

def exchange_code_for_token(code):
    """
    Exchange authorization code for access token
    
    Args:
        code (str): Authorization code from redirect
        
    Returns:
        dict: Token data including access_token, refresh_token, etc.
    """
    path = "/api/v2/auth/token/get"
    timestamp = int(time.time())
    
    # Generate signature for Public API (no access token yet)
    signature = generate_shopee_signature(path, timestamp)
    
    # Construct full URL
    base_url = BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    # URL parameters
    # IMPORTANT: partner_id must be string in URL params
    params = {
        "partner_id": PARTNER_ID,       # CRITICAL: Shopee needs this as string 
        "timestamp": timestamp,
        "sign": signature.lower()
    }
    
    # Request body
    # IMPORTANT: partner_id must be integer in request body
    data = {
        "code": code,
        "partner_id": int(PARTNER_ID),  # CRITICAL: Must be integer in JSON body
        "shop_id": 0                    # Required for token exchange
    }
    
    print(f"URL Parameters types:")
    for k, v in params.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    print(f"Request Body types:")  
    for k, v in data.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    print(f"Token Request URL: {url}")
    print(f"URL Parameters: {params}")
    print(f"Request Body: {data}")
    
    try:
        response = requests.post(url, params=params, json=data, timeout=10)
        
        print(f"Token Request Status Code: {response.status_code}")
        print(f"Token Request Headers: {dict(response.headers)}")
        
        try:
            json_response = response.json()
            print(f"Token Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and json_response.get('error') is None:
                # Extract the relevant token data
                token_data = {
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expire_in'),  # Shopee uses 'expire_in'
                    'timestamp': int(time.time()),  # Store current time
                    'shop_id': json_response.get('shop_id'),
                    'merchant_id': json_response.get('merchant_id')
                }
                
                # Save token data to file
                save_tokens_to_file(token_data)
                
                return {
                    'success': True,
                    **token_data
                }
            else:
                error_message = json_response.get('error', 'Unknown error')
                error_description = json_response.get('message', 'No description')
                
                return {
                    'success': False,
                    'error': error_message,
                    'error_description': error_description,
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text[:500]}...")
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'error_description': response.text[:500]
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {
            'success': False,
            'error': 'Request failed',
            'error_description': str(e)
        }

def refresh_access_token(refresh_token):
    """
    Refresh access token using refresh token
    
    Args:
        refresh_token (str): Refresh token
        
    Returns:
        dict: New token data
    """
    path = "/api/v2/auth/access_token/get"
    timestamp = int(time.time())
    
    # Generate signature for Public API (refreshing token)
    signature = generate_shopee_signature(path, timestamp)
    
    # Construct full URL
    base_url = BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    # URL parameters
    # IMPORTANT: partner_id must be string in URL params according to Shopee docs
    params = {
        "partner_id": PARTNER_ID,       # As string in URL parameters
        "timestamp": timestamp,
        "sign": signature.lower()
    }
    
    # Request body
    # IMPORTANT: partner_id must be integer in request body
    data = {
        "refresh_token": refresh_token,
        "partner_id": int(PARTNER_ID)   # CRITICAL: Must be integer in JSON body
    }
    
    print(f"URL Parameters types:")
    for k, v in params.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    print(f"Request Body types:")  
    for k, v in data.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    print(f"Refresh Token Request URL: {url}")
    print(f"URL Parameters: {params}")
    print(f"Request Body: {data}")
    
    try:
        response = requests.post(url, params=params, json=data, timeout=10)
        
        print(f"Refresh Token Status Code: {response.status_code}")
        print(f"Refresh Token Headers: {dict(response.headers)}")
        
        try:
            json_response = response.json()
            print(f"Refresh Token Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and json_response.get('error') is None:
                # Extract the relevant token data
                token_data = {
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expire_in'),  # Shopee uses 'expire_in'
                    'timestamp': int(time.time()),  # Store current time
                    'shop_id': json_response.get('shop_id'),
                    'merchant_id': json_response.get('merchant_id')
                }
                
                # Save token data to file
                save_tokens_to_file(token_data)
                
                return {
                    'success': True,
                    **token_data
                }
            else:
                error_message = json_response.get('error', 'Unknown error')
                error_description = json_response.get('message', 'No description')
                
                return {
                    'success': False,
                    'error': error_message,
                    'error_description': error_description,
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text[:500]}...")
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'error_description': response.text[:500]
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {
            'success': False,
            'error': 'Request failed',
            'error_description': str(e)
        }

def save_tokens_to_file(token_data):
    """
    Save token data to file
    
    Args:
        token_data (dict): Token data to save
    """
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f, indent=2)
        print(f"✅ Tokens saved to {TOKEN_FILE}")
        return True
    except Exception as e:
        print(f"❌ Error saving tokens: {e}")
        return False

def load_tokens_from_file():
    """
    Load token data from file
    
    Returns:
        dict: Token data or None if file doesn't exist
    """
    try:
        if TOKEN_FILE.exists():
            with open(TOKEN_FILE, 'r') as f:
                return json.load(f)
        else:
            print(f"❌ Token file not found: {TOKEN_FILE}")
            return None
    except Exception as e:
        print(f"❌ Error loading tokens: {e}")
        return None

def is_token_expired(token_data):
    """
    Check if token is expired
    
    Args:
        token_data (dict): Token data with timestamp and expires_in
        
    Returns:
        bool: True if token is expired, False otherwise
    """
    if not token_data or 'timestamp' not in token_data or 'expires_in' not in token_data:
        return True
    
    timestamp = token_data['timestamp']
    expires_in = token_data['expires_in']
    current_time = int(time.time())
    
    # Add some buffer (5 minutes) to ensure we refresh before expiry
    buffer_time = 300
    
    # Check if token is expired
    return current_time > (timestamp + expires_in - buffer_time)

def test_shopee_api_with_token():
    """
    Test Shopee API with access token
    """
    # Load token data
    token_data = load_tokens_from_file()
    if not token_data or 'access_token' not in token_data or 'shop_id' not in token_data:
        print("❌ No valid token data found")
        return False
    
    # Get shop info as a simple test
    path = "/api/v2/shop/get_shop_info"
    timestamp = int(time.time())
    access_token = token_data['access_token']
    shop_id = token_data['shop_id']
    
    # Generate signature for Shop API
    signature = generate_shopee_signature(path, timestamp, access_token, shop_id)
    
    # Construct full URL
    base_url = BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    # URL parameters
    # IMPORTANT: Both partner_id and shop_id must be string in URL parameters
    params = {
        "partner_id": PARTNER_ID,       # As string in URL parameters
        "timestamp": timestamp,
        "sign": signature.lower(),
        "access_token": access_token,
        "shop_id": str(shop_id)         # CRITICAL: Must be string in URL parameters
    }
    
    print(f"API Test URL Parameters types:")
    for k, v in params.items():
        print(f"- {k}: {v} (type: {type(v).__name__})")
    
    print(f"API Test URL: {url}")
    print(f"URL Parameters: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        print(f"API Test Status Code: {response.status_code}")
        print(f"API Test Headers: {dict(response.headers)}")
        
        try:
            json_response = response.json()
            print(f"API Test Response: {json.dumps(json_response, indent=2)}")
            
            return response.status_code == 200 and json_response.get('error') is None
            
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text[:500]}...")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return False

if __name__ == "__main__":
    print("Shopee API Test")
    print("===============")
    
    # Test authorization URL generation
    print("\n1. Testing authorization URL generation:")
    auth_url = get_authorization_url()
    print(f"Authorization URL successfully generated: {len(auth_url) > 0}")
    
    # If code is provided as command line argument, exchange it for a token
    if len(sys.argv) > 1 and sys.argv[1] == 'code' and len(sys.argv) > 2:
        code = sys.argv[2]
        print(f"\n2. Exchanging code for token: {code}")
        result = exchange_code_for_token(code)
        if result['success']:
            print("✅ Token exchange successful!")
        else:
            print(f"❌ Token exchange failed: {result.get('error')} - {result.get('error_description')}")
    
    # Test token refresh if we have a token
    token_data = load_tokens_from_file()
    if token_data and 'refresh_token' in token_data:
        print("\n3. Testing token refresh:")
        if is_token_expired(token_data):
            print("Token is expired, refreshing...")
            result = refresh_access_token(token_data['refresh_token'])
            if result['success']:
                print("✅ Token refresh successful!")
            else:
                print(f"❌ Token refresh failed: {result.get('error')} - {result.get('error_description')}")
        else:
            print("Token is still valid")
        
        print("\n4. Testing API with token:")
        success = test_shopee_api_with_token()
        if success:
            print("✅ API test successful!")
        else:
            print("❌ API test failed")
    else:
        print("\nNo token data available. Use the authorization URL to get a code, then run:")
        print(f"python {sys.argv[0]} code <your_code>")