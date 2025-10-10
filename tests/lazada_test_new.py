"""
Lazada API Integration Test

This file tests the Lazada API integration for the LA Collections project.
Uses the official Lazada SDK (lazop) for proper API communication.
"""
import time
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv

# Import Lazada SDK
try:
    from lazop_sdk import LazopClient, LazopRequest
except ImportError:
    print("lazop-sdk not found. Please install it with: pip install lazop-sdk")
    raise

# Load environment variables
load_dotenv()

# Lazada API credentials from environment
APP_KEY = os.getenv('LAZADA_APP_KEY')
APP_SECRET = os.getenv('LAZADA_APP_SECRET')

if not APP_KEY or not APP_SECRET:
    raise ValueError("LAZADA_APP_KEY and LAZADA_APP_SECRET must be set in .env file")

# Lazada API URLs - Philippines
LAZADA_API_BASE = "https://api.lazada.com.ph/rest"
LAZADA_AUTH_URL = "https://auth.lazada.com.ph/rest"

def get_lazop_client():
    """Get configured LazopClient instance"""
    return LazopClient(LAZADA_API_BASE, APP_KEY, APP_SECRET)

def get_access_token(auth_code: str) -> dict:
    """
    Exchange authorization code for access token using Lazada SDK
    Based on sample: client.execute(request) format
    
    Args:
        auth_code (str): Authorization code from OAuth callback
        
    Returns:
        dict: Token response data
    """
    try:
        # Create client and request using Lazada SDK (following sample format)
        client = get_lazop_client()
        request = LazopRequest('/auth/token/create')
        request.add_api_param('code', auth_code)
        # Note: uuid field is invalid according to sample, so we don't use it
        
        # Execute request
        response = client.execute(request)
        
        print(f"Response Type: {response.type}")
        print(f"Response Body: {response.body}")
        
        if response.type == 'nil' and response.body:
            # Success response
            return {
                'success': True,
                'access_token': response.body.get('access_token'),
                'refresh_token': response.body.get('refresh_token'),
                'expires_in': response.body.get('expires_in', 3600),
                'refresh_expires_in': response.body.get('refresh_expires_in'),
                'account_platform': response.body.get('account_platform'),
                'country_user_info': response.body.get('country_user_info'),
                'created_at': int(time.time())
            }
        else:
            # Error response
            return {
                'success': False,
                'error': response.body.get('message', 'Unknown error') if response.body else 'No response body',
                'code': response.body.get('code', 'Unknown') if response.body else 'Unknown',
                'type': response.type,
                'response': response.body
            }
            
    except Exception as e:
        print(f"Token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def refresh_access_token(refresh_token: str) -> dict:
    """
    Refresh access token using refresh token with Lazada SDK
    Based on sample: client.execute(request) format
    
    Args:
        refresh_token (str): Refresh token
        
    Returns:
        dict: New token response
    """
    try:
        # Create client and request using Lazada SDK (following sample format)
        client = get_lazop_client()
        request = LazopRequest('/auth/token/refresh')
        request.add_api_param('refresh_token', refresh_token)
        
        # Execute request
        response = client.execute(request)
        
        print(f"Response Type: {response.type}")
        print(f"Response Body: {response.body}")
        
        if response.type == 'nil' and response.body:
            # Success response
            return {
                'success': True,
                'access_token': response.body.get('access_token'),
                'refresh_token': response.body.get('refresh_token', refresh_token),  # Use old if not provided
                'expires_in': response.body.get('expires_in', 3600),
                'refresh_expires_in': response.body.get('refresh_expires_in'),
                'created_at': int(time.time())
            }
        else:
            # Error response
            return {
                'success': False,
                'error': response.body.get('message', 'Unknown error') if response.body else 'No response body',
                'code': response.body.get('code', 'Unknown') if response.body else 'Unknown',
                'type': response.type,
                'response': response.body
            }
            
    except Exception as e:
        print(f"Refresh token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def save_tokens_to_file(token_data: dict, filename: str = 'lazada_tokens.json') -> bool:
    """
    Save tokens to JSON file
    
    Args:
        token_data (dict): Token data to save
        filename (str): File name to save to
        
    Returns:
        bool: Success status
    """
    try:
        # Add timestamp if not present
        if 'created_at' not in token_data:
            token_data['created_at'] = int(time.time())
            
        with open(filename, 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print(f"✅ Tokens saved to {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save tokens: {e}")
        return False

def load_tokens_from_file(filename: str = 'lazada_tokens.json') -> dict:
    """
    Load tokens from JSON file
    
    Args:
        filename (str): File name to load from
        
    Returns:
        dict or None: Token data if found
    """
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                tokens = json.load(f)
            print(f"✅ Tokens loaded from {filename}")
            return tokens
        else:
            print(f"⚠️ Token file {filename} not found")
            return None
            
    except Exception as e:
        print(f"❌ Failed to load tokens: {e}")
        return None

def is_token_expired(token_data: dict) -> bool:
    """
    Check if access token is expired
    
    Args:
        token_data (dict): Token data containing created_at and expires_in
        
    Returns:
        bool: True if token is expired or expires soon
    """
    if not token_data:
        return True
    
    created_at = token_data.get('created_at', 0)
    expires_in = token_data.get('expires_in', 3600)
    
    # Handle different date formats
    if isinstance(created_at, str):
        try:
            # Try to parse as datetime string
            created_dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at = int(created_dt.timestamp())
        except ValueError:
            # If parsing fails, assume token is expired
            return True
    
    # Consider token expired if it expires in next 5 minutes (300 seconds buffer)
    expiry_time = created_at + expires_in - 300
    current_time = int(time.time())
    
    return current_time >= expiry_time

def get_authorization_url():
    """
    Generate authorization URL for getting auth code
    
    Returns:
        str: Authorization URL
    """
    # Lazada authorization URL
    auth_base_url = "https://auth.lazada.com/oauth/authorize"
    
    # Parameters for authorization
    auth_params = {
        "response_type": "code",
        "force_auth": "true",
        "redirect_uri": "https://dagmar-hittable-acceptingly.ngrok-free.dev/lazada/callback",
        "client_id": APP_KEY
    }
    
    # Create authorization URL
    auth_url = auth_base_url + "?" + urlencode(auth_params)
    return auth_url

def test_api_connection():
    """Test basic API connection"""
    try:
        client = get_lazop_client()
        print(f"✅ Lazada SDK client created successfully")
        print(f"   API Base URL: {LAZADA_API_BASE}")
        print(f"   App Key: {APP_KEY}")
        return True
    except Exception as e:
        print(f"❌ Failed to create Lazada SDK client: {e}")
        return False

if __name__ == "__main__":
    print("=== Lazada API Test ===")
    test_api_connection()