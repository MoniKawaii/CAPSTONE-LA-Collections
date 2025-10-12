"""
Configuration Module for LA Collections Analytics Platform

This module centralizes all configuration settings, credentials, API endpoints,
and token management for the LA Collections data     # Generate signature
    signature_path = "/auth/access_token/get"  # Path for signature generation (without /api/v2)
    api_path = "/api/v2/auth/access_token/get"  # Actual API path
    timestamp = int(time.time())
    signature = generate_shopee_signature(signature_path, timestamp)
    
    # Prepare request
    url = f"{SHOPEE_BASE_URL.rstrip('/')}{api_path}"tion platform.

It manages:
- Environment variable loading
- API keys and credentials
- Token management (retrieval, refresh, and validation)
- Base URLs and endpoints
- Column mapping standardization for ETL
"""
import os
import time
import json
import hashlib
import hmac
import logging
from pathlib import Path
from typing import Dict, Optional, Any, Union
from dotenv import load_dotenv

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ==========================================
# API CREDENTIALS AND BASE URLS
# ==========================================

# Platform identifiers
PLATFORM_SHOPEE = "shopee"
PLATFORM_LAZADA = "lazada"

# Shopee API Settings
SHOPEE_PARTNER_ID = os.getenv('SHOPEE_PARTNER_ID')
SHOPEE_API_KEY = os.getenv('SHOPEE_API_KEY')
SHOPEE_API_SECRET = os.getenv('SHOPEE_API_SECRET')
SHOPEE_BASE_URL = os.getenv('SHOPEE_BASE_URL', 'https://partner.test-stable.shopeemobile.com')
SHOPEE_ACCESS_TOKEN = os.getenv('SHOPEE_ACCESS_TOKEN')
SHOPEE_REFRESH_TOKEN = os.getenv('SHOPEE_REFRESH_TOKEN')

# Lazada API Settings
LAZADA_APP_KEY = os.getenv('LAZADA_APP_KEY')
LAZADA_APP_SECRET = os.getenv('LAZADA_APP_SECRET')
LAZADA_BASE_URL = os.getenv('LAZADA_BASE_URL', 'https://api.lazada.com/rest')
LAZADA_AUTH_URL = os.getenv('LAZADA_AUTH_URL', 'https://auth.lazada.com/rest')
LAZADA_ACCESS_TOKEN = os.getenv('LAZADA_ACCESS_TOKEN')
LAZADA_REFRESH_TOKEN = os.getenv('LAZADA_REFRESH_TOKEN')

# Token file paths
TOKEN_DIRECTORY = Path('tokens')
SHOPEE_TOKEN_FILE = TOKEN_DIRECTORY / 'shopee_tokens.json'
LAZADA_TOKEN_FILE = TOKEN_DIRECTORY / 'lazada_tokens.json'

# Ensure token directory exists
TOKEN_DIRECTORY.mkdir(exist_ok=True)

# ==========================================
# TOKEN MANAGEMENT - SHOPEE
# ==========================================

def load_shopee_tokens() -> Optional[Dict[str, Any]]:
    """
    Load Shopee tokens from file or environment variables.
    
    Returns:
        Optional[Dict[str, Any]]: Token data or None if no valid tokens found
    """
    # Try to load from file first
    try:
        if SHOPEE_TOKEN_FILE.exists():
            with open(SHOPEE_TOKEN_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Error loading Shopee tokens from file: {e}")
    
    # If file loading fails, try environment variables
    if SHOPEE_ACCESS_TOKEN and SHOPEE_REFRESH_TOKEN:
        return {
            "access_token": SHOPEE_ACCESS_TOKEN,
            "refresh_token": SHOPEE_REFRESH_TOKEN,
            # No timestamp info from env vars, so we can't check expiration
            "timestamp": 0,
            "expires_in": 0
        }
    
    return None

def save_shopee_tokens(token_data: Dict[str, Any]) -> bool:
    """
    Save Shopee tokens to file and update environment variables.
    
    Args:
        token_data: Token data including access_token and refresh_token
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Save to file
        with open(SHOPEE_TOKEN_FILE, 'w') as f:
            json.dump(token_data, f, indent=2)
        
        # Update environment variables in .env file
        update_env_tokens(
            access_token_key="SHOPEE_ACCESS_TOKEN",
            refresh_token_key="SHOPEE_REFRESH_TOKEN",
            access_token=token_data.get("access_token", ""),
            refresh_token=token_data.get("refresh_token", "")
        )
        
        return True
    except Exception as e:
        logger.error(f"Error saving Shopee tokens: {e}")
        return False

def is_shopee_token_expired(token_data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Check if Shopee access token is expired.
    
    Args:
        token_data: Token data (optional, will load from storage if not provided)
        
    Returns:
        bool: True if token is expired or not available, False otherwise
    """
    if token_data is None:
        token_data = load_shopee_tokens()
    
    if not token_data:
        return True
    
    # If we have timestamp and expires_in, check expiration with 5-minute buffer
    if "timestamp" in token_data and "expire_in" in token_data:
        current_time = int(time.time())
        expiry_time = token_data["timestamp"] + token_data["expire_in"] - 300
        return current_time >= expiry_time
    
    # If we can't determine expiration, assume it's valid
    return False

def generate_shopee_signature(path: str, timestamp: int, access_token: Optional[str] = None, shop_id: Optional[Union[int, str]] = None) -> str:
    """
    Generate Shopee API signature using HMAC-SHA256.
    
    This function supports both Public API and Shop API formats:
    
    Public API (used for authentication only):
    - Base string: {partner_id}{api_path}{timestamp}
    - Used when: access_token and shop_id are None
    - Mainly for: /api/v2/auth/* endpoints
    
    Shop API (used for all authenticated operations):
    - Base string: {partner_id}{api_path}{timestamp}{access_token}{shop_id}
    - Used when: access_token and shop_id are provided
    - Preferred for all operations after authentication
    
    Args:
        path: API endpoint path
        timestamp: Current timestamp in seconds
        access_token: Optional access token (required for Shop API)
        shop_id: Optional shop ID (required for Shop API)
        
    Returns:
        str: Generated signature
    """
    # Prepare the API path for signature calculation
    # According to Shopee's documentation examples, we need to include the /api/v2/ prefix in the base string
    # BUT we need to be careful with the format - Shopee's example shows the path needs to be exactly as shown in their docs
    
    # Ensure we're using the format from Shopee's documentation example
    # Shopee example shows: partner_id/api/v2/shop/get_shop_info{timestamp}{access_token}{shop_id}
    # This means we need the forward slash after partner_id
    
    # Normalize the path - KEEPING the leading slash which is required by Shopee
    if not path.startswith('/'):
        path = '/' + path
        
    # Make sure we have the proper /api/v2 prefix
    if not path.startswith('/api/v2/'):
        if path.startswith('/auth/') or path.startswith('/shop/'):
            path = f"/api/v2{path}"
        elif path.startswith('/api/'):
            # Handle case where it's /api/ but missing the v2
            path = path.replace('/api/', '/api/v2/')
            
    # Final path to use in the base string - exactly matching Shopee's format
    api_path = path
    
    # Convert partner_id to int as required by Shopee
    partner_id = int(SHOPEE_PARTNER_ID)
    
    # Get partner key (API SECRET, not API key)
    partner_key = SHOPEE_API_SECRET.encode('utf-8')  # Must be bytes for hmac
    
    # Construct base string for signature according to Shopee's documentation example
    # Default to Public API format if shop_id or access_token is None
    if shop_id is not None and access_token is not None:
        # Shop API format - what we primarily use
        logger.info("Generating signature for Shop API")
        # Format exactly as shown in Shopee's example:
        # 2001887/api/v2/shop/get_shop_info165571443159777174636562737266615546704c6d14701711
        base_string = f"{partner_id}{api_path}{timestamp}{access_token}{shop_id}"
    else:
        # For authorization endpoints (auth/token/get) - exactly as per Shopee documentation
        logger.info("Generating signature for authorization endpoint")
        # Format exactly as shown in Shopee's examples
        base_string = f"{partner_id}{api_path}{timestamp}"
    
    # Log details for debugging
    logger.info(f"SHOPEE SIGNATURE DEBUG ===================")
    logger.info(f"Original Path: '{path}'")
    logger.info(f"API Path for Signature: '{api_path}'")
    logger.info(f"Partner ID: {partner_id} (type: {type(partner_id).__name__})")
    logger.info(f"Timestamp: {timestamp} (type: {type(timestamp).__name__})")
    if access_token:
        logger.info(f"Access Token: {access_token[:5]}...{access_token[-5:] if len(access_token)>10 else ''}")
    if shop_id:
        logger.info(f"Shop ID: {shop_id} (type: {type(shop_id).__name__})")
    logger.info(f"Base String: '{base_string}'")
    logger.info(f"Partner Key (Secret) first 5 chars: {SHOPEE_API_SECRET[:5]}...")
    
    # Generate signature using HMAC-SHA256 exactly as shown in Shopee documentation
    base_string_bytes = base_string.encode('utf-8')
    
    # Calculate the signature using HMAC-SHA256
    hmac_obj = hmac.new(partner_key, base_string_bytes, hashlib.sha256)
    signature_bytes = hmac_obj.digest()
    signature = hmac_obj.hexdigest()  # Convert to lowercase hex string
    
    # Enhanced logging for debugging
    logger.info(f"Base String bytes: {base_string_bytes[:20]}... (length: {len(base_string_bytes)})")
    logger.info(f"Raw Signature bytes (first 5): {signature_bytes[:5]}")
    logger.info(f"Final Signature (hex): {signature}")
    logger.info(f"Signature Generation Example:")
    logger.info(f"HMAC-SHA256('{base_string}', partner_key) = {signature}")
    logger.info("=========================================")
    
    # Verify the signature matches Shopee's expected format (lowercase hex)
    if not all(c in '0123456789abcdef' for c in signature):
        logger.warning("WARNING: Signature contains non-hex characters!")
    
    return signature

def refresh_shopee_token() -> Optional[Dict[str, Any]]:
    """
    Refresh Shopee access token using refresh token.
    
    Returns:
        Optional[Dict[str, Any]]: New token data or None if refresh fails
    """
    # Get current refresh token
    token_data = load_shopee_tokens()
    if not token_data or "refresh_token" not in token_data:
        logger.error("No refresh token available for Shopee")
        return None
    
    refresh_token = token_data["refresh_token"]
    
    # Define path and generate timestamp
    path = "/auth/access_token/get"
    timestamp = int(time.time())

    logger.info(f"Using timestamp for refresh: {timestamp}")
    # Log timestamp in human-readable format
    from datetime import datetime
    logger.info(f"Timestamp in local time: {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # For token refresh, use our signature generation function
    signature = generate_shopee_signature(path, timestamp)
    
    # Construct full URL
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}/api/v2{path}"
    
    # URL parameters for Public API
    params = {
        "partner_id": str(SHOPEE_PARTNER_ID),  # As string in URL parameters
        "timestamp": timestamp,                # Integer timestamp
        "sign": signature.lower()              # Ensure lowercase hexadecimal
    }
    
    # Request body format
    data = {
        "refresh_token": refresh_token,
        "partner_id": int(SHOPEE_PARTNER_ID)  # Integer in body
    }
    
    # Verbose logging for debugging
    logger.info(f"TOKEN REFRESH DEBUG =======================")
    logger.info(f"URL: {url}")
    logger.info(f"URL Parameters:")
    for k, v in params.items():
        logger.info(f"  - {k}: {v} (type={type(v).__name__})")
    logger.info(f"Request Body: {data}")
    logger.info(f"============================================")
    
    try:
        # Send the request with both URL parameters and JSON body
        response = requests.post(url, params=params, json=data)
        
        # Log the complete response for debugging
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response content: {response.text[:1000]}")  # First 1000 chars
        
        response.raise_for_status()
        result = response.json()
        
        if "error" in result and result["error"] != 0:
            logger.error(f"API error: {result}")
            return None
        
        # Add timestamp for expiry tracking
        result["timestamp"] = int(time.time())
        logger.info("Token refresh successful")
        
        # Save the new tokens
        save_shopee_tokens(result)
        
        return result
    except Exception as e:
        logger.error(f"Error refreshing token: {e}")
        return None
        response.raise_for_status()
        result = response.json()
        
        if "error" in result and result["error"] != 0:
            logger.error(f"Shopee token refresh error: {result.get('message', 'Unknown error')}")
            return None
        
        # Add timestamp for expiration tracking
        result["timestamp"] = int(time.time())
        
        # Save new tokens
        save_shopee_tokens(result)
        
        return result
    except Exception as e:
        logger.error(f"Failed to refresh Shopee token: {e}")
        return None

def get_shopee_access_token() -> Optional[str]:
    """
    Get valid Shopee access token, refreshing if necessary.
    
    Returns:
        Optional[str]: Valid access token or None if unavailable
    """
    # Load tokens
    token_data = load_shopee_tokens()
    
    # Check if token is expired
    if token_data and not is_shopee_token_expired(token_data):
        return token_data.get("access_token")
    
    # Token expired or not available, try to refresh
    new_token_data = refresh_shopee_token()
    if new_token_data:
        return new_token_data.get("access_token")
    
    logger.error("Failed to get valid Shopee access token")
    return None

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def check_shopee_server_time():
    """
    Check the server time of Shopee API to detect any time differences.
    
    Returns:
        dict: Contains local_time, server_time, and time_difference in seconds
    """
    from datetime import datetime
    import requests
    
    # Get local timestamp
    local_timestamp = int(time.time())
    local_time = datetime.fromtimestamp(local_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # Make a request to Shopee API
    try:
        # Use a simple public API endpoint
        path = "/public/get_shops_by_partner"  # Path for signature generation (without /api/v2)
        api_path = "/api/v2/public/get_shops_by_partner"  # Full API path for the request
        timestamp = local_timestamp
        signature = generate_shopee_signature(path, timestamp)
        
        # Construct URL
        base_url = SHOPEE_BASE_URL.rstrip('/')
        url = f"{base_url}{api_path}"
        
        # Parameters
        params = {
            "partner_id": SHOPEE_PARTNER_ID,
            "timestamp": timestamp,
            "sign": signature
        }
        
        # Send request
        response = requests.get(url, params=params)
        
        # Get server time from response headers
        server_time_str = response.headers.get('date')
        if server_time_str:
            from email.utils import parsedate_to_datetime
            server_datetime = parsedate_to_datetime(server_time_str)
            server_timestamp = int(server_datetime.timestamp())
            server_time = server_datetime.strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculate time difference
            time_diff = local_timestamp - server_timestamp
            
            return {
                "local_time": local_time,
                "server_time": server_time,
                "time_difference": time_diff
            }
    except Exception as e:
        logger.error(f"Error checking server time: {e}")
    
    return {
        "local_time": local_time,
        "server_time": "Unknown",
        "time_difference": 0
    }

def update_env_tokens(access_token_key: str, refresh_token_key: str, access_token: str, refresh_token: str) -> bool:
    """
    Update token values in the .env file
    
    Args:
        access_token_key: Environment variable name for access token
        refresh_token_key: Environment variable name for refresh token
        access_token: Access token value
        refresh_token: Refresh token value
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        env_path = Path('.env')
        if not env_path.exists():
            logger.error("No .env file found")
            return False
            
        # Read the current .env file
        with open(env_path, 'r') as f:
            env_content = f.read()
            
        # Update tokens
        access_pattern = f"{access_token_key}="
        refresh_pattern = f"{refresh_token_key}="
        
        # Update or add access token
        if access_pattern in env_content:
            # Replace existing value
            lines = env_content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith(access_pattern):
                    lines[i] = f"{access_pattern}{access_token}"
                    break
            env_content = '\n'.join(lines)
        else:
            # Add new value
            env_content += f"\n{access_pattern}{access_token}"
            
        # Update or add refresh token
        if refresh_pattern in env_content:
            # Replace existing value
            lines = env_content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith(refresh_pattern):
                    lines[i] = f"{refresh_pattern}{refresh_token}"
                    break
            env_content = '\n'.join(lines)
        else:
            # Add new value
            env_content += f"\n{refresh_pattern}{refresh_token}"
            
        # Write back to file
        with open(env_path, 'w') as f:
            f.write(env_content)
            
        # Also update os environment variables
        os.environ[access_token_key] = access_token
        os.environ[refresh_token_key] = refresh_token
        
        return True
    except Exception as e:
        logger.error(f"Failed to update environment tokens: {e}")
        return False

# ==========================================
# COLUMN MAPPINGS FOR STANDARDIZATION
# ==========================================

# Column mappings for standardizing data across platforms
COLUMN_MAPPINGS = {
    "order": {
        PLATFORM_SHOPEE: {
            "order_id": "order_sn",
            "customer_id": "buyer_user_id", 
            "order_status": "order_status",
            "payment_method": "payment_method",
            "shipping_carrier": "shipping_carrier",
            "created_time": "create_time",
            "paid_time": "pay_time",
            "completed_time": "complete_time"
        },
        PLATFORM_LAZADA: {
            "order_id": "order_id",
            "customer_id": "customer_id",
            "order_status": "status",
            "payment_method": "payment_method",
            "shipping_carrier": "shipping_provider",
            "created_time": "created_at",
            "paid_time": "paid_time",
            "completed_time": "delivered_time"
        }
    },
    "product": {
        PLATFORM_SHOPEE: {
            "product_id": "item_id",
            "name": "item_name",
            "category": "category_path",
            "price": "price",
            "stock": "stock",
            "created_time": "create_time",
            "modified_time": "update_time",
            "status": "item_status"
        },
        PLATFORM_LAZADA: {
            "product_id": "item_id",
            "name": "name",
            "category": "primary_category",
            "price": "price",
            "stock": "quantity",
            "created_time": "created_time",
            "modified_time": "updated_time",
            "status": "status"
        }
    }
}
