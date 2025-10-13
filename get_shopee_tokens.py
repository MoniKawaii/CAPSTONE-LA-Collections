"""
Shopee OAuth Token Management Script
This script handles the complete OAuth flow for Shopee API integration
"""

import os
import sys
import time
import hmac
import hashlib
import json
import logging
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

SHOPEE_PARTNER_ID = os.getenv('SHOPEE_PARTNER_ID')
SHOPEE_API_SECRET = os.getenv('SHOPEE_API_SECRET')
SHOPEE_BASE_URL = os.getenv('SHOPEE_BASE_URL', 'https://partner.test-stable.shopeemobile.com')

# Validate credentials
if not SHOPEE_PARTNER_ID or not SHOPEE_API_SECRET:
    raise ValueError(
        "Missing Shopee credentials! Please set SHOPEE_PARTNER_ID and "
        "SHOPEE_API_SECRET in your .env file"
    )

TOKEN_FILE = Path('shopee_tokens.json')

# ============================================================================
# SIGNATURE GENERATION
# ============================================================================

def generate_signature(path: str, timestamp: int, partner_key: str) -> str:
    """
    Generates the HMAC-SHA256 signature for the Public API call.
    
    The base string format for public API is: partner_id + path + timestamp
    """
    partner_id_str = str(SHOPEE_PARTNER_ID)
    timestamp_str = str(timestamp)
    
    # Concatenate the elements without any separators (crucial!)
    base_string = f"{partner_id_str}{path}{timestamp_str}"
    
    # Calculate the HMAC-SHA256 signature
    signature = hmac.new(
        key=partner_key.encode('utf-8'),
        msg=base_string.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()
    
    return signature

# ============================================================================
# TOKEN MANAGEMENT
# ============================================================================

def load_tokens() -> dict:
    """Load tokens from file"""
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_tokens(token_data: dict) -> bool:
    """Save tokens to file"""
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f, indent=2)
        logger.info(f"Tokens saved to {TOKEN_FILE}")
        return True
    except Exception as e:
        logger.error(f"Failed to save tokens: {e}")
        return False

# ============================================================================
# OAUTH FLOW
# ============================================================================

import urllib.parse

def get_authorization_url(redirect_uri: str = "https://oscitant-brody-pseudonationally.ngrok-free.dev") -> str:
    """
    Generate the complete authorization URL for the seller to grant access.

    Args:
        redirect_uri: The URL Shopee will redirect to after authorization.

    Returns:
        The complete, signed authorization URL.
    """
    # 🌟 CORRECT PATH for the authorization link
    path = "/api/v2/shop/auth_partner"
    timestamp = int(time.time())

    # Generate signature using the correct base string logic
    signature = generate_signature(path, timestamp, SHOPEE_API_SECRET)

    # Build URL with parameters
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}{path}"

    params = {
        'partner_id': int(SHOPEE_PARTNER_ID),
        'timestamp': timestamp,
        'sign': signature,
        # The redirect URL must be URL-encoded
        'redirect': redirect_uri
    }

    # Build the final query string
    # We use urllib.parse.urlencode for safe URL construction
    query_string = urllib.parse.urlencode(params)

    full_url = f"{url}?{query_string}"

    print(f"✅ Authorization URL Generated (Timestamp: {timestamp})")
    print(f"🔗 URL: {full_url}")

    return full_url

def exchange_code_for_token(auth_code: str) -> dict:
    """
    Exchange authorization code for access token
    
    Args:
        auth_code: Authorization code from OAuth redirect
    
    Returns:
        Dictionary containing tokens or None if failed
    """
    path = "/api/v2/auth/token/get"
    timestamp = int(time.time())
    
    # Generate signature
    signature = generate_signature(path, timestamp, SHOPEE_API_SECRET)
    
    # Build request
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    params = {
        'partner_id': int(SHOPEE_PARTNER_ID),
        'timestamp': timestamp,
        'sign': signature
    }
    
    data = {
        'code': auth_code,
        'partner_id': int(SHOPEE_PARTNER_ID)
    }
    
    logger.info("Exchanging authorization code for tokens...")
    
    try:
        response = requests.post(url, params=params, json=data)
        response.raise_for_status()
        
        result = response.json()
        
        # Check for API errors
        if 'error' in result and result['error']:
            logger.error(f"API Error: {result.get('message', 'Unknown error')}")
            return None
        
        # Add timestamp for tracking
        result['obtained_at'] = int(time.time())
        
        logger.info("✅ Successfully obtained tokens")
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None

def refresh_access_token(refresh_token: str, shop_id: int) -> dict:
    """
    Refresh access token using refresh token
    
    Args:
        refresh_token: Current refresh token
        shop_id: Shop ID
    
    Returns:
        Dictionary containing new tokens or None if failed
    """
    path = "/api/v2/auth/access_token/get"
    timestamp = int(time.time())
    
    # Generate signature
    signature = generate_signature(path, timestamp, SHOPEE_API_SECRET)
    
    # Build request
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    params = {
        'partner_id': int(SHOPEE_PARTNER_ID),
        'timestamp': timestamp,
        'sign': signature
    }
    
    data = {
        'refresh_token': refresh_token,
        'partner_id': int(SHOPEE_PARTNER_ID),
        'shop_id': shop_id
    }
    
    logger.info("Refreshing access token...")
    
    try:
        response = requests.post(url, params=params, json=data)
        response.raise_for_status()
        
        result = response.json()
        
        if 'error' in result and result['error']:
            logger.error(f"API Error: {result.get('message', 'Unknown error')}")
            return None
        
        result['obtained_at'] = int(time.time())
        
        logger.info("✅ Successfully refreshed token")
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None

# ============================================================================
# MAIN INTERACTIVE FLOW
# ============================================================================

def main():
    """Main interactive token management flow"""
    
    print("\n" + "="*60)
    print("   SHOPEE API TOKEN MANAGEMENT")
    print("="*60)
    
    print(f"\nEnvironment: {SHOPEE_BASE_URL}")
    print(f"Partner ID: {SHOPEE_PARTNER_ID}")
    
    # Check for existing tokens
    existing_tokens = load_tokens()
    if existing_tokens and 'access_token' in existing_tokens:
        print(f"\n⚠️  Existing tokens found (obtained: {datetime.fromtimestamp(existing_tokens.get('obtained_at', 0)).strftime('%Y-%m-%d %H:%M:%S')})")
        choice = input("Do you want to refresh existing tokens? (y/n): ").lower()
        
        if choice == 'y' and 'refresh_token' in existing_tokens and 'shop_id' in existing_tokens:
            new_tokens = refresh_access_token(
                existing_tokens['refresh_token'],
                existing_tokens['shop_id']
            )
            
            if new_tokens:
                # Preserve shop_id
                new_tokens['shop_id'] = existing_tokens['shop_id']
                save_tokens(new_tokens)
                print("\n✅ Tokens refreshed successfully!")
                return
            else:
                print("\n❌ Token refresh failed. Proceeding with new authorization...")
    
    # Start OAuth flow
    print("\n" + "-"*60)
    print("STEP 1: AUTHORIZATION")
    print("-"*60)
    
    redirect_uri = input("\nEnter redirect URI (press Enter for 'https://example.com'): ").strip()
    if not redirect_uri:
        redirect_uri = "https://example.com"
    
    auth_url = get_authorization_url(redirect_uri)
    
    print("\n📋 Please visit this URL in your browser:\n")
    print(f"   {auth_url}\n")
    print("After authorization, you'll be redirected to:")
    print(f"   {redirect_uri}?code=AUTHORIZATION_CODE&shop_id=SHOP_ID\n")
    
    # Get authorization code
    print("-"*60)
    print("STEP 2: AUTHORIZATION CODE")
    print("-"*60)
    
    auth_code = input("\nEnter the authorization code from the redirect URL: ").strip()
    
    if not auth_code or len(auth_code) < 10:
        print("\n❌ Invalid authorization code!")
        print("The code should be a long string from the 'code' parameter in the redirect URL.")
        return
    
    shop_id = input("Enter the shop_id from the redirect URL: ").strip()
    
    if not shop_id:
        print("\n❌ Shop ID is required!")
        return
    
    # Exchange code for tokens
    print("\n" + "-"*60)
    print("STEP 3: TOKEN EXCHANGE")
    print("-"*60 + "\n")
    
    token_data = exchange_code_for_token(auth_code)
    
    if not token_data:
        print("\n❌ Failed to obtain tokens!")
        print("Please check the error messages above and try again.")
        return
    
    # Add shop_id to token data
    token_data['shop_id'] = int(shop_id)
    
    # Save tokens
    if save_tokens(token_data):
        print("\n" + "="*60)
        print("   SUCCESS!")
        print("="*60)
        print(f"\n✅ Access Token: {token_data['access_token'][:20]}...")
        print(f"✅ Refresh Token: {token_data['refresh_token'][:20]}...")
        print(f"✅ Shop ID: {token_data['shop_id']}")
        print(f"✅ Expires in: {token_data.get('expire_in', 0)} seconds")
        print(f"\n📁 Tokens saved to: {TOKEN_FILE}")
        print("\n🎉 Your Shopee API integration is ready to use!")
    else:
        print("\n⚠️  Tokens obtained but failed to save to file.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)