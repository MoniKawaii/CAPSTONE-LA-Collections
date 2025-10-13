#!/usr/bin/env python3
"""
Shopee Deauthorization Script
This script revokes/cancels Shopee API access tokens
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

TOKEN_FILE = Path('tokens/shopee_tokens.json')

# ============================================================================
# SIGNATURE GENERATION
# ============================================================================

def generate_signature(path: str, timestamp: int, access_token: str = None, 
                      shop_id: int = None) -> str:
    """
    Generate HMAC-SHA256 signature for Shopee API requests
    
    Args:
        path: API endpoint path (e.g., '/auth/token/get')
        timestamp: Unix timestamp in seconds
        access_token: Optional access token for authenticated requests
        shop_id: Optional shop ID for shop-level APIs
    
    Returns:
        Lowercase hex signature string
    """
    # Remove /api/v2 prefix if present
    if path.startswith('/api/v2'):
        path = path[7:]
    
    # Remove leading slash
    path = path.lstrip('/')
    
    # Convert partner_id to int
    partner_id = int(SHOPEE_PARTNER_ID)
    
    # Build base string according to Shopee's specification
    if shop_id and access_token:
        # Shop API
        base_string = f"{partner_id}{path}{timestamp}{access_token}{shop_id}"
    elif access_token:
        # Public API with token
        base_string = f"{partner_id}{path}{timestamp}{access_token}"
    else:
        # Public API without token (for initial authorization)
        base_string = f"{partner_id}{path}{timestamp}"
    
    # Generate HMAC-SHA256 signature
    signature = hmac.new(
        SHOPEE_API_SECRET.encode('utf-8'),
        base_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    logger.debug(f"Base string: {base_string}")
    logger.debug(f"Signature: {signature}")
    
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

# ============================================================================
# DEAUTHORIZATION
# ============================================================================

def cancel_authorization(access_token: str, shop_id: int) -> bool:
    """
    Cancel/revoke the access token
    
    Args:
        access_token: Access token to revoke
        shop_id: Shop ID associated with the token
    
    Returns:
        True if successful, False otherwise
    """
    path = "/api/v2/auth/cancel_auth_partner"
    timestamp = int(time.time())
    
    # Generate signature
    signature = generate_signature(path, timestamp, access_token, shop_id)
    
    # Build request
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}{path}"
    
    params = {
        'partner_id': int(SHOPEE_PARTNER_ID),
        'timestamp': timestamp,
        'sign': signature,
        'shop_id': shop_id,
        'access_token': access_token
    }
    
    logger.info(f"Cancelling authorization for shop ID {shop_id}...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        result = response.json()
        
        # Check for API errors
        if 'error' in result and result['error']:
            logger.error(f"API Error: {result.get('message', 'Unknown error')}")
            return False
        
        logger.info("Authorization successfully cancelled")
        return True
    
    except Exception as e:
        logger.error(f"Failed to cancel authorization: {e}")
        return False

def main():
    """Main deauthorization flow"""
    logger.info("Starting Shopee API deauthorization...")
    
    tokens = load_tokens()
    
    if not tokens:
        logger.warning("No tokens found in %s. Nothing to deauthorize.", TOKEN_FILE)
        return
    
    access_token = tokens.get('access_token')
    shop_id = tokens.get('shop_id')
    
    if not access_token or not shop_id:
        logger.error("Missing 'access_token' or 'shop_id' in token file.")
        return
    
    if cancel_authorization(access_token, shop_id):
        logger.info("Deauthorization successful.")
        try:
            TOKEN_FILE.unlink(missing_ok=True)
            logger.info("Token file %s deleted.", TOKEN_FILE)
        except Exception as e:
            logger.error("Failed to delete token file: %s", e)
    else:
        logger.error("Deauthorization failed. Check logs for details.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)