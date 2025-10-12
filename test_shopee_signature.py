#!/usr/bin/env python3
"""
Shopee Signature Test Script

This script tests the signature generation process using a known example from Shopee's documentation.
It's helpful for verifying that our signature generation matches Shopee's expectations.
"""

import os
import sys
import time
import hmac
import hashlib
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the project root to the Python path so we can import app modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.config import (
    SHOPEE_PARTNER_ID,
    SHOPEE_API_SECRET,
    generate_shopee_signature
)

def test_shopee_signature():
    """
    Test the signature generation process using an example that follows Shopee's documentation.
    
    The example from Shopee's documentation:
    - Partner ID: 2001887
    - API path: /api/v2/shop/get_shop_info
    - Timestamp: 1655714431
    - Access Token: 59777174636562737266615546704c6d
    - Shop ID: 14701711
    - Base String: 2001887/api/v2/shop/get_shop_info165571443159777174636562737266615546704c6d14701711
    """
    # Example data from Shopee's documentation
    # Using their exact values to precisely match their example
    test_partner_id = 2001887  # Using the example partner ID from Shopee
    test_path = "/api/v2/shop/get_shop_info"
    test_timestamp = 1655714431
    test_access_token = "59777174636562737266615546704c6d"
    test_shop_id = 14701711
    
    # Expected base string from Shopee's documentation
    # We'll calculate this manually to see what our function produces
    expected_base_string = f"{test_partner_id}{test_path}{test_timestamp}{test_access_token}{test_shop_id}"
    
    # Also check Shopee's exact example string - with the exact format they showed
    shopee_example_base_string = f"2001887/api/v2/shop/get_shop_info165571443159777174636562737266615546704c6d14701711"
    
    # Calculate signature using our function
    actual_signature = generate_shopee_signature(
        path=test_path,
        timestamp=test_timestamp,
        access_token=test_access_token,
        shop_id=test_shop_id
    )
    
    # Calculate signature manually for comparison
    partner_key = SHOPEE_API_SECRET.encode('utf-8')
    base_string_bytes = expected_base_string.encode('utf-8')
    manual_signature = hmac.new(partner_key, base_string_bytes, hashlib.sha256).hexdigest()
    
    logger.info(f"TEST PARAMETERS:")
    logger.info(f"Path: {test_path}")
    logger.info(f"Timestamp: {test_timestamp}")
    logger.info(f"Access Token: {test_access_token[:5]}...")
    logger.info(f"Shop ID: {test_shop_id}")
    logger.info(f"Our base string: {expected_base_string}")
    logger.info(f"Shopee example base string: {shopee_example_base_string}")
    
    # Calculate signature based on Shopee's exact example string
    shopee_example_bytes = shopee_example_base_string.encode('utf-8')
    shopee_signature = hmac.new(SHOPEE_API_SECRET.encode('utf-8'), shopee_example_bytes, hashlib.sha256).hexdigest()
    
    logger.info(f"\nSignature from our function: {actual_signature}")
    logger.info(f"Signature calculated manually: {manual_signature}")
    logger.info(f"Signature using Shopee's exact example: {shopee_signature}")
    
    if actual_signature == manual_signature:
        logger.info("✅ SUCCESS: Signatures match!")
    else:
        logger.error("❌ ERROR: Signatures do not match!")

    # Now test with a real-world authorization scenario (no access token or shop ID)
    auth_path = "/api/v2/auth/token/get"
    current_timestamp = int(time.time())
    
    auth_signature = generate_shopee_signature(
        path=auth_path,
        timestamp=current_timestamp
    )
    
    logger.info(f"\nTESTING AUTHORIZATION ENDPOINT SIGNATURE:")
    logger.info(f"Authorization Path: {auth_path}")
    logger.info(f"Current Timestamp: {current_timestamp}")
    logger.info(f"Generated Signature: {auth_signature}")

if __name__ == "__main__":
    logger.info("SHOPEE SIGNATURE TEST")
    logger.info("=====================")
    test_shopee_signature()
    logger.info("=====================")
    logger.info("Test complete!")