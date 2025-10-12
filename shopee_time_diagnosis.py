"""
Shopee API Timestamp Diagnosis Tool

This script helps diagnose timestamp-related issues with the Shopee API.
It checks:
1. Local system time
2. Shopee server time
3. Time difference between the two
4. Tests timestamp validation with the API

Usage:
    python shopee_time_diagnosis.py
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
import requests

# Add the project root to the Python path so we can import app modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from app.config import (
        SHOPEE_BASE_URL, 
        SHOPEE_PARTNER_ID, 
        SHOPEE_API_KEY,
        generate_shopee_signature,
        check_shopee_server_time
    )
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    print("Make sure you're running this script from the project root directory.")
    sys.exit(1)

def print_section(title):
    """Print a section header for better readability"""
    print("\n" + "="*60)
    print(f"🔍 {title}")
    print("="*60)

def check_system_time():
    """Check and display system time information"""
    print_section("System Time Information")
    
    # Get current system time
    current_time = time.time()
    current_datetime = datetime.fromtimestamp(current_time)
    
    print(f"Current timestamp: {int(current_time)}")
    print(f"Current time (local): {current_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Check if time module and datetime module give consistent results
    time_timestamp = int(time.time())
    datetime_timestamp = int(datetime.now().timestamp())
    
    print(f"\nTime module timestamp: {time_timestamp}")
    print(f"Datetime module timestamp: {datetime_timestamp}")
    
    if abs(time_timestamp - datetime_timestamp) > 1:
        print("❌ Warning: time.time() and datetime.now() give inconsistent results")
    else:
        print("✅ time.time() and datetime.now() are consistent")

def check_time_difference():
    """Check time difference between local system and Shopee server"""
    print_section("Shopee Server Time Check")
    
    time_info = check_shopee_server_time()
    
    print(f"Local time: {time_info['local_time']}")
    print(f"Shopee server time: {time_info['server_time']}")
    
    time_diff = time_info['time_difference']
    print(f"\nTime difference: {time_diff} seconds")
    
    if abs(time_diff) > 300:  # More than 5 minutes
        print(f"❌ Warning: Large time difference detected ({time_diff} seconds)")
        print("   This could cause timestamp validation failures")
    elif abs(time_diff) > 60:  # More than 1 minute
        print(f"⚠️ Moderate time difference detected ({time_diff} seconds)")
        print("   This might cause occasional timestamp validation issues")
    else:
        print(f"✅ Time difference is acceptable ({time_diff} seconds)")

def test_timestamp_validations():
    """Test various timestamp offsets to see which ones are accepted"""
    print_section("Timestamp Validation Test")
    
    # Define path for a simple API call
    path = "/public/get_shops_by_partner"  # Path without /api/v2 prefix for signature
    api_path = "/api/v2/public/get_shops_by_partner"  # Full API path for the request
    base_url = SHOPEE_BASE_URL.rstrip('/')
    url = f"{base_url}{api_path}"
    
    # Test various offsets
    offsets = [-120, -60, -30, 0, 30, 60, 120]
    results = []
    
    current_time = int(time.time())
    print(f"Current time: {current_time} ({datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')})")
    
    for offset in offsets:
        timestamp = current_time + offset
        
        # Generate signature
        signature = generate_shopee_signature(path, timestamp)
        
        # Parameters
        params = {
            "partner_id": SHOPEE_PARTNER_ID,
            "timestamp": timestamp,
            "sign": signature
        }
        
        try:
            logger.info(f"Testing with offset {offset} seconds...")
            response = requests.get(url, params=params)
            status_code = response.status_code
            
            try:
                response_data = response.json()
                error_code = response_data.get("error", "none")
                error_msg = response_data.get("message", "none")
            except:
                error_code = "parse_error"
                error_msg = "Could not parse JSON response"
            
            results.append({
                "offset": offset,
                "timestamp": timestamp,
                "human_time": datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                "status_code": status_code,
                "error_code": error_code,
                "error_msg": error_msg,
                "success": status_code == 200 and error_code != "error_param"
            })
            
            # Add a small delay to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error testing offset {offset}: {e}")
            results.append({
                "offset": offset,
                "timestamp": timestamp,
                "human_time": datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                "status_code": 0,
                "error_code": "exception",
                "error_msg": str(e),
                "success": False
            })
    
    # Display results
    print("\nTimestamp Validation Results:")
    print("----------------------------")
    print(f"{'Offset':>8} | {'Timestamp':>10} | {'Time':>19} | {'Status':>6} | {'Result':>10} | Message")
    print("-" * 90)
    
    for result in results:
        success_marker = "✅" if result["success"] else "❌"
        print(f"{result['offset']:>+8} | {result['timestamp']:>10} | {result['human_time']} | {result['status_code']:>6} | {success_marker} {result['error_code']:<8} | {result['error_msg']}")
    
    # Determine the best offset
    valid_results = [r for r in results if r["success"]]
    if valid_results:
        best_offsets = [r["offset"] for r in valid_results]
        avg_offset = sum(best_offsets) / len(best_offsets)
        
        print("\n✅ Found working timestamp offsets!")
        print(f"Recommended offset: {int(avg_offset)} seconds")
        print(f"Add this offset to your timestamps: timestamp = int(time.time()) {'+' if avg_offset>=0 else ''}{int(avg_offset)}")
    else:
        print("\n❌ No working timestamp offsets found.")
        print("Try larger negative offsets or check for other API issues.")

def main():
    """Main function to run diagnostics"""
    print("\n🔍 Shopee API Timestamp Diagnosis Tool")
    
    # Run all checks
    check_system_time()
    check_time_difference()
    test_timestamp_validations()
    
    print("\n" + "="*60)
    print("💡 Recommendations:")
    print("="*60)
    print("1. If you found a working offset, update your code to use it")
    print("2. Consider syncing your system clock with an NTP server")
    print("3. Add the offset to all timestamp generations in your code")
    print("   Example: timestamp = int(time.time()) - 30  # 30 seconds offset")
    print("="*60)

if __name__ == "__main__":
    main()