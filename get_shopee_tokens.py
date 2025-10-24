"""
Shopee Token Management with Environment Variables

This script handles Shopee Open API v2.0 OAuth tokens securely using 
environment variables and a JSON file for persistence.
"""

import os
import time
import json
import requests
import hmac
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Shopee API Configuration ---
SHOPEE_PARTNER_ID = int(os.getenv("SHOPEE_PARTNER_ID"))
SHOPEE_PARTNER_KEY = os.getenv("SHOPEE_PARTNER_KEY")
SHOPEE_REDIRECT_URL = os.getenv("SHOPEE_REDIRECT_URL")

# Use sandbox or production URLs based on an environment variable
# Changed to default to PRODUCTION (live) environment
IS_SANDBOX = os.getenv("SHOPEE_API_ENV", "production").lower() == "sandbox"
BASE_URL = "https://partner.test-stable.shopeemobile.com" if IS_SANDBOX else "https://partner.shopeemobile.com"

# API Paths
PATH_GET_AUTH_URL = "/api/v2/shop/auth_partner"
PATH_GET_TOKEN = "/api/v2/auth/token/get"
PATH_REFRESH_TOKEN = "/api/v2/auth/access_token/get"

TOKEN_FILE = "shopee_tokens.json"

# --- Core API Functions ---

def generate_signature(path, timestamp, access_token=None, shop_id=None, body=None):
    """
    Generates the required HMAC-SHA256 signature for Shopee API calls.
    The base string composition depends on the type of API call.
    """
    base_string = f"{SHOPEE_PARTNER_ID}{path}{timestamp}"

    # Authenticated calls require access_token and shop_id in the base string
    if access_token and shop_id:
        base_string += f"{access_token}{shop_id}"
    
    # For POST requests to get/refresh tokens, the body is part of the signature
    if body:
        base_string += json.dumps(body)

    signature = hmac.new(
        SHOPEE_PARTNER_KEY.encode('utf-8'),
        base_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    return signature

def get_authorization_url():
    """Generates the authorization URL for merchants to grant access."""
    path = PATH_GET_AUTH_URL
    timestamp = int(time.time())
    
    # --- Start of Debug Info for Support Ticket ---
    base_string_for_sign = f"{SHOPEE_PARTNER_ID}{path}{timestamp}"
    sign = generate_signature(path, timestamp)

    print("\n" + "="*30)
    print("DEBUG INFO FOR SHOPEE SUPPORT TICKET (Auth URL)")
    print(f"Endpoint Path: {path}")
    print(f"Timestamp: {timestamp}")
    print(f"Partner ID: {SHOPEE_PARTNER_ID}")
    print(f"Base String for Signature: {base_string_for_sign}")
    print(f"Generated Signature (Sign): {sign}")
    print("="*30 + "\n")
    # --- End of Debug Info ---

    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"redirect={SHOPEE_REDIRECT_URL}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )
    return url

def get_access_token(auth_code, shop_id):
    """Exchanges an authorization code for an access token."""
    path = PATH_GET_TOKEN
    timestamp = int(time.time())
    body = {
        "code": auth_code,
        "partner_id": SHOPEE_PARTNER_ID,
        "shop_id": int(shop_id)
    }
    
    sign = generate_signature(path, timestamp, body=body)
    
    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            return {
                "success": False,
                "error": data.get("message", "Unknown error"),
                "raw": data,
            }

        data["created_at"] = int(time.time())
        data["success"] = True
        return data

    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

def refresh_access_token(current_refresh_token, shop_id):
    """Refreshes an expired access token."""
    path = PATH_REFRESH_TOKEN
    timestamp = int(time.time())
    body = {
        "refresh_token": current_refresh_token,
        "partner_id": SHOPEE_PARTNER_ID,
        "shop_id": int(shop_id)
    }
    # The signature for this call also includes the body
    sign = generate_signature(path, timestamp, body=body)

    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            return {
                "success": False,
                "error": data.get("message", "Unknown error"),
                "raw": data,
            }

        data["created_at"] = int(time.time())
        data["success"] = True
        return data

    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

def get_authorization_url():
    """Generates the authorization URL for merchants to grant access."""
    timestamp = int(time.time())
    sign = generate_signature(PATH_GET_AUTH_URL, timestamp)
    url = (
        f"{BASE_URL}{PATH_GET_AUTH_URL}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"redirect={SHOPEE_REDIRECT_URL}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )
    return url

def get_access_token(auth_code, shop_id):
    """Exchanges an authorization code for an access token."""
    path = PATH_GET_TOKEN
    timestamp = int(time.time())
    body = {
        "code": auth_code,
        "partner_id": SHOPEE_PARTNER_ID,
        "shop_id": int(shop_id)
    }
    sign = generate_signature(path, timestamp, body=body)
    
    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            return {
                "success": False,
                "error": data.get("message", "Unknown error"),
                "raw": data,
            }

        data["created_at"] = int(time.time())
        data["success"] = True
        return data

    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

def refresh_access_token(current_refresh_token, shop_id):
    """Refreshes an access token using a refresh token."""
    path = PATH_REFRESH_TOKEN
    body = {
        "refresh_token": current_refresh_token,
        "partner_id": SHOPEE_PARTNER_ID,
        "shop_id": int(shop_id)
    }
    sign, timestamp = generate_signature(path, body)
    
    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            return {
                "success": False,
                "error": data.get("message", "Refresh failed"),
                "raw": data,
            }
        
        data["created_at"] = int(time.time())
        data["success"] = True
        return data

    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

# --- Token Persistence Functions ---

def save_tokens_to_file(token_data):
    """Saves token data to a JSON file."""
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f, indent=4)
        return True
    except IOError as e:
        print(f"❌ Error saving tokens to {TOKEN_FILE}: {e}")
        return False

def load_tokens_from_file():
    """Loads token data from a JSON file."""
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"❌ Error loading tokens from {TOKEN_FILE}: {e}")
        return None

def is_token_expired(token_data, margin_seconds=300):
    """Checks if the access token is expired or close to expiring."""
    if not all(k in token_data for k in ['created_at', 'expire_in']):
        return True # Assume expired if data is incomplete
    
    expires_at = token_data['created_at'] + token_data['expire_in']
    return time.time() > (expires_at - margin_seconds)

def update_env_file_tokens(access_token, refresh_token):
    """Updates the .env file with new tokens. (Optional and for development only)"""
    env_path = '.env'
    if not os.path.exists(env_path):
        print(f"⚠️ .env file not found. Skipping update.")
        return

    # Read current .env file
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Use dictionaries to ensure each variable is written only once
    env_vars = {}
    for line in lines:
        if '=' in line:
            key, val = line.strip().split('=', 1)
            env_vars[key] = val

    env_vars['SHOPEE_ACCESS_TOKEN'] = access_token
    env_vars['SHOPEE_REFRESH_TOKEN'] = refresh_token

    # Write back to .env file
    with open(env_path, 'w') as f:
        for key, val in env_vars.items():
            f.write(f"{key}={val}\n")
    
    print("✅ Updated .env file with new tokens")


# --- Main Execution Logic ---

def get_new_tokens():
    """Guides the user through the process of getting new tokens."""
    print("=== Shopee OAuth Token Generation ===\n")
    
    # Step 1: Generate authorization URL
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    print("Visit this URL in your browser to authorize the application:")
    print(f"\n{auth_url}\n")
    
    print("After authorization, you will be redirected to a URL like:")
    print(f"{SHOPEE_REDIRECT_URL}?code=YOUR_AUTH_CODE&shop_id=YOUR_SHOP_ID")
    print("Copy the 'code' and 'shop_id' parameter values from that URL.\n")
    
    # Step 2: Get auth code and shop_id from user
    auth_code = input("Enter the 'code' from the redirect URL: ").strip()
    shop_id = input("Enter the 'shop_id' from the redirect URL: ").strip()
    
    if not auth_code or not shop_id:
        print("❌ Auth code and Shop ID are required. Exiting.")
        return
    
    # Step 3: Exchange auth code for access token
    print(f"\nStep 2: Getting access token with code: {auth_code[:15]}...")
    token_result = get_access_token(auth_code, shop_id)
    
    if token_result.get('success'):
        print("✅ Successfully obtained tokens!")
        print(f"  Access Token:  {token_result['access_token'][:20]}...")
        print(f"  Refresh Token: {token_result['refresh_token'][:20]}...")
        print(f"  Expires in:    {token_result['expire_in']} seconds")
        
        # Add shop_id to the token data for future use
        token_result['shop_id'] = int(shop_id)
        
        # Step 4: Save tokens to JSON file
        if save_tokens_to_file(token_result):
            print(f"✅ Tokens saved to {TOKEN_FILE}")
        
        # Step 5: (Optional) Update .env file
        update_env_file_tokens(token_result['access_token'], token_result['refresh_token'])
        
    else:
        print(f"❌ Failed to get access token: {token_result.get('error', 'Unknown error')}")
        if 'raw' in token_result:
            print(f"   Raw response: {token_result['raw']}")

def refresh_access_token(current_refresh_token, shop_id):
    """Refreshes an access token using a refresh token."""
    path = PATH_REFRESH_TOKEN
    body = {
        "refresh_token": current_refresh_token,
        "partner_id": SHOPEE_PARTNER_ID,
        "shop_id": int(shop_id)
    }
    sign, timestamp = generate_signature(path, body)
    
    url = (
        f"{BASE_URL}{path}?"
        f"partner_id={SHOPEE_PARTNER_ID}&"
        f"timestamp={timestamp}&"
        f"sign={sign}"
    )

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=body, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            return {
                "success": False,
                "error": data.get("message", "Refresh failed"),
                "raw": data,
            }
        
        data["created_at"] = int(time.time())
        data["success"] = True
        return data

    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

def save_tokens(token_data):
    """Saves the given token data to the shopee_tokens.json file."""
    success = save_tokens_to_file(token_data)
    if success:
        print("✅ Tokens saved successfully.")
    else:
        print("❌ Failed to save tokens.")

def check_server_timestamp():
    """Compares local timestamp with Shopee server timestamp."""
    path = "/api/v2/public/get_server_timestamp"
    url = f"{BASE_URL}{path}"
    
    try:
        # This is a public endpoint, no signature needed
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        server_time = data.get("server_timestamp")
        
        if server_time:
            local_time = int(time.time())
            time_difference = abs(local_time - server_time)
            
            print("-" * 30)
            print("Timestamp Check")
            print(f"Shopee Server Timestamp: {server_time}")
            print(f"Local Timestamp:         {local_time}")
            print(f"Time Difference:         {time_difference} seconds")
            
            if time_difference > 60:
                print("\n[WARNING] Your local time is off by more than a minute from the server time.")
                print("This is a likely cause for 'Wrong sign' errors. Please sync your system clock.")
            else:
                print("\n[INFO] Your local time is in sync with the server.")
            print("-" * 30)
            
            return True
        else:
            print(f"Could not retrieve server timestamp from the response. Response: {data}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while checking server time: {e}")
        return False

# --- Main Execution ---

def main():
    print("=== Shopee Token Management ===")
    print("Environment: ", "Sandbox" if IS_SANDBOX else "Production")
    
    # Check if essential environment variables are set
    if not all([SHOPEE_PARTNER_ID, SHOPEE_PARTNER_KEY, SHOPEE_REDIRECT_URL]):
        print("❌ Critical environment variables are missing.")
        print("Please create a .env file with SHOPEE_PARTNER_ID, SHOPEE_PARTNER_KEY, and SHOPEE_REDIRECT_URL.")
        return

    # Main loop
    while True:
        print("\nWhat would you like to do?")
        print("1. Get a new Access Token (requires auth code and shop ID)")
        print("2. Refresh an existing Access Token")
        print("3. Generate Authorization URL")
        print("4. Check Shopee server timestamp")
        print("5. Exit")
        choice = input("Enter your choice (1, 2, 3, 4, or 5): ")

        if choice == '1':
            get_new_tokens()
            
        elif choice == '2':
            tokens = load_tokens_from_file()
            if not tokens or 'refresh_token' not in tokens:
                print("❌ No valid refresh token found. Please get new tokens first.")
                continue
            
            print(f"Refreshing access token for shop ID {tokens['shop_id']}...")
            result = refresh_access_token(tokens['refresh_token'], tokens['shop_id'])
            
            if result.get("success"):
                save_tokens(result)
                print("\nTokens refreshed and saved successfully.")
            else:
                print(f"\nError refreshing token: {result.get('error')}")
                print(f"Raw response: {result.get('raw')}")
                
        elif choice == '3':
            auth_url = get_authorization_url()
            print("\n--- Authorization URL ---")
            print("Copy and paste this URL into your browser to authorize the application:")
            print(auth_url)

        elif choice == '4':
            check_server_timestamp()
            
        elif choice == '5':
            print("Exiting. Goodbye!")
            break
            
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()