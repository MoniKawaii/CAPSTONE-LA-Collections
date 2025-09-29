"""
Lazada Token Management with Environment Variables

This script handles Lazada OAuth tokens securely using environment variables.
"""

import os
from dotenv import load_dotenv
from tests.lazada_test import (
    get_authorization_url, 
    get_access_token, 
    refresh_access_token,
    save_tokens_to_file,
    load_tokens_from_file,
    is_token_expired
)

# Load environment variables
load_dotenv()

def update_env_file_tokens(token_data):
    """Update .env file with new tokens"""
    env_path = '.env'
    
    # Read current .env file
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Update token lines
    updated_lines = []
    for line in lines:
        if line.startswith('LAZADA_ACCESS_TOKEN='):
            updated_lines.append(f"LAZADA_ACCESS_TOKEN={token_data['access_token']}\n")
        elif line.startswith('LAZADA_REFRESH_TOKEN=') and 'refresh_token' in token_data:
            updated_lines.append(f"LAZADA_REFRESH_TOKEN={token_data['refresh_token']}\n")
        else:
            updated_lines.append(line)
    
    # Write back to .env file
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print("✅ Updated .env file with new tokens")

def main():
    print("=== Lazada OAuth Token Generation ===\n")
    
    # Step 1: Generate authorization URL
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    print(f"Visit this URL to authorize your application:")
    print(f"{auth_url}\n")
    
    print("After authorization, you'll be redirected to:")
    print("https://your-app.com/callback?code=YOUR_AUTH_CODE&state=...")
    print("Copy the 'code' parameter value.\n")
    
    # Step 2: Get auth code from user
    auth_code = "0_135073_pGhdjQSvzcbs6ZSew6OV874W2520"
    
    if not auth_code:
        print("No auth code provided. Exiting.")
        return
    
    # Step 3: Get access token
    print(f"\nStep 2: Getting access token with code: {auth_code[:10]}...")
    token_result = get_access_token(auth_code)
    
    if token_result['success']:
        print("✅ Successfully obtained tokens!")
        print(f"Access Token: {token_result['access_token'][:20]}...")
        print(f"Refresh Token: {token_result['refresh_token'][:20]}...")
        print(f"Expires in: {token_result['expires_in']} seconds")
        print(f"Account Platform: {token_result.get('account_platform')}")
        
        # Step 4: Update .env file with tokens
        update_env_file_tokens(token_result)
        
        # Step 5: Save tokens to JSON file as backup
        if save_tokens_to_file(token_result):
            print("✅ Tokens also saved to lazada_tokens.json as backup")
        
        # Step 6: Test token refresh
        print(f"\nStep 3: Testing token refresh...")
        refresh_result = refresh_access_token(token_result['refresh_token'])
        
        if refresh_result['success']:
            print("✅ Successfully refreshed token!")
            print(f"New Access Token: {refresh_result['access_token'][:20]}...")
        else:
            print(f"❌ Token refresh failed: {refresh_result['error']}")
            
    else:
        print(f"❌ Failed to get access token: {token_result['error']}")
        print(f"Error code: {token_result.get('code', 'Unknown')}")

def test_saved_tokens():
    """Test loading and checking saved tokens"""
    print("\n=== Testing Saved Tokens ===")
    
    # Load tokens from file
    tokens = load_tokens_from_file()
    
    if tokens:
        print("✅ Tokens loaded from file")
        print(f"Access Token: {tokens['access_token'][:20]}...")
        
        # Check if expired
        if is_token_expired(tokens):
            print("⚠️ Access token is expired")
            
            # Try to refresh
            print("Attempting to refresh token...")
            refresh_result = refresh_access_token(tokens['refresh_token'])
            
            if refresh_result['success']:
                print("✅ Token refreshed successfully!")
                
                # Update tokens
                tokens.update({
                    'access_token': refresh_result['access_token'],
                    'refresh_token': refresh_result['refresh_token'],
                    'expires_in': refresh_result['expires_in'],
                    'created_at': int(time.time())
                })
                
                # Save updated tokens
                import json
                with open('lazada_tokens.json', 'w') as f:
                    json.dump(tokens, f, indent=2)
                print("✅ Updated tokens saved")
            else:
                print(f"❌ Token refresh failed: {refresh_result['error']}")
        else:
            print("✅ Access token is still valid")
    else:
        print("❌ No saved tokens found")

if __name__ == "__main__":
    import time
    
    choice = input("Choose option:\n1. Get new tokens\n2. Test saved tokens\nEnter choice (1/2): ").strip()
    
    if choice == "1":
        main()
    elif choice == "2":
        test_saved_tokens()
    else:
        print("Invalid choice. Exiting.")