"""
Shopee Token Management with Environment Variables

This script handles Shopee OAuth tokens securely using environment variables.
"""

import os
import sys
from dotenv import load_dotenv
from tests.shopee_test import (
    get_authorization_url, 
    exchange_code_for_token, 
    refresh_access_token,
    save_tokens_to_file,
    load_tokens_from_file,
    is_token_expired,
    test_shopee_api_with_token
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
        if line.startswith('SHOPEE_ACCESS_TOKEN='):
            updated_lines.append(f"SHOPEE_ACCESS_TOKEN={token_data['access_token']}\n")
        elif line.startswith('SHOPEE_REFRESH_TOKEN=') and 'refresh_token' in token_data:
            updated_lines.append(f"SHOPEE_REFRESH_TOKEN={token_data['refresh_token']}\n")
        else:
            updated_lines.append(line)
    
    # Write back to .env file
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print("✅ Updated .env file with new tokens")

def main():
    print("=== Shopee OAuth Token Generation ===\n")
    
    # Step 1: Generate authorization URL
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    
    print("\nPlease visit this URL in your browser to authorize the application:")
    print(auth_url)
    print("\nAfter authorization, you will be redirected to your callback URL with a code parameter.")
    
    # Step 2: Exchange authorization code for tokens
    code = input("\nStep 2: Enter the authorization code from the redirect URL: ")
    if code:
        print(f"\nExchanging code for token...")
        token_result = exchange_code_for_token(code)
        
        if token_result['success']:
            print("\n✅ Successfully obtained tokens!")
            print(f"Access token: {token_result['access_token'][:10]}...")
            print(f"Refresh token: {token_result['refresh_token'][:10]}...")
            print(f"Expires in: {token_result['expires_in']} seconds")
            
            # Update .env file with new tokens
            update_env_file_tokens(token_result)
        else:
            print("\n❌ Failed to obtain tokens")
            print(f"Error: {token_result.get('error')}")
            print(f"Description: {token_result.get('error_description')}")
            sys.exit(1)
    else:
        print("\n❌ No authorization code provided")
        sys.exit(1)
    
    # Step 3: Test token refresh
    print("\nStep 3: Testing token refresh...")
    token_data = load_tokens_from_file()
    
    if token_data and 'refresh_token' in token_data:
        if is_token_expired(token_data):
            print("Current token is expired, refreshing...")
            refresh_result = refresh_access_token(token_data['refresh_token'])
            
            if refresh_result['success']:
                print("\n✅ Successfully refreshed tokens!")
                print(f"New access token: {refresh_result['access_token'][:10]}...")
                print(f"New refresh token: {refresh_result['refresh_token'][:10]}...")
                print(f"Expires in: {refresh_result['expires_in']} seconds")
                
                # Update .env file with new tokens
                update_env_file_tokens(refresh_result)
            else:
                print("\n❌ Failed to refresh tokens")
                print(f"Error: {refresh_result.get('error')}")
                print(f"Description: {refresh_result.get('error_description')}")
        else:
            print("Current token is still valid, no refresh needed")
    else:
        print("\n⚠️ No token data found, skipping refresh test")
        
    # Step 4: Test API with token
    print("\nStep 4: Testing API with token...")
    api_test_result = test_shopee_api_with_token()
    if api_test_result:
        print("✅ API test successful!")
    else:
        print("❌ API test failed")

if __name__ == "__main__":
    main()