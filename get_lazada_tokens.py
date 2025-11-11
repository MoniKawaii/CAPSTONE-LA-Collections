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
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            lines = f.readlines()
    else:
        lines = []
    
    # Update token lines
    updated_lines = []
    access_token_found = False
    refresh_token_found = False
    
    for line in lines:
        if line.startswith('LAZADA_ACCESS_TOKEN='):
            updated_lines.append(f"LAZADA_ACCESS_TOKEN={token_data['access_token']}\n")
            access_token_found = True
        elif line.startswith('LAZADA_REFRESH_TOKEN=') and 'refresh_token' in token_data:
            updated_lines.append(f"LAZADA_REFRESH_TOKEN={token_data['refresh_token']}\n")
            refresh_token_found = True
        else:
            updated_lines.append(line)
    
    # Add missing token lines if not found
    if not access_token_found:
        updated_lines.append(f"LAZADA_ACCESS_TOKEN={token_data['access_token']}\n")
    if not refresh_token_found and 'refresh_token' in token_data:
        updated_lines.append(f"LAZADA_REFRESH_TOKEN={token_data['refresh_token']}\n")
    
    # Write back to .env file
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print("✅ Updated .env file with new tokens")

def save_tokens_to_multiple_locations(token_data):
    """Save tokens to both the correct location and backup locations"""
    import json
    import time
    
    # Add timestamp if not present
    if 'created_at' not in token_data:
        token_data['created_at'] = int(time.time())
    
    # Primary location: tokens/lazada_tokens.json
    primary_path = os.path.join('tokens', 'lazada_tokens.json')
    os.makedirs(os.path.dirname(primary_path), exist_ok=True)
    
    try:
        with open(primary_path, 'w') as f:
            json.dump(token_data, f, indent=2)
        print(f"✅ Tokens saved to primary location: {primary_path}")
    except Exception as e:
        print(f"⚠️ Failed to save to primary location: {e}")
    
    # Backup location: app/lazada/lazada_tokens.json
    backup_path = os.path.join('app', 'lazada', 'lazada_tokens.json')
    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
    
    try:
        with open(backup_path, 'w') as f:
            json.dump(token_data, f, indent=2)
        print(f"✅ Tokens saved to backup location: {backup_path}")
    except Exception as e:
        print(f"⚠️ Failed to save to backup location: {e}")

def load_existing_tokens():
    """Load existing tokens from multiple possible locations"""
    import json
    
    # Try multiple locations
    possible_paths = [
        os.path.join('tokens', 'lazada_tokens.json'),
        os.path.join('app', 'lazada', 'lazada_tokens.json')
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    tokens = json.load(f)
                print(f"📋 Found existing tokens at: {path}")
                return tokens, path
            except (json.JSONDecodeError, IOError) as e:
                print(f"⚠️ Could not load tokens from {path}: {e}")
    
    print("📋 No existing tokens found")
    return None, None

def main():
    print("=== Lazada OAuth Token Generation ===\n")
    
    # Check for existing tokens first
    existing_tokens, token_path = load_existing_tokens()
    
    if existing_tokens:
        print(f"📋 Found existing tokens in: {token_path}")
        
        # Check if tokens are expired
        if is_token_expired(existing_tokens):
            print("⚠️ Existing tokens are expired")
            
            # Try to refresh using existing refresh token
            if 'refresh_token' in existing_tokens:
                print("🔄 Attempting to refresh tokens...")
                refresh_result = refresh_access_token(existing_tokens['refresh_token'])
                
                if refresh_result['success']:
                    print("✅ Successfully refreshed tokens!")
                    
                    # Save refreshed tokens
                    save_tokens_to_multiple_locations(refresh_result)
                    update_env_file_tokens(refresh_result)
                    
                    print("🎉 Token refresh complete! You can now use the API.")
                    return
                else:
                    print(f"❌ Token refresh failed: {refresh_result.get('error', 'Unknown error')}")
                    print("💡 Need to get new authorization code")
            else:
                print("⚠️ No refresh token found in existing tokens")
                print("💡 Need to get new authorization code")
        else:
            print("✅ Existing tokens are still valid!")
            test_token_api_call(existing_tokens['access_token'])
            return
    
    # If we reach here, we need new tokens
    print("\n🆕 Getting new authorization tokens...")
    
    # Step 1: Generate authorization URL
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    print(f"Visit this URL to authorize your application:")
    print(f"{auth_url}\n")
    
    print("After authorization, you'll be redirected to:")
    print("https://your-app.com/callback?code=YOUR_AUTH_CODE&state=...")
    print("Copy the 'code' parameter value.\n")
    
    # Step 2: Get auth code from user
    auth_code = input("Enter the authorization code: ").strip()
    
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
        
        # Step 4: Save tokens to multiple locations
        save_tokens_to_multiple_locations(token_result)
        
        # Step 5: Update .env file with tokens
        update_env_file_tokens(token_result)
        
        # Step 6: Test the new tokens
        print(f"\nStep 3: Testing new tokens...")
        test_token_api_call(token_result['access_token'])
            
    else:
        print(f"❌ Failed to get access token: {token_result['error']}")
        print(f"Error code: {token_result.get('code', 'Unknown')}")

def test_token_api_call(access_token):
    """Test the token with a simple API call"""
    try:
        # Import here to avoid circular imports
        from app.Extraction.lazada_api_calls import LazadaDataExtractor
        
        print("🧪 Testing token with API call...")
        extractor = LazadaDataExtractor()
        
        # Override the access token for this test
        extractor.access_token = access_token
        
        # Make a simple API call to test
        import lazop_sdk as lazop
        request = lazop.LazopRequest('/products/get', 'GET')
        request.add_api_param('limit', '1')  # Just get 1 product to test
        
        response = extractor.client.execute(request, access_token)
        
        if response and response.get('code') == '0':
            print("✅ Token test successful! API is accessible.")
        else:
            print(f"⚠️ Token test failed: {response}")
            
    except Exception as e:
        print(f"⚠️ Could not test token: {e}")
        print("💡 You can still proceed - the token should work if obtained successfully")

def test_saved_tokens():
    """Test loading and checking saved tokens"""
    print("\n=== Testing Saved Tokens ===")
    
    # Load tokens from multiple locations
    existing_tokens, token_path = load_existing_tokens()
    
    if existing_tokens:
        print("✅ Tokens loaded from file")
        print(f"Access Token: {existing_tokens['access_token'][:20]}...")
        
        # Check if expired
        if is_token_expired(existing_tokens):
            print("⚠️ Access token is expired")
            
            # Try to refresh
            if 'refresh_token' in existing_tokens:
                print("Attempting to refresh token...")
                refresh_result = refresh_access_token(existing_tokens['refresh_token'])
                
                if refresh_result['success']:
                    print("✅ Token refreshed successfully!")
                    
                    # Save updated tokens to all locations
                    save_tokens_to_multiple_locations(refresh_result)
                    update_env_file_tokens(refresh_result)
                    
                    print("🎉 Token refresh and save complete!")
                    
                    # Test the refreshed token
                    test_token_api_call(refresh_result['access_token'])
                else:
                    print(f"❌ Token refresh failed: {refresh_result.get('error', 'Unknown error')}")
                    print("💡 You may need to get a new authorization code")
            else:
                print("❌ No refresh token available for refresh")
        else:
            print("✅ Access token is still valid")
            test_token_api_call(existing_tokens['access_token'])
    else:
        print("❌ No saved tokens found")

def refresh_existing_tokens():
    """Dedicated function to refresh existing tokens"""
    print("\n=== Refreshing Existing Tokens ===")
    
    existing_tokens, token_path = load_existing_tokens()
    
    if not existing_tokens:
        print("❌ No existing tokens found to refresh")
        return False
    
    if 'refresh_token' not in existing_tokens:
        print("❌ No refresh token found in existing tokens")
        return False
    
    print(f"📋 Found tokens at: {token_path}")
    print("🔄 Attempting to refresh tokens...")
    
    refresh_result = refresh_access_token(existing_tokens['refresh_token'])
    
    if refresh_result['success']:
        print("✅ Token refreshed successfully!")
        print(f"New Access Token: {refresh_result['access_token'][:20]}...")
        
        # Save refreshed tokens
        save_tokens_to_multiple_locations(refresh_result)
        update_env_file_tokens(refresh_result)
        
        # Test the refreshed token
        test_token_api_call(refresh_result['access_token'])
        
        print("🎉 Token refresh complete! Enhanced extraction can now proceed.")
        return True
    else:
        print(f"❌ Token refresh failed: {refresh_result.get('error', 'Unknown error')}")
        print("💡 You may need to get a new authorization code using option 1")
        return False

if __name__ == "__main__":
    import time
    
    print("=== Lazada Token Manager ===")
    print("Choose option:")
    print("1. Get new tokens (requires authorization)")
    print("2. Test saved tokens")
    print("3. Refresh existing tokens (RECOMMENDED)")
    print("4. Check token status")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        main()
    elif choice == "2":
        test_saved_tokens()
    elif choice == "3":
        success = refresh_existing_tokens()
        if success:
            print("\n💡 You can now run the enhanced extraction:")
            print("   python test_enhanced_extraction.py")
        else:
            print("\n💡 If refresh failed, try option 1 to get new tokens")
    elif choice == "4":
        # Just check status without refresh
        existing_tokens, token_path = load_existing_tokens()
        if existing_tokens:
            print(f"\n📋 Token Status:")
            print(f"   Location: {token_path}")
            print(f"   Access Token: {existing_tokens['access_token'][:20]}...")
            if is_token_expired(existing_tokens):
                print("   Status: ❌ EXPIRED")
                print("   💡 Use option 3 to refresh tokens")
            else:
                print("   Status: ✅ VALID")
        else:
            print("\n❌ No tokens found")
            print("   💡 Use option 1 to get new tokens")
    else:
        print("Invalid choice. Exiting.")