"""
Lazada Token Management with Environment Variables

This script handles Lazada OAuth tokens securely using environment variables.
"""

import os
import sys
import json
import time
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from tests.lazada_test import (
    get_access_token, 
    refresh_access_token,
    save_tokens_to_file,
    load_tokens_from_file,
    is_token_expired
)

# Load environment variables
load_dotenv()

def get_auth_url():
    """Generate the correct authorization URL for your app"""
    app_key = os.getenv('LAZADA_APP_KEY', '135073')
    callback_url = "https://dagmar-hittable-acceptingly.ngrok-free.dev/lazada/callback"
    
    auth_url = (
        "https://auth.lazada.com/oauth/authorize"
        f"?response_type=code"
        f"&force_auth=true"
        f"&redirect_uri={callback_url}"
        f"&client_id={app_key}"
    )
    
    return auth_url

def display_auth_instructions():
    """Display instructions for getting the auth code"""
    print("=" * 60)
    print("🔗 LAZADA AUTHORIZATION PROCESS")
    print("=" * 60)
    print()
    print("1. Copy the authorization URL below:")
    print()
    auth_url = get_auth_url()
    print(f"   {auth_url}")
    print()
    print("2. Open this URL in your browser")
    print("3. Login to your Lazada seller account")
    print("4. Authorize the application")
    print("5. You'll be redirected to your callback URL with a 'code' parameter")
    print()
    print("   Example callback:")
    print("   https://dagmar-hittable-acceptingly.ngrok-free.dev/lazada/callback?code=0_135073_aBcDeFg123456&state=...")
    print()
    print("6. Copy the 'code' value (the part after 'code=')")
    print("=" * 60)
    print()

def get_auth_code_from_user():
    """Get the authorization code from user input"""
    print("Please paste your authorization code here:")
    print("(This is the 'code' parameter from the callback URL)")
    print()
    
    while True:
        auth_code = input("Enter auth code: ").strip()
        
        if not auth_code:
            print("❌ Auth code cannot be empty. Please try again.")
            continue
            
        if len(auth_code) < 10:
            print("❌ Auth code seems too short. Please check and try again.")
            continue
            
        # Confirm with user
        print(f"\n📋 You entered: {auth_code}")
        confirm = input("Is this correct? (y/n): ").strip().lower()
        
        if confirm in ['y', 'yes']:
            return auth_code
        else:
            print("Let's try again...")
            continue

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
    access_token_updated = False
    refresh_token_updated = False
    
    for line in lines:
        if line.startswith('LAZADA_ACCESS_TOKEN='):
            updated_lines.append(f"LAZADA_ACCESS_TOKEN={token_data['access_token']}\n")
            access_token_updated = True
        elif line.startswith('LAZADA_REFRESH_TOKEN=') and 'refresh_token' in token_data:
            updated_lines.append(f"LAZADA_REFRESH_TOKEN={token_data['refresh_token']}\n")
            refresh_token_updated = True
        else:
            updated_lines.append(line)
    
    # Add tokens if they weren't in the file
    if not access_token_updated:
        updated_lines.append(f"LAZADA_ACCESS_TOKEN={token_data['access_token']}\n")
    if not refresh_token_updated and 'refresh_token' in token_data:
        updated_lines.append(f"LAZADA_REFRESH_TOKEN={token_data['refresh_token']}\n")
    
    # Write back to .env file
    with open(env_path, 'w') as f:
        f.writelines(updated_lines)
    
    print("✅ Updated .env file with new tokens")

def generate_new_tokens():
    """Interactive token generation process"""
    print("\n🚀 GENERATING NEW TOKENS")
    print("=" * 40)
    
    # Display instructions
    display_auth_instructions()
    
    # Get auth code from user
    auth_code = get_auth_code_from_user()
    
    print(f"\n⏳ Exchanging authorization code for tokens...")
    print(f"   Code: {auth_code[:15]}...")
    
    # Get access token
    token_result = get_access_token(auth_code)
    
    if token_result['success']:
        print("\n🎉 SUCCESS! Tokens obtained!")
        print("-" * 30)
        print(f"✅ Access Token: {token_result['access_token'][:20]}...")
        print(f"✅ Refresh Token: {token_result['refresh_token'][:20]}...")
        print(f"✅ Expires in: {token_result['expires_in']} seconds")
        print(f"✅ Account Platform: {token_result.get('account_platform', 'N/A')}")
        
        # Save tokens to JSON file
        if save_tokens_to_file(token_result):
            print("✅ Tokens saved to lazada_tokens.json")
        
        # Update .env file
        try:
            update_env_file_tokens(token_result)
        except Exception as e:
            print(f"⚠️ Could not update .env file: {e}")
        
        # Test token refresh
        print(f"\n🔄 Testing token refresh...")
        refresh_result = refresh_access_token(token_result['refresh_token'])
        
        if refresh_result['success']:
            print("✅ Token refresh test successful!")
        else:
            print(f"⚠️ Token refresh test failed: {refresh_result['error']}")
            
        return True
            
    else:
        print(f"\n❌ FAILED to get access token!")
        print(f"   Error: {token_result['error']}")
        print(f"   Code: {token_result.get('code', 'Unknown')}")
        
        if 'response' in token_result:
            print(f"   Response: {token_result['response']}")
            
        return False

def test_saved_tokens():
    """Test loading and checking saved tokens"""
    print("\n🔍 TESTING SAVED TOKENS")
    print("=" * 30)
    
    # Load tokens from file
    tokens = load_tokens_from_file()
    
    if not tokens:
        print("❌ No saved tokens found")
        print("   Run option 1 to generate new tokens first")
        return False
    
    print("✅ Tokens loaded from lazada_tokens.json")
    print(f"   Access Token: {tokens['access_token'][:20]}...")
    
    # Check if expired
    if is_token_expired(tokens):
        print("⚠️ Access token is expired or expiring soon")
        
        # Try to refresh
        print("🔄 Attempting to refresh token...")
        refresh_result = refresh_access_token(tokens['refresh_token'])
        
        if refresh_result['success']:
            print("✅ Token refreshed successfully!")
            
            # Update tokens
            tokens.update({
                'access_token': refresh_result['access_token'],
                'refresh_token': refresh_result.get('refresh_token', tokens['refresh_token']),
                'expires_in': refresh_result['expires_in'],
                'created_at': int(time.time())
            })
            
            # Save updated tokens
            with open('lazada_tokens.json', 'w') as f:
                json.dump(tokens, f, indent=2)
            print("✅ Updated tokens saved")
            
            return True
        else:
            print(f"❌ Token refresh failed: {refresh_result['error']}")
            return False
    else:
        print("✅ Access token is still valid!")
        
        # Show expiry info
        created_at = tokens.get('created_at', 0)
        expires_in = tokens.get('expires_in', 3600)
        
        if isinstance(created_at, (int, float)):
            remaining_time = (created_at + expires_in) - int(time.time())
            if remaining_time > 0:
                hours = remaining_time // 3600
                minutes = (remaining_time % 3600) // 60
                print(f"   Time remaining: {hours}h {minutes}m")
        
        return True

def main_menu():
    """Display main menu and handle user choices"""
    while True:
        print("\n" + "=" * 50)
        print("🔑 LAZADA TOKEN MANAGER")
        print("=" * 50)
        print()
        print("Choose an option:")
        print("1. 🆕 Generate new tokens (paste auth code)")
        print("2. 🔍 Test saved tokens")
        print("3. 📋 Show authorization URL only")
        print("4. ❌ Exit")
        print()
        
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == "1":
            success = generate_new_tokens()
            if success:
                input("\nPress Enter to continue...")
        
        elif choice == "2":
            test_saved_tokens()
            input("\nPress Enter to continue...")
        
        elif choice == "3":
            print("\n📋 AUTHORIZATION URL:")
            print("-" * 30)
            print(get_auth_url())
            print()
            print("Copy this URL and open it in your browser to get the auth code.")
            input("\nPress Enter to continue...")
        
        elif choice == "4":
            print("\n👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()