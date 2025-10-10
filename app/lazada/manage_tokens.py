"""
Lazada Token Management Utility

This script helps you manage Lazada tokens in the lazada_tokens.json file.
Integrates with get_lazada_tokens.py and uses the same SDK for token operations.
"""

import json
import os
import sys
import time
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from tests.lazada_test import (
    refresh_access_token,
    is_token_expired,
    test_api_connection
)

TOKEN_FILE = 'lazada_tokens.json'

def load_tokens():
    """Load tokens from JSON file"""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    return None

def save_tokens(token_data):
    """Save tokens to JSON file"""
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f, indent=2)
    print(f"✅ Tokens saved to {TOKEN_FILE}")

def display_tokens():
    """Display current tokens with detailed info"""
    tokens = load_tokens()
    
    if not tokens:
        print("❌ No tokens found in lazada_tokens.json")
        print("   Use get_lazada_tokens.py to generate tokens first")
        return
    
    print("\n📋 CURRENT LAZADA TOKENS")
    print("=" * 40)
    print(f"🔑 Access Token: {tokens['access_token'][:20]}...")
    
    if 'refresh_token' in tokens:
        print(f"🔄 Refresh Token: {tokens['refresh_token'][:20]}...")
    else:
        print("🔄 Refresh Token: Not available")
    
    print(f"⏱️  Expires In: {tokens.get('expires_in', 'N/A')} seconds")
    print(f"🏪 Account Platform: {tokens.get('account_platform', 'N/A')}")
    
    # Check if expired using the same logic as other files
    if is_token_expired(tokens):
        print("🔴 Status: EXPIRED or expiring soon")
        
        # Show exact expiry info
        if 'created_at' in tokens and 'expires_in' in tokens:
            created_at = tokens['created_at']
            expires_in = tokens['expires_in']
            current_time = int(time.time())
            remaining = (created_at + expires_in) - current_time
            
            if remaining > 0:
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                print(f"   Time remaining: {hours}h {minutes}m")
            else:
                print(f"   Expired {abs(remaining)} seconds ago")
    else:
        print("🟢 Status: VALID")
        
        # Show remaining time
        if 'created_at' in tokens and 'expires_in' in tokens:
            created_at = tokens['created_at']
            expires_in = tokens['expires_in']
            current_time = int(time.time())
            remaining = (created_at + expires_in) - current_time
            
            if remaining > 0:
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                print(f"   Time remaining: {hours}h {minutes}m")

def auto_refresh_token():
    """Automatically refresh expired tokens using SDK"""
    tokens = load_tokens()
    
    if not tokens:
        print("❌ No tokens found to refresh")
        return False
    
    if not tokens.get('refresh_token'):
        print("❌ No refresh token available")
        return False
    
    print("🔄 Refreshing access token using SDK...")
    
    refresh_result = refresh_access_token(tokens['refresh_token'])
    
    if refresh_result['success']:
        print("✅ Token refreshed successfully!")
        
        # Update tokens with new data
        tokens.update({
            'access_token': refresh_result['access_token'],
            'refresh_token': refresh_result.get('refresh_token', tokens['refresh_token']),
            'expires_in': refresh_result['expires_in'],
            'created_at': refresh_result['created_at']
        })
        
        save_tokens(tokens)
        print("✅ Updated tokens saved to file")
        return True
    else:
        print(f"❌ Token refresh failed: {refresh_result['error']}")
        return False

def update_access_token():
    """Manually update access token"""
    tokens = load_tokens() or {}
    
    print("\n📝 MANUAL ACCESS TOKEN UPDATE")
    print("-" * 30)
    current_token = tokens.get('access_token', 'None')
    print(f"Current token: {current_token[:20]}..." if current_token != 'None' else "Current token: None")
    
    new_token = input("Enter new access token: ").strip()
    if new_token:
        tokens['access_token'] = new_token
        tokens['created_at'] = int(time.time())
        
        # Ask for expires_in if not present
        if 'expires_in' not in tokens:
            expires_in = input("Enter expires_in (default 3600): ").strip()
            tokens['expires_in'] = int(expires_in) if expires_in else 3600
        
        save_tokens(tokens)
        print("✅ Access token updated!")
    else:
        print("❌ No token entered")

def update_refresh_token():
    """Manually update refresh token"""
    tokens = load_tokens() or {}
    
    print("\n🔄 MANUAL REFRESH TOKEN UPDATE")
    print("-" * 30)
    current_token = tokens.get('refresh_token', 'None')
    print(f"Current token: {current_token[:20]}..." if current_token != 'None' else "Current token: None")
    
    new_token = input("Enter new refresh token: ").strip()
    if new_token:
        tokens['refresh_token'] = new_token
        save_tokens(tokens)
        print("✅ Refresh token updated!")
    else:
        print("❌ No token entered")

def clear_tokens():
    """Clear all tokens"""
    print("\n🗑️  CLEAR ALL TOKENS")
    print("-" * 20)
    confirm = input("Are you sure you want to clear all tokens? (y/N): ").lower()
    if confirm == 'y':
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
            print("✅ Tokens cleared!")
        else:
            print("⚠️ No token file to clear")
    else:
        print("❌ Operation cancelled")

def test_tokens():
    """Test current tokens by checking API connection"""
    print("\n🧪 TESTING TOKENS")
    print("-" * 20)
    
    tokens = load_tokens()
    if not tokens:
        print("❌ No tokens to test")
        return
    
    # Test API connection
    if test_api_connection():
        print("✅ SDK connection successful")
    else:
        print("❌ SDK connection failed")
    
    # Check token expiry
    if is_token_expired(tokens):
        print("⚠️ Tokens are expired")
        
        if tokens.get('refresh_token'):
            refresh_choice = input("Do you want to refresh now? (y/N): ").lower()
            if refresh_choice == 'y':
                auto_refresh_token()
    else:
        print("✅ Tokens are valid")

def main():
    print("🔑 LAZADA TOKEN MANAGER")
    print("=" * 40)
    print("Integrates with get_lazada_tokens.py and lazada_tokens.json")
    
    while True:
        print("\n" + "=" * 40)
        print("Options:")
        print("1. 📋 Display current tokens")
        print("2. 🔄 Auto-refresh expired tokens")
        print("3. ✏️  Manually update access token")
        print("4. ✏️  Manually update refresh token")
        print("5. 🧪 Test tokens and API connection")
        print("6. 🗑️  Clear all tokens")
        print("7. ❌ Exit")
        
        choice = input("\nChoose option (1-7): ").strip()
        
        if choice == "1":
            display_tokens()
        elif choice == "2":
            auto_refresh_token()
        elif choice == "3":
            update_access_token()
        elif choice == "4":
            update_refresh_token()
        elif choice == "5":
            test_tokens()
        elif choice == "6":
            clear_tokens()
        elif choice == "7":
            print("\n👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()