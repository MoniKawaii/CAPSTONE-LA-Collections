"""
Lazada Token Management Utility

This script helps you manage Lazada tokens in the lazada_tokens.json file.
"""

import json
import os
from datetime import datetime

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
    print(f"Tokens saved to {TOKEN_FILE}")

def display_tokens():
    """Display current tokens"""
    tokens = load_tokens()
    
    if not tokens:
        print("No tokens found in lazada_tokens.json")
        return
    
    print("📋 Current Lazada Tokens:")
    print("=" * 30)
    print(f"Access Token: {tokens['access_token'][:20]}...")
    print(f"Refresh Token: {tokens.get('refresh_token', 'N/A')[:20]}...")
    print(f"Expires In: {tokens.get('expires_in', 'N/A')} seconds")
    print(f"Account Platform: {tokens.get('account_platform', 'N/A')}")
    
    # Check if expired
    if 'created_at' in tokens and 'expires_in' in tokens:
        import time
        created_at = tokens['created_at']
        expires_in = tokens['expires_in']
        current_time = int(time.time())
        
        if current_time > (created_at + expires_in):
            print("Token is EXPIRED")
        else:
            remaining = (created_at + expires_in) - current_time
            print(f"Token expires in {remaining} seconds")

def update_access_token():
    """Manually update access token"""
    tokens = load_tokens() or {}
    
    new_token = input("Enter new access token: ").strip()
    if new_token:
        tokens['access_token'] = new_token
        tokens['created_at'] = int(time.time())
        save_tokens(tokens)
        print("Access token updated!")

def update_refresh_token():
    """Manually update refresh token"""
    tokens = load_tokens() or {}
    
    new_token = input("Enter new refresh token: ").strip()
    if new_token:
        tokens['refresh_token'] = new_token
        save_tokens(tokens)
        print("Refresh token updated!")

def clear_tokens():
    """Clear all tokens"""
    confirm = input("Are you sure you want to clear all tokens? (y/N): ").lower()
    if confirm == 'y':
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
            print("Tokens cleared!")
        else:
            print("No token file to clear")

def main():
    print("🔑 Lazada Token Management")
    print("=" * 30)
    
    while True:
        print("\nOptions:")
        print("1. Display current tokens")
        print("2. Update access token")
        print("3. Update refresh token")
        print("4. Clear all tokens")
        print("5. Exit")
        
        choice = input("\nChoose option (1-5): ").strip()
        
        if choice == "1":
            display_tokens()
        elif choice == "2":
            update_access_token()
        elif choice == "3":
            update_refresh_token()
        elif choice == "4":
            clear_tokens()
        elif choice == "5":
            print("Goodbye!")
            break
        else:
            print("Invalid choice")

if __name__ == "__main__":
    import time
    main()