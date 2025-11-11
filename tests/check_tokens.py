#!/usr/bin/env python3
"""
Check Lazada token status and expiry
"""

import json
from datetime import datetime

def check_token_status():
    # Load token file
    try:
        with open('tokens/lazada_tokens.json', 'r') as f:
            tokens = json.load(f)
    except FileNotFoundError:
        print("❌ Token file not found")
        return
    except json.JSONDecodeError:
        print("❌ Invalid token file format")
        return

    print('📊 Lazada Token Analysis:')
    print(f'   Access token: {tokens["access_token"][:20]}...')
    print(f'   Created at: {tokens["created_at"]}')
    print(f'   Expires in: {tokens["expires_in"]} seconds')

    # Convert timestamp to readable date
    if tokens['created_at']:
        created_date = datetime.fromtimestamp(tokens['created_at'])
        print(f'   Created date: {created_date}')
        
        # Check if expired (tokens last 7 days = 604800 seconds)
        now = datetime.now()
        age_seconds = (now - created_date).total_seconds()
        expires_in = tokens['expires_in']
        
        print(f'   Token age: {age_seconds:.0f} seconds')
        print(f'   Is expired: {age_seconds > expires_in}')
        
        if age_seconds > expires_in:
            print('   ❌ Token is expired and needs refresh')
            print('   💡 The API calls are failing because tokens are expired')
            print('   🔄 Please refresh tokens via Lazada authorization flow')
            return False
        else:
            print('   ✅ Token should still be valid')
            print('   🤔 API errors may be due to other issues')
            return True
    else:
        print('   ⚠️ No creation timestamp found')
        return False

if __name__ == "__main__":
    check_token_status()