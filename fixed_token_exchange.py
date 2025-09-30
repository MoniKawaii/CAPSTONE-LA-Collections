"""
Fixed Token Exchange for Lazada - Corrected Signature Format
"""

import os
import time
import hmac
import hashlib
import requests
import json
import urllib.parse
from dotenv import load_dotenv, set_key

def exchange_auth_code_for_tokens():
    """Exchange authorization code for access and refresh tokens"""
    
    load_dotenv()
    
    print("🔄 LAZADA TOKEN EXCHANGE (FIXED)")
    print("=" * 40)
    
    # Get credentials from environment
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([app_key, app_secret]):
        print("❌ Missing app credentials in .env file")
        return False
    
    print(f"🔑 App Key: {app_key}")
    
    # Get authorization code from user
    auth_code = input("\n📝 Enter your authorization code: ").strip()
    
    if not auth_code:
        print("❌ No authorization code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code}")
    
    # Prepare token exchange request - CORRECTED FORMAT
    api_path = '/auth/token/create'
    timestamp = str(int(time.time() * 1000))
    
    # Parameters in exact order for signature
    parameters = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    print(f"\n📋 Parameters:")
    for k, v in parameters.items():
        print(f"   {k}: {v}")
    
    # Generate signature - FIXED METHOD
    # 1. Sort parameters alphabetically
    sorted_params = sorted(parameters.items())
    
    # 2. Create query string WITHOUT URL encoding for signature
    query_parts = []
    for k, v in sorted_params:
        query_parts.append(f"{k}{v}")
    
    # 3. Create string to sign: API_PATH + concatenated_params
    string_to_sign = api_path + ''.join(query_parts)
    
    print(f"\n🔐 Signature Generation:")
    print(f"   API Path: {api_path}")
    print(f"   Sorted params: {sorted_params}")
    print(f"   String to sign: {string_to_sign}")
    
    # 4. Generate HMAC-SHA256 signature
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    print(f"   Generated signature: {signature}")
    
    # Add signature to parameters
    parameters['sign'] = signature
    
    # Make token exchange request
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        print(f"\n🌐 Making request to: {url}")
        print(f"📤 Request parameters:")
        for k, v in parameters.items():
            display_v = v if k != 'sign' else f"{v[:20]}..."
            print(f"   {k}: {display_v}")
        
        # Use POST method with form data
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"\n📈 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"📋 Response JSON: {json.dumps(result, indent=2)}")
            except:
                print(f"📋 Response Text: {response.text}")
                result = {}
            
            # Check for success
            if result.get('code') == '0' or 'access_token' in result:
                # Success! Extract tokens
                access_token = result.get('access_token')
                refresh_token = result.get('refresh_token')
                expires_in = result.get('expires_in')
                
                if access_token:
                    print("\n🎉 SUCCESS! Tokens generated!")
                    print(f"✅ Access Token: {access_token}")
                    print(f"✅ Refresh Token: {refresh_token}" if refresh_token else "❌ No refresh token")
                    print(f"⏰ Expires in: {expires_in} seconds" if expires_in else "⏰ No expiry info")
                    
                    # Update .env file
                    env_file = '.env'
                    
                    print(f"\n💾 Updating {env_file}...")
                    
                    set_key(env_file, 'LAZADA_ACCESS_TOKEN', access_token)
                    if refresh_token:
                        set_key(env_file, 'LAZADA_REFRESH_TOKEN', refresh_token)
                    
                    # Add timestamp for tracking
                    set_key(env_file, 'LAZADA_TOKEN_GENERATED', str(int(time.time())))
                    
                    print("✅ Tokens saved to .env file!")
                    
                    return True
                else:
                    print("❌ No access token in response")
                    return False
                    
            else:
                print(f"❌ Token exchange failed!")
                error_code = result.get('code', 'Unknown')
                error_msg = result.get('message', 'No message')
                
                print(f"   Code: {error_code}")
                print(f"   Message: {error_msg}")
                
                # Detailed error analysis
                if error_code == 'IncompleteSignature':
                    print("\n🔍 SIGNATURE DEBUG:")
                    print(f"   App Secret (first 10 chars): {app_secret[:10]}...")
                    print(f"   String to sign: {string_to_sign}")
                    print(f"   Generated signature: {signature}")
                    
                    # Try alternative signature method
                    print("\n🔄 Trying alternative signature method...")
                    alt_query = '&'.join([f"{k}={v}" for k, v in sorted_params])
                    alt_string = api_path + alt_query
                    alt_signature = hmac.new(
                        app_secret.encode('utf-8'),
                        alt_string.encode('utf-8'),
                        hashlib.sha256
                    ).hexdigest().upper()
                    
                    print(f"   Alt string to sign: {alt_string}")
                    print(f"   Alt signature: {alt_signature}")
                
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 FIXED LAZADA TOKEN EXCHANGE")
    print("=" * 50)
    
    # Show current token status first
    load_dotenv()
    current_token = os.getenv('LAZADA_ACCESS_TOKEN')
    print(f"📋 Current token: {current_token[:30]}..." if current_token else "📋 No current token")
    
    success = exchange_auth_code_for_tokens()
    
    if success:
        print("\n🎉 TOKEN EXCHANGE SUCCESSFUL!")
        print("🚀 Next step: Run 'python quick_test_api.py' to test")
    else:
        print("\n❌ Token exchange failed")
        print("💡 Double-check your authorization code and try again")