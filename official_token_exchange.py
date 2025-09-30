"""
Official Lazada Token Exchange - Using Exact API Documentation Format
Based on: https://open.lazada.com/doc/doc.htm?spm=a2o9m.11193531.0.0.33e938e0d3d4yK#/docs/6c9a27dc-c90d-4913-ba82-6d76e4b83b0d
"""

import os
import time
import hmac
import hashlib
import requests
import json
import urllib.parse
from dotenv import load_dotenv, set_key

def official_token_exchange():
    """Official Lazada token exchange using exact documentation format"""
    
    load_dotenv()
    
    print("🔄 OFFICIAL LAZADA TOKEN EXCHANGE")
    print("=" * 45)
    
    # Get credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([app_key, app_secret]):
        print("❌ Missing credentials")
        return False
    
    print(f"🔑 App Key: {app_key}")
    
    # Get auth code
    auth_code = input("\n📝 Enter authorization code: ").strip()
    if not auth_code:
        print("❌ No auth code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code}")
    
    # EXACT API DOCUMENTATION FORMAT
    api_path = '/auth/token/create'
    timestamp = str(int(time.time() * 1000))
    
    # Step 1: Create parameters (EXACT order from docs)
    params = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Step 2: Sort parameters alphabetically by key
    sorted_params = sorted(params.items())
    
    # Step 3: Concatenate in format: key1value1key2value2...
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    
    # Step 4: Prepend API path
    string_to_sign = api_path + param_string
    
    # Step 5: Generate HMAC-SHA256 signature
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    # Add signature to params
    params['sign'] = signature
    
    print(f"\n🔐 Signature Details:")
    print(f"   API Path: {api_path}")
    print(f"   Param String: {param_string}")
    print(f"   String to Sign: {string_to_sign}")
    print(f"   Signature: {signature}")
    
    # Make request
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        print(f"\n🌐 POST Request to: {url}")
        
        # Send as form data
        response = requests.post(url, data=params, timeout=30)
        
        print(f"📈 Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"📋 Response: {json.dumps(result, indent=2)}")
            
            # Check for tokens
            if 'access_token' in result:
                access_token = result['access_token']
                refresh_token = result.get('refresh_token')
                expires_in = result.get('expires_in')
                
                print(f"\n🎉 SUCCESS!")
                print(f"✅ Access Token: {access_token}")
                if refresh_token:
                    print(f"✅ Refresh Token: {refresh_token}")
                if expires_in:
                    print(f"⏰ Expires in: {expires_in} seconds")
                
                # Save to .env
                set_key('.env', 'LAZADA_ACCESS_TOKEN', access_token)
                if refresh_token:
                    set_key('.env', 'LAZADA_REFRESH_TOKEN', refresh_token)
                set_key('.env', 'LAZADA_TOKEN_GENERATED', str(int(time.time())))
                
                print(f"💾 Tokens saved to .env!")
                return True
                
            elif result.get('code') == '0':
                print(f"✅ Request successful but no tokens in response")
                return False
                
            else:
                error_code = result.get('code', 'Unknown')
                error_msg = result.get('message', 'No message')
                
                print(f"❌ Error: {error_code}")
                print(f"   Message: {error_msg}")
                
                # Common error suggestions
                if error_code == 'InvalidAuthorizationCode':
                    print(f"\n💡 Suggestions:")
                    print(f"   • Get fresh auth code (expires in 10 minutes)")
                    print(f"   • Check if code was already used")
                elif error_code == 'IncompleteSignature':
                    print(f"\n💡 Suggestions:")
                    print(f"   • Verify app secret is correct")
                    print(f"   • Check app approval status")
                
                return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_tokens():
    """Test the new tokens"""
    print(f"\n🧪 TESTING NEW TOKENS")
    print(f"=" * 30)
    
    load_dotenv()
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([access_token, app_key, app_secret]):
        print(f"❌ Missing credentials for test")
        return False
    
    # Test with seller info endpoint
    api_path = '/seller/get'
    timestamp = str(int(time.time() * 1000))
    
    params = {
        'app_key': app_key,
        'access_token': access_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    string_to_sign = api_path + param_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params['sign'] = signature
    
    # Test request
    url = 'https://api.lazada.com/rest' + api_path
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print(f"🎉 TOKEN TEST SUCCESS!")
                print(f"✅ Lazada API is working!")
                
                # Show seller info if available
                if 'data' in result:
                    seller = result['data']
                    print(f"📊 Seller ID: {seller.get('seller_id', 'N/A')}")
                    print(f"📊 Name: {seller.get('name', 'N/A')}")
                
                return True
            else:
                print(f"❌ API Error: {result.get('code')} - {result.get('message')}")
                return False
        else:
            print(f"❌ HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 OFFICIAL LAZADA TOKEN EXCHANGE")
    print("=" * 50)
    
    success = official_token_exchange()
    
    if success:
        print(f"\n" + "="*50)
        if test_tokens():
            print(f"\n🚀 ALL SYSTEMS GO!")
            print(f"   Your Lazada integration is fully working!")
        else:
            print(f"\n⚠️ Tokens generated but test failed")
    else:
        print(f"\n❌ Token exchange failed")
        print(f"💡 Check authorization code and try again")