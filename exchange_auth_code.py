"""
Exchange Authorization Code for Fresh Lazada Tokens
This script takes your auth code and generates new access/refresh tokens
"""

import os
import time
import hmac
import hashlib
import requests
import json
from dotenv import load_dotenv, set_key

def exchange_auth_code_for_tokens():
    """Exchange authorization code for access and refresh tokens"""
    
    load_dotenv()
    
    print("🔄 LAZADA TOKEN EXCHANGE")
    print("=" * 40)
    
    # Get credentials from environment
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    redirect_uri = os.getenv('LAZADA_REDIRECT_URI')
    
    if not all([app_key, app_secret]):
        print("❌ Missing app credentials in .env file")
        print("   Need: LAZADA_APP_KEY, LAZADA_APP_SECRET")
        return False
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔄 Redirect URI: {redirect_uri}")
    
    # Get authorization code from user
    auth_code = input("\n📝 Enter your authorization code: ").strip()
    
    if not auth_code:
        print("❌ No authorization code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code[:20]}...")
    
    # Prepare token exchange request
    api_path = '/auth/token/create'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'sign_method': 'sha256',
        'timestamp': timestamp,
        'code': auth_code
    }
    
    # Generate signature for token exchange
    sorted_params = sorted(parameters.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
    string_to_sign = api_path + query_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    parameters['sign'] = signature
    
    # Make token exchange request
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        print(f"\n🌐 Calling token endpoint...")
        print(f"   URL: {url}")
        
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"📈 Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"📋 Response: {json.dumps(result, indent=2)}")
            
            if result.get('code') == '0' or 'access_token' in result:
                # Success! Extract tokens
                access_token = result.get('access_token')
                refresh_token = result.get('refresh_token')
                expires_in = result.get('expires_in')
                
                if access_token:
                    print("\n🎉 SUCCESS! Tokens generated!")
                    print(f"✅ Access Token: {access_token[:30]}...")
                    print(f"✅ Refresh Token: {refresh_token[:30]}..." if refresh_token else "❌ No refresh token")
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
                print(f"   Code: {result.get('code', 'No code')}")
                print(f"   Message: {result.get('message', 'No message')}")
                
                # Common error explanations
                error_code = result.get('code')
                if error_code == 'InvalidAuthorizationCode':
                    print("\n💡 Possible causes:")
                    print("   • Authorization code already used")
                    print("   • Authorization code expired (10 minutes)")
                    print("   • Wrong authorization code")
                elif error_code == 'IncompleteSignature':
                    print("\n💡 Possible causes:")
                    print("   • App secret incorrect")
                    print("   • Timestamp too old")
                
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def verify_new_tokens():
    """Quick verification that new tokens work"""
    
    print("\n🧪 VERIFYING NEW TOKENS")
    print("=" * 30)
    
    load_dotenv()  # Reload to get new tokens
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    if not access_token:
        print("❌ No access token found")
        return False
    
    print(f"🔍 Testing access token: {access_token[:20]}...")
    
    # Quick test with seller endpoint
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    api_path = '/seller/get'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'access_token': access_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
    sorted_params = sorted(parameters.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
    string_to_sign = api_path + query_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    parameters['sign'] = signature
    
    # Test API call
    url = 'https://api.lazada.com/rest' + api_path
    
    try:
        response = requests.get(url, params=parameters, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print("✅ Token verification SUCCESS!")
                print("🚀 Ready to use Lazada API!")
                return True
            else:
                print(f"❌ API Error: {result.get('message', 'Unknown')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 FRESH LAZADA TOKEN GENERATION")
    print("=" * 50)
    
    success = exchange_auth_code_for_tokens()
    
    if success:
        print("\n" + "="*50)
        verify_success = verify_new_tokens()
        
        if verify_success:
            print("\n🎉 ALL DONE! Your Lazada integration is now working!")
            print("🚀 You can now:")
            print("   • Pull product data from Lazada")
            print("   • Run your ETL pipeline")
            print("   • Use all Lazada API endpoints")
        else:
            print("\n⚠️ Tokens generated but verification failed")
            print("💡 Try running: python quick_test_api.py")
    else:
        print("\n❌ Token generation failed")
        print("💡 Get a fresh authorization code and try again")