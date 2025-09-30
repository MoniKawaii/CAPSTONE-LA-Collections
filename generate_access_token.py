"""
Generate Lazada Access Token
Simple script to exchange authorization code for access token
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv, set_key

def generate_access_token():
    """Generate access token from authorization code"""
    
    load_dotenv()
    
    print("🎯 GENERATE LAZADA ACCESS TOKEN")
    print("=" * 40)
    
    # Get credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([app_key, app_secret]):
        print("❌ Missing app credentials in .env")
        return False
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔐 App Secret: {app_secret}")
    
    # Get authorization code
    print(f"\n📝 Enter your authorization code:")
    print(f"   (Get from: https://auth.lazada.com/oauth/authorize?response_type=code&force_auth=true&redirect_uri=https://oauth.pstmn.io/v1/callback&client_id={app_key})")
    
    auth_code = input("\nAuthorization code: ").strip()
    
    if not auth_code:
        print("❌ No authorization code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code}")
    
    # Exchange for tokens
    timestamp = str(int(time.time() * 1000))
    
    # Parameters
    params = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
    api_path = '/auth/token/create'
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    string_to_sign = api_path + param_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params['sign'] = signature
    
    print(f"\n🔐 Generating signature...")
    print(f"   String to sign: {string_to_sign}")
    print(f"   Signature: {signature}")
    
    # Make token request
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        print(f"\n🌐 Requesting tokens from: {url}")
        
        response = requests.post(url, data=params, timeout=30)
        print(f"📈 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"📋 Response: {result}")
            
            # Check for tokens
            if 'access_token' in result:
                access_token = result['access_token']
                refresh_token = result.get('refresh_token')
                expires_in = result.get('expires_in')
                
                print(f"\n🎉 ACCESS TOKEN GENERATED SUCCESSFULLY!")
                print(f"✅ Access Token: {access_token}")
                print(f"✅ Refresh Token: {refresh_token}" if refresh_token else "❌ No refresh token")
                print(f"⏰ Expires in: {expires_in} seconds" if expires_in else "⏰ No expiry info")
                
                # Save to .env file
                print(f"\n💾 Saving tokens to .env file...")
                
                set_key('.env', 'LAZADA_ACCESS_TOKEN', access_token)
                if refresh_token:
                    set_key('.env', 'LAZADA_REFRESH_TOKEN', refresh_token)
                
                # Add generation timestamp
                set_key('.env', 'LAZADA_TOKEN_GENERATED_AT', str(int(time.time())))
                
                print(f"✅ Tokens saved to .env file!")
                
                return True
                
            elif result.get('code') == '0':
                print(f"✅ Request successful but check response format")
                print(f"📋 Full response: {result}")
                return False
                
            else:
                error_code = result.get('code', 'Unknown')
                error_msg = result.get('message', 'No message')
                
                print(f"\n❌ TOKEN GENERATION FAILED!")
                print(f"   Error Code: {error_code}")
                print(f"   Error Message: {error_msg}")
                
                # Specific error help
                if error_code == 'InvalidAuthorizationCode':
                    print(f"\n💡 SOLUTION:")
                    print(f"   • Get a fresh authorization code (expires in 10 minutes)")
                    print(f"   • Make sure you copied the full code from the redirect URL")
                elif error_code == 'IncompleteSignature':
                    print(f"\n💡 SOLUTION:")
                    print(f"   • Double-check your app secret in Lazada Developer Console")
                    print(f"   • Ensure app is approved for production")
                
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_generated_token():
    """Test the newly generated token"""
    
    print(f"\n🧪 TESTING GENERATED TOKEN")
    print(f"=" * 30)
    
    load_dotenv()  # Reload to get new token
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not access_token:
        print(f"❌ No access token found in .env")
        return False
    
    print(f"🔍 Testing token: {access_token[:30]}...")
    
    # Test with seller endpoint
    timestamp = str(int(time.time() * 1000))
    
    params = {
        'app_key': app_key,
        'access_token': access_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
    api_path = '/seller/get'
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    string_to_sign = api_path + param_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params['sign'] = signature
    
    # Make API call
    url = 'https://api.lazada.com/rest' + api_path
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print(f"🎉 TOKEN TEST SUCCESS!")
                print(f"✅ Your Lazada API integration is WORKING!")
                
                # Show seller info
                if 'data' in result:
                    seller = result['data']
                    print(f"\n📊 Connected Seller:")
                    print(f"   Seller ID: {seller.get('seller_id', 'N/A')}")
                    print(f"   Name: {seller.get('name', 'N/A')}")
                    print(f"   Email: {seller.get('email', 'N/A')}")
                
                return True
            else:
                print(f"❌ API Error: {result.get('code')} - {result.get('message')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 LAZADA ACCESS TOKEN GENERATOR")
    print("=" * 50)
    
    # Generate access token
    success = generate_access_token()
    
    if success:
        print(f"\n" + "="*50)
        # Test the token
        if test_generated_token():
            print(f"\n🚀 CONGRATULATIONS!")
            print(f"   ✅ Your Lazada access token is working!")
            print(f"   ✅ You can now access Lazada API!")
            print(f"   ✅ No more manual login required!")
        else:
            print(f"\n⚠️ Token generated but test failed")
    else:
        print(f"\n❌ Token generation failed")
        print(f"💡 Check the error above and try again")