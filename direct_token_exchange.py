"""
Direct HTTP Token Exchange - Bypass LazOP SDK
This uses raw HTTP requests to avoid any SDK signature issues
"""

import os
import time
import hmac
import hashlib
import requests
import json
from dotenv import load_dotenv, set_key
from urllib.parse import urlencode

def direct_token_exchange():
    """Exchange auth code using direct HTTP request (no SDK)"""
    
    load_dotenv()
    
    print("🔄 DIRECT HTTP TOKEN EXCHANGE")
    print("=" * 40)
    
    # Get credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([app_key, app_secret]):
        print("❌ Missing credentials")
        return False
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔐 App Secret: {app_secret[:10]}...")
    
    # Get fresh auth code
    auth_code = input("\n📝 Enter your NEW authorization code: ").strip()
    
    if not auth_code:
        print("❌ No auth code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code}")
    
    # Prepare request parameters
    timestamp = str(int(time.time() * 1000))
    
    # Method 1: Try the exact format from Lazada docs
    print(f"\n🔬 METHOD 1: Standard Format")
    
    params = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature - concatenated format
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
    
    print(f"   String to sign: {string_to_sign}")
    print(f"   Signature: {signature}")
    
    # Make request
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        response = requests.post(url, data=params, timeout=30)
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"   Response: {result}")
            
            if 'access_token' in result:
                print(f"🎉 METHOD 1 SUCCESS!")
                save_tokens(result)
                return True
            elif result.get('code') != 'IncompleteSignature':
                print(f"   Different error: {result.get('code')}")
    
    except Exception as e:
        print(f"   Error: {e}")
    
    # Method 2: Try URL encoding
    print(f"\n🔬 METHOD 2: URL Encoded Format")
    
    params2 = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # URL encode the auth code
    from urllib.parse import quote
    encoded_code = quote(auth_code, safe='')
    params2['code'] = encoded_code
    
    sorted_params2 = sorted(params2.items())
    param_string2 = ''.join([f"{k}{v}" for k, v in sorted_params2])
    string_to_sign2 = api_path + param_string2
    
    signature2 = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign2.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params2['sign'] = signature2
    
    print(f"   Encoded code: {encoded_code}")
    print(f"   String to sign: {string_to_sign2}")
    print(f"   Signature: {signature2}")
    
    try:
        response2 = requests.post(url, data=params2, timeout=30)
        print(f"   Status: {response2.status_code}")
        
        if response2.status_code == 200:
            result2 = response2.json()
            print(f"   Response: {result2}")
            
            if 'access_token' in result2:
                print(f"🎉 METHOD 2 SUCCESS!")
                save_tokens(result2)
                return True
    
    except Exception as e:
        print(f"   Error: {e}")
    
    # Method 3: Try query string format
    print(f"\n🔬 METHOD 3: Query String Format")
    
    params3 = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    sorted_params3 = sorted(params3.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params3])
    string_to_sign3 = api_path + query_string
    
    signature3 = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign3.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params3['sign'] = signature3
    
    print(f"   Query string: {query_string}")
    print(f"   String to sign: {string_to_sign3}")
    print(f"   Signature: {signature3}")
    
    try:
        response3 = requests.post(url, data=params3, timeout=30)
        print(f"   Status: {response3.status_code}")
        
        if response3.status_code == 200:
            result3 = response3.json()
            print(f"   Response: {result3}")
            
            if 'access_token' in result3:
                print(f"🎉 METHOD 3 SUCCESS!")
                save_tokens(result3)
                return True
    
    except Exception as e:
        print(f"   Error: {e}")
    
    print(f"\n❌ All methods failed with signature issues")
    print(f"💡 This suggests an app configuration problem")
    
    return False

def save_tokens(token_data):
    """Save tokens to .env file"""
    
    access_token = token_data.get('access_token')
    refresh_token = token_data.get('refresh_token')
    
    if access_token:
        set_key('.env', 'LAZADA_ACCESS_TOKEN', access_token)
        print(f"✅ Access token saved: {access_token[:30]}...")
    
    if refresh_token:
        set_key('.env', 'LAZADA_REFRESH_TOKEN', refresh_token)
        print(f"✅ Refresh token saved: {refresh_token[:30]}...")
    
    set_key('.env', 'LAZADA_TOKEN_GENERATED', str(int(time.time())))
    print(f"✅ Tokens saved to .env file!")

def verify_app_config():
    """Verify app configuration with Lazada"""
    
    print(f"\n🔍 APP CONFIGURATION VERIFICATION")
    print(f"=" * 40)
    
    load_dotenv()
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    print(f"📋 Your App Configuration:")
    print(f"   App Key: {app_key}")
    print(f"   App Secret: {app_secret[:10]}..." if app_secret else "❌ Missing")
    
    print(f"\n💡 TROUBLESHOOTING CHECKLIST:")
    print(f"   1. ✓ Check Lazada Developer Console:")
    print(f"      → https://open.lazada.com/apps/myapp")
    print(f"      → App ID: {app_key}")
    print(f"      → Status should be 'Active' or 'Approved'")
    print(f"   ")
    print(f"   2. ✓ Verify App Secret:")
    print(f"      → Copy exact secret from developer console")
    print(f"      → No extra spaces or characters")
    print(f"   ")
    print(f"   3. ✓ Check Redirect URI:")
    print(f"      → Must match exactly in app settings")
    print(f"      → https://oauth.pstmn.io/v1/callback")
    print(f"   ")
    print(f"   4. ✓ Verify App Permissions:")
    print(f"      → API access enabled")
    print(f"      → Production environment approved")

if __name__ == "__main__":
    print("🎯 DIRECT HTTP TOKEN EXCHANGE")
    print("=" * 50)
    
    # First verify app config
    verify_app_config()
    
    # Try direct token exchange
    success = direct_token_exchange()
    
    if success:
        print(f"\n🎉 TOKEN EXCHANGE SUCCESSFUL!")
        print(f"🚀 You can now use Lazada API!")
    else:
        print(f"\n❌ Token exchange failed")
        print(f"💡 Check app configuration in Lazada Developer Console")