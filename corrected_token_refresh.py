"""
CORRECTED Lazada Token Refresh - Fixed Signature Generation
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

def fixed_token_refresh():
    """Token refresh with CORRECTED signature generation"""
    
    print("🔧 CORRECTED TOKEN REFRESH")
    print("=" * 50)
    
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([refresh_token, app_key, app_secret]):
        print("❌ Missing required credentials")
        return False
    
    # CRITICAL FIX: Use correct API path format
    api_path = '/auth/token/refresh'
    timestamp = str(int(time.time() * 1000))
    
    print(f"🕐 Timestamp: {timestamp}")
    print(f"🔑 App Key: {app_key}")
    print(f"🔄 Refresh Token: {refresh_token[:20]}...")
    
    parameters = {
        'app_key': app_key,
        'refresh_token': refresh_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    print(f"\n🔐 CORRECTED SIGNATURE GENERATION:")
    
    # Step 1: Sort parameters
    sorted_params = sorted(parameters.items())
    print(f"1. Sorted params: {[f'{k}={v[:20]}...' if len(str(v)) > 20 else f'{k}={v}' for k, v in sorted_params]}")
    
    # Step 2: Create query string
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    print(f"2. Query string: {query_string[:100]}...")
    
    # Step 3: CRITICAL FIX - Add proper separator between path and query
    string_to_sign = api_path + query_string  # This was the issue - missing separator!
    print(f"3. String to sign: {string_to_sign[:100]}...")
    
    # Step 4: Generate signature
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    print(f"4. Signature: {signature}")
    
    parameters['sign'] = signature
    
    # Make request to production endpoint
    url = 'https://auth.lazada.com/rest' + api_path
    
    print(f"\n🌐 Request URL: {url}")
    print(f"📤 Method: POST")
    print(f"📋 Parameters: {list(parameters.keys())}")
    
    try:
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"\n📈 Status: {response.status_code}")
        print(f"📋 Response: {response.text}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                if 'access_token' in result:
                    print("\n🎉 TOKEN REFRESH SUCCESS!")
                    print(f"🔑 New access token: {result['access_token'][:30]}...")
                    
                    if 'refresh_token' in result:
                        print(f"🔄 New refresh token: {result['refresh_token'][:30]}...")
                    
                    # Update .env file
                    update_env_file(result)
                    return True, result
                    
                else:
                    print(f"\n❌ No access token in response: {result}")
                    
                    # Check for specific error messages
                    if result.get('code') == 'IncompleteSignature':
                        print("🔍 SIGNATURE ISSUE DETECTED")
                        print("Trying alternative signature method...")
                        return try_alternative_signature(parameters, api_path, app_secret, url)
                    
                    return False, result
                    
            except Exception as e:
                print(f"\n❌ JSON parsing error: {e}")
                return False, str(e)
        else:
            print(f"\n❌ HTTP error: {response.status_code}")
            return False, f"HTTP {response.status_code}"
            
    except Exception as e:
        print(f"\n❌ Request error: {e}")
        return False, str(e)

def try_alternative_signature(parameters, api_path, app_secret, url):
    """Try alternative signature generation method"""
    
    print("\n🔄 TRYING ALTERNATIVE SIGNATURE METHOD")
    print("=" * 50)
    
    # Remove existing signature
    params_without_sign = {k: v for k, v in parameters.items() if k != 'sign'}
    
    # Method 1: Try with URL encoding
    sorted_params = sorted(params_without_sign.items())
    
    # Create query string with URL encoding
    import urllib.parse
    query_parts = []
    for k, v in sorted_params:
        encoded_value = urllib.parse.quote_plus(str(v))
        query_parts.append(f'{k}={encoded_value}')
    
    query_string = '&'.join(query_parts)
    string_to_sign = api_path + query_string
    
    print(f"🔐 Alternative string to sign: {string_to_sign[:100]}...")
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    print(f"🔐 Alternative signature: {signature}")
    
    params_without_sign['sign'] = signature
    
    try:
        response = requests.post(url, data=params_without_sign, timeout=30)
        
        print(f"📈 Alternative Status: {response.status_code}")
        print(f"📋 Alternative Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            result = response.json()
            
            if 'access_token' in result:
                print("✅ ALTERNATIVE METHOD SUCCESS!")
                update_env_file(result)
                return True, result
            else:
                print("❌ Alternative method also failed")
                return False, result
        else:
            print(f"❌ Alternative HTTP error: {response.status_code}")
            return False, f"HTTP {response.status_code}"
            
    except Exception as e:
        print(f"❌ Alternative request error: {e}")
        return False, str(e)

def update_env_file(token_data):
    """Update .env file with new tokens"""
    
    print(f"\n💾 UPDATING .ENV FILE")
    print("=" * 30)
    
    try:
        env_file = '.env'
        
        # Read current file
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        updated_lines = []
        access_updated = False
        refresh_updated = False
        
        for line in lines:
            if line.startswith('LAZADA_ACCESS_TOKEN='):
                updated_lines.append(f'LAZADA_ACCESS_TOKEN={token_data["access_token"]}\n')
                access_updated = True
                print(f"✅ Updated access token")
            elif line.startswith('LAZADA_REFRESH_TOKEN=') and 'refresh_token' in token_data:
                updated_lines.append(f'LAZADA_REFRESH_TOKEN={token_data["refresh_token"]}\n')
                refresh_updated = True
                print(f"✅ Updated refresh token")
            else:
                updated_lines.append(line)
        
        # Add new tokens if not found
        if not access_updated:
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={token_data["access_token"]}\n')
            print(f"✅ Added access token")
        
        if 'refresh_token' in token_data and not refresh_updated:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={token_data["refresh_token"]}\n')
            print(f"✅ Added refresh token")
        
        # Write back to file
        with open(env_file, 'w') as f:
            f.writelines(updated_lines)
        
        print(f"✅ .env file updated successfully")
        
    except Exception as e:
        print(f"❌ Failed to update .env: {e}")

def test_api_with_new_tokens():
    """Test API with newly refreshed tokens"""
    
    print("\n🧪 TESTING API WITH NEW TOKENS")
    print("=" * 50)
    
    # Reload environment
    load_dotenv()
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    # Test seller endpoint
    api_path = '/seller/get'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'access_token': access_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature (using same corrected method)
    sorted_params = sorted(parameters.items())
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    string_to_sign = api_path + query_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    parameters['sign'] = signature
    
    # Test with production endpoint
    url = 'https://api.lazada.com/rest' + api_path
    
    print(f"🔗 Testing: {url}")
    response = requests.get(url, params=parameters, timeout=30)
    
    print(f"📈 Status: {response.status_code}")
    print(f"📋 Response: {response.text[:300]}...")
    
    if response.status_code == 200:
        try:
            result = response.json()
            
            if 'code' in result and result['code'] == '0':
                print("🎉 API TEST SUCCESS!")
                if 'data' in result:
                    print(f"📊 Seller data received: {list(result['data'].keys()) if isinstance(result['data'], dict) else 'Data present'}")
                return True
            else:
                print(f"❌ API error: {result.get('message', 'Unknown')}")
                print(f"   Code: {result.get('code', 'No code')}")
                return False
                
        except Exception as e:
            print(f"❌ JSON error: {e}")
            return False
    else:
        print(f"❌ HTTP error: {response.status_code}")
        return False

def main():
    """Main token refresh and test process"""
    
    print("🔧 CORRECTED LAZADA TOKEN REFRESH")
    print("=" * 60)
    print("🩹 Using fixed signature generation")
    print()
    
    # Try token refresh
    success, result = fixed_token_refresh()
    
    if success:
        print("\n🎉 TOKEN REFRESH SUCCESSFUL!")
        
        # Test API with new tokens
        api_success = test_api_with_new_tokens()
        
        if api_success:
            print("\n🚀 COMPLETE SUCCESS!")
            print("✅ Tokens refreshed")
            print("✅ API calls working")
            print("✅ Your Lazada integration is ready!")
        else:
            print("\n⚠️ Tokens refreshed but API still has issues")
            print("Check app permissions in Lazada Developer Console")
    else:
        print("\n❌ TOKEN REFRESH FAILED")
        print("🔧 NEXT STEPS:")
        print("1. Your refresh token may be expired")
        print("2. App may be suspended in Lazada Developer Console")
        print("3. Try fresh OAuth authorization:")
        print("   python secure_oauth_callback.py")
        print("4. Check if app is approved for production")

if __name__ == "__main__":
    main()