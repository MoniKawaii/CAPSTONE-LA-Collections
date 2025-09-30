"""
Quick Lazada Token Status and Fix
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def decode_token_info():
    """Analyze token to understand its status"""
    
    print("🔍 TOKEN ANALYSIS")
    print("=" * 50)
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    print(f"Access Token: {access_token[:30]}...")
    print(f"Refresh Token: {refresh_token[:30]}...")
    
    # Try to decode token timestamps (if they contain timestamp info)
    try:
        # Lazada tokens usually start with seller ID + timestamp
        if access_token.startswith('50000'):
            seller_part = access_token[:15]
            print(f"Seller ID from token: {seller_part}")
            
            # Check if this matches env
            env_seller = os.getenv('LAZADA_SELLER_ID')
            if env_seller and seller_part.startswith(env_seller[:5]):
                print("✅ Token appears to match seller ID")
            else:
                print("⚠️ Token seller ID mismatch")
    
    except Exception as e:
        print(f"Could not analyze token: {e}")

def test_environment_detection():
    """Detect if we're in sandbox or production"""
    
    print("\n🌍 ENVIRONMENT DETECTION")
    print("=" * 50)
    
    # Check token patterns to determine environment
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    if access_token:
        # Production tokens usually start with specific patterns
        if access_token.startswith('50000'):
            print("🔍 Token pattern suggests: PRODUCTION environment")
            print("📍 Recommended endpoints:")
            print("   API: https://api.lazada.com/rest")
            print("   Auth: https://auth.lazada.com/rest")
        else:
            print("🔍 Token pattern suggests: SANDBOX environment")
            print("📍 Recommended endpoints:")
            print("   API: https://api.lazada.com.ph/rest (Philippines)")
            print("   Auth: https://auth.lazada.com.ph/rest (Philippines)")

def manual_token_refresh():
    """Manual token refresh with detailed debugging"""
    
    print("\n🔄 MANUAL TOKEN REFRESH")
    print("=" * 50)
    
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([refresh_token, app_key, app_secret]):
        print("❌ Missing required credentials")
        return False
    
    # Use production endpoint for refresh
    api_path = '/auth/token/refresh'
    timestamp = str(int(time.time() * 1000))
    
    print(f"🕐 Using timestamp: {timestamp}")
    print(f"🔑 App Key: {app_key}")
    print(f"🔄 Refresh Token: {refresh_token[:20]}...")
    
    parameters = {
        'app_key': app_key,
        'refresh_token': refresh_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    print(f"📋 Parameters before signing: {list(parameters.keys())}")
    
    # Generate signature step by step
    print(f"\n🔐 SIGNATURE GENERATION:")
    sorted_params = sorted(parameters.items())
    print(f"1. Sorted params: {sorted_params}")
    
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    print(f"2. Query string: {query_string}")
    
    string_to_sign = api_path + query_string
    print(f"3. String to sign: {string_to_sign}")
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    print(f"4. Generated signature: {signature}")
    
    parameters['sign'] = signature
    
    # Try refresh with production endpoint
    url = 'https://auth.lazada.com/rest' + api_path
    
    print(f"\n🌐 Making request to: {url}")
    print(f"📤 Request method: POST")
    print(f"📋 Final parameters: {list(parameters.keys())}")
    
    try:
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"\n📈 Response Status: {response.status_code}")
        print(f"📄 Response Headers: {dict(response.headers)}")
        print(f"📋 Response Body: {response.text}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                if 'access_token' in result:
                    print("\n🎉 TOKEN REFRESH SUCCESS!")
                    print(f"🔑 New access token: {result['access_token'][:30]}...")
                    
                    if 'refresh_token' in result:
                        print(f"🔄 New refresh token: {result['refresh_token'][:30]}...")
                    
                    # Update .env file
                    update_env_with_new_tokens(result)
                    return True
                    
                else:
                    print(f"\n❌ Refresh failed: {result}")
                    return False
                    
            except Exception as e:
                print(f"\n❌ JSON parsing failed: {e}")
                return False
        else:
            print(f"\n❌ HTTP error {response.status_code}")
            return False
            
    except Exception as e:
        print(f"\n❌ Request failed: {e}")
        return False

def update_env_with_new_tokens(token_data):
    """Update .env file with new tokens"""
    
    print("\n💾 UPDATING .ENV FILE")
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

def test_with_fresh_tokens():
    """Test API with fresh tokens"""
    
    print("\n🧪 TESTING WITH FRESH TOKENS")
    print("=" * 50)
    
    # Reload environment to get updated tokens
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
    
    # Generate signature
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
    print(f"📋 Response: {response.text[:200]}...")
    
    if response.status_code == 200:
        try:
            result = response.json()
            
            if 'code' in result and result['code'] == '0':
                print("🎉 API TEST SUCCESS with fresh tokens!")
                return True
            else:
                print(f"❌ API error: {result.get('message', 'Unknown')}")
                return False
                
        except Exception as e:
            print(f"❌ JSON error: {e}")
            return False
    else:
        print(f"❌ HTTP error: {response.status_code}")
        return False

def main():
    """Main diagnostic and fix process"""
    
    print("🔧 LAZADA TOKEN STATUS & FIX")
    print("=" * 60)
    print("🩺 Analyzing and fixing token issues")
    print()
    
    # Step 1: Analyze current tokens
    decode_token_info()
    
    # Step 2: Detect environment
    test_environment_detection()
    
    # Step 3: Try manual refresh
    print(f"\n🔄 Attempting token refresh...")
    refresh_success = manual_token_refresh()
    
    # Step 4: Test with fresh tokens if refresh worked
    if refresh_success:
        test_success = test_with_fresh_tokens()
        
        if test_success:
            print("\n🎉 PROBLEM SOLVED!")
            print("✅ Tokens refreshed successfully")
            print("✅ API calls now working")
            print("\n🚀 Your Lazada integration is ready!")
        else:
            print("\n⚠️ Tokens refreshed but API still failing")
            print("Check app permissions in Lazada Developer Console")
    else:
        print("\n❌ Token refresh failed")
        print("🔧 SOLUTIONS:")
        print("1. Check if app is active in Lazada Developer Console")
        print("2. Verify you're using correct environment (sandbox vs production)")
        print("3. Try fresh OAuth authorization")
        print("4. Contact Lazada support if app is suspended")

if __name__ == "__main__":
    main()