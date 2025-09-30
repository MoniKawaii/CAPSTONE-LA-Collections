"""
Fixed Lazada API Test - Correcting the issues found in diagnostics
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv
import sys
sys.path.append('./app')

load_dotenv()

def test_api_with_correct_method():
    """Test API call with correct HTTP method and endpoint"""
    
    print("🔧 FIXED API TEST")
    print("=" * 50)
    
    try:
        access_token = os.getenv('LAZADA_ACCESS_TOKEN')
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test seller info endpoint with GET method
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
        
        # Test multiple endpoints
        endpoints = [
            ('Philippines API', 'https://api.lazada.com.ph/rest'),
            ('Global API', 'https://api.lazada.com/rest')
        ]
        
        for name, base_url in endpoints:
            print(f"\n🌐 Testing {name}: {base_url}")
            
            # Use GET method with query parameters
            url = base_url + api_path
            
            print(f"🔗 GET request to: {url}")
            print(f"📊 Parameters: {list(parameters.keys())}")
            
            response = requests.get(url, params=parameters, timeout=30)
            
            print(f"📈 Status Code: {response.status_code}")
            print(f"📋 Response: {response.text[:300]}...")
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    
                    if 'code' in result:
                        if result['code'] == '0':
                            print(f"✅ {name} SUCCESS!")
                            return True, result
                        else:
                            print(f"❌ {name} Error: {result.get('message', 'Unknown error')}")
                            print(f"   Code: {result['code']}")
                    else:
                        print(f"✅ {name} SUCCESS (no error code)")
                        return True, result
                        
                except Exception as e:
                    print(f"❌ {name} JSON parsing error: {e}")
            else:
                print(f"❌ {name} HTTP error: {response.status_code}")
        
        return False, "All endpoints failed"
        
    except Exception as e:
        print(f"❌ API test failed: {e}")
        return False, str(e)

def test_token_refresh_fixed():
    """Test token refresh with correct endpoint"""
    
    print("\n🔄 FIXED TOKEN REFRESH TEST")
    print("=" * 50)
    
    try:
        refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Token refresh endpoint
        api_path = '/auth/token/refresh'
        timestamp = str(int(time.time() * 1000))
        
        parameters = {
            'app_key': app_key,
            'refresh_token': refresh_token,
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
        
        # Test multiple auth endpoints
        auth_endpoints = [
            ('Global Auth', 'https://auth.lazada.com/rest'),
            ('Philippines Auth (backup)', 'https://auth.lazada.com.ph/rest')
        ]
        
        for name, base_url in auth_endpoints:
            print(f"\n🔐 Testing {name}: {base_url}")
            
            url = base_url + api_path
            
            try:
                print(f"🔗 POST to: {url}")
                response = requests.post(url, data=parameters, timeout=30)
                
                print(f"📈 Status Code: {response.status_code}")
                print(f"📋 Response: {response.text[:300]}...")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        
                        if 'access_token' in result:
                            print(f"✅ {name} Token refresh SUCCESS!")
                            print(f"🔑 New access token: {result['access_token'][:20]}...")
                            if 'refresh_token' in result:
                                print(f"🔑 New refresh token: {result['refresh_token'][:20]}...")
                            return True, result
                        else:
                            print(f"❌ {name} Refresh failed: {result}")
                    except Exception as e:
                        print(f"❌ {name} JSON parsing error: {e}")
                else:
                    print(f"❌ {name} HTTP error: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ {name} Connection error: {e}")
                continue
        
        return False, "All refresh endpoints failed"
        
    except Exception as e:
        print(f"❌ Token refresh test failed: {e}")
        return False, str(e)

def test_product_api():
    """Test product-related APIs"""
    
    print("\n📦 PRODUCT API TEST")
    print("=" * 50)
    
    try:
        access_token = os.getenv('LAZADA_ACCESS_TOKEN')
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test products endpoint
        api_path = '/products/get'
        timestamp = str(int(time.time() * 1000))
        
        parameters = {
            'app_key': app_key,
            'access_token': access_token,
            'sign_method': 'sha256',
            'timestamp': timestamp,
            'limit': '10',
            'offset': '0'
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
        
        # Test with global endpoint
        url = 'https://api.lazada.com/rest' + api_path
        
        print(f"🔗 GET request to: {url}")
        response = requests.get(url, params=parameters, timeout=30)
        
        print(f"📈 Status Code: {response.status_code}")
        print(f"📋 Response: {response.text[:300]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                if 'code' in result and result['code'] == '0':
                    print("✅ Product API SUCCESS!")
                    if 'data' in result:
                        products = result['data'].get('products', [])
                        print(f"📦 Found {len(products)} products")
                    return True, result
                else:
                    print(f"❌ Product API error: {result.get('message', 'Unknown error')}")
                    return False, result
                    
            except Exception as e:
                print(f"❌ JSON parsing error: {e}")
                return False, response.text
        else:
            print(f"❌ HTTP error: {response.status_code}")
            return False, response.text
            
    except Exception as e:
        print(f"❌ Product API test failed: {e}")
        return False, str(e)

def main():
    """Run fixed tests"""
    
    print("🔧 LAZADA API FIXED TESTS")
    print("=" * 60)
    print("🩹 Testing with corrected methods and endpoints")
    print()
    
    # Test 1: Seller API with correct method
    api_success, api_result = test_api_with_correct_method()
    
    # Test 2: Token refresh with correct endpoint
    refresh_success, refresh_result = test_token_refresh_fixed()
    
    # Test 3: Product API
    product_success, product_result = test_product_api()
    
    # Summary
    print("\n📊 FIXED TEST SUMMARY")
    print("=" * 30)
    print(f"Seller API (GET): {'✅' if api_success else '❌'}")
    print(f"Token Refresh: {'✅' if refresh_success else '❌'}")
    print(f"Product API: {'✅' if product_success else '❌'}")
    
    if any([api_success, refresh_success, product_success]):
        print("\n🎉 SUCCESS! At least one API endpoint is working!")
        
        if refresh_success:
            print("\n🔄 RECOMMENDATION: Update your .env with new tokens")
            print("The refresh returned new tokens that should be saved")
        
        if api_success or product_success:
            print("\n✅ Your current tokens are working!")
            print("You can proceed with your data integration")
    else:
        print("\n❌ All tests failed. Possible issues:")
        print("1. Tokens may be expired")
        print("2. App may be suspended or restricted")
        print("3. Network/firewall issues")
        print("4. Sandbox vs Production environment mismatch")
        
        print("\n🔧 NEXT STEPS:")
        print("1. Check Lazada Developer Console")
        print("2. Try fresh OAuth authorization")
        print("3. Verify app is active and approved")

if __name__ == "__main__":
    main()