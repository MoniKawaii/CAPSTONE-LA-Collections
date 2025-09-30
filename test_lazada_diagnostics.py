"""
Comprehensive Lazada API Test and Diagnostic Tool
Tests all aspects of your Lazada integration
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

def test_environment():
    """Test environment configuration"""
    
    print("🔍 ENVIRONMENT CONFIGURATION TEST")
    print("=" * 50)
    
    required_vars = [
        'LAZADA_ACCESS_TOKEN',
        'LAZADA_REFRESH_TOKEN', 
        'LAZADA_APP_KEY',
        'LAZADA_APP_SECRET',
        'LAZADA_SELLER_ID',
        'LAZADA_COUNTRY'
    ]
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive data
            if 'TOKEN' in var or 'SECRET' in var:
                display_value = f"{value[:20]}..." if len(value) > 20 else value
            else:
                display_value = value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: Missing")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n❌ Missing environment variables: {missing_vars}")
        return False
    
    print("\n✅ All environment variables present")
    return True

def test_signature_generation():
    """Test signature generation"""
    
    print("\n🔐 SIGNATURE GENERATION TEST")
    print("=" * 50)
    
    try:
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test signature with simple parameters
        api_path = '/seller/get'
        timestamp = str(int(time.time() * 1000))
        
        parameters = {
            'app_key': app_key,
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
        
        print(f"✅ String to sign: {string_to_sign[:50]}...")
        print(f"✅ Generated signature: {signature[:20]}...")
        print(f"✅ Signature generation working")
        
        return signature, parameters
        
    except Exception as e:
        print(f"❌ Signature generation failed: {e}")
        return None, None

def test_endpoint_connectivity():
    """Test connectivity to Lazada endpoints"""
    
    print("\n🌐 ENDPOINT CONNECTIVITY TEST")
    print("=" * 50)
    
    endpoints = {
        'Philippines API': 'https://api.lazada.com.ph/rest',
        'Philippines Auth': 'https://auth.lazada.com.ph/rest',
        'Global API': 'https://api.lazada.com/rest',
        'Global Auth': 'https://auth.lazada.com/rest'
    }
    
    results = {}
    
    for name, url in endpoints.items():
        try:
            print(f"Testing {name}: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ {name}: Reachable (200)")
                results[name] = 'success'
            elif response.status_code == 404:
                print(f"⚠️ {name}: Endpoint exists but path not found (404)")
                results[name] = 'reachable'
            else:
                print(f"⚠️ {name}: Status {response.status_code}")
                results[name] = f'status_{response.status_code}'
                
        except requests.exceptions.Timeout:
            print(f"❌ {name}: Timeout")
            results[name] = 'timeout'
        except requests.exceptions.ConnectionError:
            print(f"❌ {name}: Connection error")
            results[name] = 'connection_error'
        except Exception as e:
            print(f"❌ {name}: {e}")
            results[name] = 'error'
    
    return results

def test_api_call_with_current_token():
    """Test API call with current access token"""
    
    print("\n🧪 API CALL TEST WITH CURRENT TOKEN")
    print("=" * 50)
    
    try:
        access_token = os.getenv('LAZADA_ACCESS_TOKEN')
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test seller info endpoint
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
        
        # Try Philippines endpoint first
        url = 'https://api.lazada.com.ph/rest' + api_path
        
        print(f"🔗 Testing: {url}")
        print(f"📊 Parameters: {list(parameters.keys())}")
        
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"📈 Status Code: {response.status_code}")
        print(f"📋 Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                if 'code' in result:
                    if result['code'] == '0' or result.get('type') == 'SUCCESS':
                        print("✅ API call successful!")
                        return True, result
                    else:
                        print(f"❌ API error: {result.get('message', 'Unknown error')}")
                        return False, result
                else:
                    print("✅ API call successful (no error code)")
                    return True, result
                    
            except Exception as e:
                print(f"❌ JSON parsing error: {e}")
                return False, response.text
        else:
            print(f"❌ HTTP error: {response.status_code}")
            return False, response.text
            
    except Exception as e:
        print(f"❌ API test failed: {e}")
        return False, str(e)

def test_with_lazop_sdk():
    """Test using the custom lazop SDK"""
    
    print("\n🔧 LAZOP SDK TEST")
    print("=" * 50)
    
    try:
        from app import lazop
        
        access_token = os.getenv('LAZADA_ACCESS_TOKEN')
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test with Philippines endpoint
        url = 'https://api.lazada.com.ph/rest'
        client = lazop.LazopClient(url, app_key, app_secret)
        
        # Test seller endpoint
        request = lazop.LazopRequest('/seller/get')
        response = client.execute(request, access_token)
        
        print(f"📊 SDK Response Type: {response.type}")
        print(f"📋 SDK Response Code: {response.code}")
        print(f"💬 SDK Response Message: {response.message}")
        
        if hasattr(response, 'body'):
            print(f"📄 SDK Response Body: {str(response.body)[:200]}...")
        
        if response.type == 'SUCCESS' or response.code == '0':
            print("✅ SDK test successful!")
            return True, response
        else:
            print("❌ SDK test failed")
            return False, response
            
    except ImportError as e:
        print(f"❌ Cannot import lazop SDK: {e}")
        return False, str(e)
    except Exception as e:
        print(f"❌ SDK test error: {e}")
        return False, str(e)

def test_token_refresh():
    """Test token refresh functionality"""
    
    print("\n🔄 TOKEN REFRESH TEST")
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
        
        # Try refresh
        url = 'https://auth.lazada.com.ph/rest' + api_path
        
        print(f"🔗 Testing refresh: {url}")
        response = requests.post(url, data=parameters, timeout=30)
        
        print(f"📈 Status Code: {response.status_code}")
        print(f"📋 Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                if 'access_token' in result:
                    print("✅ Token refresh successful!")
                    print(f"🔑 New access token: {result['access_token'][:20]}...")
                    return True, result
                else:
                    print(f"❌ Refresh failed: {result}")
                    return False, result
                    
            except Exception as e:
                print(f"❌ JSON parsing error: {e}")
                return False, response.text
        else:
            print(f"❌ Refresh HTTP error: {response.status_code}")
            return False, response.text
            
    except Exception as e:
        print(f"❌ Token refresh test failed: {e}")
        return False, str(e)

def diagnose_issues():
    """Analyze test results and provide recommendations"""
    
    print("\n🔍 DIAGNOSTIC ANALYSIS")
    print("=" * 50)
    
    print("📋 POSSIBLE ISSUES:")
    print("1. 🕐 Token Expiration:")
    print("   • Access tokens expire after a few hours")
    print("   • Solution: Use token refresh")
    print()
    print("2. 🌏 Wrong Environment:")
    print("   • Sandbox vs Production endpoints")
    print("   • Solution: Verify endpoint URLs")
    print()
    print("3. 🔐 Signature Issues:")
    print("   • Incorrect parameter sorting")
    print("   • Wrong timestamp format")
    print("   • Solution: Check signature generation")
    print()
    print("4. 🌐 Network Issues:")
    print("   • Firewall blocking requests")
    print("   • DNS resolution problems")
    print("   • Solution: Check connectivity")
    print()
    print("5. 📱 App Configuration:")
    print("   • Wrong app settings in Lazada")
    print("   • Incorrect permissions")
    print("   • Solution: Check developer console")

def main():
    """Run comprehensive diagnostics"""
    
    print("🩺 LAZADA API COMPREHENSIVE DIAGNOSTIC")
    print("=" * 60)
    print("🔍 Testing all aspects of your Lazada integration")
    print()
    
    # Test 1: Environment
    env_ok = test_environment()
    
    # Test 2: Signature
    if env_ok:
        sig_ok, params = test_signature_generation()
    else:
        print("\n⏭️ Skipping signature test due to environment issues")
        sig_ok = False
    
    # Test 3: Connectivity
    connectivity = test_endpoint_connectivity()
    
    # Test 4: API Call
    if env_ok and sig_ok:
        api_ok, api_result = test_api_call_with_current_token()
    else:
        print("\n⏭️ Skipping API test due to previous issues")
        api_ok = False
    
    # Test 5: SDK
    if env_ok:
        sdk_ok, sdk_result = test_with_lazop_sdk()
    else:
        print("\n⏭️ Skipping SDK test due to environment issues")
        sdk_ok = False
    
    # Test 6: Token Refresh
    if env_ok:
        refresh_ok, refresh_result = test_token_refresh()
    else:
        print("\n⏭️ Skipping refresh test due to environment issues")
        refresh_ok = False
    
    # Summary
    print("\n📊 TEST SUMMARY")
    print("=" * 30)
    print(f"Environment: {'✅' if env_ok else '❌'}")
    print(f"Signature: {'✅' if sig_ok else '❌'}")
    print(f"Connectivity: {'✅' if any('success' in str(v) for v in connectivity.values()) else '❌'}")
    print(f"API Call: {'✅' if api_ok else '❌'}")
    print(f"SDK: {'✅' if sdk_ok else '❌'}")
    print(f"Token Refresh: {'✅' if refresh_ok else '❌'}")
    
    # Recommendations
    if not (api_ok or sdk_ok):
        diagnose_issues()
        
        print("\n🔧 RECOMMENDED NEXT STEPS:")
        if not refresh_ok:
            print("1. 🔄 Try refreshing your access token")
            print("   Run: python -c \"from app.lazada_token_manager import *; refresh_tokens()\"")
        
        print("2. 🧪 Test with fresh authorization")
        print("   Run: python secure_oauth_callback.py")
        
        print("3. 🔍 Check Lazada developer console")
        print("   Verify app settings at: https://open.lazada.com/apps/myapp")

if __name__ == "__main__":
    main()