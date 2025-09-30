"""
Comprehensive Lazada App Verification
This script checks all possible issues with your app configuration
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

def comprehensive_app_check():
    """Check all aspects of Lazada app configuration"""
    
    load_dotenv()
    
    print("🔍 COMPREHENSIVE LAZADA APP VERIFICATION")
    print("=" * 50)
    
    # Check environment variables
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    redirect_uri = os.getenv('LAZADA_REDIRECT_URI')
    
    print("📋 STEP 1: Environment Variables")
    print(f"   App Key: {app_key}")
    print(f"   App Secret: {app_secret}")
    print(f"   Redirect URI: {redirect_uri}")
    
    # Check for common issues
    issues = []
    
    if not app_key or not app_secret:
        issues.append("Missing app_key or app_secret")
    
    if app_secret and (app_secret.startswith("'") or app_secret.startswith('"')):
        issues.append("App secret has quotes - remove them")
    
    if redirect_uri and (redirect_uri.startswith("'") or redirect_uri.startswith('"')):
        issues.append("Redirect URI has quotes - remove them")
    
    if len(app_secret) < 20:
        issues.append("App secret seems too short")
    
    if issues:
        print(f"\n❌ CONFIGURATION ISSUES FOUND:")
        for issue in issues:
            print(f"   • {issue}")
        return False
    else:
        print(f"✅ Environment variables look good")
    
    # Test signature generation with known values
    print(f"\n📋 STEP 2: Signature Generation Test")
    
    test_params = {
        'app_key': app_key,
        'sign_method': 'sha256',
        'timestamp': '1234567890000'  # Fixed timestamp for testing
    }
    
    api_path = '/test/signature'
    sorted_params = sorted(test_params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    string_to_sign = api_path + param_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    print(f"   Test string: {string_to_sign}")
    print(f"   Test signature: {signature}")
    print(f"✅ Signature generation working")
    
    # Test with Lazada auth endpoint availability
    print(f"\n📋 STEP 3: Lazada Auth Endpoint Test")
    
    try:
        # Test if the auth endpoint is reachable
        test_url = 'https://auth.lazada.com/rest/auth/token/create'
        
        # Send a minimal request just to test connectivity
        test_response = requests.post(test_url, data={'test': 'connectivity'}, timeout=10)
        
        print(f"   Auth endpoint status: {test_response.status_code}")
        
        if test_response.status_code == 200:
            result = test_response.json()
            print(f"   Response type: {result.get('type', 'Unknown')}")
            print(f"✅ Auth endpoint is reachable")
        else:
            print(f"⚠️ Unexpected status code from auth endpoint")
            
    except Exception as e:
        print(f"❌ Cannot reach auth endpoint: {e}")
        return False
    
    # Check if app secret might be wrong
    print(f"\n📋 STEP 4: App Secret Validation")
    
    print(f"   Current app secret: {app_secret}")
    print(f"   Length: {len(app_secret)} characters")
    print(f"   Contains special chars: {'Yes' if any(c in app_secret for c in '!@#$%^&*()') else 'No'}")
    
    print(f"\n💡 NEXT STEPS:")
    print(f"   1. Verify in Lazada Developer Console:")
    print(f"      → Go to: https://open.lazada.com/apps/myapp")
    print(f"      → Find App ID: {app_key}")
    print(f"      → Copy EXACT app secret (case sensitive)")
    print(f"      → Check app status is 'Active'")
    print(f"   ")
    print(f"   2. Check redirect URI matches EXACTLY:")
    print(f"      → Your .env: {redirect_uri}")
    print(f"      → Should be: https://oauth.pstmn.io/v1/callback")
    print(f"   ")
    print(f"   3. Verify app permissions:")
    print(f"      → API access enabled")
    print(f"      → Production environment access")
    
    return True

def test_with_corrected_secret():
    """Allow user to test with a corrected app secret"""
    
    print(f"\n🔧 APP SECRET CORRECTION TEST")
    print(f"=" * 40)
    
    load_dotenv()
    current_secret = os.getenv('LAZADA_APP_SECRET')
    
    print(f"Current app secret: {current_secret}")
    
    new_secret = input(f"\nEnter corrected app secret (or press Enter to skip): ").strip()
    
    if not new_secret:
        print("Skipping secret correction test")
        return
    
    print(f"Testing with new secret: {new_secret}")
    
    # Test token exchange with new secret
    app_key = os.getenv('LAZADA_APP_KEY')
    auth_code = input("Enter authorization code to test: ").strip()
    
    if not auth_code:
        print("No auth code provided for test")
        return
    
    # Try token exchange with new secret
    timestamp = str(int(time.time() * 1000))
    
    params = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    api_path = '/auth/token/create'
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    string_to_sign = api_path + param_string
    
    signature = hmac.new(
        new_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    params['sign'] = signature
    
    url = 'https://auth.lazada.com/rest' + api_path
    
    try:
        response = requests.post(url, data=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            if 'access_token' in result:
                print(f"🎉 SUCCESS! Corrected secret works!")
                print(f"Access token: {result['access_token'][:30]}...")
                
                # Update .env file
                from dotenv import set_key
                set_key('.env', 'LAZADA_APP_SECRET', new_secret)
                print(f"✅ Updated .env file with corrected secret")
                
            elif result.get('code') == 'IncompleteSignature':
                print(f"❌ Still getting IncompleteSignature with new secret")
                print(f"   This confirms the app secret is the issue")
            else:
                print(f"❌ Different error: {result.get('code')} - {result.get('message')}")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    print("🎯 LAZADA APP COMPREHENSIVE VERIFICATION")
    print("=" * 60)
    
    if comprehensive_app_check():
        test_with_corrected_secret()
    else:
        print(f"\n❌ Basic configuration issues found")
        print(f"💡 Fix the environment variables first")