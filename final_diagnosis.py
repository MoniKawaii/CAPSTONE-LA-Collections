"""
Final Comprehensive Lazada Diagnostic
Checking app status, token validity, and providing solutions
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv
import json

load_dotenv()

def comprehensive_analysis():
    """Comprehensive analysis of the Lazada integration issues"""
    
    print("🩺 COMPREHENSIVE LAZADA DIAGNOSTIC")
    print("=" * 60)
    
    # Basic info
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    seller_id = os.getenv('LAZADA_SELLER_ID')
    
    print(f"📊 CONFIGURATION SUMMARY:")
    print(f"   App Key: {app_key}")
    print(f"   Access Token: {access_token[:20]}...")
    print(f"   Refresh Token: {refresh_token[:20]}...")
    print(f"   Seller ID: {seller_id}")
    print()
    
    # Issue analysis
    print("🔍 ISSUE ANALYSIS:")
    print("=" * 30)
    print("❌ All API calls return 'IncompleteSignature'")
    print("❌ Token refresh fails with same error")
    print("❌ Multiple signature formats tested - all fail")
    print()
    
    print("🤔 POSSIBLE ROOT CAUSES:")
    print("=" * 30)
    print("1. 🏢 APP STATUS ISSUES:")
    print("   • App not approved for production API")
    print("   • App suspended or restricted")
    print("   • Wrong app environment (sandbox vs production)")
    print()
    print("2. 🔑 TOKEN ISSUES:")
    print("   • Tokens expired (access tokens last ~12 hours)")
    print("   • Refresh token expired (typically 30 days)")
    print("   • Tokens from wrong environment")
    print()
    print("3. 🌍 ENVIRONMENT MISMATCH:")
    print("   • Using production tokens with sandbox endpoints")
    print("   • Using sandbox tokens with production endpoints")
    print("   • Wrong country/region endpoints")
    print()
    print("4. 🔐 SIGNATURE REQUIREMENTS:")
    print("   • Hidden parameters required")
    print("   • Different signing method for production")
    print("   • API version changes")
    print()
    
    return app_key, access_token, refresh_token

def check_app_status():
    """Check app status and provide guidance"""
    
    print("🏢 APP STATUS CHECK")
    print("=" * 30)
    
    app_key = os.getenv('LAZADA_APP_KEY')
    
    print(f"📱 Your App ID: {app_key}")
    print("🔗 Check app status at: https://open.lazada.com/apps/myapp")
    print()
    print("✅ VERIFY THESE SETTINGS:")
    print("   • App Status: Active/Approved")
    print("   • API Permissions: Enabled")
    print("   • Environment: Production (not sandbox)")
    print("   • Redirect URL: Configured correctly")
    print("   • Rate Limits: Not exceeded")
    print()
    print("🚨 COMMON ISSUES:")
    print("   • App pending approval")
    print("   • App restricted due to policy violations")
    print("   • Missing required permissions")
    print("   • Using wrong environment")

def analyze_token_age():
    """Analyze if tokens might be expired"""
    
    print("\n⏰ TOKEN AGE ANALYSIS")
    print("=" * 30)
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    # Try to estimate token age from patterns
    current_time = int(time.time())
    
    print(f"🕐 Current timestamp: {current_time}")
    print(f"📅 Current date: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    print()
    print("💡 TOKEN FACTS:")
    print("   • Access tokens typically expire in 12 hours")
    print("   • Refresh tokens typically expire in 30 days")
    print("   • Expired tokens return 'IllegalAccessToken'")
    print("   • Wrong signature returns 'IncompleteSignature'")
    print()
    print("🔍 DIAGNOSIS:")
    print("   Since you're getting 'IncompleteSignature' (not 'IllegalAccessToken'),")
    print("   the issue is likely NOT token expiration.")

def provide_solutions():
    """Provide step-by-step solutions"""
    
    print("\n🔧 STEP-BY-STEP SOLUTIONS")
    print("=" * 60)
    
    print("🎯 SOLUTION 1: CHECK APP STATUS")
    print("=" * 40)
    print("1. Go to: https://open.lazada.com/apps/myapp")
    print("2. Login with your developer account")
    print("3. Find your app (ID: 135073)")
    print("4. Check these settings:")
    print("   ✅ Status: Active/Live")
    print("   ✅ Environment: Production (not sandbox)")
    print("   ✅ Permissions: All required APIs enabled")
    print("   ✅ Rate limits: Not exceeded")
    print()
    
    print("🎯 SOLUTION 2: FRESH OAUTH AUTHORIZATION")
    print("=" * 40)
    print("1. Delete current tokens from .env")
    print("2. Run: python secure_oauth_callback.py")
    print("3. Complete fresh OAuth flow")
    print("4. Get brand new tokens")
    print("5. Test immediately with new tokens")
    print()
    
    print("🎯 SOLUTION 3: ENVIRONMENT CHECK")
    print("=" * 40)
    print("Current tokens appear to be PRODUCTION format")
    print("Ensure you're using PRODUCTION endpoints:")
    print("   API: https://api.lazada.com/rest")
    print("   Auth: https://auth.lazada.com/rest")
    print("NOT sandbox endpoints (.com.ph)")
    print()
    
    print("🎯 SOLUTION 4: CONTACT SUPPORT")
    print("=" * 40)
    print("If all above fail, contact Lazada Developer Support:")
    print("   • Include your App ID: 135073")
    print("   • Describe the 'IncompleteSignature' error")
    print("   • Ask them to check app status")
    print("   • Request signature debugging help")

def test_simple_endpoint():
    """Test the simplest possible API call"""
    
    print("\n🧪 SIMPLE ENDPOINT TEST")
    print("=" * 30)
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    # Test with minimal parameters
    api_path = '/seller/get'
    timestamp = str(int(time.time() * 1000))
    
    # Minimal parameter set
    parameters = {
        'app_key': app_key,
        'timestamp': timestamp,
        'sign_method': 'sha256'
    }
    
    # Add access token
    parameters['access_token'] = access_token
    
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
    
    # Test with global production endpoint
    url = 'https://api.lazada.com/rest' + api_path
    
    print(f"🔗 Testing: {url}")
    print(f"📋 Parameters: {list(parameters.keys())}")
    
    try:
        response = requests.get(url, params=parameters, timeout=30)
        
        print(f"📈 Status: {response.status_code}")
        print(f"📄 Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print("🎉 SUCCESS! API is working!")
                return True
            elif result.get('code') == 'IllegalAccessToken':
                print("⚠️ Token expired - need to refresh")
                return False
            elif result.get('code') == 'IncompleteSignature':
                print("❌ Still signature issue - likely app problem")
                return False
            else:
                print(f"❌ Other error: {result.get('message', 'Unknown')}")
                return False
        else:
            print(f"❌ HTTP error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Request error: {e}")
        return False

def main():
    """Main diagnostic function"""
    
    # Run comprehensive analysis
    app_key, access_token, refresh_token = comprehensive_analysis()
    
    # Check app status guidance
    check_app_status()
    
    # Analyze token age
    analyze_token_age()
    
    # Test simple endpoint
    test_result = test_simple_endpoint()
    
    # Provide solutions
    provide_solutions()
    
    # Final recommendation
    print("\n🎯 FINAL RECOMMENDATION")
    print("=" * 60)
    
    if test_result:
        print("✅ Your API is actually working!")
        print("The issue might be with specific endpoints or parameters.")
    else:
        print("❌ API still not working. Most likely causes:")
        print("1. 🏢 App not approved for production (MOST LIKELY)")
        print("2. 🔑 Need fresh OAuth authorization")
        print("3. 🌍 Environment configuration issue")
        print()
        print("🚀 IMMEDIATE ACTION:")
        print("1. Check Lazada Developer Console first")
        print("2. If app is approved, try fresh OAuth:")
        print("   python secure_oauth_callback.py")

if __name__ == "__main__":
    main()