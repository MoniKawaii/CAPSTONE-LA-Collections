"""
Quick Lazada API Test - Use after getting fresh tokens
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

def quick_test():
    """Quick test of Lazada API with current tokens"""
    
    load_dotenv()
    
    print("🧪 QUICK LAZADA API TEST")
    print("=" * 40)
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([access_token, app_key, app_secret]):
        print("❌ Missing credentials in .env file")
        return False
    
    print(f"🔑 App Key: {app_key}")
    print(f"🎟️ Access Token: {access_token[:20]}...")
    
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
        print(f"\n🌐 Testing: {url}")
        response = requests.get(url, params=parameters, timeout=30)
        
        print(f"📈 Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print("🎉 SUCCESS! API is working!")
                print(f"📊 Seller info received")
                
                if 'data' in result:
                    seller_data = result['data']
                    if isinstance(seller_data, dict):
                        print(f"   Seller ID: {seller_data.get('seller_id', 'N/A')}")
                        print(f"   Name: {seller_data.get('name', 'N/A')}")
                        print(f"   Email: {seller_data.get('email', 'N/A')}")
                
                return True
                
            elif result.get('code') == 'IllegalAccessToken':
                print("❌ Token expired - run fresh OAuth")
                return False
                
            elif result.get('code') == 'IncompleteSignature':
                print("❌ Signature issue - check app approval")
                return False
                
            else:
                print(f"❌ API Error: {result.get('message', 'Unknown')}")
                print(f"   Code: {result.get('code', 'No code')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

if __name__ == "__main__":
    success = quick_test()
    
    if success:
        print("\n✅ YOUR LAZADA INTEGRATION IS WORKING!")
        print("🚀 Ready to pull data from Lazada API")
    else:
        print("\n❌ Still having issues. Next steps:")
        print("1. Check Lazada Developer Console app status")
        print("2. Run fresh OAuth: python secure_oauth_callback.py")
        print("3. Verify app is approved for production API")