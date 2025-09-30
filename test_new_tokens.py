"""
Quick Token Test - Verify Your New Tokens Work
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

def test_new_tokens():
    """Test the newly obtained tokens"""
    
    load_dotenv()
    
    print("🧪 TESTING NEW LAZADA TOKENS")
    print("=" * 35)
    
    # Get credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔐 App Secret: {app_secret[:10]}...")
    print(f"🎟️ Access Token: {access_token[:30]}..." if access_token else "❌ No access token")
    
    if not all([app_key, app_secret, access_token]):
        print("❌ Missing credentials")
        return False
    
    # Test with seller endpoint
    api_path = '/seller/get'
    timestamp = str(int(time.time() * 1000))
    
    params = {
        'app_key': app_key,
        'access_token': access_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
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
        print(f"\n🌐 Testing API call: {url}")
        response = requests.get(url, params=params, timeout=30)
        
        print(f"📈 Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('code') == '0':
                print(f"🎉 TOKEN TEST SUCCESS!")
                print(f"✅ Your Lazada API integration is WORKING!")
                
                # Show seller info
                if 'data' in result:
                    seller = result['data']
                    print(f"\n📊 Seller Information:")
                    print(f"   Seller ID: {seller.get('seller_id', 'N/A')}")
                    print(f"   Name: {seller.get('name', 'N/A')}")
                    print(f"   Email: {seller.get('email', 'N/A')}")
                    print(f"   Status: {seller.get('status', 'N/A')}")
                
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
    print("🎯 LAZADA TOKEN VERIFICATION")
    print("=" * 40)
    
    if test_new_tokens():
        print(f"\n🚀 CONGRATULATIONS!")
        print(f"   ✅ Your Lazada tokens are working perfectly!")
        print(f"   ✅ You can now pull data from Lazada API!")
        print(f"   ✅ No more seller login required!")
        print(f"   ✅ Your ETL pipeline can run automatically!")
    else:
        print(f"\n❌ Token test failed")
        print(f"💡 Check the error above for details")