"""
Final Token Exchange - Using Correct App Secret
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv, set_key

def final_token_exchange():
    """Final token exchange with correct app secret"""
    
    load_dotenv()
    
    print("🔄 FINAL LAZADA TOKEN EXCHANGE")
    print("=" * 40)
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔐 App Secret: {app_secret}")
    
    # Use the auth code that worked before
    auth_code = "0_135073_LJEXJIPtTlPI2kGIuY3g2lmI3394"
    print(f"✅ Auth Code: {auth_code}")
    
    # Token exchange
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
        app_secret.encode('utf-8'),
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
                access_token = result['access_token']
                refresh_token = result.get('refresh_token')
                
                print(f"\n🎉 SUCCESS!")
                print(f"✅ Access Token: {access_token}")
                print(f"✅ Refresh Token: {refresh_token}")
                
                # Update .env file
                set_key('.env', 'LAZADA_ACCESS_TOKEN', access_token)
                if refresh_token:
                    set_key('.env', 'LAZADA_REFRESH_TOKEN', refresh_token)
                
                print(f"✅ Tokens saved to .env!")
                return True
            else:
                print(f"❌ Error: {result.get('code')} - {result.get('message')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    if final_token_exchange():
        print(f"\n🚀 NOW TEST YOUR TOKENS:")
        print(f"   python test_new_tokens.py")
    else:
        print(f"\n❌ Token exchange failed")