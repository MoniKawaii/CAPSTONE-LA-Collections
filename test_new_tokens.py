"""
Quick test script for new Lazada tokens
"""

import os
from dotenv import load_dotenv
import sys
sys.path.append('./app')
import lazop

load_dotenv()

def test_new_tokens():
    """Test new tokens with seller info API"""
    
    url = 'https://api.lazada.com.ph/rest'
    appkey = os.getenv('LAZADA_APP_KEY')
    appSecret = os.getenv('LAZADA_APP_SECRET')
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    print("🧪 Testing new tokens...")
    print(f"App Key: {appkey}")
    print(f"Access Token: {access_token[:20]}...")
    
    client = lazop.LazopClient(url, appkey, appSecret)
    request = lazop.LazopRequest('/seller/get')
    response = client.execute(request, access_token)
    
    print(f"Response type: {response.type}")
    print(f"Response code: {response.code}")
    
    if response.type == 'SUCCESS' or 'seller' in str(response.body).lower():
        print("✅ SUCCESS! New tokens are working!")
        print("🎉 Your automated token management system is now ready!")
        return True
    else:
        print("❌ FAILED! Check the tokens again.")
        print(f"Response: {response.body}")
        return False

if __name__ == "__main__":
    test_new_tokens()