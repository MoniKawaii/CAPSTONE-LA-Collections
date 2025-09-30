"""
Test script using official Lazada SDK pattern
"""

import sys
import os
from dotenv import load_dotenv

# Add app directory to path
sys.path.append('./app')

# Import the SDK (either official or our implementation)
try:
    import lazop
    print("✅ Using Lazada SDK")
except ImportError:
    print("❌ Lazada SDK not found")
    exit(1)

# Load environment variables
load_dotenv()

def test_token_refresh():
    """Test token refresh using your exact code pattern"""
    
    # Your credentials
    url = 'https://auth.lazada.com/rest'
    appkey = os.getenv('LAZADA_APP_KEY')
    appSecret = os.getenv('LAZADA_APP_SECRET')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    print(f"App Key: {appkey}")
    print(f"Refresh Token: {refresh_token[:20]}...")
    
    # Your exact code pattern:
    client = lazop.LazopClient(url, appkey, appSecret)
    request = lazop.LazopRequest('/auth/token/refresh')
    request.add_api_param('refresh_token', refresh_token)
    response = client.execute(request)
    
    print("Response type:", response.type)
    print("Response body:", response.body)
    
    # If successful, extract new tokens
    if hasattr(response, 'body') and isinstance(response.body, dict):
        if 'access_token' in response.body:
            new_access_token = response.body['access_token']
            new_refresh_token = response.body.get('refresh_token', refresh_token)
            
            print(f"\n✅ SUCCESS! New tokens obtained:")
            print(f"New Access Token: {new_access_token[:20]}...")
            print(f"New Refresh Token: {new_refresh_token[:20]}...")
            
            # Update .env file
            update_env_tokens(new_access_token, new_refresh_token)
            return True
        else:
            print(f"\n❌ No access token in response: {response.body}")
            return False
    else:
        print(f"\n❌ Unexpected response format: {response.body}")
        return False

def update_env_tokens(access_token, refresh_token):
    """Update .env file with new tokens"""
    env_file = '.env'
    
    # Read current .env content
    with open(env_file, 'r') as f:
        lines = f.readlines()
    
    # Update token lines
    updated_lines = []
    for line in lines:
        if line.startswith('LAZADA_ACCESS_TOKEN='):
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        elif line.startswith('LAZADA_REFRESH_TOKEN='):
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        else:
            updated_lines.append(line)
    
    # Write back to .env
    with open(env_file, 'w') as f:
        f.writelines(updated_lines)
    
    print(f"✅ Updated .env file with new tokens")

def test_api_call():
    """Test API call with new tokens"""
    url = 'https://api.lazada.com.ph/rest'
    appkey = os.getenv('LAZADA_APP_KEY')
    appSecret = os.getenv('LAZADA_APP_SECRET')
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    client = lazop.LazopClient(url, appkey, appSecret)
    request = lazop.LazopRequest('/seller/get')
    response = client.execute(request, access_token)
    
    print("\n🧪 Testing API call with new tokens:")
    print("Seller Info Response type:", response.type)
    print("Seller Info Response body:", response.body)

if __name__ == "__main__":
    print("🚀 Testing Lazada SDK Token Refresh")
    print("=" * 50)
    
    # Test token refresh
    if test_token_refresh():
        print("\n" + "=" * 50)
        print("🧪 Testing API call with refreshed tokens...")
        test_api_call()
    else:
        print("\n❌ Token refresh failed. Check your credentials.")