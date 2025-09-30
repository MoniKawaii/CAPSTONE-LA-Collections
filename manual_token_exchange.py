"""
Manual token exchange script for Lazada OAuth
Use this when Postman doesn't catch the callback automatically
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

def exchange_auth_code_for_tokens():
    """Exchange authorization code for access tokens"""
    
    print("🔄 Manual Token Exchange for Lazada")
    print("=" * 50)
    
    # Get the authorization code
    print("\nIf the seller was redirected to a URL like:")
    print("https://oauth.pstmn.io/v1/callback?code=0_135073_xxxxx&state=")
    print("\nPlease extract the authorization code from the URL.")
    
    auth_code = input("\nEnter the authorization code (after 'code='): ").strip()
    
    if not auth_code:
        print("❌ Authorization code is required!")
        return False
    
    # App credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    print(f"\nUsing App Key: {app_key}")
    print(f"Authorization Code: {auth_code[:20]}...")
    
    # Exchange code for tokens
    api_path = '/auth/token/create'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'code': auth_code,
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
    
    # Make token exchange request - Philippines sandbox
    url = 'https://auth.lazada.com.ph/rest' + api_path
    response = requests.post(url, data=parameters, timeout=30)
    
    print(f"\nToken Exchange Response:")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200:
        try:
            result = response.json()
            
            if 'access_token' in result:
                access_token = result['access_token']
                refresh_token = result.get('refresh_token', '')
                
                print(f"\n✅ SUCCESS! Tokens obtained:")
                print(f"Access Token: {access_token[:20]}...")
                if refresh_token:
                    print(f"Refresh Token: {refresh_token[:20]}...")
                
                # Update .env file
                update_env_file(access_token, refresh_token)
                
                # Test the tokens
                test_tokens(access_token)
                
                return True
            else:
                print(f"❌ No access token in response: {result}")
                return False
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            return False
    else:
        print("❌ Token exchange failed!")
        return False

def generate_new_auth_url():
    """Generate a new authorization URL for manual process"""
    
    app_key = os.getenv('LAZADA_APP_KEY')
    callback_url = "https://oauth.pstmn.io/v1/callback"
    
    # Philippines sandbox authorization URL
    auth_url = f"https://auth.lazada.com.ph/oauth/authorize?response_type=code&force_auth=true&redirect_uri={callback_url}&client_id={app_key}"
    
    print("\n🇵🇭 Philippines Sandbox Authorization URL:")
    print("=" * 50)
    print(auth_url)
    print("\n📋 Instructions for seller:")
    print("1. Copy the URL above")
    print("2. Open it in a browser")
    print("3. Login with Philippines Lazada seller credentials")
    print("4. Click 'Authorize'")
    print("5. Copy the ENTIRE callback URL after redirect")
    print("6. Extract the 'code=' parameter and run this script again")
    
    return auth_url

def update_env_file(access_token, refresh_token):
    """Update .env file with new tokens"""
    
    env_file = '.env'
    
    try:
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        updated_lines = []
        access_updated = False
        refresh_updated = False
        
        for line in lines:
            if line.startswith('LAZADA_ACCESS_TOKEN='):
                updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
                access_updated = True
            elif line.startswith('LAZADA_REFRESH_TOKEN=') and refresh_token:
                updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
                refresh_updated = True
            else:
                updated_lines.append(line)
        
        if not access_updated:
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        if refresh_token and not refresh_updated:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        
        with open(env_file, 'w') as f:
            f.writelines(updated_lines)
        
        print(f"✅ Updated {env_file} with new tokens")
        
    except Exception as e:
        print(f"❌ Error updating .env: {e}")

def test_tokens(access_token):
    """Test the newly obtained tokens"""
    
    print("\n🧪 Testing new tokens...")
    
    try:
        import lazop
        
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test seller API
        url = 'https://api.lazada.com.ph/rest'
        client = lazop.LazopClient(url, app_key, app_secret)
        request = lazop.LazopRequest('/seller/get')
        response = client.execute(request, access_token)
        
        print(f"API Test Result:")
        print(f"Type: {response.type}")
        print(f"Code: {response.code}")
        
        if response.type == 'SUCCESS' or response.code == '0':
            print("✅ SUCCESS! Tokens are working!")
            print("🎉 Your Lazada API integration is now operational!")
        else:
            print("❌ Token test failed!")
            print(f"Response: {response.body}")
            
    except Exception as e:
        print(f"⚠️ Could not test tokens: {e}")

def main():
    """Main function with options"""
    
    print("🔑 Lazada Token Manual Exchange")
    print("=" * 50)
    print("Choose an option:")
    print("1. Exchange authorization code for tokens")
    print("2. Generate new authorization URL")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == '1':
        exchange_auth_code_for_tokens()
    elif choice == '2':
        generate_new_auth_url()
    else:
        print("Invalid choice!")

if __name__ == "__main__":
    main()