"""
Get sandbox authorization tokens for Lazada Singapore sandbox
"""

import os
from dotenv import load_dotenv

load_dotenv()

def generate_sandbox_auth_url():
    """Generate authorization URL for Singapore sandbox"""
    
    app_key = os.getenv('LAZADA_APP_KEY')
    callback_url = "https://oauth.pstmn.io/v1/callback"
    
    # Singapore sandbox authorization URL
    auth_url = f"https://auth.lazada.sg/oauth/authorize?response_type=code&force_auth=true&redirect_uri={callback_url}&client_id={app_key}"
    
    print("🧪 SANDBOX Authorization Setup")
    print("=" * 50)
    print(f"App Key: {app_key}")
    print(f"Environment: Singapore Sandbox")
    print(f"API URL: https://api.lazada.sg/rest")
    print(f"Auth URL: https://auth.lazada.sg/rest")
    
    print("\n🔗 Sandbox Authorization URL:")
    print(auth_url)
    
    print("\n📋 Instructions:")
    print("1. Copy the URL above")
    print("2. Open it in a browser") 
    print("3. Login with seller's Lazada SINGAPORE account")
    print("4. Click 'Authorize' to approve the app")
    print("5. Copy the authorization code from callback URL")
    print("6. Run the token exchange script")
    
    print("\n⚠️  IMPORTANT:")
    print("- Use SINGAPORE Lazada seller account")
    print("- Sandbox may have limited data")
    print("- Perfect for development and testing")
    
    return auth_url

def exchange_sandbox_code():
    """Exchange authorization code for sandbox tokens"""
    
    import time
    import hmac
    import hashlib
    import requests
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    print("\n🔄 Sandbox Token Exchange")
    print("=" * 30)
    
    auth_code = input("Enter authorization code from callback URL: ").strip()
    
    if not auth_code:
        print("❌ Authorization code required!")
        return
    
    # Token exchange for Singapore sandbox
    api_path = '/auth/token/create'
    base_url = 'https://auth.lazada.sg/rest'  # Singapore sandbox
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
    
    print(f"Making request to: {base_url}{api_path}")
    
    # Exchange code for tokens
    response = requests.post(f"{base_url}{api_path}", data=parameters, timeout=30)
    
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    if response.status_code == 200:
        try:
            result = response.json()
            
            if 'access_token' in result:
                access_token = result['access_token']
                refresh_token = result.get('refresh_token', '')
                
                print(f"\n✅ SUCCESS! Sandbox tokens obtained:")
                print(f"Access Token: {access_token}")
                print(f"Refresh Token: {refresh_token}")
                
                # Update .env file
                update_env_with_sandbox_tokens(access_token, refresh_token)
                
                return True
            else:
                print(f"❌ No access token in response")
                return False
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            return False
    else:
        print("❌ Token exchange failed!")
        return False

def update_env_with_sandbox_tokens(access_token, refresh_token):
    """Update .env file with sandbox tokens"""
    
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
        
        # Add new tokens if not found
        if not access_updated:
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        if refresh_token and not refresh_updated:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        
        # Add sandbox environment marker
        updated_lines.append(f'LAZADA_ENVIRONMENT=sandbox\n')
        
        with open(env_file, 'w') as f:
            f.writelines(updated_lines)
        
        print(f"✅ Updated {env_file} with sandbox tokens")
        
    except Exception as e:
        print(f"❌ Error updating .env: {e}")

def main():
    """Main function"""
    
    print("Choose an option:")
    print("1. Generate sandbox authorization URL")
    print("2. Exchange authorization code for tokens")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == '1':
        generate_sandbox_auth_url()
    elif choice == '2':
        exchange_sandbox_code()
    else:
        print("Invalid choice!")

if __name__ == "__main__":
    main()