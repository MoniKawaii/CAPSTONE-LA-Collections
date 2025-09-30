"""
Test script for tokens obtained from ISV Console
"""

def test_isv_tokens():
    """Test tokens from ISV Console"""
    
    # Get tokens from user input
    print("🔑 Testing tokens from ISV Console")
    print("=" * 50)
    
    access_token = input("Enter new ACCESS TOKEN: ").strip()
    refresh_token = input("Enter new REFRESH TOKEN: ").strip()
    
    if not access_token or not refresh_token:
        print("❌ Both tokens are required!")
        return False
    
    # Update .env file
    update_env_file(access_token, refresh_token)
    
    # Test the tokens
    import os
    import sys
    from dotenv import load_dotenv
    
    sys.path.append('./app')
    import lazop
    
    # Reload environment
    load_dotenv()
    
    appkey = os.getenv('LAZADA_APP_KEY')
    appSecret = os.getenv('LAZADA_APP_SECRET')
    
    print(f"\n🧪 Testing with App Key: {appkey}")
    print(f"Access Token: {access_token[:20]}...")
    
    # Test seller info API
    url = 'https://api.lazada.com.ph/rest'
    client = lazop.LazopClient(url, appkey, appSecret)
    request = lazop.LazopRequest('/seller/get')
    response = client.execute(request, access_token)
    
    print(f"\nResponse Type: {response.type}")
    print(f"Response Code: {response.code}")
    
    if response.type == 'SUCCESS' or response.code == '0':
        print("✅ SUCCESS! ISV Console tokens are working!")
        print("🎉 Your Lazada API integration is now operational!")
        
        # Test automated token manager
        print("\n🔄 Testing automated token manager...")
        try:
            from app.lazada_token_manager import create_token_manager_from_env
            tm = create_token_manager_from_env()
            status = tm.get_token_status()
            print(f"✅ Token Manager Status: {status['access_token_valid']}")
            print("🚀 Automated refresh system is ready!")
        except Exception as e:
            print(f"⚠️ Token manager test: {e}")
        
        return True
    else:
        print("❌ Token test failed!")
        print(f"Response: {response.body}")
        return False

def update_env_file(access_token, refresh_token):
    """Update .env file with new tokens"""
    
    env_file = '.env'
    
    try:
        # Read current content
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        # Update token lines
        updated_lines = []
        access_updated = False
        refresh_updated = False
        
        for line in lines:
            if line.startswith('LAZADA_ACCESS_TOKEN='):
                updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
                access_updated = True
            elif line.startswith('LAZADA_REFRESH_TOKEN='):
                updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
                refresh_updated = True
            else:
                updated_lines.append(line)
        
        # Add tokens if they don't exist
        if not access_updated:
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        if not refresh_updated:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        
        # Write back
        with open(env_file, 'w') as f:
            f.writelines(updated_lines)
        
        print(f"✅ Updated {env_file} with new tokens")
        
    except Exception as e:
        print(f"❌ Error updating .env file: {e}")

if __name__ == "__main__":
    test_isv_tokens()