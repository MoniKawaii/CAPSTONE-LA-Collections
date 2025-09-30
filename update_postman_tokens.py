"""
Helper script to update .env with Postman tokens
"""

def update_tokens_from_postman():
    """Update .env file with tokens obtained from Postman"""
    
    print("🔑 Update .env with Postman OAuth Tokens")
    print("=" * 50)
    
    # Get tokens from user
    print("\nAfter completing OAuth in Postman:")
    access_token = input("Enter ACCESS TOKEN from Postman: ").strip()
    refresh_token = input("Enter REFRESH TOKEN from Postman (if available): ").strip()
    
    if not access_token:
        print("❌ Access token is required!")
        return False
    
    # Read current .env
    env_file = '.env'
    try:
        with open(env_file, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ {env_file} not found!")
        return False
    
    # Update lines
    updated_lines = []
    access_updated = False
    refresh_updated = False
    
    for line in lines:
        if line.startswith('LAZADA_ACCESS_TOKEN='):
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
            access_updated = True
            print(f"✅ Updated access token: {access_token[:20]}...")
        elif line.startswith('LAZADA_REFRESH_TOKEN=') and refresh_token:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
            refresh_updated = True
            print(f"✅ Updated refresh token: {refresh_token[:20]}...")
        else:
            updated_lines.append(line)
    
    # Add if not found
    if not access_updated:
        updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        print(f"✅ Added access token: {access_token[:20]}...")
    
    if refresh_token and not refresh_updated:
        updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        print(f"✅ Added refresh token: {refresh_token[:20]}...")
    
    # Write back
    with open(env_file, 'w') as f:
        f.writelines(updated_lines)
    
    print(f"\n✅ Successfully updated {env_file}")
    
    # Test the tokens
    test_new_tokens()
    
    return True

def test_new_tokens():
    """Test the newly obtained tokens"""
    
    print("\n🧪 Testing new tokens...")
    
    import os
    import sys
    from dotenv import load_dotenv
    
    # Reload environment
    load_dotenv()
    
    sys.path.append('./app')
    try:
        import lazop
        
        appkey = os.getenv('LAZADA_APP_KEY')
        appSecret = os.getenv('LAZADA_APP_SECRET')
        access_token = os.getenv('LAZADA_ACCESS_TOKEN')
        
        # Test seller API
        url = 'https://api.lazada.com.ph/rest'
        client = lazop.LazopClient(url, appkey, appSecret)
        request = lazop.LazopRequest('/seller/get')
        response = client.execute(request, access_token)
        
        print(f"API Test - Type: {response.type}")
        print(f"API Test - Code: {response.code}")
        
        if response.type == 'SUCCESS' or response.code == '0':
            print("✅ SUCCESS! Postman tokens are working!")
            print("🎉 Your Lazada API integration is operational!")
            
            # Test automated system
            try:
                from app.lazada_token_manager import create_token_manager_from_env
                tm = create_token_manager_from_env()
                status = tm.get_token_status()
                print(f"🔄 Automated system ready: {status['auto_refresh_enabled']}")
            except Exception as e:
                print(f"⚠️ Automated system: {e}")
            
        else:
            print("❌ Token test failed!")
            print(f"Response: {response.body}")
    
    except ImportError:
        print("⚠️ Could not import lazop module for testing")
    except Exception as e:
        print(f"❌ Test error: {e}")

if __name__ == "__main__":
    update_tokens_from_postman()