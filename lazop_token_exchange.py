import os
from dotenv import load_dotenv, set_key

# Import LazOP SDK
try:
    from lazop_sdk import LazopClient, LazopRequest
    print("✅ LazOP SDK imported successfully")
except ImportError:
    print("❌ LazOP SDK not installed!")
    print("📦 Install with: pip install lazop-sdk")
    exit(1)

def exchange_auth_code_with_lazop():
    """Exchange authorization code for tokens using LazOP SDK"""
    
    load_dotenv()
    
    print("🔄 LAZADA TOKEN EXCHANGE WITH LAZOP SDK")
    print("=" * 45)
    
    # Get credentials from .env
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not all([app_key, app_secret]):
        print("Missing credentials in .env file")
        print("   Need: LAZADA_APP_KEY and LAZADA_APP_SECRET")
        return False
    
    print(f"App Key: {app_key}")
    
    # Get authorization code from user
    auth_code = input("\n📝 Enter your authorization code: ").strip()
    
    if not auth_code:
        print("No authorization code provided")
        return False
    
    print(f"✅ Auth Code: {auth_code}")
    
    # YOUR CODE SNIPPET - PROPERLY FORMATTED:
    try:
        # Initialize LazOP client
        url = 'https://auth.lazada.com/rest'  # Auth endpoint
        client = LazopClient(url, app_key, app_secret)
        
        # Create token request
        request = LazopRequest('/auth/token/create')
        request.add_api_param('code', auth_code)
        request.add_api_param('uuid', '')  # Leave empty as suggested
        
        # Execute request
        print(f"\n🌐 Making token exchange request...")
        response = client.execute(request)
        
        # Print response details
        print(f"📈 Response Type: {response.type}")
        print(f"📋 Response Body: {response.body}")
        
        # Parse response
        if response.type == 'nil' or not response.body:
            print("❌ Empty response from API")
            return False
        
        # Response body is already a dictionary
        result = response.body
        print(f"📋 Parsed Response: {result}")
        
        # Check for tokens in response
        if 'access_token' in result:
            access_token = result['access_token']
            refresh_token = result.get('refresh_token')
            expires_in = result.get('expires_in')
            
            print(f"\n🎉 SUCCESS! Tokens received!")
            print(f"✅ Access Token: {access_token}")
            print(f"✅ Refresh Token: {refresh_token}" if refresh_token else "❌ No refresh token")
            print(f"⏰ Expires in: {expires_in} seconds" if expires_in else "⏰ No expiry info")
            
            # Save to .env file
            print(f"\n💾 Saving tokens to .env file...")
            
            set_key('.env', 'LAZADA_ACCESS_TOKEN', access_token)
            if refresh_token:
                set_key('.env', 'LAZADA_REFRESH_TOKEN', refresh_token)
            
            import time
            set_key('.env', 'LAZADA_TOKEN_GENERATED', str(int(time.time())))
            
            print(f"✅ Tokens saved to .env file!")
            
            return True
            
        elif result.get('code') == '0':
            print(f"✅ Request successful but no tokens in response")
            print(f"📋 Full response: {result}")
            return False
            
        else:
            error_code = result.get('code', 'Unknown')
            error_msg = result.get('message', 'No message')
            
            print(f"❌ Token exchange failed!")
            print(f"   Code: {error_code}")
            print(f"   Message: {error_msg}")
            
            return False
            
    except Exception as e:
        print(f"❌ Error during token exchange: {e}")
        return False

def test_tokens_with_lazop():
    """Test the new tokens using LazOP SDK"""
    
    print(f"\n🧪 TESTING TOKENS WITH LAZOP")
    print(f"=" * 35)
    
    load_dotenv()  # Reload to get new tokens
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    
    if not all([app_key, app_secret, access_token]):
        print(f"❌ Missing credentials for test")
        return False
    
    try:
        # Initialize client for API calls
        api_url = 'https://api.lazada.com/rest'
        client = LazopClient(api_url, app_key, app_secret)
        
        # Test with seller info endpoint
        request = LazopRequest('/seller/get')
        request.add_api_param('access_token', access_token)
        
        print(f"🌐 Testing API call...")
        response = client.execute(request)
        
        print(f"📈 Response Type: {response.type}")
        print(f"📋 Response Body: {response.body}")
        
        # Parse response
        if response.body:
            # Response body is already a dictionary
            result = response.body
            
            if result.get('code') == '0':
                    print(f"🎉 TOKEN TEST SUCCESS!")
                    print(f"✅ Lazada API is working with LazOP SDK!")
                    
                    # Show seller info if available
                    if 'data' in result:
                        seller = result['data']
                        print(f"📊 Seller ID: {seller.get('seller_id', 'N/A')}")
                        print(f"📊 Name: {seller.get('name', 'N/A')}")
                        print(f"📊 Email: {seller.get('email', 'N/A')}")
                    
                    return True
            else:
                print(f"❌ API Error: {result.get('code')} - {result.get('message')}")
                return False
        else:
            print(f"❌ Empty response from API")
            return False
            
    except Exception as e:
        print(f"❌ Error during API test: {e}")
        return False

if __name__ == "__main__":
    print("🎯 LAZADA TOKEN EXCHANGE WITH LAZOP SDK")
    print("=" * 50)
    
    # Step 1: Exchange authorization code for tokens
    success = exchange_auth_code_with_lazop()
    
    if success:
        print(f"\n" + "="*50)
        # Step 2: Test the new tokens
        if test_tokens_with_lazop():
            print(f"\n🚀 ALL SYSTEMS GO!")
            print(f"   Your Lazada integration is working with LazOP SDK!")
        else:
            print(f"\n⚠️ Tokens obtained but test failed")
    else:
        print(f"\n❌ Token exchange failed")
        print(f"💡 Check your authorization code and try again")