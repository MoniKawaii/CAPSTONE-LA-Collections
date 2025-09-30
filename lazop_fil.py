"""
LazOP Refresh Token Script
Using the official Lazada LazOP SDK to refresh access tokens
"""

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

def refresh_lazada_token():
    """Refresh Lazada access token using refresh token"""
    
    load_dotenv()
    
    print("🔄 LAZADA TOKEN REFRESH WITH LAZOP")
    print("=" * 40)
    
    # Get credentials from .env
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    if not all([app_key, app_secret]):
        print("❌ Missing app credentials in .env file")
        return False
    
    if not refresh_token:
        print("❌ No refresh token found in .env file")
        print("💡 You need to get tokens first with authorization code")
        return False
    
    print(f"🔑 App Key: {app_key}")
    print(f"🔐 App Secret: {app_secret[:10]}...")
    print(f"🔄 Refresh Token: {refresh_token[:20]}...")
    
    try:
        # Initialize LazOP client for auth endpoint
        url = 'https://auth.lazada.com/rest'
        client = LazopClient(url, app_key, app_secret)
        
        # Create refresh token request
        request = LazopRequest('/auth/token/refresh')
        request.add_api_param('refresh_token', refresh_token)
        
        print(f"\n🌐 Refreshing token...")
        
        # Execute request
        response = client.execute(request)
        
        print(f"📈 Response Type: {response.type}")
        print(f"📋 Response Body: {response.body}")
        
        # Check response
        if response.body and isinstance(response.body, dict):
            result = response.body
            
            if 'access_token' in result:
                new_access_token = result['access_token']
                new_refresh_token = result.get('refresh_token', refresh_token)
                expires_in = result.get('expires_in')
                
                print(f"\n🎉 TOKEN REFRESH SUCCESS!")
                print(f"✅ New Access Token: {new_access_token}")
                print(f"✅ New Refresh Token: {new_refresh_token}")
                print(f"⏰ Expires in: {expires_in} seconds" if expires_in else "⏰ No expiry info")
                
                # Update .env file
                print(f"\n💾 Updating .env file...")
                
                set_key('.env', 'LAZADA_ACCESS_TOKEN', new_access_token)
                set_key('.env', 'LAZADA_REFRESH_TOKEN', new_refresh_token)
                
                import time
                set_key('.env', 'LAZADA_TOKEN_REFRESHED_AT', str(int(time.time())))
                
                print(f"✅ Tokens updated in .env file!")
                
                return True
                
            elif result.get('code') == '0':
                print(f"✅ Refresh successful but no new token in response")
                print(f"📋 Full response: {result}")
                return False
                
            else:
                error_code = result.get('code', 'Unknown')
                error_msg = result.get('message', 'No message')
                
                print(f"\n❌ TOKEN REFRESH FAILED!")
                print(f"   Error Code: {error_code}")
                print(f"   Error Message: {error_msg}")
                
                # Common error explanations
                if error_code == 'InvalidRefreshToken':
                    print(f"\n💡 SOLUTION:")
                    print(f"   • Refresh token has expired")
                    print(f"   • Get fresh tokens with authorization code")
                    print(f"   • Run: python generate_access_token.py")
                elif error_code == 'IncompleteSignature':
                    print(f"\n💡 SOLUTION:")
                    print(f"   • Check app secret is correct")
                    print(f"   • Verify app is approved")
                
                return False
        else:
            print(f"❌ Invalid response format")
            return False
            
    except Exception as e:
        print(f"❌ Error during token refresh: {e}")
        return False

def test_refreshed_token():
    """Test the refreshed token"""
    
    print(f"\n🧪 TESTING REFRESHED TOKEN")
    print(f"=" * 30)
    
    load_dotenv()  # Reload to get updated token
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not access_token:
        print(f"❌ No access token found")
        return False
    
    try:
        # Test with API endpoint
        api_url = 'https://api.lazada.com/rest'
        client = LazopClient(api_url, app_key, app_secret)
        
        # Test seller endpoint
        request = LazopRequest('/seller/get')
        request.add_api_param('access_token', access_token)
        
        response = client.execute(request)
        
        if response.body and isinstance(response.body, dict):
            result = response.body
            
            if result.get('code') == '0':
                print(f"🎉 TOKEN TEST SUCCESS!")
                print(f"✅ Refreshed token is working!")
                
                # Show seller info
                if 'data' in result:
                    seller = result['data']
                    print(f"\n📊 Seller Information:")
                    print(f"   Seller ID: {seller.get('seller_id', 'N/A')}")
                    print(f"   Name: {seller.get('name', 'N/A')}")
                    print(f"   Email: {seller.get('email', 'N/A')}")
                
                return True
            else:
                print(f"❌ API Error: {result.get('code')} - {result.get('message')}")
                return False
        else:
            print(f"❌ Invalid API response")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 LAZOP TOKEN REFRESH UTILITY")
    print("=" * 50)
    
    # Refresh the token
    success = refresh_lazada_token()
    
    if success:
        print(f"\n" + "="*50)
        # Test the refreshed token
        if test_refreshed_token():
            print(f"\n🚀 TOKEN REFRESH COMPLETE!")
            print(f"   ✅ Your access token has been refreshed!")
            print(f"   ✅ Ready to use Lazada API!")
        else:
            print(f"\n⚠️ Token refreshed but test failed")
    else:
        print(f"\n❌ Token refresh failed")
        print(f"💡 You may need to get fresh tokens with authorization code")