"""
Sandbox environment configuration for Lazada API
Use this while waiting for production approval
"""

# Sandbox URLs for different regions
SANDBOX_URLS = {
    'singapore': 'https://api.lazada.sg/rest',
    'thailand': 'https://api.lazada.co.th/rest', 
    'malaysia': 'https://api.lazada.com.my/rest',
    'vietnam': 'https://api.lazada.vn/rest',
    'philippines': 'https://api.lazada.com.ph/rest'  # May still be production
}

SANDBOX_AUTH_URL = 'https://auth.lazada.sg/rest'  # Singapore sandbox for auth

def test_sandbox_environment():
    """Test your app with sandbox environment"""
    
    import os
    import sys
    from dotenv import load_dotenv
    
    sys.path.append('./app')
    import lazop
    
    load_dotenv()
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    print("🧪 Testing Sandbox Environment")
    print("=" * 50)
    
    # Test different sandbox regions
    for region, url in SANDBOX_URLS.items():
        print(f"\nTesting {region.upper()} sandbox: {url}")
        
        try:
            client = lazop.LazopClient(url, app_key, app_secret)
            request = lazop.LazopRequest('/seller/get')
            
            # This will fail without access token, but should show different error
            response = client.execute(request)
            
            print(f"Response Type: {response.type}")
            print(f"Response Code: {response.code}")
            print(f"Response Message: {response.message}")
            
            # If not "IncompleteSignature", this region works
            if response.code != 'IncompleteSignature':
                print(f"✅ {region.upper()} sandbox accepts your credentials!")
                
                # Try to get authorization URL for this region
                callback_url = "https://oauth.pstmn.io/v1/callback"
                auth_url = f"https://auth.lazada.sg/oauth/authorize?response_type=code&force_auth=true&redirect_uri={callback_url}&client_id={app_key}"
                
                print(f"\n🔗 Sandbox Authorization URL for {region}:")
                print(auth_url)
                print("\nUse this URL to get sandbox tokens!")
                
                return region, url
            else:
                print(f"❌ {region.upper()} sandbox: Signature issue")
                
        except Exception as e:
            print(f"❌ {region.upper()} error: {e}")
    
    return None, None

if __name__ == "__main__":
    working_region, working_url = test_sandbox_environment()
    
    if working_region:
        print(f"\n🎉 SUCCESS! Use {working_region.upper()} sandbox for development")
        print(f"API URL: {working_url}")
        print(f"Auth URL: https://auth.lazada.sg/rest")
    else:
        print("\n❌ No working sandbox found. Contact Lazada support.")