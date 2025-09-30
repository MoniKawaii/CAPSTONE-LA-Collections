"""
Lazada Authorization URL Generator
This script generates the OAuth authorization link you need to visit
"""

import os
import urllib.parse
from dotenv import load_dotenv

def generate_auth_url():
    """Generate Lazada OAuth authorization URL"""
    
    load_dotenv()
    
    print("🔗 LAZADA AUTHORIZATION URL GENERATOR")
    print("=" * 45)
    
    # Get app credentials from .env
    app_key = os.getenv('LAZADA_APP_KEY')
    redirect_uri = os.getenv('LAZADA_REDIRECT_URI', 'https://oauth.pstmn.io/v1/callback')
    country = os.getenv('LAZADA_COUNTRY', 'ph')  # Default to Philippines
    
    if not app_key:
        print("❌ Missing LAZADA_APP_KEY in .env file")
        return None
    
    print(f"🔑 App Key: {app_key}")
    print(f"🌏 Country: {country.upper()}")
    print(f"🔄 Redirect URI: {redirect_uri}")
    
    # Lazada OAuth parameters
    auth_params = {
        'response_type': 'code',
        'force_auth': 'true',
        'redirect_uri': redirect_uri,
        'client_id': app_key,
        'country': country
    }
    
    # Base OAuth URL for different countries
    oauth_urls = {
        'ph': 'https://auth.lazada.com/oauth/authorize',  # Philippines
        'sg': 'https://auth.lazada.com/oauth/authorize',  # Singapore
        'my': 'https://auth.lazada.com/oauth/authorize',  # Malaysia
        'th': 'https://auth.lazada.com/oauth/authorize',  # Thailand
        'vn': 'https://auth.lazada.com/oauth/authorize',  # Vietnam
        'id': 'https://auth.lazada.com/oauth/authorize'   # Indonesia
    }
    
    base_url = oauth_urls.get(country.lower(), oauth_urls['ph'])
    
    # Generate query string
    query_string = urllib.parse.urlencode(auth_params)
    auth_url = f"{base_url}?{query_string}"
    
    print(f"\n🎯 AUTHORIZATION URL:")
    print(f"=" * 60)
    print(f"{auth_url}")
    print(f"=" * 60)
    
    print(f"\n📋 INSTRUCTIONS:")
    print(f"1. 📋 Copy the URL above")
    print(f"2. 🌐 Open it in your web browser")
    print(f"3. 🔐 Login with your Lazada seller account")
    print(f"4. ✅ Click 'Authorize' to approve your app")
    print(f"5. 📝 Copy the 'code' parameter from the redirect URL")
    print(f"6. 🔄 Use that code in the token exchange script")
    
    print(f"\n💡 WHAT TO EXPECT:")
    print(f"   • You'll be redirected to: {redirect_uri}")
    print(f"   • Look for URL like: {redirect_uri}?code=YOUR_AUTH_CODE&state=...")
    print(f"   • Copy the value after 'code='")
    
    return auth_url

def generate_auth_url_with_custom_redirect():
    """Generate auth URL with custom redirect URI"""
    
    print(f"\n🔧 CUSTOM REDIRECT URI")
    print(f"=" * 30)
    
    custom_redirect = input("Enter your redirect URI (or press Enter for default): ").strip()
    
    if custom_redirect:
        # Update .env with custom redirect
        from dotenv import set_key
        set_key('.env', 'LAZADA_REDIRECT_URI', custom_redirect)
        print(f"✅ Updated .env with redirect URI: {custom_redirect}")
        
        # Regenerate URL
        return generate_auth_url()
    else:
        print(f"Using default redirect URI")
        return generate_auth_url()

def quick_auth_setup():
    """Quick setup for getting authorization"""
    
    print(f"\n🚀 QUICK AUTH SETUP")
    print(f"=" * 25)
    
    load_dotenv()
    app_key = os.getenv('LAZADA_APP_KEY')
    
    if not app_key:
        print(f"❌ No app key found. Please add LAZADA_APP_KEY to .env file")
        return
    
    print(f"Choose your setup:")
    print(f"1. Use Postman OAuth (recommended for testing)")
    print(f"2. Use custom redirect URI")
    print(f"3. Use localhost (requires local server)")
    
    choice = input(f"\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        # Use Postman OAuth
        from dotenv import set_key
        set_key('.env', 'LAZADA_REDIRECT_URI', 'https://oauth.pstmn.io/v1/callback')
        print(f"✅ Set redirect to Postman OAuth")
        return generate_auth_url()
        
    elif choice == "2":
        return generate_auth_url_with_custom_redirect()
        
    elif choice == "3":
        # Use localhost
        from dotenv import set_key
        set_key('.env', 'LAZADA_REDIRECT_URI', 'http://localhost:8001/callback')
        print(f"✅ Set redirect to localhost:8001")
        print(f"⚠️ Make sure to run a local server on port 8001")
        return generate_auth_url()
        
    else:
        print(f"Invalid choice. Using default setup.")
        return generate_auth_url()

if __name__ == "__main__":
    print("🎯 LAZADA OAUTH AUTHORIZATION SETUP")
    print("=" * 50)
    
    # Check if we have basic setup
    load_dotenv()
    app_key = os.getenv('LAZADA_APP_KEY')
    
    if not app_key:
        print(f"❌ Please add LAZADA_APP_KEY to your .env file first")
        print(f"   Example: LAZADA_APP_KEY=123456")
        exit(1)
    
    # Generate auth URL
    auth_url = quick_auth_setup()
    
    if auth_url:
        print(f"\n🎉 SUCCESS!")
        print(f"📋 Your authorization URL is ready!")
        print(f"🌐 Visit the URL above to get your authorization code")
        
        # Optionally open in browser
        try:
            import webbrowser
            open_browser = input(f"\nOpen URL in browser automatically? (y/n): ").strip().lower()
            if open_browser == 'y':
                webbrowser.open(auth_url)
                print(f"✅ Opened in default browser")
        except:
            print(f"💡 Copy and paste the URL manually")
    else:
        print(f"❌ Failed to generate authorization URL")