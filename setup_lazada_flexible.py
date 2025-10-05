"""
Alternative Lazada Integration Setup

This script provides multiple setup options including manual callback URL setup
for cases where ngrok authentication is not available.
"""

import asyncio
import webbrowser
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_environment():
    """Check if required environment variables are set"""
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    if not app_key or not app_secret:
        print("❌ Missing Lazada credentials in .env file")
        print("Please add:")
        print("LAZADA_APP_KEY=your_app_key")
        print("LAZADA_APP_SECRET=your_app_secret")
        return False
    
    print(f"✅ Lazada App Key: {app_key}")
    print(f"✅ App Secret: {app_secret[:20]}...")
    return True

def setup_option_1_ngrok():
    """Setup with ngrok (requires ngrok auth token)"""
    print("\n🔧 Option 1: ngrok Setup")
    print("=" * 30)
    print("This requires an ngrok account and auth token.")
    print("1. Sign up at: https://dashboard.ngrok.com/signup")
    print("2. Get your auth token: https://dashboard.ngrok.com/get-started/your-authtoken")
    print("3. Install auth token: ngrok config add-authtoken YOUR_TOKEN")
    
    use_ngrok = input("\nDo you have ngrok configured? (y/n): ").lower().strip()
    
    if use_ngrok == 'y':
        try:
            from app.lazada_service import lazada_service
            
            # Try to setup ngrok
            tunnel_url = lazada_service.setup_ngrok_tunnel(port=8000)
            auth_url = lazada_service.get_authorization_url()
            
            print(f"\n✅ ngrok tunnel: {tunnel_url}")
            print(f"🔗 Authorization URL: {auth_url}")
            
            open_browser = input("\n🌐 Open authorization URL in browser? (y/n): ").lower().strip()
            if open_browser == 'y':
                webbrowser.open(auth_url)
            
            return True
            
        except Exception as e:
            print(f"❌ ngrok setup failed: {e}")
            return False
    
    return False

def setup_option_2_manual():
    """Setup with manual callback URL"""
    print("\n🔧 Option 2: Manual Callback Setup")
    print("=" * 35)
    print("This uses a custom callback URL that you control.")
    
    print("\nYou have two sub-options:")
    print("A. Use localhost (for development)")
    print("B. Use your own domain/server")
    
    choice = input("\nChoose (A/B): ").upper().strip()
    
    if choice == 'A':
        callback_url = "http://localhost:8000/lazada/callback"
        print(f"\n📋 Use this callback URL in Lazada Developer Console:")
        print(f"   {callback_url}")
    elif choice == 'B':
        custom_domain = input("Enter your domain (e.g., https://yourdomain.com): ").strip()
        callback_url = f"{custom_domain}/lazada/callback"
        print(f"\n📋 Use this callback URL in Lazada Developer Console:")
        print(f"   {callback_url}")
    else:
        print("Invalid choice.")
        return False
    
    # Generate authorization URL
    try:
        from app.lazada_service import LazadaOAuthService
        
        # Create service instance without ngrok
        service = LazadaOAuthService()
        
        # Generate auth URL with manual callback
        app_key = os.getenv('LAZADA_APP_KEY')
        auth_url = f"https://auth.lazada.com/oauth/authorize?response_type=code&force_auth=true&redirect_uri={callback_url}&client_id={app_key}"
        
        print(f"\n🔗 Authorization URL:")
        print(f"   {auth_url}")
        
        print(f"\n📋 NEXT STEPS:")
        print(f"1. Add this callback URL to your Lazada App settings:")
        print(f"   {callback_url}")
        print(f"2. Start your FastAPI server: uvicorn main:app --reload --port 8000")
        print(f"3. Visit the authorization URL above")
        print(f"4. After authorization, use the API endpoints")
        
        open_browser = input("\n🌐 Open authorization URL in browser? (y/n): ").lower().strip()
        if open_browser == 'y':
            webbrowser.open(auth_url)
        
        return True
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

def setup_option_3_postman():
    """Setup instructions for Postman workflow"""
    print("\n🔧 Option 3: Postman/Manual Token Exchange")
    print("=" * 40)
    print("If you prefer the traditional Postman approach:")
    
    app_key = os.getenv('LAZADA_APP_KEY')
    
    # Use a dummy callback for Postman
    postman_callback = "https://postman-echo.com/get"
    auth_url = f"https://auth.lazada.com/oauth/authorize?response_type=code&force_auth=true&redirect_uri={postman_callback}&client_id={app_key}"
    
    print(f"\n🔗 Authorization URL (for Postman):")
    print(f"   {auth_url}")
    
    print(f"\n📋 POSTMAN WORKFLOW:")
    print(f"1. Visit the authorization URL above")
    print(f"2. Copy the 'code' parameter from the redirected URL")
    print(f"3. Use POST /lazada/exchange-code endpoint with the code")
    print(f"4. Tokens will be saved automatically")
    
    print(f"\n🚀 Or use existing token exchange scripts:")
    print(f"   - generate_access_token.py")
    print(f"   - get_lazada_tokens.py")
    
    open_browser = input("\n🌐 Open authorization URL in browser? (y/n): ").lower().strip()
    if open_browser == 'y':
        webbrowser.open(auth_url)
    
    return True

def main():
    print("🚀 Lazada Integration Setup - Multiple Options")
    print("=" * 50)
    
    # Check environment
    if not check_environment():
        return
    
    print(f"\n🔧 SETUP OPTIONS:")
    print(f"1. ngrok (automatic tunnel) - requires ngrok auth")
    print(f"2. Manual callback URL - use your own domain/localhost")
    print(f"3. Postman/Manual workflow - traditional approach")
    
    choice = input(f"\nChoose setup option (1/2/3): ").strip()
    
    success = False
    
    if choice == "1":
        success = setup_option_1_ngrok()
    elif choice == "2":
        success = setup_option_2_manual()
    elif choice == "3":
        success = setup_option_3_postman()
    else:
        print("Invalid choice.")
        return
    
    if success:
        print(f"\n🎉 Setup complete!")
        print(f"\n📚 Next steps:")
        print(f"   - Start FastAPI: uvicorn main:app --reload --port 8000")
        print(f"   - Test with: curl http://localhost:8000/lazada/status")
        print(f"   - View API docs: http://localhost:8000/docs")
    else:
        print(f"\n❌ Setup incomplete. Try a different option.")

if __name__ == "__main__":
    main()