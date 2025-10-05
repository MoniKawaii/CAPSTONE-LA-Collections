"""
ngrok Setup Helper

This script helps set up ngrok authentication for the Lazada integration.
"""

import os
from pyngrok import ngrok, conf

def setup_ngrok_auth():
    """Setup ngrok authentication"""
    print("🔧 ngrok Authentication Setup")
    print("=" * 35)
    
    print("1. Visit: https://dashboard.ngrok.com/signup")
    print("2. Sign up for a free account")
    print("3. Go to: https://dashboard.ngrok.com/get-started/your-authtoken")
    print("4. Copy your auth token")
    
    auth_token = input("\n🔑 Enter your ngrok auth token: ").strip()
    
    if not auth_token:
        print("❌ No auth token provided")
        return False
    
    try:
        # Set the auth token
        ngrok.set_auth_token(auth_token)
        
        print("✅ Auth token set successfully!")
        
        # Test with a simple tunnel
        print("🧪 Testing ngrok connection...")
        tunnel = ngrok.connect(8000, "http")
        tunnel_url = tunnel.public_url
        
        print(f"✅ Test successful! Tunnel created: {tunnel_url}")
        
        # Kill the test tunnel
        ngrok.kill()
        
        print("✅ ngrok is ready to use!")
        return True
        
    except Exception as e:
        print(f"❌ ngrok setup failed: {e}")
        print("\nTroubleshooting:")
        print("- Make sure your auth token is correct")
        print("- Check your internet connection")
        print("- Try visiting https://dashboard.ngrok.com/get-started/your-authtoken again")
        return False

def main():
    print("🚀 ngrok Setup for Lazada Integration")
    print("=" * 40)
    
    if setup_ngrok_auth():
        print("\n🎉 ngrok setup complete!")
        print("\nNow you can run:")
        print("python setup_lazada.py")
        print("or")
        print("python setup_lazada_flexible.py")
    else:
        print("\n❌ Setup failed. Please try again.")

if __name__ == "__main__":
    main()