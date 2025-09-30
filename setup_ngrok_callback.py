"""
Ngrok-based OAuth callback setup for Lazada
Use this if localhost is not accepted by Lazada
"""

import os
import subprocess
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

def setup_ngrok_tunnel():
    """Set up ngrok tunnel for OAuth callback"""
    
    print("🌐 Setting up ngrok tunnel for OAuth callback...")
    print("=" * 50)
    
    # Check if ngrok is installed
    try:
        result = subprocess.run(['ngrok', 'version'], capture_output=True, text=True)
        print(f"✅ Ngrok found: {result.stdout.strip()}")
    except FileNotFoundError:
        print("❌ Ngrok not found!")
        print("📥 Please install ngrok:")
        print("   1. Go to https://ngrok.com/download")
        print("   2. Download and install ngrok")
        print("   3. Sign up for free account")
        print("   4. Run: ngrok authtoken YOUR_TOKEN")
        return None
    
    # Start ngrok tunnel
    print("\n🚀 Starting ngrok tunnel on port 8000...")
    
    try:
        # Start ngrok in background
        process = subprocess.Popen(['ngrok', 'http', '8000', '--log=stdout'], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
        
        # Wait a moment for ngrok to start
        time.sleep(3)
        
        # Get ngrok tunnel info
        try:
            response = requests.get('http://localhost:4040/api/tunnels')
            tunnels = response.json()
            
            if tunnels['tunnels']:
                public_url = tunnels['tunnels'][0]['public_url']
                print(f"✅ Ngrok tunnel active: {public_url}")
                print(f"   Local: http://localhost:8000")
                print(f"   Public: {public_url}")
                
                return public_url, process
            else:
                print("❌ No active tunnels found")
                return None, None
                
        except Exception as e:
            print(f"❌ Could not get tunnel info: {e}")
            print("   Please check ngrok manually at http://localhost:4040")
            return None, process
            
    except Exception as e:
        print(f"❌ Could not start ngrok: {e}")
        return None, None

def generate_oauth_url_with_ngrok():
    """Generate OAuth URL with ngrok tunnel"""
    
    print("\n🔗 OAUTH SETUP WITH NGROK")
    print("=" * 50)
    
    # Setup ngrok tunnel
    tunnel_info = setup_ngrok_tunnel()
    
    if not tunnel_info[0]:
        print("❌ Could not establish ngrok tunnel")
        return
    
    public_url, process = tunnel_info
    app_key = os.getenv('LAZADA_APP_KEY')
    
    # Generate authorization URL
    auth_url = f"https://auth.lazada.com.ph/oauth/authorize?response_type=code&force_auth=true&redirect_uri={public_url}&client_id={app_key}"
    
    print(f"\n🎯 CONFIGURATION STEPS:")
    print("=" * 30)
    print(f"1. Update Lazada app redirect URL to: {public_url}")
    print("   Go to: https://open.lazada.com/apps/myapp")
    print("   Update redirect URL setting")
    print()
    print(f"2. Use this authorization URL:")
    print(f"   {auth_url}")
    print()
    print("3. Run the OAuth callback server:")
    print("   python oauth_callback_server.py")
    print()
    print("⚠️  IMPORTANT:")
    print("   • Keep ngrok running during OAuth process")
    print("   • Use the ngrok URL in Lazada developer center")
    print("   • The tunnel will stay active until you stop it")
    
    print(f"\n🛑 To stop ngrok tunnel later:")
    print("   Press Ctrl+C or kill the ngrok process")
    
    return public_url, auth_url

if __name__ == "__main__":
    generate_oauth_url_with_ngrok()