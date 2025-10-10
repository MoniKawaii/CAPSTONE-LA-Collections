"""
Lazada Integration Startup Guide

This script helps you get started with the Lazada integration using ngrok.
"""

import asyncio
import webbrowser
from app.lazada_service import lazada_service

async def main():
    print("Lazada Integration Setup")
    print("=" * 50)
    
    try:
        # Step 1: Setup ngrok tunnel
        print("\n 1 Setting up ngrok tunnel...")
        tunnel_url = lazada_service.setup_ngrok_tunnel(port=8000)
        
        # Step 2: Generate authorization URL
        print("\n 2 Generating authorization URL...")
        auth_url = lazada_service.get_authorization_url()
        
        print(f"\n Setup complete!")
        print(f"📱 ngrok tunnel: {tunnel_url}")
        print(f"🔗 OAuth callback: {tunnel_url}/lazada/callback")
        print(f"🌐 Authorization URL: {auth_url}")
        
        print("\n" + "=" * 50)
        print(" NEXT STEPS:")
        print("1. Start your FastAPI server: uvicorn main:app --reload --port 8000")
        print("2. Visit the authorization URL above to get access tokens")
        print("3. Use the API endpoints to fetch Lazada data automatically")
        print("=" * 50)
        
        # Option to open browser automatically
        open_browser = input("\n Open authorization URL in browser? (y/n): ").lower().strip()
        if open_browser == 'y':
            webbrowser.open(auth_url)
            print("Browser opened!")
        
        print("\n Setup complete! Your Lazada integration is ready to use.")
        
    except Exception as e:
        print(f" Setup failed: {e}")
        print("\nTroubleshooting:")
        print("- Make sure ngrok is installed")
        print("- Check your .env file has LAZADA_APP_KEY and LAZADA_APP_SECRET")
        print("- Ensure your network allows ngrok connections")

if __name__ == "__main__":
    asyncio.run(main())