"""
Ngrok setup helper for Lazada OAuth
"""

import os
import subprocess
import requests
import zipfile
from pathlib import Path

def check_ngrok_installed():
    """Check if ngrok is already installed"""
    try:
        result = subprocess.run(['ngrok', 'version'], capture_output=True, text=True)
        print(f"✅ Ngrok already installed: {result.stdout.strip()}")
        return True
    except FileNotFoundError:
        print("❌ Ngrok not found")
        return False

def download_ngrok():
    """Download ngrok for Windows"""
    
    print("📥 Downloading ngrok for Windows...")
    
    # Ngrok download URL for Windows
    url = "https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-windows-amd64.zip"
    
    # Create ngrok directory
    ngrok_dir = Path.home() / "ngrok"
    ngrok_dir.mkdir(exist_ok=True)
    
    # Download file
    zip_path = ngrok_dir / "ngrok.zip"
    
    try:
        print(f"🌐 Downloading from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✅ Downloaded to: {zip_path}")
        
        # Extract zip
        print("📦 Extracting ngrok...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(ngrok_dir)
        
        # Remove zip file
        zip_path.unlink()
        
        exe_path = ngrok_dir / "ngrok.exe"
        if exe_path.exists():
            print(f"✅ Ngrok extracted to: {exe_path}")
            return str(exe_path)
        else:
            print("❌ Ngrok executable not found after extraction")
            return None
            
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

def setup_ngrok():
    """Complete ngrok setup process"""
    
    print("🔧 NGROK SETUP FOR LAZADA OAUTH")
    print("=" * 50)
    
    # Check if already installed
    if check_ngrok_installed():
        print("✅ Ngrok is ready to use!")
        return True
    
    # Download ngrok
    ngrok_path = download_ngrok()
    if not ngrok_path:
        return False
    
    # Add to PATH (optional)
    print(f"\n📋 SETUP INSTRUCTIONS:")
    print("1. ✅ Ngrok downloaded successfully")
    print("2. 🔑 Get your auth token:")
    print("   • Go to: https://dashboard.ngrok.com/signup")
    print("   • Sign up for free account")
    print("   • Get auth token: https://dashboard.ngrok.com/get-started/your-authtoken")
    print("3. 🔧 Configure auth token:")
    print(f"   • Run: {ngrok_path} authtoken YOUR_TOKEN")
    print("4. 🚀 Ready to use secure OAuth!")
    
    return True

if __name__ == "__main__":
    setup_ngrok()