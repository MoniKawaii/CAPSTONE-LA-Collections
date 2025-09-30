"""
HTTPS OAuth callback server for Lazada using ngrok
This provides the secure HTTPS URL that Lazada requires
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv
import sys
import threading
import subprocess
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import webbrowser

sys.path.append('./app')
load_dotenv()

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handler for OAuth callback"""
    
    def do_GET(self):
        """Handle GET request with authorization code"""
        global auth_code_received, authorization_code
        
        # Parse the callback URL
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        if 'code' in query_params:
            authorization_code = query_params['code'][0]
            auth_code_received = True
            
            # Send success response
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            success_page = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>🎉 Lazada OAuth Success</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f0f8ff; }}
                    .container {{ max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                    .success {{ color: #28a745; font-size: 24px; margin: 20px; }}
                    .code {{ background: #f8f9fa; padding: 15px; border-radius: 5px; word-break: break-all; border: 1px solid #dee2e6; }}
                    .emoji {{ font-size: 48px; margin: 20px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="emoji">🔐</div>
                    <h1>Lazada OAuth Authorization Successful!</h1>
                    <div class="success">✅ Authorization code received via HTTPS</div>
                    <p><strong>Authorization Code:</strong></p>
                    <div class="code">{}</div>
                    <p><strong>🔄 Token exchange in progress...</strong></p>
                    <p>You can close this window. The token exchange will complete automatically.</p>
                    <hr>
                    <small>Secure HTTPS callback powered by ngrok</small>
                </div>
            </body>
            </html>
            """.format(authorization_code)
            
            self.wfile.write(success_page.encode())
            
            print(f"\n✅ Authorization code received via HTTPS: {authorization_code[:50]}...")
            
        else:
            # Send error response
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            error_page = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>❌ Lazada OAuth Error</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #fff5f5; }}
                    .container {{ max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                    .error {{ color: #dc3545; font-size: 24px; margin: 20px; }}
                    .emoji {{ font-size: 48px; margin: 20px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="emoji">🚫</div>
                    <h1>Authorization Failed</h1>
                    <div class="error">No authorization code received</div>
                    <p>Please try the authorization process again.</p>
                </div>
            </body>
            </html>
            """
            
            self.wfile.write(error_page.encode())
    
    def log_message(self, format, *args):
        """Suppress default log messages"""
        pass

def setup_ngrok_tunnel(port=8001):
    """Set up ngrok tunnel for HTTPS OAuth callback"""
    
    print("🔒 Setting up SECURE HTTPS tunnel with ngrok...")
    print("=" * 60)
    
    # Check if ngrok is installed
    try:
        result = subprocess.run(['ngrok', 'version'], capture_output=True, text=True)
        print(f"✅ Ngrok found: {result.stdout.strip()}")
    except FileNotFoundError:
        print("❌ Ngrok not found!")
        print()
        print("📥 INSTALL NGROK (FREE):")
        print("1. Go to: https://ngrok.com/download")
        print("2. Download ngrok for Windows")
        print("3. Extract to a folder (e.g., C:\\ngrok)")
        print("4. Add to PATH or run from folder")
        print("5. Sign up: https://dashboard.ngrok.com/signup")
        print("6. Get auth token: https://dashboard.ngrok.com/get-started/your-authtoken")
        print("7. Run: ngrok authtoken YOUR_TOKEN")
        print()
        print("💡 Then run this script again!")
        return None, None
    
    # Start ngrok tunnel
    print(f"🚀 Starting HTTPS tunnel on port {port}...")
    
    try:
        # Start ngrok in background with HTTPS
        process = subprocess.Popen(['ngrok', 'http', str(port), '--log=stdout'], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
        
        # Wait for ngrok to start
        print("⏳ Waiting for ngrok to establish tunnel...")
        time.sleep(5)
        
        # Get ngrok tunnel info
        try:
            response = requests.get('http://localhost:4040/api/tunnels', timeout=10)
            tunnels = response.json()
            
            if tunnels['tunnels']:
                # Find HTTPS tunnel
                https_url = None
                for tunnel in tunnels['tunnels']:
                    if tunnel['public_url'].startswith('https://'):
                        https_url = tunnel['public_url']
                        break
                
                if https_url:
                    print(f"✅ SECURE HTTPS tunnel active!")
                    print(f"   Local:  http://localhost:{port}")
                    print(f"   Public: {https_url}")
                    print(f"   🔒 SSL secured by ngrok")
                    
                    return https_url, process
                else:
                    print("❌ No HTTPS tunnel found")
                    return None, process
            else:
                print("❌ No active tunnels found")
                return None, process
                
        except Exception as e:
            print(f"❌ Could not get tunnel info: {e}")
            print("   Please check ngrok manually at http://localhost:4040")
            return None, process
            
    except Exception as e:
        print(f"❌ Could not start ngrok: {e}")
        return None, None

def start_local_server(port=8001):
    """Start the local callback server"""
    global server
    
    server = HTTPServer(('localhost', port), OAuthCallbackHandler)
    print(f"🌐 Starting local server on http://localhost:{port}")
    
    # Start server in a separate thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    return server

def exchange_auth_code_for_tokens(auth_code):
    """Exchange authorization code for access tokens"""
    
    print(f"\n🔄 Exchanging authorization code for tokens...")
    
    # App credentials
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    # Exchange code for tokens
    api_path = '/auth/token/create'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'code': auth_code,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Generate signature
    sorted_params = sorted(parameters.items())
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    string_to_sign = api_path + query_string
    
    signature = hmac.new(
        app_secret.encode('utf-8'),
        string_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    
    parameters['sign'] = signature
    
    # Make token exchange request - Philippines sandbox
    url = 'https://auth.lazada.com.ph/rest' + api_path
    
    print(f"🔗 Making token exchange request to: {url}")
    response = requests.post(url, data=parameters, timeout=30)
    
    print(f"📊 Response Status: {response.status_code}")
    print(f"📋 Response: {response.text}")
    
    if response.status_code == 200:
        try:
            result = response.json()
            
            if 'access_token' in result:
                access_token = result['access_token']
                refresh_token = result.get('refresh_token', '')
                
                print(f"\n🎉 SUCCESS! Tokens obtained:")
                print(f"Access Token: {access_token[:30]}...")
                if refresh_token:
                    print(f"Refresh Token: {refresh_token[:30]}...")
                
                # Update .env file
                update_env_file(access_token, refresh_token)
                
                # Test the tokens
                test_tokens(access_token)
                
                return True
            else:
                print(f"❌ Token exchange failed: {result}")
                return False
                
        except Exception as e:
            print(f"❌ Error parsing token response: {e}")
            return False
    else:
        print(f"❌ Token exchange failed with status {response.status_code}")
        return False

def update_env_file(access_token, refresh_token):
    """Update .env file with new tokens"""
    
    env_file = '.env'
    
    try:
        # Read existing .env file
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        # Update or add token lines
        updated_lines = []
        access_updated = False
        refresh_updated = False
        
        for line in lines:
            if line.startswith('LAZADA_ACCESS_TOKEN='):
                updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
                access_updated = True
            elif line.startswith('LAZADA_REFRESH_TOKEN='):
                if refresh_token:
                    updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
                    refresh_updated = True
                else:
                    updated_lines.append(line)
            else:
                updated_lines.append(line)
        
        # Add tokens if they weren't found
        if not access_updated:
            updated_lines.append(f'LAZADA_ACCESS_TOKEN={access_token}\n')
        
        if refresh_token and not refresh_updated:
            updated_lines.append(f'LAZADA_REFRESH_TOKEN={refresh_token}\n')
        
        with open(env_file, 'w') as f:
            f.writelines(updated_lines)
        
        print(f"✅ Updated {env_file} with new tokens")
        
    except Exception as e:
        print(f"❌ Error updating .env: {e}")

def test_tokens(access_token):
    """Test the newly obtained tokens"""
    
    print("\n🧪 Testing new tokens...")
    
    try:
        from app import lazop
        
        app_key = os.getenv('LAZADA_APP_KEY')
        app_secret = os.getenv('LAZADA_APP_SECRET')
        
        # Test seller API
        url = 'https://api.lazada.com.ph/rest'
        client = lazop.LazopClient(url, app_key, app_secret)
        request = lazop.LazopRequest('/seller/get')
        response = client.execute(request, access_token)
        
        print(f"🔍 API Test Result:")
        print(f"Type: {response.type}")
        print(f"Code: {response.code}")
        
        if response.type == 'SUCCESS' or response.code == '0':
            print("✅ SUCCESS! Tokens are working!")
            print("🎉 Your Lazada API integration is now operational!")
        else:
            print("❌ Token test failed!")
            print(f"Response: {response.body}")
            
    except Exception as e:
        print(f"⚠️ Could not test tokens: {e}")

def main():
    """Main OAuth flow with HTTPS ngrok tunnel"""
    global auth_code_received, authorization_code, server
    
    auth_code_received = False
    authorization_code = None
    ngrok_process = None
    
    print("🔒 SECURE LAZADA OAUTH WITH HTTPS")
    print("=" * 60)
    print("🛡️ Using ngrok for secure HTTPS tunnel")
    print()
    
    # Get app key
    app_key = os.getenv('LAZADA_APP_KEY')
    if not app_key:
        print("❌ LAZADA_APP_KEY not found in .env file")
        return
    
    try:
        # Setup ngrok HTTPS tunnel
        https_url, ngrok_process = setup_ngrok_tunnel()
        
        if not https_url:
            print("❌ Could not establish HTTPS tunnel")
            print("   Please install ngrok and try again")
            return
        
        # Start local callback server
        print(f"\n🌐 Starting local callback server...")
        server = start_local_server()
        
        # Generate authorization URL with HTTPS callback
        auth_url = f"https://auth.lazada.com.ph/oauth/authorize?response_type=code&force_auth=true&redirect_uri={https_url}&client_id={app_key}"
        
        print(f"\n🔒 SECURE HTTPS Authorization URL:")
        print("=" * 60)
        print(auth_url)
        
        print(f"\n📋 SETUP INSTRUCTIONS:")
        print("1. 🔧 Update Lazada app redirect URL to:")
        print(f"   {https_url}")
        print("   Go to: https://open.lazada.com/apps/myapp")
        print("   Save the configuration")
        print()
        print("2. 🌐 Authorization will open automatically")
        print("3. 🔑 Login with Philippines Lazada seller account")
        print("4. ✅ Click 'Authorize' to approve")
        print("5. 🔄 Token exchange happens automatically")
        
        # Wait for user confirmation
        input("\nPress Enter after updating Lazada app redirect URL...")
        
        # Open browser automatically
        try:
            webbrowser.open(auth_url)
            print("\n🌐 Opening secure authorization URL...")
        except:
            print("\n⚠️ Could not open browser. Please copy the URL above.")
        
        # Wait for callback
        print("\n⏳ Waiting for HTTPS OAuth callback...")
        
        # Wait up to 10 minutes for callback
        timeout = 600  # 10 minutes
        start_time = time.time()
        
        while not auth_code_received and (time.time() - start_time) < timeout:
            time.sleep(1)
        
        if auth_code_received:
            print(f"\n🎉 Secure authorization successful!")
            
            # Exchange code for tokens
            success = exchange_auth_code_for_tokens(authorization_code)
            
            if success:
                print("\n🚀 HTTPS OAuth flow completed successfully!")
                print("🔒 Your secure Lazada API integration is ready!")
            else:
                print("\n❌ Token exchange failed. Please try again.")
        else:
            print(f"\n⏰ Timeout waiting for authorization callback.")
            print("Please try again.")
            
    finally:
        # Cleanup
        try:
            if 'server' in globals() and server:
                server.shutdown()
                server.server_close()
        except:
            pass
            
        try:
            if ngrok_process:
                ngrok_process.terminate()
                print("\n🛑 Ngrok tunnel stopped")
        except:
            pass

if __name__ == "__main__":
    main()