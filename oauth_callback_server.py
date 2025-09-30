"""
Local OAuth callback server for Lazada authorization
This creates a temporary local server to receive the OAuth callback
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv
import sys
import threading
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
                <title>Lazada OAuth Success</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
                    .success { color: green; font-size: 24px; margin: 20px; }
                    .code { background: #f5f5f5; padding: 10px; border-radius: 5px; word-break: break-all; }
                </style>
            </head>
            <body>
                <h1>🎉 Authorization Successful!</h1>
                <div class="success">✅ Authorization code received successfully</div>
                <p><strong>Authorization Code:</strong></p>
                <div class="code">{}</div>
                <p>You can close this window. The token exchange will happen automatically.</p>
            </body>
            </html>
            """.format(authorization_code)
            
            self.wfile.write(success_page.encode())
            
            print(f"\n✅ Authorization code received: {authorization_code[:50]}...")
            
        else:
            # Send error response
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            error_page = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Lazada OAuth Error</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
                    .error { color: red; font-size: 24px; margin: 20px; }
                </style>
            </head>
            <body>
                <h1>❌ Authorization Failed</h1>
                <div class="error">No authorization code received</div>
                <p>Please try the authorization process again.</p>
            </body>
            </html>
            """
            
            self.wfile.write(error_page.encode())
    
    def log_message(self, format, *args):
        """Suppress default log messages"""
        pass

def start_callback_server(port=8001):
    """Start the local callback server"""
    global server
    
    server = HTTPServer(('localhost', port), OAuthCallbackHandler)
    print(f"🌐 Starting callback server on http://localhost:{port}")
    print("   Waiting for OAuth callback...")
    
    # Start server in a separate thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    return f"http://localhost:{port}"

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
                
                print(f"\n✅ SUCCESS! Tokens obtained:")
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
    """Main OAuth flow with local server"""
    global auth_code_received, authorization_code, server
    
    auth_code_received = False
    authorization_code = None
    
    print("🚀 Lazada OAuth with Local Callback Server")
    print("=" * 50)
    
    # Get app key
    app_key = os.getenv('LAZADA_APP_KEY')
    if not app_key:
        print("❌ LAZADA_APP_KEY not found in .env file")
        return
    
    # Start local callback server
    port = 8001
    callback_url = start_callback_server(port)
    
    # Generate authorization URL
    auth_url = f"https://auth.lazada.com.ph/oauth/authorize?response_type=code&force_auth=true&redirect_uri={callback_url}&client_id={app_key}"
    
    print(f"\n🇵🇭 Philippines Sandbox Authorization URL:")
    print("=" * 50)
    print(auth_url)
    
    print(f"\n📋 Instructions:")
    print("1. The authorization URL will open automatically in your browser")
    print("2. Login with your Philippines Lazada seller account")
    print("3. Click 'Authorize' to approve the app")
    print("4. The callback will be handled automatically")
    print("5. Token exchange will happen automatically")
    
    # Open browser automatically
    try:
        webbrowser.open(auth_url)
        print("\n🌐 Opening browser automatically...")
    except:
        print("\n⚠️ Could not open browser automatically. Please copy the URL above.")
    
    # Wait for callback
    print("\n⏳ Waiting for authorization callback...")
    
    # Wait up to 5 minutes for callback
    timeout = 300  # 5 minutes
    start_time = time.time()
    
    while not auth_code_received and (time.time() - start_time) < timeout:
        time.sleep(1)
    
    # Stop server
    try:
        server.shutdown()
        server.server_close()
    except:
        pass
    
    if auth_code_received:
        print(f"\n🎉 Authorization successful!")
        
        # Exchange code for tokens
        success = exchange_auth_code_for_tokens(authorization_code)
        
        if success:
            print("\n🚀 OAuth flow completed successfully!")
            print("Your Lazada API integration is now ready!")
        else:
            print("\n❌ Token exchange failed. Please try again.")
    else:
        print(f"\n⏰ Timeout waiting for authorization callback.")
        print("Please try again or use manual token exchange.")

if __name__ == "__main__":
    main()