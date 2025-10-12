"""
Simple Callback Server for Shopee API OAuth

This script runs a simple HTTP server that captures the authorization code
from Shopee OAuth redirects. Use with ngrok to receive the callback.

Usage:
    python shopee_callback_server.py
"""

import http.server
import socketserver
from urllib.parse import urlparse, parse_qs

class CallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests to capture the authorization code"""
        # Parse URL and extract code parameter
        query_components = parse_qs(urlparse(self.path).query)
        code = query_components.get('code', [''])[0]
        
        # Create response HTML
        response_html = f"""
        <html>
        <head>
            <title>Shopee API Authorization</title>
            <style>
                body {{ font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; }}
                .success {{ color: green; }}
                .code {{ background-color: #f0f0f0; padding: 10px; border-radius: 4px; word-break: break-all; }}
                .instructions {{ margin-top: 20px; }}
            </style>
        </head>
        <body>
            <h1>Shopee API Authorization</h1>
            {'<div class="success"><h2>✅ Authorization Successful!</h2></div>' if code else '<h2>❌ No authorization code received</h2>'}
            
            {f'<div><strong>Your authorization code is:</strong></div><div class="code">{code}</div>' if code else ''}
            
            <div class="instructions">
                <p><strong>Next steps:</strong></p>
                <ol>
                    <li>Copy this authorization code</li>
                    <li>Return to your terminal</li>
                    <li>Paste the code when prompted</li>
                </ol>
            </div>
        </body>
        </html>
        """
        
        # Send response
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(response_html.encode('utf-8'))
        
        # Print code to terminal for easy copy/paste
        if code:
            print("\n" + "="*50)
            print(f"✅ Authorization code received: {code}")
            print("Copy this code and paste it into the get_shopee_tokens.py prompt")
            print("="*50 + "\n")

def run_server(port=8000):
    """Run the callback server on the specified port"""
    server_address = ('', port)
    httpd = socketserver.TCPServer(server_address, CallbackHandler)
    
    print(f"Starting callback server on port {port}...")
    print(f"Local URL: http://localhost:{port}")
    print("\nWaiting for Shopee OAuth callback...\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.server_close()

if __name__ == "__main__":
    run_server()