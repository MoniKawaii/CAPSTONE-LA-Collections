"""
Lazada Open Platform SDK for Python
Based on official Lazada API documentation
"""

import time
import hmac
import hashlib
import requests
from typing import Dict, Any, Optional
import json


class LazopRequest:
    """Lazada API Request class"""
    
    def __init__(self, api_path: str, http_method: str = 'GET'):
        self.api_path = api_path
        self.http_method = http_method.upper()
        self.api_params = {}
        self.file_params = {}
    
    def add_api_param(self, key: str, value: str):
        """Add API parameter"""
        self.api_params[key] = value
    
    def add_file_param(self, key: str, value):
        """Add file parameter"""
        self.file_params[key] = value


class LazopResponse:
    """Lazada API Response class"""
    
    def __init__(self, response: requests.Response):
        self.status_code = response.status_code
        self.headers = response.headers
        self._response = response
        
        try:
            self.body = response.json()
        except:
            self.body = response.text
        
        # Set response type based on content
        if isinstance(self.body, dict):
            self.type = self.body.get('type', 'SUCCESS')
            self.code = self.body.get('code', '0')
            self.message = self.body.get('message', '')
        else:
            self.type = 'SUCCESS' if response.status_code == 200 else 'ERROR'
            self.code = str(response.status_code)
            self.message = self.body if isinstance(self.body, str) else ''


class LazopClient:
    """Lazada Open Platform Client"""
    
    def __init__(self, server_url: str, app_key: str, app_secret: str):
        self.server_url = server_url.rstrip('/')
        self.app_key = app_key
        self.app_secret = app_secret
    
    def execute(self, request: LazopRequest, access_token: Optional[str] = None) -> LazopResponse:
        """Execute API request"""
        timestamp = str(int(time.time() * 1000))
        
        # Build system parameters
        sys_params = {
            'app_key': self.app_key,
            'sign_method': 'sha256',
            'timestamp': timestamp
        }
        
        if access_token:
            sys_params['access_token'] = access_token
        
        # Combine system and API parameters
        all_params = {**sys_params, **request.api_params}
        
        # Generate signature
        signature = self._generate_signature(request.api_path, all_params)
        all_params['sign'] = signature
        
        # Make request
        url = f"{self.server_url}{request.api_path}"
        
        if request.http_method == 'GET':
            response = requests.get(url, params=all_params, timeout=30)
        else:
            response = requests.post(url, data=all_params, files=request.file_params, timeout=30)
        
        return LazopResponse(response)
    
    def _generate_signature(self, api_path: str, parameters: Dict[str, str]) -> str:
        """Generate API signature"""
        # Sort parameters
        sorted_params = sorted(parameters.items())
        
        # Create query string
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
        
        # Create string to sign - FIXED: No separator needed between path and query
        string_to_sign = api_path + query_string
        
        # Generate signature
        signature = hmac.new(
            self.app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()
        
        return signature


# Convenience functions for common endpoints
def refresh_token(app_key: str, app_secret: str, refresh_token: str, 
                 server_url: str = 'https://auth.lazada.com/rest') -> LazopResponse:
    """Refresh access token using refresh token"""
    client = LazopClient(server_url, app_key, app_secret)
    request = LazopRequest('/auth/token/refresh', 'POST')
    request.add_api_param('refresh_token', refresh_token)
    
    return client.execute(request)


def get_seller_info(app_key: str, app_secret: str, access_token: str,
                   server_url: str = 'https://api.lazada.com.ph/rest') -> LazopResponse:
    """Get seller information"""
    client = LazopClient(server_url, app_key, app_secret)
    request = LazopRequest('/seller/get')
    
    return client.execute(request, access_token)


# Example usage and testing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    print("Testing Lazada SDK...")
    
    # Test token refresh
    print("Attempting token refresh...")
    response = refresh_token(app_key, app_secret, refresh_token)
    
    print(f"Response type: {response.type}")
    print(f"Response code: {response.code}")
    print(f"Response message: {response.message}")
    print(f"Response body: {json.dumps(response.body, indent=2)}")