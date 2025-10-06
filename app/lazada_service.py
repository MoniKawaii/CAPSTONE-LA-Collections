"""
Lazada OAuth Service with ngrok Integration

This service handles:
1. ngrok tunnel setup for OAuth callback
2. Automatic token exchange and refresh
3. API data retrieval using Lazada SDK
"""

import os
import json
import time
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from lazop_sdk import LazopClient, LazopRequest
from pyngrok import ngrok
from fastapi import HTTPException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class LazadaOAuthService:
    def __init__(self):
        self.app_key = os.getenv('LAZADA_APP_KEY')
        self.app_secret = os.getenv('LAZADA_APP_SECRET')
        
        if not self.app_key or not self.app_secret:
            raise ValueError("LAZADA_APP_KEY and LAZADA_APP_SECRET must be set in .env file")
        
        # Lazada API endpoints (Philippines)
        self.api_url = 'https://api.lazada.com.ph/rest'
        self.auth_url = 'https://auth.lazada.com/rest'
        
        # Initialize client
        self.client = LazopClient(self.api_url, self.app_key, self.app_secret)
        
        # Token storage
        self.tokens_file = 'lazada_tokens.json'
        self.current_tokens = self.load_tokens()
        
        # ngrok tunnel
        self.tunnel_url = None
        
    def setup_ngrok_tunnel(self, port: int = 8000) -> str:
        """
        Set up ngrok tunnel for OAuth callback
        
        Args:
            port (int): Local port to tunnel
            
        Returns:
            str: Public ngrok URL
        """
        try:
            # Kill existing tunnels
            ngrok.kill()
            
            # Create new tunnel
            tunnel = ngrok.connect(port, "http")
            self.tunnel_url = tunnel.public_url
            
            print(f"✅ ngrok tunnel established: {self.tunnel_url}")
            print(f"📋 Use this callback URL in Lazada Developer Console:")
            print(f"   {self.tunnel_url}/lazada/callback")
            
            return self.tunnel_url
            
        except Exception as e:
            print(f"❌ Failed to setup ngrok tunnel: {e}")
            raise HTTPException(status_code=500, detail=f"ngrok setup failed: {e}")
    
    def get_authorization_url(self, redirect_uri: str = None) -> str:
        """
        Generate Lazada OAuth authorization URL
        
        Args:
            redirect_uri (str): OAuth callback URL
            
        Returns:
            str: Authorization URL
        """
        if not redirect_uri and self.tunnel_url:
            redirect_uri = f"{self.tunnel_url}/lazada/callback"
        elif not redirect_uri:
            raise ValueError("redirect_uri required when ngrok tunnel not established")
        
        # OAuth parameters
        params = {
            'response_type': 'code',
            'force_auth': 'true',
            'redirect_uri': redirect_uri,
            'client_id': self.app_key
        }
        
        # Build authorization URL
        auth_url = f"https://auth.lazada.com/oauth/authorize"
        param_str = "&".join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{auth_url}?{param_str}"
        
        print(f"🔗 Authorization URL generated:")
        print(f"   {full_url}")
        
        return full_url
    
    async def exchange_authorization_code(self, auth_code: str) -> Dict[str, Any]:
        """
        Exchange authorization code for access and refresh tokens
        
        Args:
            auth_code (str): Authorization code from OAuth callback
            
        Returns:
            Dict: Token response data
        """
        try:
            # Create token request
            request = LazopRequest('/auth/token/create')
            request.add_api_param('code', auth_code)
            
            # Execute request
            response = self.client.execute(request)
            
            if response.type == 'ISP' or response.type == 'ISV':
                raise HTTPException(
                    status_code=400, 
                    detail=f"Lazada API Error: {response.message}"
                )
            
            # Parse response
            token_data = {
                'access_token': response.body.get('access_token'),
                'refresh_token': response.body.get('refresh_token'),
                'expires_in': response.body.get('expires_in', 3600),
                'account_platform': response.body.get('account_platform'),
                'created_at': int(time.time())
            }
            
            # Save tokens
            await self.save_tokens(token_data)
            self.current_tokens = token_data
            
            print(f"✅ Tokens obtained successfully!")
            print(f"   Access Token: {token_data['access_token'][:20]}...")
            print(f"   Expires in: {token_data['expires_in']} seconds")
            
            return token_data
            
        except Exception as e:
            print(f"❌ Token exchange failed: {e}")
            raise HTTPException(status_code=400, detail=f"Token exchange failed: {e}")
    
    async def refresh_access_token(self, refresh_token: str = None) -> Dict[str, Any]:
        """
        Refresh access token using refresh token
        
        Args:
            refresh_token (str): Refresh token (optional, will use stored token)
            
        Returns:
            Dict: New token data
        """
        if not refresh_token:
            if not self.current_tokens or 'refresh_token' not in self.current_tokens:
                raise HTTPException(status_code=400, detail="No refresh token available")
            refresh_token = self.current_tokens['refresh_token']
        
        try:
            # Create refresh request
            request = LazopRequest('/auth/token/refresh')
            request.add_api_param('refresh_token', refresh_token)
            
            # Execute request
            response = self.client.execute(request)
            
            if response.type == 'ISP' or response.type == 'ISV':
                raise HTTPException(
                    status_code=400, 
                    detail=f"Token refresh failed: {response.message}"
                )
            
            # Parse response
            token_data = {
                'access_token': response.body.get('access_token'),
                'refresh_token': response.body.get('refresh_token', refresh_token),
                'expires_in': response.body.get('expires_in', 3600),
                'account_platform': response.body.get('account_platform'),
                'created_at': int(time.time())
            }
            
            # Save tokens
            await self.save_tokens(token_data)
            self.current_tokens = token_data
            
            print(f"✅ Token refreshed successfully!")
            print(f"   New Access Token: {token_data['access_token'][:20]}...")
            
            return token_data
            
        except Exception as e:
            print(f"❌ Token refresh failed: {e}")
            raise HTTPException(status_code=400, detail=f"Token refresh failed: {e}")
    
    def is_token_expired(self) -> bool:
        """
        Check if current access token is expired
        
        Returns:
            bool: True if token is expired or expires soon
        """
        if not self.current_tokens:
            return True
        
        created_at = self.current_tokens.get('created_at', 0)
        expires_in = self.current_tokens.get('expires_in', 3600)
        
        # Handle different date formats
        if isinstance(created_at, str):
            try:
                # Try to parse as datetime string
                from datetime import datetime
                created_dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                created_at = int(created_dt.timestamp())
            except ValueError:
                # If parsing fails, assume token is expired
                return True
        
        # Consider token expired if it expires in next 5 minutes
        expiry_time = created_at + expires_in - 300  # 5 min buffer
        current_time = int(time.time())
        
        return current_time >= expiry_time
    
    async def ensure_valid_token(self) -> str:
        """
        Ensure we have a valid access token, refresh if necessary
        
        Returns:
            str: Valid access token
        """
        if not self.current_tokens:
            raise HTTPException(status_code=401, detail="No tokens available. Please authorize first.")
        
        if self.is_token_expired():
            print("🔄 Token expired, refreshing...")
            await self.refresh_access_token()
        
        return self.current_tokens['access_token']
    
    async def get_seller_info(self) -> Dict[str, Any]:
        """
        Get seller information from Lazada API
        
        Returns:
            Dict: Seller information
        """
        access_token = await self.ensure_valid_token()
        
        try:
            # Create seller info request
            request = LazopRequest('/seller/get')
            request.add_api_param('access_token', access_token)
            
            # Execute request
            response = self.client.execute(request)
            
            if response.type == 'ISP' or response.type == 'ISV':
                raise HTTPException(
                    status_code=400, 
                    detail=f"API Error: {response.message}"
                )
            
            return response.body
            
        except Exception as e:
            print(f"❌ Failed to get seller info: {e}")
            raise HTTPException(status_code=400, detail=f"Failed to get seller info: {e}")
    
    async def get_products(self, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """
        Get products from Lazada API
        
        Args:
            limit (int): Number of products to fetch
            offset (int): Offset for pagination
            
        Returns:
            Dict: Products data
        """
        access_token = await self.ensure_valid_token()
        
        try:
            # Create products request
            request = LazopRequest('/products/get', 'GET')
            request.add_api_param('limit', str(limit))
            request.add_api_param('offset', str(offset))
            
            # Execute request with access token
            response = self.client.execute(request, access_token)
            
            if response.type == 'ISP' or response.type == 'ISV':
                raise HTTPException(
                    status_code=400, 
                    detail=f"API Error: {response.message}"
                )
            
            return response.body
            
        except Exception as e:
            print(f"❌ Failed to get products: {e}")
            raise HTTPException(status_code=400, detail=f"Failed to get products: {e}")
    
    async def get_orders(self, 
                        created_after: str = None, 
                        created_before: str = None,
                        limit: int = 50,
                        offset: int = 0) -> Dict[str, Any]:
        """
        Get orders from Lazada API
        
        Args:
            created_after (str): ISO format date string
            created_before (str): ISO format date string
            limit (int): Number of orders to fetch
            offset (int): Offset for pagination
            
        Returns:
            Dict: Orders data
        """
        access_token = await self.ensure_valid_token()
        
        try:
            # Create orders request
            request = LazopRequest('/orders/get', 'GET')
            request.add_api_param('limit', str(limit))
            request.add_api_param('offset', str(offset))
            request.add_api_param('sort_by', 'updated_at')
            request.add_api_param('sort_direction', 'DESC')
            
            # Add required date parameters (API requires either created_after or updated_after)
            if created_after:
                request.add_api_param('created_after', created_after)
            else:
                # Default to last 30 days if not specified
                from datetime import datetime, timedelta
                default_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S+08:00')
                request.add_api_param('created_after', default_date)
                
            if created_before:
                request.add_api_param('created_before', created_before)
            
            # Execute request with access token
            response = self.client.execute(request, access_token)
            
            if response.type == 'ISP' or response.type == 'ISV':
                raise HTTPException(
                    status_code=400, 
                    detail=f"API Error: {response.message}"
                )
            
            return response.body
            
        except Exception as e:
            print(f"❌ Failed to get orders: {e}")
            raise HTTPException(status_code=400, detail=f"Failed to get orders: {e}")
    
    def load_tokens(self) -> Optional[Dict[str, Any]]:
        """
        Load tokens from file
        
        Returns:
            Dict or None: Token data
        """
        try:
            if os.path.exists(self.tokens_file):
                with open(self.tokens_file, 'r') as f:
                    tokens = json.load(f)
                print(f"✅ Tokens loaded from {self.tokens_file}")
                return tokens
        except Exception as e:
            print(f"⚠️ Failed to load tokens: {e}")
        
        return None
    
    async def save_tokens(self, token_data: Dict[str, Any]) -> bool:
        """
        Save tokens to JSON file only (not .env)
        
        Args:
            token_data (Dict): Token data to save
            
        Returns:
            bool: Success status
        """
        try:
            # Save to JSON file only
            with open(self.tokens_file, 'w') as f:
                json.dump(token_data, f, indent=2)
            
            print(f"✅ Tokens saved to {self.tokens_file}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to save tokens: {e}")
            return False
    
    def stop_ngrok(self):
        """Stop ngrok tunnel"""
        try:
            ngrok.kill()
            print("🛑 ngrok tunnel stopped")
        except:
            pass

# Global service instance
lazada_service = LazadaOAuthService()