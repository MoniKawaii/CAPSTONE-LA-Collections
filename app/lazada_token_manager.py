"""
Enhanced Lazada Token Management System

This module provides automated token refresh functionality for Lazada API
including persistent storage and proactive refresh scheduling.
"""

import os
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import requests
import hmac
import hashlib
from dotenv import load_dotenv, set_key
import schedule

class LazadaTokenManager:
    """Advanced Lazada token management with automation and persistence"""
    
    def __init__(self, app_key: str, app_secret: str, initial_access_token: str, 
                 initial_refresh_token: str, env_file_path: str = '.env'):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = initial_access_token
        self.refresh_token = initial_refresh_token
        self.env_file_path = env_file_path
        
        # Token metadata
        self.token_created_at = datetime.now()
        self.token_expires_in = 3600  # Default 1 hour
        self.refresh_token_expires_in = 7776000  # 90 days
        
        # Automation settings
        self.auto_refresh_enabled = True
        self.refresh_margin_seconds = 300  # Refresh 5 minutes before expiry
        self.scheduler_thread = None
        self.scheduler_running = False
        
        # Load token metadata if exists
        self._load_token_metadata()
        
        # Start automated refresh if enabled
        if self.auto_refresh_enabled:
            self.start_automated_refresh()
    
    def _load_token_metadata(self):
        """Load token metadata from file"""
        metadata_file = 'lazada_token_metadata.json'
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    self.token_created_at = datetime.fromisoformat(metadata.get('created_at', datetime.now().isoformat()))
                    self.token_expires_in = metadata.get('expires_in', 3600)
                    self.refresh_token_expires_in = metadata.get('refresh_expires_in', 7776000)
                    print(f"✅ Loaded token metadata: expires in {self.token_expires_in}s")
            except Exception as e:
                print(f"⚠️ Could not load token metadata: {e}")
    
    def _save_token_metadata(self):
        """Save token metadata to file"""
        metadata = {
            'created_at': self.token_created_at.isoformat(),
            'expires_in': self.token_expires_in,
            'refresh_expires_in': self.refresh_token_expires_in,
            'last_refresh': datetime.now().isoformat()
        }
        
        try:
            with open('lazada_token_metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            print(f"⚠️ Could not save token metadata: {e}")
    
    def generate_signature(self, api_path: str, parameters: Dict[str, str]) -> str:
        """Generate signature for Lazada API"""
        sorted_params = sorted(parameters.items())
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
        string_to_sign = api_path + query_string
        
        signature = hmac.new(
            self.app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()
        
        return signature
    
    def refresh_access_token(self) -> Dict[str, Any]:
        """Refresh access token using refresh token"""
        print("🔄 Refreshing Lazada access token...")
        
        api_path = '/auth/token/refresh'
        base_url = 'https://auth.lazada.com.ph/rest'  # Philippines auth sandbox
        
        timestamp = str(int(time.time() * 1000))
        parameters = {
            'app_key': self.app_key,
            'timestamp': timestamp,
            'sign_method': 'sha256',
            'refresh_token': self.refresh_token
        }
        
        signature = self.generate_signature(api_path, parameters)
        parameters['sign'] = signature
        
        try:
            response = requests.post(f"{base_url}{api_path}", data=parameters, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'access_token' in data:
                    # Update tokens
                    old_access_token = self.access_token
                    self.access_token = data['access_token']
                    
                    # Update refresh token if provided
                    if 'refresh_token' in data:
                        self.refresh_token = data['refresh_token']
                    
                    # Update metadata
                    self.token_created_at = datetime.now()
                    self.token_expires_in = data.get('expires_in', 3600)
                    self.refresh_token_expires_in = data.get('refresh_expires_in', 7776000)
                    
                    # Persist to .env file
                    self._update_env_file()
                    
                    # Save metadata
                    self._save_token_metadata()
                    
                    print(f"✅ Access token refreshed successfully")
                    print(f"   New token expires in: {self.token_expires_in} seconds")
                    
                    return {
                        'success': True,
                        'access_token': self.access_token,
                        'refresh_token': self.refresh_token,
                        'expires_in': self.token_expires_in,
                        'old_access_token': old_access_token
                    }
                else:
                    error_msg = data.get('message', 'Unknown error')
                    print(f"❌ Token refresh failed: {error_msg}")
                    return {
                        'success': False,
                        'error': error_msg,
                        'response': data
                    }
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"❌ Token refresh HTTP error: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg
                }
                
        except Exception as e:
            error_msg = f"Token refresh exception: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    def _update_env_file(self):
        """Update .env file with new tokens"""
        try:
            # Update environment variables in memory
            os.environ['LAZADA_ACCESS_TOKEN'] = self.access_token
            os.environ['LAZADA_REFRESH_TOKEN'] = self.refresh_token
            
            # Update .env file
            set_key(self.env_file_path, 'LAZADA_ACCESS_TOKEN', self.access_token)
            set_key(self.env_file_path, 'LAZADA_REFRESH_TOKEN', self.refresh_token)
            
            print(f"✅ Updated tokens in {self.env_file_path}")
            
        except Exception as e:
            print(f"⚠️ Could not update .env file: {e}")
    
    def is_token_expired(self, margin_seconds: int = 300) -> bool:
        """Check if token is expired or will expire soon"""
        expiry_time = self.token_created_at + timedelta(seconds=self.token_expires_in)
        margin_time = datetime.now() + timedelta(seconds=margin_seconds)
        return margin_time >= expiry_time
    
    def is_refresh_token_expired(self) -> bool:
        """Check if refresh token is expired"""
        expiry_time = self.token_created_at + timedelta(seconds=self.refresh_token_expires_in)
        return datetime.now() >= expiry_time
    
    def get_valid_access_token(self) -> Optional[str]:
        """Get a valid access token, refreshing if necessary"""
        if self.is_refresh_token_expired():
            print("❌ Refresh token has expired. Manual re-authentication required.")
            return None
        
        if self.is_token_expired():
            print("🔄 Access token expired, refreshing...")
            result = self.refresh_access_token()
            if result['success']:
                return self.access_token
            else:
                print(f"❌ Failed to refresh token: {result['error']}")
                return None
        
        return self.access_token
    
    def start_automated_refresh(self):
        """Start automated token refresh scheduler"""
        if self.scheduler_running:
            return
        
        # Schedule refresh 5 minutes before expiry
        refresh_interval = max(self.token_expires_in - self.refresh_margin_seconds, 300)
        
        def refresh_job():
            if self.is_token_expired(self.refresh_margin_seconds):
                print("⏰ Scheduled token refresh triggered")
                self.refresh_access_token()
        
        # Schedule the job
        schedule.every(refresh_interval).seconds.do(refresh_job)
        
        # Start scheduler in background thread
        def run_scheduler():
            self.scheduler_running = True
            while self.scheduler_running:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
        
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        print(f"✅ Automated token refresh started (interval: {refresh_interval}s)")
    
    def stop_automated_refresh(self):
        """Stop automated token refresh scheduler"""
        self.scheduler_running = False
        schedule.clear()
        print("⏹️ Automated token refresh stopped")
    
    def get_token_status(self) -> Dict[str, Any]:
        """Get current token status and metadata"""
        expiry_time = self.token_created_at + timedelta(seconds=self.token_expires_in)
        refresh_expiry_time = self.token_created_at + timedelta(seconds=self.refresh_token_expires_in)
        
        return {
            'access_token_valid': not self.is_token_expired(),
            'access_token_expires_at': expiry_time.isoformat(),
            'access_token_expires_in_seconds': max(0, (expiry_time - datetime.now()).total_seconds()),
            'refresh_token_valid': not self.is_refresh_token_expired(),
            'refresh_token_expires_at': refresh_expiry_time.isoformat(),
            'refresh_token_expires_in_seconds': max(0, (refresh_expiry_time - datetime.now()).total_seconds()),
            'auto_refresh_enabled': self.auto_refresh_enabled,
            'scheduler_running': self.scheduler_running
        }

def create_token_manager_from_env(env_file_path: str = '.env') -> LazadaTokenManager:
    """Create token manager from environment variables"""
    load_dotenv(env_file_path)
    
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    if not all([app_key, app_secret, access_token, refresh_token]):
        raise ValueError("Missing required Lazada credentials in environment variables")
    
    return LazadaTokenManager(
        app_key=app_key,
        app_secret=app_secret,
        initial_access_token=access_token,
        initial_refresh_token=refresh_token,
        env_file_path=env_file_path
    )

# Example usage
if __name__ == "__main__":
    try:
        # Create token manager
        token_manager = create_token_manager_from_env()
        
        # Get token status
        status = token_manager.get_token_status()
        print("📊 Token Status:")
        for key, value in status.items():
            print(f"   {key}: {value}")
        
        # Test getting valid token
        valid_token = token_manager.get_valid_access_token()
        if valid_token:
            print(f"✅ Valid access token obtained: {valid_token[:20]}...")
        else:
            print("❌ Could not obtain valid access token")
        
        # Keep running for demonstration
        print("🔄 Token manager running... Press Ctrl+C to stop")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            token_manager.stop_automated_refresh()
            print("👋 Token manager stopped")
            
    except Exception as e:
        print(f"❌ Error: {e}")