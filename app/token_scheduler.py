"""
Automatic Token Refresh Scheduler

This module provides background task scheduling for automatic Lazada token refresh.
"""

import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from app.lazada_service import lazada_service

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TokenRefreshScheduler:
    def __init__(self):
        self.refresh_task: Optional[asyncio.Task] = None
        self.is_running = False
        
    async def start_automatic_refresh(self, check_interval_minutes: int = 5):
        """
        Start automatic token refresh background task
        
        Args:
            check_interval_minutes (int): How often to check token expiry (in minutes)
        """
        if self.is_running:
            logger.info("Token refresh scheduler is already running")
            return
            
        self.is_running = True
        self.refresh_task = asyncio.create_task(
            self._refresh_loop(check_interval_minutes)
        )
        logger.info(f"✅ Started automatic token refresh (checking every {check_interval_minutes} minutes)")
    
    async def stop_automatic_refresh(self):
        """Stop automatic token refresh"""
        if self.refresh_task and not self.refresh_task.done():
            self.refresh_task.cancel()
            try:
                await self.refresh_task
            except asyncio.CancelledError:
                pass
        
        self.is_running = False
        logger.info("🛑 Stopped automatic token refresh")
    
    async def _refresh_loop(self, check_interval_minutes: int):
        """
        Background loop that checks and refreshes tokens
        
        Args:
            check_interval_minutes (int): Check interval in minutes
        """
        check_interval_seconds = check_interval_minutes * 60
        
        while self.is_running:
            try:
                # Check if we have tokens and if they need refreshing
                if lazada_service.current_tokens:
                    created_at = lazada_service.current_tokens.get('created_at', 0)
                    expires_in = lazada_service.current_tokens.get('expires_in', 3600)
                    
                    # Calculate time until expiry
                    expiry_time = created_at + expires_in
                    current_time = int(time.time())
                    time_until_expiry = expiry_time - current_time
                    
                    # Refresh if token expires in next 10 minutes
                    if time_until_expiry <= 600:  # 10 minutes
                        logger.info("🔄 Token expires soon, refreshing automatically...")
                        
                        try:
                            await lazada_service.refresh_access_token()
                            logger.info("✅ Token refreshed automatically")
                        except Exception as e:
                            logger.error(f"❌ Automatic token refresh failed: {e}")
                    else:
                        minutes_until_expiry = time_until_expiry // 60
                        logger.info(f"ℹ️ Token is valid for {minutes_until_expiry} more minutes")
                else:
                    logger.warning("⚠️ No tokens available for refresh")
                
                # Wait for next check
                await asyncio.sleep(check_interval_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in token refresh loop: {e}")
                await asyncio.sleep(check_interval_seconds)
    
    def get_status(self) -> dict:
        """
        Get current scheduler status
        
        Returns:
            dict: Status information
        """
        return {
            "is_running": self.is_running,
            "has_task": self.refresh_task is not None,
            "task_done": self.refresh_task.done() if self.refresh_task else None
        }

# Global scheduler instance
token_scheduler = TokenRefreshScheduler()

# Auto-start functions for FastAPI events
async def start_scheduler():
    """Start the token refresh scheduler (called on app startup)"""
    import time
    await token_scheduler.start_automatic_refresh(check_interval_minutes=5)

async def stop_scheduler():
    """Stop the token refresh scheduler (called on app shutdown)"""
    await token_scheduler.stop_automatic_refresh()