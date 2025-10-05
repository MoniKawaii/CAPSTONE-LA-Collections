#!/usr/bin/env python3
"""
Test Lazada Orders API

This script tests the Lazada /orders/get API using your current tokens
"""

import json
import os
import sys
import asyncio
from app.lazada_service import LazadaOAuthService

async def test_orders_api():
    """Test the orders API"""
    print("🔍 Testing Lazada Orders API...")
    print("=" * 40)
    
    try:
        # Initialize the service
        lazada_service = LazadaOAuthService()
        
        # Test getting orders
        print("📦 Fetching orders...")
        result = await lazada_service.get_orders()
        
        if result:
            print("✅ Orders API Success!")
            print(f"📊 Response: {json.dumps(result, indent=2)}")
        else:
            print("No orders returned")
            
    except Exception as e:
        print(f"Error testing orders API: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_orders_api())