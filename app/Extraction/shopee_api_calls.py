"""
Shopee API Integration for LA Collections Analytics Platform

This module handles Shopee API calls for data extraction from the Shopee platform.
It integrates with config.py for credentials and token management.

The module provides functions for:
- Fetching orders and order details
- Fetching products and product details
- Fetching traffic/marketing data
- Error handling and automatic retries

All data is returned in standardized dictionaries ready for ETL processing.
"""

import time
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import backoff

import requests
from urllib.parse import urlencode

from app.config import (
    # API credentials and endpoints
    SHOPEE_PARTNER_ID,
    SHOPEE_API_KEY,
    SHOPEE_API_SECRET,
    SHOPEE_BASE_URL,
    
    # Token management functions
    get_shopee_access_token,
    generate_shopee_signature,
    
    # Constants
    PLATFORM_SHOPEE,
    COLUMN_MAPPINGS
)

# Set up module logger
logger = logging.getLogger(__name__)

# Define exception for rate limiting
class RateLimitExceeded(Exception):
    """Exception raised when API rate limit is exceeded"""
    pass

# Define exception for API errors
class ShopeeAPIError(Exception):
    """Exception raised for Shopee API errors"""
    pass

# Define backoff strategy for API rate limiting
def backoff_handler(details: Dict[str, Any]) -> None:
    """Handler for backoff events, logs retry attempts"""
    logger.warning(
        f"Backing off {details['wait']:.1f} seconds after {details['tries']} tries "
        f"calling function {details['target'].__name__} with args {details['args']} and kwargs "
        f"{details['kwargs']}"
    )

# Check if response indicates rate limiting
def is_rate_limited(e: Exception) -> bool:
    """Check if exception indicates rate limiting"""
    if isinstance(e, requests.exceptions.HTTPError):
        return e.response.status_code == 429
    elif isinstance(e, ShopeeAPIError):
        return "rate" in str(e).lower() or "too many requests" in str(e).lower()
    return False

@backoff.on_exception(
    backoff.expo,
    (RateLimitExceeded, requests.exceptions.HTTPError),
    max_tries=5,
    on_backoff=backoff_handler,
    giveup=lambda e: not is_rate_limited(e)
)
def make_api_request(
    path: str, 
    method: str = "GET", 
    params: Optional[Dict[str, Any]] = None, 
    data: Optional[Dict[str, Any]] = None,
    shop_id: int = 0
) -> Dict[str, Any]:
    """
    Make authenticated request to Shopee API with automatic retries.
    
    Args:
        path: API endpoint path (without base URL)
        method: HTTP method (GET or POST)
        params: URL query parameters
        data: Request body for POST requests
        shop_id: Shopee shop ID (default: 0 for partner auth)
        
    Returns:
        Dict[str, Any]: API response as JSON dictionary
        
    Raises:
        ShopeeAPIError: If API returns an error
        requests.exceptions.HTTPError: For HTTP errors
        requests.exceptions.RequestException: For request failures
    """
    # Get access token with automatic refresh if needed
    access_token = get_shopee_access_token()
    if not access_token:
        raise ShopeeAPIError("Failed to get valid access token")
    
    # Generate timestamp
    timestamp = int(time.time())
    
    # For Shop APIs, we need to include access_token and shop_id in the signature
    # This is critical for Shop API calls to work correctly
    signature = generate_shopee_signature(
        path=path,
        timestamp=timestamp,
        access_token=access_token,
        shop_id=shop_id if shop_id else None
    )
    
    # Prepare URL
    url = f"{SHOPEE_BASE_URL.rstrip('/')}{path}"
    
    # Prepare parameters
    request_params = {
        "partner_id": SHOPEE_PARTNER_ID,
        "timestamp": timestamp,
        "sign": signature,
        "access_token": access_token
    }
    
    # Add shop_id only if it's provided (needed for Shop APIs)
    if shop_id:
        request_params["shop_id"] = shop_id
    
    # Add additional parameters if provided
    if params:
        request_params.update(params)
    
    # Prepare headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        logger.debug(f"Making {method} request to {path}")
        
        if method.upper() == "GET":
            response = requests.get(url, params=request_params, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(url, params=request_params, json=data, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse JSON response
        result = response.json()
        
        # Check for API errors
        if "error" in result and result["error"] != 0:
            error_message = result.get("message", "Unknown error")
            error_code = result.get("error", 0)
            
            # Check for rate limiting
            if "too many requests" in error_message.lower() or error_code == 429:
                raise RateLimitExceeded(f"Rate limit exceeded: {error_message}")
            
            raise ShopeeAPIError(f"API error {error_code}: {error_message}")
        
        return result
    except requests.exceptions.RequestException as e:
        # Log and re-raise the exception
        logger.error(f"Request failed: {e}")
        raise

def get_shop_info() -> Dict[str, Any]:
    """
    Get basic shop information
    
    Returns:
        Dict[str, Any]: Shop information
    """
    path = "/api/v2/shop/get_shop_info"

def get_orders(
    time_from: Optional[int] = None,
    time_to: Optional[int] = None,
    order_status: Optional[str] = None,
    page_size: int = 100,
    cursor: str = "",
    time_range_field: str = "create_time"
) -> Dict[str, Any]:
    """
    Get orders with optional filters.
    
    Args:
        time_from: Start timestamp (default: 3 months ago)
        time_to: End timestamp (default: current time)
        order_status: Filter by order status
        page_size: Number of orders per page (max 100)
        cursor: Pagination cursor
        time_range_field: Field to use for time range (create_time or update_time)
        
    Returns:
        Dict[str, Any]: Orders data with pagination info
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    path = "/api/v2/order/get_order_list"
    
    # Set default time range if not provided
    if time_from is None:
        # Default to 3 months ago
        time_from = int((datetime.now() - timedelta(days=90)).timestamp())
    
    if time_to is None:
        time_to = int(time.time())
    
    # Prepare parameters
    params = {
        "time_from": time_from,
        "time_to": time_to,
        "time_range_field": time_range_field,
        "page_size": page_size
    }
    
    # Add optional parameters
    if cursor:
        params["cursor"] = cursor
        
    if order_status:
        params["order_status"] = order_status
    
    return make_api_request(path, params=params)

def get_all_orders(
    time_from: Optional[int] = None,
    time_to: Optional[int] = None,
    order_status: Optional[str] = None,
    time_range_field: str = "create_time"
) -> List[Dict[str, Any]]:
    """
    Get all orders with pagination handling.
    
    Args:
        time_from: Start timestamp (default: 3 months ago)
        time_to: End timestamp (default: current time)
        order_status: Filter by order status
        time_range_field: Field to use for time range (create_time or update_time)
        
    Returns:
        List[Dict[str, Any]]: List of all orders
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    all_orders = []
    more = True
    cursor = ""
    page_size = 100
    
    while more:
        try:
            response = get_orders(
                time_from=time_from,
                time_to=time_to,
                order_status=order_status,
                page_size=page_size,
                cursor=cursor,
                time_range_field=time_range_field
            )
            
            # Extract orders from response
            if "response" in response and "order_list" in response["response"]:
                orders = response["response"]["order_list"]
                all_orders.extend(orders)
                
                # Check if more orders are available
                more = response["response"].get("more", False)
                cursor = response["response"].get("next_cursor", "")
                
                logger.info(f"Retrieved {len(orders)} orders, total: {len(all_orders)}")
            else:
                logger.warning("No orders found in response")
                more = False
            
            # Add delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error retrieving orders: {e}")
            raise
    
    return all_orders

def get_order_details(order_sn_list: List[str]) -> Dict[str, Any]:
    """
    Get detailed information for multiple orders.
    
    Args:
        order_sn_list: List of order serial numbers
        
    Returns:
        Dict[str, Any]: Order details
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    path = "/api/v2/order/get_order_detail"
    
    # Shopee API can handle maximum 50 orders per request
    max_orders_per_request = 50
    
    if len(order_sn_list) <= max_orders_per_request:
        # Single request for small batches
        data = {"order_sn_list": order_sn_list}
        return make_api_request(path, method="POST", data=data)
    else:
        # Multiple requests for larger batches
        all_order_details = []
        
        for i in range(0, len(order_sn_list), max_orders_per_request):
            batch = order_sn_list[i:i+max_orders_per_request]
            data = {"order_sn_list": batch}
            
            response = make_api_request(path, method="POST", data=data)
            
            if "response" in response and "order_list" in response["response"]:
                all_order_details.extend(response["response"]["order_list"])
            
            # Add delay to avoid rate limiting
            time.sleep(0.5)
        
        # Format the response to match Shopee API format
        return {
            "error": 0,
            "message": "success",
            "response": {
                "order_list": all_order_details
            }
        }

def get_products(
    page_size: int = 100,
    offset: int = 0,
    item_status: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get products with pagination.
    
    Args:
        page_size: Number of products per page (max 100)
        offset: Offset for pagination
        item_status: Filter by item status (NORMAL, DELETED, UNLIST, BANNED)
        
    Returns:
        Dict[str, Any]: Products data
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    path = "/api/v2/product/get_item_list"
    
    # Prepare parameters
    params = {
        "page_size": page_size,
        "offset": offset
    }
    
    # Add optional filters
    if item_status:
        params["item_status"] = item_status
    
    return make_api_request(path, params=params)

def get_all_products(item_status: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get all products with pagination handling.
    
    Args:
        item_status: Filter by item status (NORMAL, DELETED, UNLIST, BANNED)
        
    Returns:
        List[Dict[str, Any]]: List of all products
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    all_products = []
    more = True
    offset = 0
    page_size = 100
    
    while more:
        try:
            response = get_products(
                page_size=page_size,
                offset=offset,
                item_status=item_status
            )
            
            # Extract products from response
            if "response" in response and "item" in response["response"]:
                products = response["response"]["item"]
                all_products.extend(products)
                
                # Check if more products are available
                more = len(products) >= page_size
                offset += len(products)
                
                logger.info(f"Retrieved {len(products)} products, total: {len(all_products)}")
            else:
                logger.warning("No products found in response")
                more = False
            
            # Add delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error retrieving products: {e}")
            raise
    
    return all_products

def get_product_details(item_id_list: List[int]) -> Dict[str, Any]:
    """
    Get detailed information for multiple products.
    
    Args:
        item_id_list: List of item IDs
        
    Returns:
        Dict[str, Any]: Product details
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    path = "/api/v2/product/get_item_base_info"
    
    # Shopee API can handle maximum 50 products per request
    max_products_per_request = 50
    
    if len(item_id_list) <= max_products_per_request:
        # Single request for small batches
        data = {"item_id_list": item_id_list}
        return make_api_request(path, method="POST", data=data)
    else:
        # Multiple requests for larger batches
        all_product_details = []
        
        for i in range(0, len(item_id_list), max_products_per_request):
            batch = item_id_list[i:i+max_products_per_request]
            data = {"item_id_list": batch}
            
            response = make_api_request(path, method="POST", data=data)
            
            if "response" in response and "item_list" in response["response"]:
                all_product_details.extend(response["response"]["item_list"])
            
            # Add delay to avoid rate limiting
            time.sleep(0.5)
        
        # Format the response to match Shopee API format
        return {
            "error": 0,
            "message": "success",
            "response": {
                "item_list": all_product_details
            }
        }

def get_traffic(
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    page_size: int = 50,
    item_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get shop traffic data.
    
    Args:
        start_time: Start timestamp (default: 30 days ago)
        end_time: End timestamp (default: current time)
        page_size: Number of items per page
        item_id: Filter by specific item ID
        
    Returns:
        Dict[str, Any]: Traffic data
        
    Raises:
        ShopeeAPIError: If API returns an error
    """
    path = "/api/v2/data_service/get_shop_traffic"
    
    # Set default time range if not provided
    if start_time is None:
        # Default to 30 days ago
        start_time = int((datetime.now() - timedelta(days=30)).timestamp())
    
    if end_time is None:
        end_time = int(time.time())
    
    # Prepare parameters
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "page_size": page_size
    }
    
    # Add optional filters
    if item_id:
        params["item_id"] = item_id
    
    return make_api_request(path, params=params)

def standardize_order_data(raw_order_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize Shopee order data according to column mappings.
    
    Args:
        raw_order_data: Raw order data from Shopee API
        
    Returns:
        Dict[str, Any]: Standardized order data
    """
    mappings = COLUMN_MAPPINGS["order"][PLATFORM_SHOPEE]
    standardized = {}
    
    for standard_key, raw_key in mappings.items():
        standardized[standard_key] = raw_order_data.get(raw_key)
    
    # Add platform identifier
    standardized["platform"] = PLATFORM_SHOPEE
    
    return standardized

def standardize_product_data(raw_product_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize Shopee product data according to column mappings.
    
    Args:
        raw_product_data: Raw product data from Shopee API
        
    Returns:
        Dict[str, Any]: Standardized product data
    """
    mappings = COLUMN_MAPPINGS["product"][PLATFORM_SHOPEE]
    standardized = {}
    
    for standard_key, raw_key in mappings.items():
        standardized[standard_key] = raw_product_data.get(raw_key)
    
    # Add platform identifier
    standardized["platform"] = PLATFORM_SHOPEE
    
    return standardized

def test_api_connection() -> bool:
    """
    Test connection to Shopee API.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        shop_info = get_shop_info()
        logger.info(f"API connection successful: {shop_info}")
        return True
    except Exception as e:
        logger.error(f"API connection failed: {e}")
        return False

if __name__ == "__main__":
    # Set up console logging when script is run directly
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    
    # Test the API connection
    if test_api_connection():
        print("✅ Shopee API connection successful!")
    else:
        print("❌ Shopee API connection failed!")
