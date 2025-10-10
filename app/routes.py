#FastAPI endpoints

from fastapi import APIRouter, UploadFile, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
import pandas as pd
import io
from app.csv_etl import process_csv_file
from app.lazada_service import lazada_service
from app.lazada_data_service import lazada_data_service

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile, platform: str = Form(...)):
    contents = await file.read()
    file_like = io.StringIO(contents.decode("utf-8"))
    
    result = process_csv_file(file_like, platform)
    
    if result["status"] == "success":
        return {"message": f"Uploaded {result['inserted']} rows from {platform}", "inserted": result["inserted"]}
    else:
        raise HTTPException(status_code=400, detail=result["detail"])

# Lazada OAuth and API routes
@router.get("/lazada/setup-ngrok")
async def setup_ngrok():
    """Setup ngrok tunnel for OAuth callback"""
    try:
        tunnel_url = lazada_service.setup_ngrok_tunnel()
        auth_url = lazada_service.get_authorization_url()
        
        return {
            "tunnel_url": tunnel_url,
            "callback_url": f"{tunnel_url}/lazada/callback",
            "authorization_url": auth_url,
            "message": "Visit the authorization_url to get access tokens"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lazada/auth-url")
async def get_auth_url():
    """Get Lazada authorization URL"""
    try:
        auth_url = lazada_service.get_authorization_url()
        return {
            "authorization_url": auth_url,
            "callback_url": f"{lazada_service.tunnel_url}/lazada/callback" if lazada_service.tunnel_url else "Setup ngrok first"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lazada/callback")
async def lazada_callback(
    code: str = Query(..., description="Authorization code"),
    state: str = Query(None, description="State parameter")
):
    """Handle Lazada OAuth callback"""
    try:
        # Exchange authorization code for tokens
        token_data = await lazada_service.exchange_authorization_code(code)
        
        # Return success page
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Lazada OAuth Success</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .success {{ color: green; }}
                .info {{ background: #f0f0f0; padding: 20px; margin: 20px 0; }}
                .token {{ font-family: monospace; word-break: break-all; }}
            </style>
        </head>
        <body>
            <h1 class="success">✅ Lazada OAuth Authorization Successful!</h1>
            <div class="info">
                <h3>Token Information:</h3>
                <p><strong>Access Token:</strong> <span class="token">{token_data['access_token'][:20]}...</span></p>
                <p><strong>Refresh Token:</strong> <span class="token">{token_data['refresh_token'][:20]}...</span></p>
                <p><strong>Expires In:</strong> {token_data['expires_in']} seconds</p>
                <p><strong>Account Platform:</strong> {token_data.get('account_platform', 'N/A')}</p>
            </div>
            <p>Your tokens have been saved and are ready to use for API calls.</p>
            <p>You can now close this window.</p>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        error_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Lazada OAuth Error</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .error {{ color: red; }}
            </style>
        </head>
        <body>
            <h1 class="error">❌ Lazada OAuth Authorization Failed</h1>
            <p><strong>Error:</strong> {str(e)}</p>
            <p>Please try the authorization process again.</p>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=400)

@router.post("/lazada/refresh-token")
async def refresh_token():
    """Manually refresh access token"""
    try:
        token_data = await lazada_service.refresh_access_token()
        return {
            "message": "Token refreshed successfully",
            "access_token": f"{token_data['access_token'][:20]}...",
            "expires_in": token_data['expires_in']
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/lazada/exchange-code")
async def exchange_code(
    code: str = Query(..., description="Authorization code from OAuth flow")
):
    """Manually exchange authorization code for tokens"""
    try:
        token_data = await lazada_service.exchange_authorization_code(code)
        return {
            "message": "Tokens obtained successfully",
            "access_token": f"{token_data['access_token'][:20]}...",
            "refresh_token": f"{token_data['refresh_token'][:20]}...",
            "expires_in": token_data['expires_in'],
            "account_platform": token_data.get('account_platform')
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/lazada/seller")
async def get_seller_info():
    """Get seller information"""
    try:
        seller_info = await lazada_service.get_seller_info()
        return seller_info
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/lazada/products")
async def get_products(
    limit: int = Query(50, description="Number of products to fetch"),
    offset: int = Query(0, description="Offset for pagination")
):
    """Get products from Lazada"""
    try:
        products = await lazada_service.get_products(limit=limit, offset=offset)
        return products
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/lazada/orders")
async def get_orders(
    created_after: str = Query(None, description="ISO format date string"),
    created_before: str = Query(None, description="ISO format date string"),
    limit: int = Query(50, description="Number of orders to fetch"),
    offset: int = Query(0, description="Offset for pagination")
):
    """Get orders from Lazada"""
    try:
        orders = await lazada_service.get_orders(
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            offset=offset
        )
        return orders
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/lazada/status")
async def get_lazada_status():
    """Get current Lazada integration status"""
    try:
        status = {
            "has_tokens": lazada_service.current_tokens is not None,
            "token_expired": lazada_service.is_token_expired(),
            "ngrok_tunnel": lazada_service.tunnel_url,
            "callback_url": f"{lazada_service.tunnel_url}/lazada/callback" if lazada_service.tunnel_url else None
        }
        
        if lazada_service.current_tokens:
            status["token_info"] = {
                "access_token": f"{lazada_service.current_tokens['access_token'][:20]}...",
                "created_at": lazada_service.current_tokens.get('created_at'),
                "expires_in": lazada_service.current_tokens.get('expires_in')
            }
        
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Data Sync Routes
@router.post("/lazada/sync/products")
async def sync_products(
    limit: int = Query(100, description="Number of products to sync"),
    save_to_db: bool = Query(True, description="Save to database")
):
    """Sync products from Lazada API to database"""
    try:
        result = await lazada_data_service.fetch_and_process_products(
            limit=limit,
            save_to_db=save_to_db
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/lazada/sync/orders")
async def sync_orders(
    days_back: int = Query(7, description="Days back to fetch orders"),
    save_to_db: bool = Query(True, description="Save to database")
):
    """Sync orders from Lazada API to database"""
    try:
        result = await lazada_data_service.fetch_and_process_orders(
            days_back=days_back,
            save_to_db=save_to_db
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/lazada/sync/all")
async def sync_all_data(
    products_limit: int = Query(100, description="Number of products to sync"),
    orders_days_back: int = Query(7, description="Days back for orders")
):
    """Run complete Lazada data synchronization"""
    try:
        result = await lazada_data_service.run_automated_sync(
            products_limit=products_limit,
            orders_days_back=orders_days_back
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lazada/preview/products")
async def preview_products(limit: int = Query(10, description="Number of products to preview")):
    """Preview products from Lazada API without saving to database"""
    try:
        result = await lazada_data_service.fetch_and_process_products(
            limit=limit,
            save_to_db=False
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lazada/preview/orders")
async def preview_orders(days_back: int = Query(7, description="Days back to preview")):
    """Preview orders from Lazada API without saving to database"""
    try:
        result = await lazada_data_service.fetch_and_process_orders(
            days_back=days_back,
            save_to_db=False
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

