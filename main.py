from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from dotenv import load_dotenv
import io

from app.etl import process_csv_file
from app.star_schema_etl import process_csv_to_star_schema
from app.lazada_api_client import fetch_and_load_lazada_data
from app.mock_lazada_data import load_mock_data_to_star_schema
from app.lazada_token_manager import LazadaTokenManager, create_token_manager_from_env

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    platform: str = Form(...)
):
    """
    Upload CSV file + platform ("Lazada" or "Shopee"),
    transform with mapping, and return DataFrame info (without saving to DB).
    """
    try:
        
        contents = await file.read()
        file_like = io.StringIO(contents.decode("utf-8"))
        
        # process 
        result = process_csv_file(file_like, platform, save_to_db=False)
        
        if result["status"] == "success":
            df = result["dataframe"]
            return {
                "message": f"Processed {result['rows_processed']} rows from {platform}",
                "rows_processed": result["rows_processed"],
                "columns": list(df.columns),
                "data": df.to_dict('records'),  # Return all data
                "dataframe_shape": df.shape
            }
        else:
            return {"status": "error", "message": result["detail"]}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/upload-to-warehouse")
async def upload_csv_to_warehouse(
    file: UploadFile = File(...),
    platform: str = Form(...)
):
    """
    Upload CSV file and load into star schema data warehouse.
    Creates dimension records and fact table entries.
    """
    try:
        contents = await file.read()
        
        # Save temporary file for processing
        temp_filename = f"temp_{file.filename}"
        with open(temp_filename, "wb") as temp_file:
            temp_file.write(contents)
        
        # Process using star schema ETL
        result = process_csv_to_star_schema(temp_filename, platform)
        
        # Clean up temp file
        import os
        os.remove(temp_filename)
        
        return result
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/fetch-lazada-data")
async def fetch_lazada_api_data():
    """
    Fetch data directly from Lazada API and load into star schema.
    Uses OAuth tokens to authenticate and retrieve orders, products, and metrics.
    """
    try:
        result = fetch_and_load_lazada_data()
        return result
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/generate-mock-lazada-data")
async def generate_mock_lazada_data():
    """
    Generate realistic mock Lazada data and load into star schema.
    Useful for testing and demonstration purposes while API issues are resolved.
    """
    try:
        result = load_mock_data_to_star_schema()
        return result
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/lazada-token-status")
async def get_lazada_token_status():
    """
    Get current Lazada token status and expiration information.
    Shows automated refresh status and token validity.
    """
    try:        
        token_manager = create_token_manager_from_env()
        status = token_manager.get_token_status()
        
        return {
            "status": "success",
            "token_status": status,
            "message": "Token status retrieved successfully"
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/refresh-lazada-token")
async def refresh_lazada_token():
    """
    Manually refresh Lazada access token.
    Useful for testing or forcing refresh outside of automated schedule.
    """
    try:        
        token_manager = create_token_manager_from_env()
        result = token_manager.refresh_access_token()
        
        if result['success']:
            return {
                "status": "success",
                "message": "Token refreshed successfully",
                "new_token_expires_in": result.get('expires_in', 'Unknown'),
                "access_token_preview": result['access_token'][:20] + "..." if result['access_token'] else None
            }
        else:
            return {
                "status": "error", 
                "message": f"Token refresh failed: {result['error']}"
            }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
async def root():
    return {
        "message": "LA Collections Data Pipeline API",
        "endpoints": {
            "POST /upload": "Upload CSV for DataFrame processing",
            "POST /upload-to-warehouse": "Upload CSV to star schema warehouse",
            "POST /fetch-lazada-data": "Fetch live data from Lazada API",
            "POST /generate-mock-lazada-data": "Generate mock Lazada data for testing",
            "GET /lazada-token-status": "Check Lazada token status and expiration",
            "POST /refresh-lazada-token": "Manually refresh Lazada access token",
            "GET /": "This documentation"
        },
        "status": "Data warehouse ready with star schema",
        "platforms": {
            "1": "Lazada",
            "2": "Shopee"
        },
        "features": {
            "automated_token_refresh": "Enabled for Lazada API",
            "persistent_token_storage": "Tokens saved to .env file",
            "proactive_refresh": "Tokens refreshed before expiration"
        }
    }
