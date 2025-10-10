from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from dotenv import load_dotenv
import io

from app.csv_etl import process_csv_file
from app.routes import router
from app.token_scheduler import start_scheduler, stop_scheduler

load_dotenv()

app = FastAPI(
    title="LA Collections API",
    description="API for Lazada and Shopee data processing with automatic token management",
    version="1.0.0"
)

# Include routes
app.include_router(router)

# Add a simple root endpoint for testing
@app.get("/")
async def root():
    return {"message": "LA Collections API is running", "status": "online"}

# Add startup and shutdown events for token scheduler
@app.on_event("startup")
async def startup_event():
    """Start background services on app startup"""
    await start_scheduler()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on app shutdown"""
    await stop_scheduler()

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
