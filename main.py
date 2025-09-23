from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from dotenv import load_dotenv
import io

from app.etl import process_csv_file

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
                "sample_data": df.head(3).to_dict('records') if len(df) > 0 else [],
                "dataframe_shape": df.shape
            }
        else:
            return {"status": "error", "message": result["detail"]}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}
