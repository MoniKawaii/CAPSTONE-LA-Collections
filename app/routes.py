#FastAPI endpoints

from fastapi import APIRouter, UploadFile, Form, HTTPException
import pandas as pd
import io
from app.etl import process_csv_file

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

