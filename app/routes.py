#FastAPI endpoints

from fastapi import APIRouter, UploadFile, Form
import pandas as pd
import io
from app.etl import transform, load

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile, platform: str = Form(...)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    df = transform(df, platform)
    load(df)
    return {"message": f"Uploaded {len(df)} rows from {platform}"}

