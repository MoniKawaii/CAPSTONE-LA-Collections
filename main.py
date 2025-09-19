from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from dotenv import load_dotenv
import io

from app.etl import transform, load

# Load environment variables
load_dotenv()

app = FastAPI()

# Allow frontend to talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # for dev, allow all. In prod, restrict to your frontend URL
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
    transform with mapping, and load into Supabase.
    """
    try:
        # Read file into pandas DataFrame
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # Transform data
        transformed_df = transform(df, platform)

        # Load into Supabase
        response = load(transformed_df)

        return {
            "status": "success",
            "platform": platform,
            "rows_inserted": len(transformed_df),
            "response": str(response),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
