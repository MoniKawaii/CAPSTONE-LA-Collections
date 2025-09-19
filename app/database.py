#Database connection (SQLAlchemy engine)

from sqlalchemy import create_engine
from app.config import SUPABASE_DB_URL

engine = create_engine(SUPABASE_DB_URL)
