"""
Database connection helper for LA Collections
Simple functions to connect to the local SQLite database
"""

import sqlite3
import pandas as pd

def get_connection():
    """Get SQLite database connection"""
    return sqlite3.connect("la_collections.db")

def query_to_dataframe(query):
    """Execute query and return as pandas DataFrame"""
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()

def execute_query(query):
    """Execute a query (INSERT, UPDATE, DELETE)"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()

def get_table_info():
    """Get information about all tables"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [row[0] for row in cursor.fetchall()]
        
        table_info = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            table_info[table] = count
            
        return table_info
    finally:
        conn.close()

# Example usage:
# from db_helper import query_to_dataframe, get_table_info
# 
# # Get customer data
# customers = query_to_dataframe("SELECT * FROM Dim_Customer LIMIT 10")
# 
# # Get table summary
# tables = get_table_info()
# print(tables)
