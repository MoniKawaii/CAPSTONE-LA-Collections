import pandas as pd
import psycopg2
from io import StringIO
import os 

# from [.py file name] import [function_name] 

from shopee_transform import get_shopee_transactions
from lazada_transform import get_lazada_transactions

def get_combined_transactions():
    print("Fetching standardized data from transformation codes...")
    shopee_df = get_shopee_transactions()
    lazada_df = get_lazada_transactions()
    
    shopee_df['source'] = 'Shopee'
    lazada_df['source'] = 'Lazada'

    combined_df = pd.concat([shopee_df, lazada_df], ignore_index=True)
    
    return combined_df

def load_data_with_upsert(df, table_name, db_conn_string):
    conn = None
    try:
        conn = psycopg2.connect(db_conn_string)
        conn.autocommit = False # Ensures the operation is atomic
        cursor = conn.cursor()

        # Create an in-memory CSV file from the DataFrame
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False) 
        csv_buffer.seek(0)
        
        # Create a temporary table with the same structure as the destination
        print("Creating temporary table...")
        cursor.execute(f"CREATE TEMP TABLE temp_import (LIKE {table_name} INCLUDING DEFAULTS);")

        print("Copying data to temporary table...")
        cursor.copy_expert(f"COPY temp_import FROM STDIN WITH (FORMAT CSV)", csv_buffer)

        # Execute the upsert query using ON CONFLICT (for PostgreSQL)
        # Query contents to be changed to match actual table columns
        upsert_query = f"""
        INSERT INTO {table_name} (
            transaction_id, product_name, quantity, price, customer_name, transaction_date, source
        )
        SELECT 
            transaction_id, product_name, quantity, price, customer_name, transaction_date, source
        FROM temp_import
        ON CONFLICT (transaction_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            customer_name = EXCLUDED.customer_name,
            transaction_date = EXCLUDED.transaction_date,
            source = EXCLUDED.source;
        """
        print("Executing upsert...")
        cursor.execute(upsert_query)
        conn.commit()
        
        print(f"Successfully upserted {len(df)} records into '{table_name}'.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")
        if conn:
            conn.rollback() # Rollback if an error occurs
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Get database connection string from environment variables for security
    DB_CONNECTION_STRING = os.getenv("SUPABASE_DB_URL") 
    TABLE_NAME = "ecommerce_transactions"

    if not DB_CONNECTION_STRING:
        print("Error: SUPABASE_DB_URL environment variable is not set. Please add it to GitHub Secrets.")
    else:
        print("Starting data loading process...")
        
        final_df = get_combined_transactions()
        
        # Change table_name into the actual table from DB
        # DB_CONNECTION_STRING should use .env properties
        load_data_with_upsert(final_df, TABLE_NAME, DB_CONNECTION_STRING)
        
        print("Data loading process finished.")