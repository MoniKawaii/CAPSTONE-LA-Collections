import pandas as pd
import psycopg2
from io import StringIO
import os 
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import local database helper
import sys
sys.path.append('..')
try:
    from db_helper import get_connection
    USING_LOCAL_DB = True
except ImportError:
    USING_LOCAL_DB = False

# from [.py file name] import [function_name] 
# NOTE: These imports are commented out as the modules don't exist yet
# from shopee_transform import get_shopee_transactions
# from lazada_transform import get_lazada_transactions

def get_combined_transactions():
    """Get combined transactions from CSV files or transformation modules"""
    print("Fetching standardized data from transformation codes...")
    
    # Try to get data from transformation modules
    try:
        # from shopee_transform import get_shopee_transactions
        # from lazada_transform import get_lazada_transactions
        
        # shopee_df = get_shopee_transactions()
        # lazada_df = get_lazada_transactions()
        # combined_df = pd.concat([shopee_df, lazada_df], ignore_index=True)
        
        # For now, skip the import and go directly to CSV fallback
        raise ImportError("Transformation modules not available")
        
    except ImportError:
        print("⚠️  Transformation modules not found, using CSV files...")
        
        # Fallback: load from existing CSV files
        csv_dir = Path("../app/Transformed")
        fact_orders_file = csv_dir / "fact_orders.csv"
        
        if fact_orders_file.exists():
            combined_df = pd.read_csv(fact_orders_file)
            print(f"✅ Loaded {len(combined_df):,} records from fact_orders.csv")
        else:
            print("❌ No data source available")
            return pd.DataFrame()
    
    return combined_df

def get_cloud_db_connection():
    """Get cloud database connection using environment variables"""
    # Try different database URL environment variables in priority order
    db_url = (
        os.getenv('DATABASE_URL') or  # Pooler URL (preferred)
        os.getenv('DIRECT_URL') or    # Direct URL (fallback)
        os.getenv('SUPABASE_DB_URL')  # Legacy URL (last resort)
    )
    
    if not db_url:
        raise ValueError("No cloud database URL found in environment variables")
    
    try:
        logger.info(f"🔗 Connecting to: {db_url[:50]}...")
        conn = psycopg2.connect(db_url)
        logger.info("✅ Connected to cloud database (Supabase)")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Cloud database connection failed: {e}")
        raise

def load_data_with_upsert(df, table_name, use_cloud=True):
    """Load data with upsert functionality - prioritizes cloud database"""
    
    if use_cloud:
        try:
            return load_data_postgresql(df, table_name)
        except Exception as e:
            logger.warning(f"Cloud database failed, falling back to local: {e}")
            if USING_LOCAL_DB:
                return load_data_sqlite(df, table_name)
            else:
                raise e
    else:
        if USING_LOCAL_DB:
            return load_data_sqlite(df, table_name)
        else:
            return load_data_postgresql(df, table_name)

def load_data_sqlite(df, table_name):
    """Load data into SQLite database"""
    try:
        conn = get_connection()
        
        # Clear existing data and insert new data
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name};")
        
        # Insert new data
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        print(f"✅ Successfully loaded {len(df):,} records into '{table_name}' (SQLite)")
        conn.close()
        return True
        
    except Exception as error:
        print(f"❌ SQLite loading error: {error}")
        return False

def clean_dataframe(df, table_name):
    """Clean DataFrame to fix common data issues"""
    # Convert numpy types to Python types
    for col in df.columns:
        if df[col].dtype == 'int64':
            df[col] = df[col].astype('Int64')  # Nullable integer
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('Float64')  # Nullable float
        elif df[col].dtype == 'object':
            # Handle mixed types in object columns
            df[col] = df[col].astype(str)
            # Convert 'nan' strings back to None
            df[col] = df[col].replace('nan', None)
    
    # Table-specific data fixes
    if table_name == 'Dim_Product_Variant':
        logger.info(f"🔧 Original columns: {list(df.columns)}")
        
        # Fix column name if needed (CSV uses variant_key, DB expects product_variant_key)
        if 'variant_key' in df.columns and 'product_variant_key' not in df.columns:
            df = df.rename(columns={'variant_key': 'product_variant_key'})
            logger.info("🔧 Renamed variant_key to product_variant_key in Dim_Product_Variant")
        
        # Remove extra columns that don't exist in database schema
        # Database expects: product_variant_key, product_key, platform_sku_id, variant_sku, 
        #                   variant_attribute_1, variant_attribute_2, variant_attribute_3, platform_key
        expected_columns = [
            'product_variant_key', 'product_key', 'platform_sku_id', 'variant_sku',
            'variant_attribute_1', 'variant_attribute_2', 'variant_attribute_3', 'platform_key'
        ]
        
        # Drop extra columns that don't match database schema
        columns_to_drop = [col for col in df.columns if col not in expected_columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.info(f"🔧 Dropped extra columns: {columns_to_drop}")
        
        # Reorder columns to match database schema
        df = df.reindex(columns=expected_columns)
        
        # Debug: show unique platform_key values before filtering
        logger.info(f"🔍 Unique platform_key values before filtering: {sorted(df['platform_key'].unique())}")
        
        # Filter out invalid platform_key values (should be 1 or 2 only)
        valid_platforms = [1, 2]
        original_count = len(df)
        df = df[df['platform_key'].isin(valid_platforms)]
        logger.info(f"🔍 Unique platform_key values after filtering: {sorted(df['platform_key'].unique())}")
        
        if len(df) != original_count:
            logger.info(f"🔧 Filtered Dim_Product_Variant: {original_count} -> {len(df)} records (removed invalid platform_key values)")
        
        # Remove rows with NULL or invalid product_variant_key
        if 'product_variant_key' in df.columns:
            before = len(df)
            df = df.dropna(subset=['product_variant_key'])
            df = df[df['product_variant_key'] > 0]  # Ensure positive keys
            if len(df) != before:
                logger.info(f"🔧 Removed rows with invalid product_variant_key: {before} -> {len(df)} records")
        
        # Remove duplicates on product_variant_key if any
        if 'product_variant_key' in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=['product_variant_key'])
            if len(df) != before:
                logger.info(f"🔧 Dropped duplicate product_variant_key: {before} -> {len(df)} records")
        
        logger.info(f"🔧 Final columns: {list(df.columns)}")
        return df
        
    elif table_name == 'Fact_Orders':
        logger.info(f"🔧 Original columns: {list(df.columns)}")
        
        # Fix order_item_key if it contains strings like "OI00000001"
        if 'order_item_key' in df.columns and df['order_item_key'].dtype == 'object':
            df['order_item_key'] = range(1, len(df) + 1)
            logger.info("🔧 Fixed order_item_key to sequential integers in Fact_Orders")
        
        # Ensure product_variant_key exists and is valid
        if 'product_variant_key' in df.columns:
            # Remove rows with NULL or 0 product_variant_key
            before = len(df)
            df = df.dropna(subset=['product_variant_key'])
            df = df[df['product_variant_key'] > 0]
            if len(df) != before:
                logger.info(f"🔧 Removed rows with invalid product_variant_key: {before} -> {len(df)} records")
            
            # Load valid product_variant_keys from cleaned Dim_Product_Variant
            pv_path = None
            for path in [Path("app/Transformed/dim_product_variant.csv"), 
                        Path("../app/Transformed/dim_product_variant.csv"),
                        Path("Transformed/dim_product_variant.csv")]:
                if path.exists():
                    pv_path = path
                    break
            
            if pv_path and pv_path.exists():
                logger.info(f"🔍 Loading product variant reference from: {pv_path}")
                pv_df = pd.read_csv(pv_path)
                
                # Fix column name in variant data
                if 'variant_key' in pv_df.columns and 'product_variant_key' not in pv_df.columns:
                    pv_df = pv_df.rename(columns={'variant_key': 'product_variant_key'})
                
                # Filter valid platform keys in variant data
                pv_df = pv_df[pv_df['platform_key'].isin([1, 2])]
                valid_variant_keys = set(pv_df['product_variant_key'].dropna())
                logger.info(f"🔍 Found {len(valid_variant_keys)} valid product_variant_keys: {sorted(list(valid_variant_keys))[:10]}...")
                
                before = len(df)
                df = df[df['product_variant_key'].isin(valid_variant_keys)]
                if len(df) != before:
                    logger.info(f"🔧 Filtered Fact_Orders for valid product_variant_key: {before} -> {len(df)} records")
            else:
                logger.warning("⚠️ Could not find dim_product_variant.csv for reference validation")
        
        logger.info(f"🔧 Final columns: {list(df.columns)}")
        return df
    
    # If not a special table, return as is
    return df

def load_data_postgresql(df, table_name):
    """Load data into PostgreSQL database with robust error handling"""
    conn = None
    try:
        conn = get_cloud_db_connection()
        cursor = conn.cursor()

        # Clean the dataframe
        df = clean_dataframe(df, table_name)
        
        if df.empty:
            logger.warning(f"⚠️ No data to load for {table_name} after cleaning")
            return True

        logger.info(f"📊 Loading {len(df)} records to {table_name}")

        # Clear existing data
        cursor.execute(f'DELETE FROM "{table_name}"')
        logger.info(f"🧹 Cleared existing data from {table_name}")

        # Get database column structure
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        db_columns = cursor.fetchall()
        column_names = [col[0] for col in db_columns]
        
        logger.info(f"📋 Database columns: {column_names}")

        # Prepare insert statement
        placeholders = ', '.join(['%s'] * len(column_names))
        column_list = ', '.join([f'"{col}"' for col in column_names])
        insert_sql = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'

        # Insert data in batches for better performance
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_data = []
            
            for _, row in batch.iterrows():
                # Map CSV columns to database columns
                row_data = []
                for j, col_name in enumerate(column_names):
                    if j < len(row):
                        value = row.iloc[j]
                        # Handle pandas NA/NaN values and explicit None values
                        if pd.isna(value) or value is None or str(value).lower() == 'none':
                            row_data.append(None)
                        else:
                            row_data.append(value)
                    else:
                        row_data.append(None)
                
                batch_data.append(row_data)
            
            # Execute batch insert
            cursor.executemany(insert_sql, batch_data)
            conn.commit()
            total_inserted += len(batch_data)
            
            if total_inserted % 1000 == 0 or total_inserted == len(df):
                logger.info(f"📥 Inserted {total_inserted:,}/{len(df):,} records...")

        # Verify final count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        final_count = cursor.fetchone()[0]
        
        logger.info(f"✅ {table_name}: {final_count:,} records loaded successfully")
        return True

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"❌ PostgreSQL error for {table_name}: {error}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def test_database_connection():
    """Test database connection and show current status"""
    try:
        logger.info("🔍 Testing database connection...")
        conn = get_cloud_db_connection()
        cursor = conn.cursor()
        
        # Get database info
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"✅ Connected to: {version[:50]}...")
        
        # List tables and row counts
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"📋 Found {len(tables)} tables:")
        total_records = 0
        
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cursor.fetchone()[0]
            total_records += count
            logger.info(f"   {table}: {count:,} records")
        
        logger.info(f"📊 Total records in database: {total_records:,}")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False

def load_all_csv_files(use_cloud=True):
    """Load all CSV files from Transformed directory to database"""
    # Try different possible paths for the Transformed directory
    possible_paths = [
        Path("app/Transformed"),  # When running from root
        Path("../app/Transformed"),  # When running from app directory 
        Path("Transformed"),  # When running from app directory
        Path("./app/Transformed")  # Alternative root path
    ]
    
    csv_dir = None
    for path in possible_paths:
        if path.exists():
            csv_dir = path
            break
    
    if not csv_dir:
        logger.error(f"Transformed directory not found. Tried: {[str(p) for p in possible_paths]}")
        return False
    
    logger.info(f"📁 Using CSV directory: {csv_dir}")
    
    # Only load tables that are not already loaded: dim_product_variant and fact_orders
    csv_files = {
        'dim_product_variant.csv': 'Dim_Product_Variant',
        'fact_orders.csv': 'Fact_Orders'
    }
    
    total_records = 0
    successful_loads = 0
    start_time = time.time()
    
    logger.info(f"🚀 Starting CSV loading to {'cloud' if use_cloud else 'local'} database...")
    
    for csv_file, table_name in csv_files.items():
        csv_path = csv_dir / csv_file
        
        if not csv_path.exists():
            logger.warning(f"⚠️ CSV file not found: {csv_file}")
            continue
        
        logger.info(f"\n{'='*50}")
        
        try:
            # Load CSV data
            df = pd.read_csv(csv_path)
            logger.info(f"📊 Loading {csv_file} -> {table_name} ({len(df):,} records)")
            
            # Load to database
            if load_data_with_upsert(df, table_name, use_cloud):
                successful_loads += 1
                total_records += len(df)
                logger.info(f"✅ {table_name}: {len(df):,} records loaded")
            else:
                logger.error(f"❌ Failed to load {table_name}")
                
        except Exception as e:
            logger.error(f"❌ Error processing {csv_file}: {e}")
        
        # Brief pause between tables
        time.sleep(0.5)
    
    # Final summary
    elapsed_time = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 LOADING SUMMARY")
    logger.info(f"✅ Files processed: {successful_loads}/{len(csv_files)}")
    logger.info(f"📈 Total records loaded: {total_records:,}")
    logger.info(f"⏱️ Time elapsed: {elapsed_time:.1f} seconds")
    logger.info(f"🌐 Database: {'Cloud (Supabase)' if use_cloud else 'Local (SQLite)'}")
    logger.info(f"{'='*60}")
    
    return successful_loads == len(csv_files)


if __name__ == "__main__":
    """Main execution with command line options"""
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        action = sys.argv[1].lower()
        
        if action == "test":
            print("🔍 Testing database connection...")
            test_database_connection()
            
        elif action == "load":
            print("🚀 Loading all CSV files to cloud database...")
            load_all_csv_files(use_cloud=True)
            
        elif action == "local":
            print("� Loading all CSV files to local database...")
            load_all_csv_files(use_cloud=False)
            
        elif action == "help":
            print("📋 Available commands:")
            print("  python loading_script.py test    - Test database connection")
            print("  python loading_script.py load    - Load CSV to cloud database")
            print("  python loading_script.py local   - Load CSV to local database")
            print("  python loading_script.py help    - Show this help")
            
        else:
            print(f"❌ Unknown command: {action}")
            print("Use 'python loading_script.py help' for available commands")
    else:
        # Default action: test connection and load to cloud
        print("🌟 LA Collections - Automated CSV Loading")
        print("=" * 50)
        
        # Test connection first
        if test_database_connection():
            print("\n🚀 Starting CSV loading process...")
            load_all_csv_files(use_cloud=True)
        else:
            print("❌ Cannot proceed with loading due to connection issues")