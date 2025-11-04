"""
Split Large Shopee Payment Detail File
=====================================

This script splits the large shopee_paymentdetail_raw.json file into smaller chunks
to avoid file size limits and improve processing performance.
"""

import json
import os
import math

def split_payment_file(input_file, max_size_mb=80, output_dir=None):
    """
    Split large payment detail JSON file into smaller chunks
    
    Args:
        input_file (str): Path to input JSON file
        max_size_mb (int): Maximum size per chunk in MB
        output_dir (str): Output directory (default: same as input)
    """
    if output_dir is None:
        output_dir = os.path.dirname(input_file)
    
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    
    print(f"🔄 Splitting {input_file}...")
    print(f"📁 Output directory: {output_dir}")
    print(f"📊 Max size per chunk: {max_size_mb} MB")
    
    try:
        # Load the data
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        total_records = len(data)
        print(f"📋 Total records: {total_records}")
        
        # Calculate approximate records per chunk based on file size
        file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
        print(f"📏 Original file size: {file_size_mb:.2f} MB")
        
        # Estimate records per chunk
        avg_record_size = file_size_mb / total_records
        records_per_chunk = int(max_size_mb / avg_record_size * 0.9)  # 90% to be safe
        
        total_chunks = math.ceil(total_records / records_per_chunk)
        print(f"🧮 Estimated {records_per_chunk} records per chunk")
        print(f"📦 Will create {total_chunks} chunks")
        
        # Split the data
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * records_per_chunk
            end_idx = min((chunk_num + 1) * records_per_chunk, total_records)
            
            chunk_data = data[start_idx:end_idx]
            chunk_filename = f"{base_filename}_part{chunk_num + 1}.json"
            chunk_path = os.path.join(output_dir, chunk_filename)
            
            # Save chunk
            with open(chunk_path, 'w', encoding='utf-8') as f:
                json.dump(chunk_data, f, ensure_ascii=False, indent=2)
            
            chunk_size_mb = os.path.getsize(chunk_path) / (1024 * 1024)
            print(f"✅ Created {chunk_filename}: {len(chunk_data)} records, {chunk_size_mb:.2f} MB")
        
        print(f"\n🎉 Split complete! Created {total_chunks} chunks in {output_dir}")
        return total_chunks
        
    except FileNotFoundError:
        print(f"❌ File not found: {input_file}")
        return 0
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0


def load_split_payment_files(base_filename="shopee_paymentdetail_raw", staging_dir=None):
    """
    Load data from split payment detail files
    
    Args:
        base_filename (str): Base filename without extension
        staging_dir (str): Staging directory path
    
    Returns:
        list: Combined payment detail data
    """
    if staging_dir is None:
        staging_dir = os.path.join(os.path.dirname(__file__), '..', 'Staging')
    
    all_data = []
    chunk_num = 1
    
    while True:
        chunk_filename = f"{base_filename}_part{chunk_num}.json"
        chunk_path = os.path.join(staging_dir, chunk_filename)
        
        if not os.path.exists(chunk_path):
            break
        
        try:
            with open(chunk_path, 'r', encoding='utf-8') as f:
                chunk_data = json.load(f)
                all_data.extend(chunk_data)
                print(f"✅ Loaded {len(chunk_data)} records from {chunk_filename}")
        except Exception as e:
            print(f"❌ Error loading {chunk_filename}: {e}")
            break
        
        chunk_num += 1
    
    print(f"📊 Total payment records loaded: {len(all_data)}")
    return all_data


if __name__ == "__main__":
    # Define paths
    staging_dir = os.path.join(os.path.dirname(__file__), '..', 'Staging')
    input_file = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')
    
    # Check if original file exists
    if os.path.exists(input_file):
        print("🔍 Found large payment detail file, splitting...")
        split_payment_file(input_file, max_size_mb=80, output_dir=staging_dir)
    else:
        # Try alternative filename
        alt_file = os.path.join(staging_dir, 'shopee_paymentdetail_2_raw.json')
        if os.path.exists(alt_file):
            print("🔍 Found alternative payment detail file, splitting...")
            split_payment_file(alt_file, max_size_mb=80, output_dir=staging_dir)
        else:
            print("❌ No payment detail files found to split")
    
    print("\n🧪 Testing load function...")
    test_data = load_split_payment_files(staging_dir=staging_dir)
    print(f"✅ Test load successful: {len(test_data)} records")