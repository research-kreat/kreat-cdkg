import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm
import ast
import os
import sys
from dotenv import load_dotenv
load_dotenv()
# ==========================================
# CONFIGURATION
# ==========================================
CONFIG = {
    # MongoDB Connection String (Update this!)
    "MONGO_URI": os.getenv("MONGO_URI"), 
    "DB_NAME": os.getenv("MONGO_DB"),
    "COLLECTION_NAME": "temp",
    
    # Path to your huge CSV file
    "CSV_FILE_PATH": "/Users/user/Desktop/processed_file.csv",
    
    # Batch size as requested
    "BATCH_SIZE": 100,
    
    # Column specifically containing the vector/embedding
    # We need to parse this from a string "[0.1, ...]" to a list [0.1, ...]
    "EMBEDDING_COLUMN": "ai_embeddings" 
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_total_rows(file_path):
    """
    Quickly counts lines in a file to give tqdm a total number for the progress bar.
    This is faster than loading the file.
    """
    print("Calculating total rows for progress estimation...")
    with open(file_path, "rb") as f:
        # Count line breaks, subtract 1 for header
        num_lines = sum(1 for _ in f) - 1
    return num_lines

def safe_literal_eval(val):
    """
    Safely converts a string representation of a list into an actual list.
    e.g., "[-0.1, 0.4]" -> [-0.1, 0.4]
    Returns None if conversion fails or value is empty.
    """
    if pd.isna(val) or val == "":
        return None
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return val # Return original if it's not a list structure

def clean_chunk(df):
    """
    Cleans a pandas DataFrame chunk before insertion.
    1. Converts embedding strings to lists.
    2. Converts NaN values to None (MongoDB specific).
    """
    # 1. Convert Embeddings from String to List
    if CONFIG["EMBEDDING_COLUMN"] in df.columns:
        df[CONFIG["EMBEDDING_COLUMN"]] = df[CONFIG["EMBEDDING_COLUMN"]].apply(safe_literal_eval)

    # 2. Handle NaN/NaT (Pandas uses NaN, MongoDB prefers None/Null)
    # We convert the dataframe to object type to allow None replacement
    df = df.astype(object).where(pd.notnull(df), None)
    
    return df

# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    # 1. Establish MongoDB Connection
    try:
        client = MongoClient(CONFIG["MONGO_URI"])
        db = client[CONFIG["DB_NAME"]]
        collection = db[CONFIG["COLLECTION_NAME"]]
        # Test connection
        client.admin.command('ping')
        print(f"✅ Connected to MongoDB: {CONFIG['DB_NAME']}.{CONFIG['COLLECTION_NAME']}")
    except errors.ConnectionFailure as e:
        print(f"❌ Could not connect to MongoDB: {e}")
        sys.exit(1)

    # 2. Check File
    if not os.path.exists(CONFIG["CSV_FILE_PATH"]):
        print(f"❌ File not found: {CONFIG['CSV_FILE_PATH']}")
        sys.exit(1)

    # 3. Get Total Rows (for the progress bar)
    total_rows = get_total_rows(CONFIG["CSV_FILE_PATH"])
    total_chunks = (total_rows // CONFIG["BATCH_SIZE"]) + 1
    
    print(f"🚀 Starting upload of {total_rows:,} records in batches of {CONFIG['BATCH_SIZE']}...")

    # 4. Process CSV in Chunks
    # pd.read_csv with chunksize returns an iterator (textfilereader)
    # This ensures we only load 100 rows into RAM at a time.
    chunk_iterator = pd.read_csv(
        CONFIG["CSV_FILE_PATH"], 
        chunksize=CONFIG["BATCH_SIZE"],
        dtype=str, # Read all as string initially to prevent weird float conversions on IDs
        keep_default_na=True
    )

    inserted_count = 0
    
    try:
        # Wrap the iterator with tqdm for the progress bar
        for chunk in tqdm(chunk_iterator, total=total_chunks, unit="batch"):
            
            # Clean and Transform Data
            clean_df = clean_chunk(chunk)
            
            # Convert to list of dictionaries (records) for MongoDB
            records = clean_df.to_dict("records")
            
            if records:
                try:
                    # Insert the batch
                    # ordered=False continues inserting remaining documents if one fails (e.g. duplicate ID)
                    collection.insert_many(records, ordered=False)
                    inserted_count += len(records)
                except errors.BulkWriteError as bwe:
                    # Handle duplicates or write errors gracefully
                    print(f"\n⚠️ Bulk write error (likely duplicate _ids): {bwe.details['writeErrors'][:1]}")
                    inserted_count += bwe.details['nInserted']

    except KeyboardInterrupt:
        print("\n🛑 Process interrupted by user.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        client.close()
        print("\n" + "="*30)
        print(f"🎉 Job Complete.")
        print(f"Total Records Inserted: {inserted_count:,}")
        print("="*30)

if __name__ == "__main__":
    main()