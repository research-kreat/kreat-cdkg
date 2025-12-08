import ast
from pymongo import MongoClient, UpdateOne, WriteConcern
from tqdm import tqdm
from dotenv import load_dotenv
import os   
load_dotenv()
# ==========================================
# CONFIGURATION
# ==========================================
CONFIG = {
    "MONGO_URI": os.getenv("MONGO_URI"),
    "DB_NAME": os.getenv("MONGO_DB"),
    "COLLECTION_NAME": "temp",
    "BATCH_SIZE": 1000, # Higher batch size is better for updates
    "VECTOR_COLUMNS": ["embedding", "ai_embeddings"]
}

def safe_literal_eval(val):
    """Converts string "[...]" to list [...], returns None if fails."""
    if not isinstance(val, str):
        return val # Already a list or None
    try:
        val = val.strip()
        if val.startswith('[') and val.endswith(']'):
            return ast.literal_eval(val)
        return val
    except (ValueError, SyntaxError):
        return val

def main():
    client = MongoClient(CONFIG["MONGO_URI"])
    db = client[CONFIG["DB_NAME"]]
    collection = db[CONFIG["COLLECTION_NAME"]]
    
    # 1. Build a filter to find ONLY documents where fields are Strings
    # This ensures we don't re-process records that are already fixed.
    query_filter = {
        "$or": [
            {col: {"$type": "string"}} for col in CONFIG["VECTOR_COLUMNS"]
        ]
    }

    # Count documents that need fixing
    print("🔍 Scanning database for records with String-type embeddings...")
    total_to_fix = collection.count_documents(query_filter)
    
    if total_to_fix == 0:
        print("✅ No records found needing updates! All vectors appear to be correct.")
        return

    print(f"⚠️ Found {total_to_fix:,} documents to update.")
    
    # 2. Iterate and Batch Update
    cursor = collection.find(query_filter, batch_size=CONFIG["BATCH_SIZE"])
    
    bulk_ops = []
    processed_count = 0

    # Using tqdm for progress bar
    pbar = tqdm(total=total_to_fix, unit="docs")

    for doc in cursor:
        updates = {}
        
        # Check and convert each column
        for col in CONFIG["VECTOR_COLUMNS"]:
            if col in doc and isinstance(doc[col], str):
                converted_val = safe_literal_eval(doc[col])
                
                # Only stage update if conversion actually changed the type to list
                if isinstance(converted_val, list):
                    updates[col] = converted_val

        # If we found fields to update, add to bulk operations
        if updates:
            bulk_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": updates})
            )

        # Execute Batch when full
        if len(bulk_ops) >= CONFIG["BATCH_SIZE"]:
            collection.bulk_write(bulk_ops, ordered=False)
            processed_count += len(bulk_ops)
            pbar.update(len(bulk_ops))
            bulk_ops = [] # Reset batch

    # Flush remaining operations
    if bulk_ops:
        collection.bulk_write(bulk_ops, ordered=False)
        processed_count += len(bulk_ops)
        pbar.update(len(bulk_ops))

    pbar.close()
    client.close()
    
    print("\n" + "="*30)
    print(f"🎉 Update Complete.")
    print(f"Total Documents Fixed: {processed_count:,}")
    print("="*30)

if __name__ == "__main__":
    main()