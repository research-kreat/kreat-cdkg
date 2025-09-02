import os
import csv
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
csv.field_size_limit(1_000_000_000) 

def get_total_lines(filepath):
    """Efficiently count lines in a file for the progress bar."""
    with open(filepath, 'rb') as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read
        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)
    return lines

def validate_csv_against_mongo():
    """
    Validates that no patent_ids from a given CSV file exist in the MongoDB collection.
    """
    # --- 1. Load Configuration ---
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    # This should be the path to your NEWLY FILTERED CSV file
    filtered_csv_path = "f2000_output.csv"

    if not os.path.exists(filtered_csv_path):
        print(f"Error: Filtered CSV file '{filtered_csv_path}' not found.")
        return

    # --- 2. Fetch All Patent IDs from MongoDB ---
    print("Connecting to MongoDB to fetch all existing patent_ids...")
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Fetch all documents and store their IDs in a set for fast lookups
        processed_docs = collection.find({}, {"patent_id": 1, "_id": 0})
        mongo_patent_ids = {str(doc['patent_id']) for doc in processed_docs if 'patent_id' in doc}
        
        print(f"Found {len(mongo_patent_ids)} unique patent_ids in MongoDB.")
    except Exception as e:
        print(f"An error occurred while connecting to MongoDB: {e}")
        return
    finally:
        if 'client' in locals() and client:
            client.close()

    # --- 3. Read CSV and Check for Matches ---
    print(f"\nValidating '{filtered_csv_path}' against MongoDB IDs...")
    found_matches = []
    
    try:
        total_lines = get_total_lines(filtered_csv_path)
        
        with open(filtered_csv_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            # Skip header
            header = next(reader)
            # Assuming patent_id is the first column (index 0)
            patent_id_index = 0

            for row in tqdm(reader, total=total_lines - 1, desc="Checking CSV rows"):
                try:
                    csv_patent_id = row[patent_id_index]
                    # Check if the ID from the CSV exists in the set of Mongo IDs
                    if csv_patent_id in mongo_patent_ids:
                        found_matches.append(csv_patent_id)
                except IndexError:
                    print(f"Warning: Skipping a malformed row: {row}")

    except Exception as e:
        print(f"An unexpected error occurred during CSV processing: {e}")
        return

    # --- 4. Report Final Validation Result ---
    print("\n--------------------")
    print("Validation Complete.")
    
    if not found_matches:
        print("\n✅ SUCCESS: Validation passed!")
        print("No patent_ids from the CSV were found in the MongoDB collection.")
    else:
        print("\n❌ FAILURE: Validation failed!")
        print(f"Found {len(found_matches)} patent_id(s) that exist in BOTH the CSV and MongoDB:")
        # Print the first 10 matches for brevity
        for match in found_matches[:10]:
            print(f"  - {match}")
        if len(found_matches) > 10:
            print(f"  ... and {len(found_matches) - 10} more.")
            
    print("--------------------")


if __name__ == "__main__":
    validate_csv_against_mongo()