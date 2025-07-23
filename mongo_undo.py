import csv
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
csv.field_size_limit(1_000_000_000)
# Load environment variables from .env
load_dotenv()

# MongoDB config
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

def delete_patent_ids_from_mongo(csv_file):
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Load patent_ids from CSV
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        patent_ids = []
        for row in reader:
            pid_str = row.get('patent_id', '').strip()
            try:
                patent_ids.append(int(pid_str))
            except ValueError:
                continue  # Skip if not a valid int

    # Track deletions with tqdm
    deleted_count = 0
    for pid in tqdm(patent_ids, desc="Deleting by patent_id", unit="doc"):
        result = collection.delete_one({'patent_id': pid})
        deleted_count += result.deleted_count

    print(f"\n✅ Deleted {deleted_count} documents from MongoDB.")

# Example usage
csv_path = 'partn_cleaned.csv'
delete_patent_ids_from_mongo(csv_path)