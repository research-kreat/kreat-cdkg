import os
from pymongo import MongoClient
from dotenv import load_dotenv

def get_all_field_names(collection, sample_size=1000):
    """
    Efficiently gets all unique field names from a collection by sampling documents.
    Uses an aggregation pipeline to perform the work on the server.
    """
    print(f"Sampling {sample_size} documents from '{collection.name}' to discover all field names...")
    pipeline = [
        {"$limit": sample_size},
        {"$project": {"fields": {"$objectToArray": "$$ROOT"}}},
        {"$unwind": "$fields"},
        {"$group": {"_id": None, "allFields": {"$addToSet": "$fields.k"}}}
    ]
    try:
        result = list(collection.aggregate(pipeline))
        if not result:
            return set()
        return set(result[0]['allFields'])
    except Exception as e:
        print(f"An error occurred during field name aggregation: {e}")
        return set()

def find_overlapping_patent_ids(collection1, collection2_name):
    """
    Finds patent_ids from collection1 that also exist in collection2.
    Uses the $lookup aggregation stage for an efficient server-side join.
    """
    print(f"Checking for overlapping patent_ids between '{collection1.name}' and '{collection2_name}'...")
    pipeline = [
        {
            "$lookup": {
                "from": collection2_name,
                "localField": "patent_id",
                "foreignField": "patent_id",
                "as": "matches_in_other_collection"
            }
        },
        {
            "$match": {
                "matches_in_other_collection": {"$ne": []}
            }
        },
        {
            "$project": {
                "_id": 0,
                "patent_id": 1
            }
        }
    ]
    try:
        overlapping_docs = list(collection1.aggregate(pipeline))
        return [doc['patent_id'] for doc in overlapping_docs]
    except Exception as e:
        print(f"An error occurred during the overlap check: {e}")
        return []

def main():
    """
    Main function to run the validation between two MongoDB collections.
    """
    # --- Configuration ---
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")

    # !! IMPORTANT !! -> Set your two collection names here
    COLLECTION_1_NAME = "cdkg"
    COLLECTION_2_NAME = "temp"
    
    # Number of documents to scan in each collection to find field names.
    # 1000 is usually enough unless you have a very inconsistent schema.
    SCHEMA_SAMPLE_SIZE = 7000

    if not all([mongo_uri, db_name]):
        print("Error: MONGO_URI or MONGO_DB not found in .env file.")
        return

    client = None
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]

        collection1 = db[COLLECTION_1_NAME]
        collection2 = db[COLLECTION_2_NAME]

        print(f"Starting validation for database: '{db_name}'")
        print("--------------------------------------------------\n")

        # === Task 1: Validate Field Names (Schema) ===
        print("TASK 1: Validating Field Names (Schema)")
        fields1 = get_all_field_names(collection1, sample_size=SCHEMA_SAMPLE_SIZE)
        fields2 = get_all_field_names(collection2, sample_size=SCHEMA_SAMPLE_SIZE)

        if not fields1 or not fields2:
            print("Could not retrieve field names from one or both collections. Exiting schema check.")
        elif fields1 == fields2:
            print("\n✅ SUCCESS: Field names are identical in both collections.")
            print(f"   (Found {len(fields1)} unique fields)")
        else:
            print("\n❌ FAILURE: Field names do not match.")
            only_in_1 = fields1 - fields2
            only_in_2 = fields2 - fields1
            if only_in_1:
                print(f"\n   Fields ONLY in '{COLLECTION_1_NAME}':")
                for field in sorted(list(only_in_1)):
                    print(f"     - {field}")
            if only_in_2:
                print(f"\n   Fields ONLY in '{COLLECTION_2_NAME}':")
                for field in sorted(list(only_in_2)):
                    print(f"     - {field}")
        
        print("\n--------------------------------------------------\n")

        # === Task 2: Validate patent_id Uniqueness ===
        print("TASK 2: Validating patent_id Uniqueness")
        
        # We only need to check one way, as an overlap is an overlap.
        overlaps = find_overlapping_patent_ids(collection1, COLLECTION_2_NAME)

        if not overlaps:
            print("\n✅ SUCCESS: No matching patent_ids found between the two collections.")
        else:
            print(f"\n❌ FAILURE: Found {len(overlaps)} overlapping patent_id(s).")
            print("   These IDs exist in BOTH collections:")
            # Print the first 20 overlapping IDs for brevity
            for pid in overlaps[:20]:
                print(f"     - {pid}")
            if len(overlaps) > 20:
                print(f"     ... and {len(overlaps) - 20} more.")

    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        if client:
            client.close()
            print("\n--------------------------------------------------")
            print("MongoDB connection closed.")

if __name__ == "__main__":
    main()