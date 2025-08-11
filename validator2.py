import os
import sys
from pymongo import MongoClient
from tqdm import tqdm
from collections import defaultdict


def get_mongo_client(uri):
    """Establishes connection to MongoDB."""
    try:
        client = MongoClient(uri)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("MongoDB connection successful.")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Please ensure your MongoDB instance is running and the URI is correct.")
        sys.exit(1)


def analyze_function_equivalency(client, db_name='your_db_name'):
    """
    Scans the 'cdkg' collection to validate its internal functional consistency.
    It checks if each 'equivalent_function' exists as a primary 'function'
    in at least one other document within the collection.
    """
    db = client[db_name]
    patents_collection = db['cdkg']

    # --- Step 1: Build a master lookup set of all primary functions ---
    print("Building master lookup set of all primary 'function' values from 'cdkg' collection...")
    all_patents = list(patents_collection.find({}))

    all_primary_functions = set()

    for patent in all_patents:
        if 'function' in patent and patent.get('function'):
            all_primary_functions.add(patent.get('function'))

    print(f"Built master list with {len(all_primary_functions)} unique primary functions.")

    # --- Step 2: Iterate through each patent and check its equivalent_function ---
    total_docs = len(all_patents)
    if total_docs == 0:
        print("The 'cdkg' collection is empty. No documents to process.")
        return

    print(f"\nScanning {total_docs} documents to validate function equivalency...")

    consistent_count = 0
    inconsistent_count = 0

    for patent in tqdm(all_patents, total = total_docs, desc = "Validating Function Equivalency"):
        # A patent is considered inconsistent if it's missing the equivalent_function field
        if 'equivalent_function' not in patent or not patent.get('equivalent_function'):
            inconsistent_count += 1
            continue

        # Check if the equivalent function exists in the master set of primary functions
        if patent.get('equivalent_function') in all_primary_functions:
            consistent_count += 1
        else:
            inconsistent_count += 1

    print(f"\nAnalysis complete.")

    # --- Step 3: Print the summary of counts ---
    print("\n--- Function Equivalency Analysis ---")
    print(f"Total Documents Analyzed: {total_docs}")
    print("-" * 65)
    print(
        f"Consistent Patents:     {consistent_count} (The 'equivalent_function' exists as a primary 'function' elsewhere in the collection)")
    print(
        f"Inconsistent Patents:   {inconsistent_count} (The 'equivalent_function' is missing or does not exist as a primary 'function')")


if __name__ == "__main__":
    # IMPORTANT: Replace with your MongoDB connection details
    MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net"
    DB_NAME = "KG"  # Replace with your database name

    mongo_client = get_mongo_client(MONGO_URI)

    if mongo_client:
        analyze_function_equivalency(mongo_client, DB_NAME)
        mongo_client.close()
        print("\nMongoDB connection closed.")

"""PS C:\Users\itsge\PycharmProjects\kreat-cdkg> python3 validator2.py
MongoDB connection successful.
Building master lookup set of all primary 'function' values from 'cdkg' collection...
Built master list with 384 unique primary functions.

Scanning 6287 documents to validate function equivalency...
Validating Function Equivalency: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████| 6287/6287 [00:00<00:00, 1389189.19it/s]

Analysis complete.

--- Function Equivalency Analysis ---
Total Documents Analyzed: 6287
-----------------------------------------------------------------
Consistent Patents:     5971 (The 'equivalent_function' exists as a primary 'function' elsewhere in the collection)
Inconsistent Patents:   316 (The 'equivalent_function' is missing or does not exist as a primary 'function')

MongoDB connection closed.
PS C:\Users\itsge\PycharmProjects\kreat-cdkg> python3 validator.py 
MongoDB connection successful.
Building master lookup sets from all documents in 'cdkg' collection...
Built master list with 64 unique domains.
Built master list with 154 unique sub-industries.

Scanning 6287 documents to analyze consistency...
Analyzing Patents: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 6287/6287 [00:00<00:00, 535934.58it/s]

Analysis complete.

--- Overall Consistency Analysis ---
Total Documents Analyzed: 6287
----------------------------------------
Fully Consistent (4/4 checks passed):     16
Partially Consistent (1-3 checks passed): 5080
Fully Inconsistent (0/4 checks passed):   1191

--- Breakdown of Inconsistent Checks ---
Total individual checks that failed: 18119
----------------------------------------
Adjacent Domain check failed:      3041 times
Complementary Domain check failed: 3694 times
Adjacent Sub-Industry check failed:  5370 times
Complementary Sub-Industry check failed: 6014 times

MongoDB connection closed."""