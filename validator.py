import os
import sys
from pymongo import MongoClient
from tqdm import tqdm
from collections import defaultdict


def get_mongo_client(uri):
    """Establishes connection to MongoDB."""
    try:
        client = MongoClient(uri)
        client.admin.command('ismaster')
        print("MongoDB connection successful.")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Please ensure your MongoDB instance is running and the URI is correct.")
        sys.exit(1)


def analyze_consistency_details(client, db_name='your_db_name'):
    """
    Scans the 'cdkg' collection and provides a detailed breakdown of consistency levels
    and specifically which checks are failing.
    """
    db = client[db_name]
    patents_collection = db['cdkg']

    # --- Step 1: Build master lookup sets from all documents in the collection ---
    print("Building master lookup sets from all documents in 'cdkg' collection...")
    all_patents = list(patents_collection.find({}))

    all_taxonomy_domains = set()
    all_sub_industries = set()

    required_fields = ['taxonomy_domain', 'sub_industry']

    for patent in all_patents:
        if not all(field in patent for field in required_fields):
            continue
        all_taxonomy_domains.add(patent.get('taxonomy_domain'))
        all_sub_industries.add(patent.get('sub_industry'))

    print(f"Built master list with {len(all_taxonomy_domains)} unique domains.")
    print(f"Built master list with {len(all_sub_industries)} unique sub-industries.")

    # --- Step 2: Iterate through each patent again and categorize its consistency ---
    total_docs = len(all_patents)
    if total_docs == 0:
        print("The 'cdkg' collection is empty. No documents to process.")
        return

    print(f"\nScanning {total_docs} documents to analyze consistency...")

    # Structures to hold the counts
    consistency_counts = defaultdict(int)
    failure_counts = defaultdict(int)

    for patent in tqdm(all_patents, total = total_docs, desc = "Analyzing Patents"):
        consistency_score = 0

        check_fields = ['adjacent_domain', 'complementary_domain', 'adjacent_sub_industry',
                        'complementary_sub_industry']
        if not all(field in patent for field in check_fields):
            consistency_counts[0] += 1  # Score of 0 for missing fields
            failure_counts['missing_taxonomy_fields'] += 1
            continue

        # --- Perform all four consistency checks and score them ---
        if patent.get('adjacent_domain') in all_taxonomy_domains:
            consistency_score += 1
        else:
            failure_counts['adjacent_domain_failed'] += 1

        if patent.get('complementary_domain') in all_taxonomy_domains:
            consistency_score += 1
        else:
            failure_counts['complementary_domain_failed'] += 1

        if patent.get('adjacent_sub_industry') in all_sub_industries:
            consistency_score += 1
        else:
            failure_counts['adjacent_sub_industry_failed'] += 1

        if patent.get('complementary_sub_industry') in all_sub_industries:
            consistency_score += 1
        else:
            failure_counts['complementary_sub_industry_failed'] += 1

        # Increment the counter for the calculated score
        consistency_counts[consistency_score] += 1

    print(f"\nAnalysis complete.")

    # --- Step 3: Print the summary of counts ---
    print("\n--- Overall Consistency Analysis ---")
    print(f"Total Documents Analyzed: {total_docs}")
    print("-" * 40)

    fully_consistent_count = consistency_counts.get(4, 0)
    partially_3_count = consistency_counts.get(3, 0)
    partially_2_count = consistency_counts.get(2, 0)
    partially_1_count = consistency_counts.get(1, 0)
    fully_inconsistent_count = consistency_counts.get(0, 0)

    print(f"Fully Consistent (4/4 checks passed):     {fully_consistent_count}")
    print(f"Partially Consistent (1-3 checks passed): {partially_1_count + partially_2_count + partially_3_count}")
    print(f"Fully Inconsistent (0/4 checks passed):   {fully_inconsistent_count}")

    print("\n--- Breakdown of Inconsistent Checks ---")
    print(f"Total individual checks that failed: {sum(failure_counts.values())}")
    print("-" * 40)
    print(f"Adjacent Domain check failed:      {failure_counts.get('adjacent_domain_failed', 0)} times")
    print(f"Complementary Domain check failed: {failure_counts.get('complementary_domain_failed', 0)} times")
    print(f"Adjacent Sub-Industry check failed:  {failure_counts.get('adjacent_sub_industry_failed', 0)} times")
    print(
        f"Complementary Sub-Industry check failed: {failure_counts.get('complementary_sub_industry_failed', 0)} times")
    if 'missing_taxonomy_fields' in failure_counts:
        print(f"Documents with missing fields: {failure_counts.get('missing_taxonomy_fields', 0)} times")


if __name__ == "__main__":
    # IMPORTANT: Replace with your MongoDB connection details
    MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net"
    DB_NAME = "KG"  # Replace with your database name

    mongo_client = get_mongo_client(MONGO_URI)

    if mongo_client:
        analyze_consistency_details(mongo_client, DB_NAME)
        mongo_client.close()
        print("\nMongoDB connection closed.")
