import os
import csv
from pymongo import MongoClient
from tqdm import tqdm

csv.field_size_limit(1_000_000_000)

# --- CONFIGURATION ---
# It's best practice to use environment variables, but you can hardcode them here for simplicity.
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net")
MONGO_DB = os.getenv("MONGO_DB", "KG")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "cdkg")

# Number of random documents to fetch
SAMPLE_SIZE = 100

# Name of the output CSV file
OUTPUT_CSV_FILE = "mongo_random_sample.csv"

# --- Use the comprehensive, unified list for column ordering ---
PREFERRED_HEADERS = [
    "_id", "knowledge_type", "title", "full_text", "publication_date", "updated_at",
    "local_url", "technology_stack", "keywords", "country", "references", "pdf_link",
    "source_url", "source_date", "domain", "sub_domain", "inventors", "assignee_names",
    "relevance_score", "data_quality_score", "patent_type", "num_claims", "summary",
    "patent_id", "assignee_org", "foreign_citation_count", "local_citation_count",
    "cpc_type", "wipo_kind", "cpc_class_title", "cpc_subclass_title",
    "cpc_group_title", "ipc_classifications", "cpc_classifications",
    "doi_url", "publisher", "journal_name", "journal_volume", "journal_issue",
    "journal_pages", "doi", "authors", "cited_by", "abstract",
    "ai_generated_abstract", "use_case_examples", "market_trends",
    "customer_behavior", "competitor_data", "embedding", "ai_embedding"
]


def export_random_docs_to_csv():
    """
    Connects to MongoDB, fetches a random sample of documents, and writes them to a CSV file.
    """
    print("--- MongoDB Random Sample to CSV Exporter ---")

    # --- 1. CONNECT TO MONGODB ---
    try:
        print(f"Connecting to MongoDB at {MONGO_URI}...")
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        # Ping the server to check the connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB.")
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return

    # --- 2. FETCH RANDOM DOCUMENTS ---
    try:
        print(f"Fetching {SAMPLE_SIZE} random documents from collection '{MONGO_COLLECTION}'...")
        # The $sample operator is the most efficient way to get a random sample
        aggregation_pipeline = [{'$sample': {'size': SAMPLE_SIZE}}]
        random_docs = list(collection.aggregate(aggregation_pipeline))
        
        if not random_docs:
            print("❌ No documents were returned from the database. Exiting.")
            return
            
        print(f"✅ Successfully fetched {len(random_docs)} documents.")

    except Exception as e:
        print(f"❌ An error occurred while fetching documents: {e}")
        client.close()
        return

    # --- 3. DETERMINE CSV HEADERS (with preferred order) ---
    print("Determining all unique headers from the sample...")
    all_found_headers = set()
    for doc in random_docs:
        all_found_headers.update(doc.keys())
    
    # Build the final header list
    final_headers = []
    # Add preferred headers that are actually present in the data
    for header in PREFERRED_HEADERS:
        if header in all_found_headers:
            final_headers.append(header)
            all_found_headers.remove(header)
            
    # Add any remaining headers (not in the preferred list) alphabetically
    final_headers.extend(sorted(list(all_found_headers)))
        
    print(f"Found {len(final_headers)} unique headers.")

    # --- 4. WRITE TO CSV FILE ---
    try:
        print(f"Writing documents to '{OUTPUT_CSV_FILE}'...")
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            # Use DictWriter which can handle rows with missing keys gracefully
            writer = csv.DictWriter(f, fieldnames=final_headers)
            
            # Write the header row
            writer.writeheader()
            
            # Write the document data
            for doc in tqdm(random_docs, desc="Writing rows"):
                # MongoDB's _id is an ObjectId, which is not CSV-friendly. Convert it to a string.
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                writer.writerow(doc)
        
        print(f"\n✅ Successfully saved {len(random_docs)} documents to '{OUTPUT_CSV_FILE}'.")

    except Exception as e:
        print(f"❌ An error occurred while writing the CSV file: {e}")
    finally:
        client.close()
        print("MongoDB connection closed.")


if __name__ == "__main__":
    export_random_docs_to_csv()
