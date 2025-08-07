import os
import csv
from pymongo import MongoClient
from tqdm import tqdm

# Set a high limit for CSV field size to handle potentially large fields
csv.field_size_limit(1_000_000_000)

# --- CONFIGURATION ---
# It's best practice to use environment variables, but you can hardcode them here for simplicity.
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net")
MONGO_DB = os.getenv("MONGO_DB", "KG")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "cdkg")

# Name of the output CSV file
OUTPUT_CSV_FILE = "mongo_full_export.csv"

# --- Use the comprehensive, unified list for column ordering ---
# These headers will be the columns in the CSV.
# If a document is missing a field, it will be blank in the CSV.
# If a document has extra fields not listed here, they will be ignored.
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


def export_all_docs_to_csv():
    """
    Connects to MongoDB, fetches ALL documents from a collection, 
    and writes them to a CSV file.
    """
    print("--- MongoDB Full Collection to CSV Exporter ---")

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

    # --- 2. FETCH ALL DOCUMENTS ---
    try:
        # First, get a count of all documents for the progress bar.
        total_docs = collection.count_documents({})
        print(f"Found {total_docs} documents in collection '{MONGO_COLLECTION}'.")
        
        if total_docs == 0:
            print("Collection is empty. Nothing to export. Exiting.")
            client.close()
            return
            
        # Use find({}) to get a cursor to all documents.
        # This is memory-efficient as it doesn't load all documents into memory at once.
        print("Fetching all documents...")
        cursor = collection.find({})

    except Exception as e:
        print(f"❌ An error occurred while preparing to fetch documents: {e}")
        client.close()
        return

    # --- 3. WRITE TO CSV FILE ---
    try:
        print(f"Writing {total_docs} documents to '{OUTPUT_CSV_FILE}'...")
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            # Use DictWriter with the predefined headers.
            # `extrasaction='ignore'` ensures that fields in the doc but not in headers are ignored.
            writer = csv.DictWriter(f, fieldnames=PREFERRED_HEADERS, extrasaction='ignore')
            
            # Write the header row
            writer.writeheader()
            
            # Iterate over the cursor and write each document to the CSV
            # tqdm will show a progress bar
            for doc in tqdm(cursor, total=total_docs, desc="Writing rows"):
                # MongoDB's _id is an ObjectId, which is not CSV-friendly. Convert it to a string.
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                writer.writerow(doc)
        
        print(f"\n✅ Successfully saved {total_docs} documents to '{OUTPUT_CSV_FILE}'.")

    except Exception as e:
        print(f"❌ An error occurred while writing the CSV file: {e}")
    finally:
        # It's important to close the client and the cursor
        cursor.close()
        client.close()
        print("MongoDB connection closed.")


if __name__ == "__main__":
    export_all_docs_to_csv()