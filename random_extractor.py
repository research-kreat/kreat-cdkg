# REQUIREMENTS: pip install pymongo pandas

import pymongo
import pandas as pd
import logging

# --- Configuration ---
# Replace with your MongoDB connection details
MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net/" 
MONGO_DB_NAME = "KG"
MONGO_COLLECTION_NAME = "cdkg"
SAMPLE_SIZE = 100
OUTPUT_CSV_FILE = "mongo_random_sample.csv"

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_random_documents():
    """
    Connects to MongoDB, fetches a random sample of documents, 
    and saves them to a CSV file.
    """
    try:
        # --- Connect to MongoDB ---
        logging.info(f"Connecting to MongoDB at {MONGO_URI}...")
        client = pymongo.MongoClient(MONGO_URI)
        
        # Verify connection
        client.admin.command('ping') 
        
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        logging.info(f"Successfully connected to database '{MONGO_DB_NAME}' and collection '{MONGO_COLLECTION_NAME}'.")

        # --- Define the Aggregation Pipeline ---
        # The $sample operator randomly selects the specified number of documents.
        pipeline = [
            { "$sample": { "size": SAMPLE_SIZE } }
        ]

        # --- Execute the Query ---
        logging.info(f"Fetching {SAMPLE_SIZE} random documents...")
        random_documents = list(collection.aggregate(pipeline))
        
        if not random_documents:
            logging.warning("No documents were returned from the collection. The collection might be empty.")
            return

        logging.info(f"Successfully retrieved {len(random_documents)} documents.")

        # --- Convert to Pandas DataFrame and Save to CSV ---
        logging.info("Converting data to DataFrame...")
        df = pd.DataFrame(random_documents)

        logging.info(f"Saving documents to '{OUTPUT_CSV_FILE}'...")
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        
        logging.info(f"✅ Successfully exported {len(df)} random documents to {OUTPUT_CSV_FILE}")

    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"❌ MongoDB Connection Failed: {e}. Please check your MONGO_URI.")
    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
    finally:
        if 'client' in locals():
            client.close()
            logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    export_random_documents()