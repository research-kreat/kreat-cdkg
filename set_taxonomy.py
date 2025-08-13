import os
import sys
from pymongo import MongoClient
from tqdm import tqdm


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


def enrich_patent_data(client, db_name='KG'):
    """
    Iterates through patents, finds the best taxonomy match via vector search,
    and updates the patent document.
    """
    db = client[db_name]
    patents_collection = db['cdkg']
    taxonomy_collection = db['industry_taxonomy']

    # --- Step 1: Get the total count for the progress bar ---
    try:
        total_docs = patents_collection.count_documents({})
        if total_docs == 0:
            print("The 'cdkg' collection is empty. No documents to process.")
            return
    except Exception as e:
        print(f"Error counting documents in 'cdkg' collection: {e}")
        return

    print(f"Found {total_docs} documents to process in the 'cdkg' collection.")

    # --- Step 2: Iterate through each patent with a progress bar ---
    # We use find({}) with no_cursor_timeout=True for long-running operations
    patent_cursor = patents_collection.find({}, no_cursor_timeout = True)

    for patent in tqdm(patent_cursor, total = total_docs, desc = "Enriching Patents"):
        patent_id = patent['_id']

        # Ensure the document has an embedding to query with
        if 'embedding' not in patent or not patent['embedding']:
            print(f"Skipping document with ID {patent_id} as it has no 'embedding' field.")
            continue

        query_vector = patent['embedding']

        # --- Step 3: Perform the vector search ---
        vector_search_pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index",  # Make sure this is your index name
                    "path": "embedding",
                    "queryVector": query_vector,
                    "numCandidates": 10,  # Number of candidates to consider
                    "limit": 1  # We only want the top 1 result
                }
            },
            {
                "$project": {
                    "_id": 0,  # Exclude the ID of the taxonomy doc
                    "sector": 1,
                    "domain": 1,
                    "sub_industry": 1,
                    "adjacent_domain": 1,
                    "adjacent_sub_industry": 1,
                    "complementary_domain": 1,
                    "complementary_sub_industry": 1,
                    "score": {"$meta": "vectorSearchScore"}  # Optionally, get the similarity score
                }
            }
        ]

        try:
            results = list(taxonomy_collection.aggregate(vector_search_pipeline))

            if results:
                top_match = results[0]

                # --- Step 4: Prepare and execute the update operation ---
                update_payload = {
                    "$set": {
                        "domain": top_match.get("sector"),  # Overwrite old 'domain' with new 'sector'
                        "taxonomy_domain": top_match.get("domain"),  # Add new field for taxonomy's domain
                        "sub_industry": top_match.get("sub_industry"),
                        "adjacent_domain": top_match.get("adjacent_domain"),
                        "adjacent_sub_industry": top_match.get("adjacent_sub_industry"),
                        "complementary_domain": top_match.get("complementary_domain"),
                        "complementary_sub_industry": top_match.get("complementary_sub_industry"),
                        "taxonomy_match_score": top_match.get("score")
                    },
                    "$unset": {
                        "sub_domain": ""  # Remove the old 'sub_domain' field
                    }
                }

                patents_collection.update_one({'_id': patent_id}, update_payload)
            else:
                # This case is unlikely if the taxonomy collection is populated, but good practice to handle
                print(f"No vector search match found for document ID {patent_id}.")

        except Exception as e:
            print(f"An error occurred while processing document ID {patent_id}: {e}")

    patent_cursor.close()
    print("\nEnrichment process complete.")


if __name__ == "__main__":
    # IMPORTANT: Replace with your MongoDB connection details
    # For local instance: "mongodb://localhost:27017/"
    # For Atlas, get the connection string from the UI
    MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net"
    DB_NAME = "KG"  # Replace with your database name

    mongo_client = get_mongo_client(MONGO_URI)

    if mongo_client:
        enrich_patent_data(mongo_client, DB_NAME)
        mongo_client.close()
        print("MongoDB connection closed.")
