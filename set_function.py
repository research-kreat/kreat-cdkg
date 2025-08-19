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


def enrich_patent_with_functions(client, db_name='your_db_name'):
    """
    Iterates through patents, finds the best functional taxonomy match via vector search,
    and updates the patent document with functional data.
    """
    db = client[db_name]
    patents_collection = db['temp']
    function_taxonomy_collection = db['function_taxonomy']

    # --- Step 1: Get the total count for the progress bar ---
    try:
        total_docs = patents_collection.count_documents({})
        if total_docs == 0:
            print("The 'temp' collection is empty. No documents to process.")
            return
    except Exception as e:
        print(f"Error counting documents in 'temp' collection: {e}")
        return

    print(f"Found {total_docs} documents to process in the 'temp' collection.")

    # --- Step 2: Iterate through each patent with a progress bar ---
    patent_cursor = patents_collection.find({})

    for patent in tqdm(patent_cursor, total = total_docs, desc = "Enriching Patents with Functions"):
        patent_id = patent['_id']

        # Ensure the document has an embedding to query with
        if 'embedding' not in patent or not patent['embedding']:
            tqdm.write(f"Skipping document with ID {patent_id} as it has no 'embedding' field.")
            continue

        query_vector = patent['embedding']

        # --- Step 3: Perform the vector search against the function_taxonomy ---
        # IMPORTANT: Ensure your vector index in 'function_taxonomy' is also named 'vector_index'
        vector_search_pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index",  # The index on the 'function_taxonomy' collection
                    "path": "embedding",
                    "queryVector": query_vector,
                    "numCandidates": 10,
                    "limit": 1
                }
            },
            {
                "$project": {
                    "_id": 0,  # Exclude the ID of the taxonomy doc
                    "major_function": 1,
                    "sub_function": 1,
                    "equivalent_domain": 1,
                    "equivalent_function": 1,
                    "score": {"$meta": "vectorSearchScore"}
                }
            }
        ]

        try:
            results = list(function_taxonomy_collection.aggregate(vector_search_pipeline))

            if results:
                top_match = results[0]

                # --- Step 4: Prepare and execute the update operation ---
                update_payload = {
                    "$set": {
                        "function_sector": top_match.get("major_function"),  # major_function -> function_sector
                        "function": top_match.get("sub_function"),  # sub_function -> function
                        "equivalent_domain": top_match.get("equivalent_domain"),
                        "equivalent_function": top_match.get("equivalent_function"),
                        "function_match_score": top_match.get("score")
                    }
                }

                patents_collection.update_one({'_id': patent_id}, update_payload)
            else:
                tqdm.write(f"No vector search match found for document ID {patent_id} in function_taxonomy.")

        except Exception as e:
            tqdm.write(f"An error occurred while processing document ID {patent_id}: {e}")

    print("\nFunctional enrichment process complete.")


if __name__ == "__main__":
    # IMPORTANT: Replace with your MongoDB connection details
    MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net"
    DB_NAME = "KG"  # Replace with your database name

    mongo_client = get_mongo_client(MONGO_URI)

    if mongo_client:
        enrich_patent_with_functions(mongo_client, DB_NAME)
        mongo_client.close()
        print("MongoDB connection closed.")
