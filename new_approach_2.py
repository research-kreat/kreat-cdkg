import os
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")  # Replace with your Atlas URI
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = "temp"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Connect
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_temp = db[COLLECTION_NAME]
col_industry = db["taxonomy_industries"]
col_function = db["taxonomy_functions"]

print(f"Loading model: {EMBEDDING_MODEL}...")
model = SentenceTransformer(EMBEDDING_MODEL)

def get_full_match(collection, query_embedding, index_name="vector_index"):
    """
    Finds the best match and returns the FULL document (excluding the large embedding vector).
    """
    pipeline = [
        {
            "$vectorSearch": {
                "index": index_name,
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": 50,
                "limit": 1
            }
        },
        {
            "$addFields": {
                "match_score": {"$meta": "vectorSearchScore"}
            }
        },
        {
            "$project": {
                "embedding": 0,       # Exclude the vector to save space
                "signature_text": 0   # Exclude the concatenated string used for training
            }
        }
    ]
    
    try:
        result = list(collection.aggregate(pipeline))
        return result[0] if result else None
    except Exception as e:
        print(f"  ⚠️ Vector search failed: {e}")
        return None

def enrich_records_fully():
    # Fetch all records in 'temp'
    cursor = col_temp.find({})
    total_found = col_temp.count_documents({})
    
    print(f"Enriching {total_found} records with FULL taxonomy data...")
    
    count = 0
    updated_count = 0
    
    for doc in cursor:
        count += 1
        if count % 100 == 0:
            print(f"Processing {count}/{total_found}...")

        try:
            # 1. Get Embedding
            query_vec = doc.get("ai_embeddings") or doc.get("embedding")
            if hasattr(query_vec, "tolist"): query_vec = query_vec.tolist()
            
            if not query_vec:
                text = f"{doc.get('title', '')} {doc.get('abstract', '')}"
                if len(text) < 5: continue
                query_vec = model.encode(text).tolist()

            # 2. Get FULL Industry Data
            ind_data = get_full_match(col_industry, query_vec)
            
            # 3. Get FULL Function Data
            func_data = get_full_match(col_function, query_vec)
            
            # 4. Clean keys for Mongo (Mongo doesn't like keys with dots '.')
            # We replace dots with underscores just in case
            if ind_data:
                ind_data = {k.replace('.', '_'): v for k, v in ind_data.items()}
            if func_data:
                func_data = {k.replace('.', '_'): v for k, v in func_data.items()}

            # 5. Construct the Update Object
            # We nest them to avoid collisions (both have 'Sector', 'Score', etc.)
            update_payload = {}
            if ind_data:
                update_payload["taxonomy_data.industry"] = ind_data
            if func_data:
                update_payload["taxonomy_data.function"] = func_data

            # 6. Update
            if update_payload:
                col_temp.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_payload}
                )
                updated_count += 1

        except Exception as e:
            print(f"❌ Error on {doc.get('_id')}: {e}")

    print(f"\n🎉 DONE! Fully enriched {updated_count} records.")

if __name__ == "__main__":
    enrich_records_fully()