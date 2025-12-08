import os
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI") 
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = "temp"  # Your main patents collection
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Connect to Database
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_temp = db[COLLECTION_NAME]
col_industry = db["taxonomy_industries"]
col_function = db["taxonomy_functions"]  # The clean collection we just fixed

print(f"Loading embedding model: {EMBEDDING_MODEL}...")
model = SentenceTransformer(EMBEDDING_MODEL)

def get_best_match(collection, query_embedding):
    """
    Finds the single best match from the taxonomy collection using vector search.
    """
    pipeline = [
        {
            "$vectorSearch": {
                "index": "vector_index", 
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": 100,  # Higher count for better accuracy
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
                "embedding": 0,       # Exclude vector to save space
                "signature_text": 0,  # Exclude training text
                "_id": 0              # Exclude ID to allow embedding into patent doc
            }
        }
    ]
    
    try:
        result = list(collection.aggregate(pipeline))
        return result[0] if result else None
    except Exception as e:
        print(f"⚠️ Vector search failed: {e}")
        return None

def re_enrich_patents():
    # 1. HARD RESET: Remove the old, messy taxonomy_data
    print("WARNING: Wiping 'taxonomy_data' from all patent records...")
    result = col_temp.update_many({}, {"$unset": {"taxonomy_data": ""}})
    print(f"✅ Cleared data from {result.modified_count} records.")
    
    # 2. Fetch patents to process
    # We project only what we need to minimize network load
    cursor = col_temp.find({}, {"_id": 1, "title": 1, "abstract": 1, "ai_embeddings": 1, "embedding": 1})
    total_found = col_temp.count_documents({})
    
    print(f"🚀 Starting Re-enrichment for {total_found} patents...")
    
    count = 0
    updated_count = 0
    
    for doc in cursor:
        count += 1
        if count % 1000 == 0:
            print(f"Processing {count}/{total_found}...")

        try:
            # --- A. Get Query Vector ---
            # Priority: existing ai_embeddings -> existing embedding -> generate from text
            query_vec = doc.get("ai_embeddings") or doc.get("embedding")
            
            # Ensure it's a list (not a numpy array)
            if hasattr(query_vec, "tolist"): 
                query_vec = query_vec.tolist()
            
            # Fallback: Generate vector if missing
            if not query_vec:
                text_content = f"{doc.get('title', '')} {doc.get('abstract', '')}"
                # Skip empty records
                if len(text_content.strip()) < 5: 
                    continue
                query_vec = model.encode(text_content).tolist()

            # --- B. Find Matches in Clean Taxonomy ---
            ind_data = get_best_match(col_industry, query_vec)
            func_data = get_best_match(col_function, query_vec)
            
            # --- C. Sanitize Keys for Mongo ---
            # MongoDB doesn't allow dots in field names (e.g., "U.S. Sector" -> "U_S_ Sector")
            if ind_data:
                ind_data = {k.replace('.', '_'): v for k, v in ind_data.items()}
            if func_data:
                func_data = {k.replace('.', '_'): v for k, v in func_data.items()}

            # --- D. Update the Patent ---
            update_payload = {}
            if ind_data:
                update_payload["taxonomy_data.industry"] = ind_data
            if func_data:
                update_payload["taxonomy_data.function"] = func_data

            if update_payload:
                col_temp.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_payload}
                )
                updated_count += 1

        except Exception as e:
            print(f"❌ Error on doc {doc.get('_id')}: {e}")

    print(f"\n🎉 DONE! Successfully re-enriched {updated_count} patents.")

if __name__ == "__main__":
    re_enrich_patents()