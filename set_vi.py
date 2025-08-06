import pandas as pd
from neo4j import GraphDatabase
import json
import os
from tqdm import tqdm # For a nice progress bar

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
# --- Neo4j Connection ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  # <-- IMPORTANT: CHANGE THIS

# --- Data Source ---
CSV_FILE_PATH = "mongo_random_sample.csv"  # <-- IMPORTANT: SET YOUR CSV FILE PATH

# --- Vector Index Configuration ---
# This is the name for the first vector index
INDEX_NAME_ORIGINAL = "patentOriginalEmbeddingIndex"
# This is the name for the second vector index
INDEX_NAME_AI = "patentAiEmbeddingIndex"
# The property name on the :Patent node for the original embedding
PROPERTY_NAME_ORIGINAL = "embedding"
# The property name on the :Patent node for the AI-generated embedding
PROPERTY_NAME_AI = "ai_embeddings"
# The dimension of your vectors (you specified 384)
VECTOR_DIMENSION = 384
# The similarity metric to use (cosine is great for semantic similarity)
SIMILARITY_METRIC = "cosine"


# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================

def get_driver():
    """Establishes a connection to the Neo4j database."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def check_neo4j_connectivity(driver):
    """Checks if the Neo4j service is running."""
    try:
        driver.verify_connectivity()
        print("✅ Neo4j connection successful.")
        return True
    except Exception as e:
        print(f"❌ CRITICAL: Could not connect to Neo4j. Please check your credentials and database status.\nError: {e}")
        return False

def create_vector_indexes(driver):
    """Creates the vector indexes in Neo4j if they don't already exist."""
    print("Creating vector indexes (if they don't exist)...")
    
    # Query for the original embedding index
    index_query_original = f"""
    CREATE VECTOR INDEX {INDEX_NAME_ORIGINAL} IF NOT EXISTS
    FOR (p:Patent) ON (p.{PROPERTY_NAME_ORIGINAL})
    OPTIONS {{indexConfig: {{
        `vector.dimensions`: {VECTOR_DIMENSION},
        `vector.similarity_function`: '{SIMILARITY_METRIC}'
    }}}}
    """
    
    # Query for the AI-generated embedding index
    index_query_ai = f"""
    CREATE VECTOR INDEX {INDEX_NAME_AI} IF NOT EXISTS
    FOR (p:Patent) ON (p.{PROPERTY_NAME_AI})
    OPTIONS {{indexConfig: {{
        `vector.dimensions`: {VECTOR_DIMENSION},
        `vector.similarity_function`: '{SIMILARITY_METRIC}'
    }}}}
    """
    
    with driver.session(database="neo4j") as session:
        try:
            session.run(index_query_original)
            print(f"   -> Successfully created or confirmed index '{INDEX_NAME_ORIGINAL}'.")
            session.run(index_query_ai)
            print(f"   -> Successfully created or confirmed index '{INDEX_NAME_AI}'.")
            print("Waiting for indexes to come online (this may take a moment)...")
            # This query waits until the indexes are populated and ready to use.
            session.run("CALL db.awaitIndexes(300000)") # Wait up to 5 minutes (300,000 ms)
            print("✅ Vector indexes are online and ready.")
        except Exception as e:
            print(f"❌ An error occurred during index creation: {e}")
            raise

def parse_embedding_string(embedding_str):
    """
    Safely parses a string representation of a list into a list of floats.
    Handles potential malformed strings.
    """
    if not embedding_str or not isinstance(embedding_str, str):
        return None
    try:
        # The string looks like a list, so we can use json.loads for safe parsing
        # after removing potential newlines and extra spaces.
        clean_str = embedding_str.replace('\n', '').replace(' ', '')
        # json.loads is safer than eval()
        embedding_list = json.loads(clean_str)
        if isinstance(embedding_list, list) and len(embedding_list) == VECTOR_DIMENSION:
            return [float(i) for i in embedding_list]
        else:
            print(f"[WARNING] Parsed list is not a valid {VECTOR_DIMENSION}-dimension vector. Skipping.")
            return None
    except (json.JSONDecodeError, ValueError) as e:
        print(f"[WARNING] Could not parse embedding string: '{embedding_str[:50]}...'. Error: {e}. Skipping.")
        return None

# ==============================================================================
# --- MAIN EXECUTION ---
# ==============================================================================
def main():
    """Main function to run the entire vector ingestion pipeline."""
    print("🚀 Starting Neo4j Vector Ingestion Pipeline")
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CRITICAL: The data file '{CSV_FILE_PATH}' was not found.")
        return

    driver = get_driver()
    
    if not check_neo4j_connectivity(driver):
        driver.close()
        return
        
    create_vector_indexes(driver)
    
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        # We only need the patent_id and the two embedding columns
        df_vectors = df[['patent_id', 'embedding', 'ai_embeddings']].copy()
        df_vectors.dropna(subset=['patent_id'], inplace=True)
        df_vectors = df_vectors.fillna('')
        
        total_rows = len(df_vectors)
        print(f"\nFound {total_rows} patents to update with vector embeddings.")

        with driver.session(database="neo4j") as session:
            # Using tqdm for a progress bar
            for _, row in tqdm(df_vectors.iterrows(), total=total_rows, desc="Updating Patent Vectors"):
                patent_id = int(row['patent_id']) # Ensure patent_id is an integer
                
                original_embedding = parse_embedding_string(row['embedding'])
                ai_embedding = parse_embedding_string(row['ai_embeddings'])
                
                # Build the query dynamically based on which embeddings are valid
                if original_embedding or ai_embedding:
                    query = """
                    MATCH (p:Patent {patent_id: $patent_id})
                    SET p += $properties
                    """
                    properties_to_set = {}
                    if original_embedding:
                        properties_to_set[PROPERTY_NAME_ORIGINAL] = original_embedding
                    if ai_embedding:
                        properties_to_set[PROPERTY_NAME_AI] = ai_embedding
                    
                    session.run(query, patent_id=patent_id, properties=properties_to_set)

        print("\n✅ Vector data ingestion complete.")

    except Exception as e:
        print(f"\n❌ An unexpected error occurred during the main process: {e}")
    finally:
        print("\n--- Pipeline Finished ---")
        driver.close()

if __name__ == "__main__":
    main()
