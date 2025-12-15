import os
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
NEO4J_URI = os.getenv("NEO4J_URI2", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER2", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD2", "password")

# The 8 Immutable Laws of Engineering
UNIVERSAL_FUNCTIONS = [
    "Assemble", "Support", "Regulate", "Store", 
    "Communicate", "Convert", "Separate", "Move"
]

model = SentenceTransformer('all-MiniLM-L6-v2')

def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def patch_graph():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    print("⏳ Embedding the 8 Universal Master Functions...")
    master_embeddings = {k: model.encode(k) for k in UNIVERSAL_FUNCTIONS}

    with driver.session() as session:
        # 1. Create Master Nodes
        print("🏗️  Creating Master Nodes in Neo4j...")
        for func in UNIVERSAL_FUNCTIONS:
            session.run("MERGE (m:MasterFunction {name: $name})", name=func)

        # 2. Fetch Existing Universal Functions from Graph
        print("🔍 Fetching existing Universal/Specific functions...")
        # We target nodes that patents are already connected to
        query = """
        MATCH (n) 
        WHERE (n:UniversalFunction OR n:SpecificFunction) AND n.embedding IS NOT NULL
        RETURN elementId(n) as id, n.name as name, n.embedding as embedding, labels(n) as labels
        """
        results = session.run(query).data()
        print(f"   > Found {len(results)} existing function nodes.")

        # 3. Calculate Similarity and Link
        print("🔗 Linking existing nodes to the 8 Masters...")
        
        updates = []
        for node in tqdm(results):
            node_emb = node['embedding']
            
            # Find best match among the 8
            best_score = -1
            best_master = None
            
            for master_name, master_emb in master_embeddings.items():
                score = cosine_similarity(node_emb, master_emb)
                if score > best_score:
                    best_score = score
                    best_master = master_name
            
            # If similarity is decent, queue the update
            if best_score > 0.35: 
                updates.append({
                    "node_id": node['id'],
                    "master": best_master,
                    "score": float(best_score)
                })

        # 4. Batch Execute Writes
        print(f"💾 Writing {len(updates)} relationships to Neo4j...")
        batch_query = """
        UNWIND $batch as row
        MATCH (n) WHERE elementId(n) = row.node_id
        MATCH (m:MasterFunction {name: row.master})
        MERGE (n)-[r:CLASSIFIED_AS_MASTER]->(m)
        SET r.score = row.score
        """
        
        # Batch size 1000
        batch_size = 1000
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            session.run(batch_query, batch=batch)

    driver.close()
    print("✅ Graph Patch Complete! The 8 Universal Highways are built.")

if __name__ == "__main__":
    patch_graph()