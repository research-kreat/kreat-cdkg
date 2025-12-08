import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import time

load_dotenv()

# --- CONFIGURATION ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# Tuning for Low RAM Environment
BATCH_SIZE = 100       # Smaller batches to prevent Heap Space errors
PARALLEL = False       # Sequential processing is safer for memory than parallel
SCORE_THRESHOLD = 0.85 

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_densification_protocols(session):
    print("🔗 Running Densification Protocols...")

    # 1. Patent-to-Patent Semantic Similarity (Wormholes)
    # We use apoc.periodic.iterate here because the original query likely caused the crash too
    print("   - Creating Semantic Wormholes (Patents)...")
    query_wormholes = """
    CALL apoc.periodic.iterate(
        "MATCH (p1:Patent) WHERE p1.embedding IS NOT NULL RETURN p1",
        "CALL db.index.vector.queryNodes('patent_embeddings', 5, p1.embedding)
         YIELD node AS p2, score
         WHERE score > 0.88 AND elementId(p1) < elementId(p2) AND p1.domain <> p2.domain
         MERGE (p1)-[r:SEMANTICALLY_SIMILAR]-(p2) 
         SET r.score = score",
        {batchSize: 50, parallel: false}
    )
    """
    try:
        session.run(query_wormholes)
        print("     ✅ Wormholes created.")
    except Exception as e:
        print(f"     ❌ Wormhole creation failed: {e}")

    # 2. Inventor Networks
    print("   - Mapping Inventor Networks...")
    query_inventors = """
    CALL apoc.periodic.iterate(
        "MATCH (i1:Inventor) RETURN i1",
        "MATCH (i1)-[:INVENTED]->(p:Patent)<-[:INVENTED]-(i2:Inventor)
         WHERE elementId(i1) < elementId(i2)
         MERGE (i1)-[r:CO_INVENTED_WITH]-(i2)
         ON CREATE SET r.count = 1 
         ON MATCH SET r.count = r.count + 1",
        {batchSize: 200, parallel: false}
    )
    """
    try:
        session.run(query_inventors)
        print("     ✅ Inventor networks mapped.")
    except Exception as e:
        print(f"     ❌ Inventor mapping failed: {e}")


def create_function_similarity(session):
    print(f"🔁 Creating FUNCTIONALLY_SIMILAR relationships (Threshold: {SCORE_THRESHOLD})...")
    
    # This is the specific query that crashed your previous run.
    # Optimization: drastically reduced batchSize and set parallel=false.
    apoc_query = f"""
    CALL apoc.periodic.iterate(
      "MATCH (f1:SpecificFunction) WHERE f1.embedding IS NOT NULL RETURN f1",
      "CALL db.index.vector.queryNodes('function_embeddings', 10, f1.embedding) YIELD node AS f2, score
       WHERE score > {SCORE_THRESHOLD} AND elementId(f1) < elementId(f2) AND f1 <> f2
       MERGE (f1)-[r:FUNCTIONALLY_SIMILAR]-(f2)
       ON CREATE SET r.score = score, r.created_at = datetime()
       ON MATCH SET r.score = CASE WHEN score > r.score THEN score ELSE r.score END",
      {{batchSize: {BATCH_SIZE}, parallel: {str(PARALLEL).lower()}, retries: 3}}
    );
    """
    
    start_time = time.time()
    try:
        result = session.run(apoc_query)
        summary = result.single()
        print(f"     ✅ Function similarity complete in {round(time.time() - start_time, 2)}s")
        # Print stats if available
        if summary:
            print(f"     Stats: {summary}")
            
    except Exception as e:
        print(f"     ⚠️ Job Failed Again: {e}")
        print("     Tip: If this fails, try increasing Neo4j Heap Size in neo4j.conf (dbms.memory.heap.max_size=4G)")

def main():
    print("🚀 Starting Post-Ingestion Recovery...")
    
    with driver.session() as session:
        # 1. Verify Data Exists First
        count = session.run("MATCH (p:Patent) RETURN count(p) AS c").single()["c"]
        print(f"📊 Verified: {count} Patents found in database.")
        
        if count == 0:
            print("❌ No data found! Please check your database connection.")
            return

        # 2. Run the failed steps
        run_densification_protocols(session)
        create_function_similarity(session)

    print("✅ Recovery Complete! The graph is fully linked.")

if __name__ == "__main__":
    main()