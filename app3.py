import query_embedding
from neo4j import GraphDatabase
from collections import Counter
import numpy as np

# === CONFIGURATION ===
NEO4J_URI = "neo4j+s://5c46741d.databases.neo4j.io"  # Replace if needed
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1NIHHaxYLJLRkfa8-Lw48haKxVZDUIaHMTo2Yt5wdvI"  # Replace with your Neo4j password
VECTOR_INDEX_NAME = "document-embedding-index"  # Ensure this exists
K_NEIGHBORS = 100

# === GET USER QUERY ===
query = input("Enter query: ")

# === EMBED THE QUERY ===
embedded_query = query_embedding.embed(query)
print("🔎 Embedded query:", embedded_query[:5], "...")  # Preview

# === CONNECT TO NEO4J AND FETCH MATCHES ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    # Query vector index
    result = session.run(f"""
    CALL db.index.vector.queryNodes($indexName, $k, $embedding)
    YIELD node, score
    MATCH (node)-[:HAS_DOMAIN]->(d:Domain)
    RETURN node.title AS title, node.abstract AS abstract, d.name AS domain, node.embedding AS embedding, score
    """, {
        "indexName": VECTOR_INDEX_NAME,
        "k": K_NEIGHBORS,
        "embedding": embedded_query
    })

    records = result.data()

# === IDENTIFY MAIN DOMAIN ===
domains = [r["domain"] for r in records if r["domain"]]
main_domain = Counter(domains).most_common(1)[0][0]
print(f"\n📘 Detected main domain from query context: {main_domain}")

# === WEIGHTED SORT: PRIORITIZE OTHER DOMAINS ===
weighted_matches = []
for r in records:
    original_score = r["score"]
    boost_factor = 1.2 if r["domain"] != main_domain else 1.0
    weighted_matches.append({
        "title": r["title"],
        "abstract": r["abstract"],
        "domain": r["domain"],
        "original_score": original_score,
        "adjusted_score": original_score * boost_factor
    })

# Sort by adjusted score
weighted_matches.sort(key=lambda x: x["adjusted_score"], reverse=True)

# === DISPLAY TOP 10 MATCHES ===
print("\n🔍 Top Matching Patents (prioritized by cross-domain relevance):")
for i, m in enumerate(weighted_matches[:10]):
    print(f"\n#{i+1} — Score: {m['adjusted_score']:.4f} ({'Other domain' if m['domain'] != main_domain else 'Same domain'})")
    print(f"Title   : {m['title']}")
    print(f"Abstract: {m['abstract']}")
    print(f"Domain  : {m['domain']}")

# === OPTIONAL: Generate prompt for LLM ===
prompt = f"""
User Problem: {query}

Top relevant patents from Neo4j (cross-domain prioritized):

{chr(10).join([
    f"Title: {m['title']}\nDomain: {m['domain']}\nScore: {m['adjusted_score']:.4f}\nAbstract: {m['abstract']}"
    for m in weighted_matches[:10]
])}

Questions:
1. What ideas from other domains can be applied to solve the user's problem?
2. Are there any novel or surprising insights revealed by these cross-domain matches?
"""

# You can send `prompt` to an LLM or save it
