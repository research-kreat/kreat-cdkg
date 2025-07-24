
import query_embedding
from neo4j import GraphDatabase
import numpy as np

# === CONFIGURATION ===
NEO4J_URI = "neo4j+s://5c46741d.databases.neo4j.io"  # Change if needed
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1NIHHaxYLJLRkfa8-Lw48haKxVZDUIaHMTo2Yt5wdvI"  # 🔐 Replace with your Neo4j password

# === QUERY VECTOR: AUTOMOBILE SAFETY DEVICES ===


print("Enter query: ")
query = input()

embedded_query=query_embedding.embed(query)
print(embedded_query)


# === COSINE SIMILARITY FUNCTION ===
def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

# === CONNECT TO NEO4J AND FETCH PATENTS ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    result = session.run("""
        MATCH (p:Patent)-[:HAS_DOMAIN]->(d:Domain)
WHERE p.embedding IS NOT NULL
RETURN p.title AS title, p.abstract AS abstract, p.embedding AS embedding, d.name AS Domain

    """)

    matches = []
    for record in result:
        embedding = record["embedding"]
        if embedding and len(embedding) == len(embedded_query):
            score = cosine_similarity(embedded_query, embedding)
            if score > 0.4:  # Adjust similarity threshold as needed
                matches.append((record["title"], record["abstract"], score,record["Domain"]))

    # Sort and display top 10 results
    matches.sort(key=lambda x: x[2], reverse=True)

    print("\n🔎 Top Matching Patents:")
    for i, (title, abstract, score,domain) in enumerate(matches[:10]):
        print(f"\n#{i+1} — Score: {score:.4f}")
        print(f"Title   : {title}")
        print(f"Abstract: {abstract}")
        print(f"Domain: {domain}")



prompt=f"""
User Problem: {query}

Here are 10 relevant patents based on semantic similarity:

{chr(10).join([
    f"Title: {m['title']}\nScore: {m['score']:.4f}\nAI Abstract: {m['ai_generated_abstract']}\nUse Case: {m['use_case_examples']}" for m in matches
])}

Can you explain how these relate to the user’s problem?
What are the solutions, gaps, or novel ideas?
"""

