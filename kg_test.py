from pymongo import MongoClient
from neo4j import GraphDatabase

# === CONFIGURATION ===
MONGO_URI = "mongodb+srv://data:July2025@cluster0.peeh3.mongodb.net/"
MONGO_DB = "KG"
MONGO_COLLECTION = "cdkg"

NEO4J_URI = "neo4j+s://5c46741d.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1NIHHaxYLJLRkfa8-Lw48haKxVZDUIaHMTo2Yt5wdvI"  # 🔐 Replace with your Neo4j password

# === CONNECT TO DATABASES ===
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COLLECTION]

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# === FUNCTION TO CREATE NODES & RELATIONSHIPS ===
def insert_patent(tx, doc):
    patent_id = str(doc.get("patent_id"))
    title = doc.get("title", "")
    abstract = doc.get("abstract", "")
    pub_date = doc.get("publication_date", "")
    inventors = [i.strip() for i in doc.get("inventors", "").split(",") if i.strip()]
    classifications = [c.strip() for c in doc.get("cpc_classifications", "").split(",") if c.strip()]
    assignee = doc.get("assignee_org", "")
    tech = doc.get("technology_stack", "")
    domain = doc.get("domain", "")
    subdomain = doc.get("sub_domain", "")
    
    embedding = doc.get("embedding", [])  # <-- ADD THIS LINE
    # Ensure it’s a list of floats
    if isinstance(embedding, list):
        embedding = [float(x) for x in embedding if isinstance(x, (float, int))]

    tx.run("""
        MERGE (p:Patent {id: $patent_id})
        SET p.title = $title,
            p.abstract = $abstract,
            p.pub_date = $pub_date,
            p.embedding = $embedding  // <-- SET embedding here

        FOREACH (name IN $inventors |
            MERGE (i:Inventor {name: name})
            MERGE (p)-[:INVENTED_BY]->(i)
        )

        FOREACH (code IN $classifications |
            MERGE (c:Classification {code: code})
            MERGE (p)-[:CLASSIFIED_AS]->(c)
        )

        WITH p
        WHERE $assignee <> ""
        MERGE (a:Assignee {name: $assignee})
        MERGE (p)-[:ASSIGNED_TO]->(a)

        WITH p
        WHERE $tech <> ""
        MERGE (t:Technology {name: $tech})
        MERGE (p)-[:USES_TECHNOLOGY]->(t)

        WITH p
        WHERE $domain <> ""
        MERGE (d:Domain {name: $domain})
        MERGE (p)-[:HAS_DOMAIN]->(d)

        WITH p
        WHERE $subdomain <> ""
        MERGE (s:SubDomain {name: $subdomain})
        MERGE (p)-[:HAS_SUBDOMAIN]->(s)
    """, {
        "patent_id": patent_id,
        "title": title,
        "abstract": abstract,
        "pub_date": pub_date,
        "inventors": inventors,
        "classifications": classifications,
        "assignee": assignee,
        "tech": tech,
        "domain": domain,
        "subdomain": subdomain,
        "embedding": embedding  # <-- PASS EMBEDDING HERE
    })



# === RUN THE MIGRATION ===
with neo4j_driver.session() as session:
    count = 0
    for record in mongo_collection.find():
        try:
            session.execute_write(insert_patent, record)
            count += 1
            print(f"✅ Imported record #{count} — Patent ID: {record.get('patent_id')}")
        except Exception as e:
            print(f"❌ Failed to import Patent ID: {record.get('patent_id')}\nError: {e}")

print(f"\n🎉 Finished importing {count} records.")
