from pymongo import MongoClient
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()
 # 🔐 Replace with your Neo4j password
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

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
    keywords=[k.strip() for k in doc.get("keywords","").split(",") if k.strip()]

    tx.run("""
        MERGE (p:Patent {id: $patent_id})
        SET p.title = $title, p.abstract = $abstract, p.pub_date = $pub_date

        FOREACH (name IN $inventors |
            MERGE (i:Inventor {name: name})
            MERGE (p)-[:INVENTED_BY]->(i)
        )
        FOREACH (kw IN $keywords |
            MERGE (k:Keyword {name: kw})
            MERGE (p)-[:HAS_KEYWORD]->(k)
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
        "keywords":keywords
    })


# === RUN THE MIGRATION ===
with neo4j_driver.session() as session:
    count = 0
    for record in MONGO_COLLECTION.find():
        try:
            session.execute_write(insert_patent, record)
            count += 1
            print(f"✅ Imported record #{count} — Patent ID: {record.get('patent_id')}")
        except Exception as e:
            print(f"❌ Failed to import Patent ID: {record.get('patent_id')}\nError: {e}")

print(f"\n🎉 Finished importing {count} records.")
