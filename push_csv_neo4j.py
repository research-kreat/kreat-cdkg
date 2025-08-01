import csv
from neo4j import GraphDatabase
csv.field_size_limit(1_000_000_000)
# === CONFIGURATION ===
CSV_FILE_PATH = "final_aerospace.csv"  # 🔁 Replace with your actual CSV file path

NEO4J_URI = "neo4j+s://5c46741d.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1NIHHaxYLJLRkfa8-Lw48haKxVZDUIaHMTo2Yt5wdvI"  # 🔐 Replace with your Neo4j password

# === CONNECT TO NEO4J ===
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# === FUNCTION TO CREATE NODES & RELATIONSHIPS ===
def insert_patent(tx, doc):
    patent_id = str(doc.get("patent_id"))
    title = doc.get("title", "")
    abstract = doc.get("abstract", "")
    full_text = doc.get("full_text", "")
    pub_date = doc.get("publication_date", "")
    num_claims = int(doc.get("num_claims", 0) or 0)
    patent_type = doc.get("patent_type", "")
    domain = doc.get("domain", "")
    subdomain = doc.get("sub_domain", "")
    knowledge_type = doc.get("knowledge_type", "")
    wipo_kind = doc.get("wipo_kind", "")
    use_case = doc.get("use_case_examples", "").strip()

    inventors = [i.strip() for i in doc.get("inventors", "").split(",") if i.strip()]
    cpc_classifications = [c.strip() for c in doc.get("cpc_classifications", "").split(",") if c.strip()]
    ipc_classifications = [i.strip() for i in doc.get("ipc_classifications", "").split(",") if i.strip()]
    tech_stack = [t.strip() for t in doc.get("technology_stack", "").split(",") if t.strip()]
    keywords = [k.strip() for k in doc.get("keywords", "").split(",") if k.strip()]

    embedding_str = doc.get("embedding", "")
    embedding = [float(x) for x in embedding_str.strip("[]").split(",") if x.strip()] if embedding_str else []

    tx.run("""
        MERGE (p:Patent {id: $patent_id})
        SET 
            p.title = $title,
            p.abstract = $abstract,
            p.full_text = $full_text,
            p.pub_date = $pub_date,
            p.num_claims = $num_claims,
            p.patent_type = $patent_type,
            p.embedding = $embedding

        FOREACH (name IN $inventors |
            MERGE (i:Inventor {name: name})
            MERGE (p)-[:INVENTED_BY]->(i)
        )

        FOREACH (code IN $cpc_classifications |
            MERGE (c:CPCClassification {code: code})
            MERGE (p)-[:HAS_CPC]->(c)
        )

        FOREACH (code IN $ipc_classifications |
            MERGE (i:IPCClassification {code: code})
            MERGE (p)-[:HAS_IPC]->(i)
        )

        FOREACH (tech IN $tech_stack |
            MERGE (t:Technology {name: tech})
            MERGE (p)-[:USES_TECH]->(t)
        )

        FOREACH (_ IN CASE WHEN $domain <> "" THEN [1] ELSE [] END |
            MERGE (d:Domain {name: $domain})
            MERGE (p)-[:HAS_DOMAIN]->(d)
        )

        FOREACH (_ IN CASE WHEN $subdomain <> "" THEN [1] ELSE [] END |
            MERGE (s:SubDomain {name: $subdomain})
            MERGE (p)-[:HAS_SUBDOMAIN]->(s)
        )

        FOREACH (_ IN CASE WHEN $knowledge_type <> "" THEN [1] ELSE [] END |
            MERGE (kt:KnowledgeType {name: $knowledge_type})
            MERGE (p)-[:HAS_TYPE]->(kt)
        )

        FOREACH (kw IN $keywords |
            MERGE (k:Keyword {word: kw})
            MERGE (p)-[:HAS_KEYWORD]->(k)
        )

        FOREACH (_ IN CASE WHEN $use_case <> "" THEN [1] ELSE [] END |
            MERGE (u:UseCase {description: $use_case})
            MERGE (p)-[:HAS_USE_CASE]->(u)
        )

        FOREACH (_ IN CASE WHEN $wipo_kind <> "" THEN [1] ELSE [] END |
            MERGE (w:PatentKind {type: $wipo_kind})
            MERGE (p)-[:HAS_KIND]->(w)
        )
    """, {
        "patent_id": patent_id,
        "title": title,
        "abstract": abstract,
        "full_text": full_text,
        "pub_date": pub_date,
        "num_claims": num_claims,
        "patent_type": patent_type,
        "embedding": embedding,
        "inventors": inventors,
        "cpc_classifications": cpc_classifications,
        "ipc_classifications": ipc_classifications,
        "tech_stack": tech_stack,
        "domain": domain,
        "subdomain": subdomain,
        "knowledge_type": knowledge_type,
        "keywords": keywords,
        "use_case": use_case,
        "wipo_kind": wipo_kind
    })


# === RUN THE MIGRATION FROM CSV ===
with open(CSV_FILE_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    with neo4j_driver.session() as session:
        count = 0
        for record in reader:
            try:
                session.execute_write(insert_patent, record)
                count += 1
                print(f"✅ Imported record #{count} — Patent ID: {record.get('patent_id')}")
            except Exception as e:
                print(f"❌ Failed to import Patent ID: {record.get('patent_id')}\nError: {e}")

print(f"\n🎉 Finished importing {count} records.")
