import os
import csv
from neo4j import GraphDatabase
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm

# Set a high limit for CSV field size in case of very large text fields
# This is kept in case you ever need to revert to a CSV source.
csv.field_size_limit(1_000_000_000)

# === CONFIGURATION ===
# It's best practice to use environment variables for credentials
# Neo4j Credentials
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://5c46741d.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "1NIHHaxYLJLRkfa8-Lw48haKxVZDUIaHMTo2Yt5wdvI")

# MongoDB Credentials
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net")
MONGO_DB = os.getenv("MONGO_DB", "KG")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "cdkg")


# === CONNECT TO DATABASES ===
# Use try-except blocks for robust connection handling
try:
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    neo4j_driver.verify_connectivity()
    print("✅ Successfully connected to Neo4j.")
except Exception as e:
    print(f"❌ Failed to connect to Neo4j: {e}")
    exit()

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Ping the server to check the connection
    mongo_client.admin.command('ping')
    print("✅ Successfully connected to MongoDB.")
except Exception as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    exit()


# === FUNCTION TO CREATE NODES & RELATIONSHIPS (REVISED FOR DETAILED SCHEMA) ===
def insert_document(tx, doc):
    # Use .get() with a fallback to handle both 'patent_id' and 'id' columns
    doc_id = doc.get("patent_id") or doc.get("id")

    # Skip record if no primary ID is found to prevent creating orphaned nodes
    if not doc_id:
        # This will be visible in the tqdm progress bar if it happens frequently
        return

    # --- Prepare Data from Document ---

    # Determine node labels based on available data.
    labels = ["Document"]
    if doc.get("patent_type"):
        labels.append("Patent")
    if doc.get("journal_name"):
        labels.append("Publication")

    # Helper function to safely split comma-separated strings
    def split_field(field_name):
        value = doc.get(field_name, "")
        # Ensure value is a string before splitting
        if isinstance(value, str):
            return [s.strip() for s in value.split(",") if s.strip()]
        return []

    # Prepare lists from comma-separated strings
    inventors = split_field("inventors")
    authors = split_field("authors")
    assignees = split_field("assignee_names")
    references = split_field("references")
    cited_by_list = split_field("cited_by")
    cpc_class = split_field("cpc_classifications")
    ipc_class = split_field("ipc_classifications")
    tech_stack = split_field("technology_stack")
    keywords = split_field("keywords")
    market_trends = split_field("market_trends")
    customer_behaviors = split_field("customer_behavior")
    competitors = split_field("competitor_data")

    # Parse embedding fields (which might be lists already in MongoDB)
    embedding = doc.get("embedding", [])
    ai_embedding = doc.get("ai_embeddings", [])

    # Safely parse the publication date for the time tree
    pub_date_str = doc.get("publication_date", "")
    year, month, day = None, None, None
    if pub_date_str:
        try:
            # Attempt to parse a standard date format
            dt = datetime.strptime(pub_date_str, '%Y-%m-%d')
            year, month, day = dt.year, dt.month, dt.day
        except (ValueError, TypeError):
            # Silently skip date parsing errors to not clutter the progress bar
            pass

    # Create a dictionary of properties for the main Document node
    doc_properties = {
        "title": doc.get("title", ""), "abstract": doc.get("abstract", ""),
        "ai_generated_abstract": doc.get("ai_generated_abstract", ""),
        "summary": doc.get("summary", ""), "full_text": doc.get("full_text", ""),
        "pub_date": pub_date_str, "updated_at": doc.get("updated_at", ""),
        "source_date": doc.get("source_date", ""), "patent_type": doc.get("patent_type", ""),
        "country_code": doc.get("country", ""), "pdf_link": doc.get("pdf_link", ""),
        "source_url": doc.get("source_url", ""), "local_url": doc.get("local_url", ""),
        "doi": doc.get("doi", ""), "doi_url": doc.get("doi_url", ""), "publisher": doc.get("publisher", ""),
        "embedding": embedding, "ai_embedding": ai_embedding,
        "num_claims": int(doc.get("num_claims") or 0),
        "relevance_score": float(doc.get("relevance_score") or 0.0),
        "data_quality_score": float(doc.get("data_quality_score") or 0.0),
        "foreign_citation_count": int(doc.get("foreign_citation_count") or 0),
        "local_citation_count": int(doc.get("local_citation_count") or 0)
    }

    # --- The main Cypher query, now updated for a highly relational model ---
    # NOTE: This query uses APOC library for dynamic labels. Ensure APOC is installed in your Neo4j instance.
    query = """
    MERGE (p:Document {id: $doc_id})
    SET p += $props
    WITH p
    CALL apoc.create.addLabels(p, $labels) YIELD node
    WITH node as p

    // People and Organizations
    FOREACH (name IN $inventors | MERGE (i:Inventor {name: name}) MERGE (p)-[:INVENTED_BY]->(i)
        FOREACH (_ IN CASE WHEN $assignee_org <> "" THEN [1] ELSE [] END |
            MERGE (org:Organization {name: $assignee_org}) MERGE (i)-[:AFFILIATED_WITH]->(org)))
    FOREACH (name IN $authors | MERGE (a:Author {name: name}) MERGE (p)-[:AUTHORED_BY]->(a)
        FOREACH (_ IN CASE WHEN $assignee_org <> "" THEN [1] ELSE [] END |
            MERGE (org:Organization {name: $assignee_org}) MERGE (a)-[:AFFILIATED_WITH]->(org)))
    FOREACH (name IN $assignees |
        MERGE (assignee:Assignee {name: name}) MERGE (p)-[:ASSIGNED_TO]->(assignee)
        FOREACH (_ IN CASE WHEN $assignee_org <> "" THEN [1] ELSE [] END |
            MERGE (org:Organization {name: $assignee_org})
            MERGE (assignee)-[:WORKS_FOR]->(org)
            MERGE (p)-[:ASSIGNED_TO_ORG]->(org)))

    // Citation Network
    FOREACH (cited_id IN $references | MERGE (cited:Document {id: cited_id}) MERGE (p)-[:CITES]->(cited))
    FOREACH (citing_id IN $cited_by | MERGE (citing:Document {id: citing_id}) MERGE (citing)-[:CITES]->(p))

    // Hierarchical CPC Classification
    FOREACH (code IN $cpc_class |
        MERGE (cpc_group:CPC_Group {code: code, title: $cpc_group_title}) MERGE (p)-[:HAS_CLASSIFICATION]->(cpc_group)
        FOREACH (_ IN CASE WHEN $cpc_subclass_title <> "" THEN [1] ELSE [] END |
            MERGE (cpc_subclass:CPC_Subclass {title: $cpc_subclass_title}) MERGE (cpc_group)-[:PART_OF]->(cpc_subclass)
            FOREACH (_ IN CASE WHEN $cpc_class_title <> "" THEN [1] ELSE [] END |
                MERGE (cpc_class_node:CPC_Class {title: $cpc_class_title}) MERGE (cpc_subclass)-[:PART_OF]->(cpc_class_node))))
    
    // IPC Classification
    FOREACH (code IN $ipc_class | MERGE (i:IPC {code: code}) MERGE (p)-[:HAS_CLASSIFICATION]->(i))

    // Publication Entities
    FOREACH (_ IN CASE WHEN $publisher <> "" THEN [1] ELSE [] END | MERGE (pub:Publisher {name: $publisher}) MERGE (p)-[:PUBLISHED_BY]->(pub))
    FOREACH (_ IN CASE WHEN $journal_name <> "" THEN [1] ELSE [] END |
        MERGE (jnl:Journal {name: $journal_name})
        SET jnl.volume = $journal_volume, jnl.issue = $journal_issue, jnl.pages = $journal_pages
        MERGE (p)-[:APPEARS_IN]->(jnl))

    // Concepts and Business Intelligence
    FOREACH (tech IN $tech_stack | MERGE (t:Technology {name: tech}) MERGE (p)-[:USES_TECH]->(t))
    FOREACH (kw IN $keywords | MERGE (k:Keyword {word: kw}) MERGE (p)-[:HAS_KEYWORD]->(k))
    FOREACH (trend IN $market_trends | MERGE (mt:MarketTrend {trend: trend}) MERGE (p)-[:RELATES_TO_TREND]->(mt))
    FOREACH (behavior IN $customer_behaviors | MERGE (cb:CustomerBehavior {behavior: behavior}) MERGE (p)-[:INDICATES_BEHAVIOR]->(cb))
    FOREACH (competitor IN $competitors | MERGE (comp:Competitor {name: competitor}) MERGE (p)-[:MENTIONS_COMPETITOR]->(comp))

    // Categorical Data & Hierarchies
    FOREACH (_ IN CASE WHEN $domain <> "" THEN [1] ELSE [] END |
        MERGE (d:Domain {name: $domain}) MERGE (p)-[:HAS_DOMAIN]->(d)
        FOREACH (_ IN CASE WHEN $sub_domain <> "" THEN [1] ELSE [] END |
            MERGE (s:SubDomain {name: $sub_domain}) MERGE (p)-[:HAS_SUBDOMAIN]->(s) MERGE (s)-[:SUBDOMAIN_OF]->(d)))
    FOREACH (_ IN CASE WHEN $knowledge_type <> "" THEN [1] ELSE [] END | MERGE (kt:KnowledgeType {type: $knowledge_type}) MERGE (p)-[:HAS_KNOWLEDGE_TYPE]->(kt))
    FOREACH (_ IN CASE WHEN $country <> "" THEN [1] ELSE [] END | MERGE (c:Country {code: $country}) MERGE (p)-[:FILED_IN]->(c))
    FOREACH (_ IN CASE WHEN $wipo_kind <> "" THEN [1] ELSE [] END | MERGE (w:WipoKind {kind: $wipo_kind}) MERGE (p)-[:HAS_KIND]->(w))
    FOREACH (_ IN CASE WHEN $use_case <> "" THEN [1] ELSE [] END | MERGE (u:UseCase {description: $use_case}) MERGE (p)-[:HAS_USE_CASE]->(u))
    
    // Time Tree for Publication Date
    FOREACH (_ IN CASE WHEN $year IS NOT NULL THEN [1] ELSE [] END |
        MERGE (y:Year {year: $year})
        MERGE (m:Month {month: $month, year: $year})
        MERGE (d:Day {day: $day, month: $month, year: $year})
        MERGE (p)-[:PUBLISHED_ON]->(d)
        MERGE (d)-[:IN_MONTH]->(m)
        MERGE (m)-[:IN_YEAR]->(y))
    """
    
    # Pass all the prepared data as parameters to the query
    tx.run(query, {
        "doc_id": doc_id, "props": doc_properties, "labels": labels,
        "inventors": inventors, "authors": authors, "assignees": assignees,
        "assignee_org": doc.get("assignee_org", ""),
        "references": references, "cited_by": cited_by_list,
        "cpc_class": cpc_class, "ipc_class": ipc_class,
        "cpc_class_title": doc.get("cpc_class_title", ""),
        "cpc_subclass_title": doc.get("cpc_subclass_title", ""),
        "cpc_group_title": doc.get("cpc_group_title", ""),
        "tech_stack": tech_stack, "keywords": keywords,
        "domain": doc.get("domain", ""), "sub_domain": doc.get("sub_domain", ""),
        "country": doc.get("country", ""), "wipo_kind": doc.get("wipo_kind", ""),
        "knowledge_type": doc.get("knowledge_type", ""),
        "use_case": doc.get("use_case_examples", ""),
        "publisher": doc.get("publisher", ""), "journal_name": doc.get("journal_name", ""),
        "journal_volume": doc.get("journal_volume", ""), "journal_issue": doc.get("journal_issue", ""),
        "journal_pages": doc.get("journal_pages", ""),
        "market_trends": market_trends, "customer_behaviors": customer_behaviors,
        "competitors": competitors,
        "year": year, "month": month, "day": day
    })

# === RUN THE MIGRATION FROM MONGODB ===
def run_migration():
    try:
        # Fetch all documents at once to get a total for tqdm
        documents = list(collection.find({}))
        total_docs = len(documents)
        print(f"🔍 Found {total_docs} documents to migrate from MongoDB.")

        if not documents:
            print("No documents found. Exiting.")
            return

        with neo4j_driver.session(database="neo4j") as session:
            print("🚀 Starting data migration...")
            # Wrap the documents list with tqdm for a progress bar
            for record in tqdm(documents, desc="Migrating Documents"):
                try:
                    session.execute_write(insert_document, record)
                except Exception as e:
                    doc_id_for_error = record.get("patent_id") or record.get("id", "N/A")
                    # Use tqdm.write to print without disturbing the progress bar
                    tqdm.write(f"❌ Failed to import record with ID: {doc_id_for_error}\n   Error: {e}")

        print(f"\n🎉 Finished importing {total_docs} records.")
    except Exception as e:
        print(f"❌ An error occurred during migration: {e}")
    finally:
        neo4j_driver.close()
        mongo_client.close()
        print("✅ Database connections closed.")

if __name__ == "__main__":
    run_migration()
