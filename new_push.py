import pandas as pd
from neo4j import GraphDatabase
import spacy
import requests
import json
import os
import time
from tqdm import tqdm # For a nice progress bar

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
# --- Neo4j Connection ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  # <-- IMPORTANT: CHANGE THIS

# --- Ollama LLM Connection ---
OLLAMA_API_URL = "http://localhost:11434/api/generate"
LLM_MODEL = "mistral:latest"
ENABLE_LLM_LAYER = True  # Set to False to skip the slowest layer for a quick run

# --- Data Source ---
CSV_FILE_PATH = "mongo_random_sample.csv"  # <-- IMPORTANT: SET YOUR CSV FILE PATH

# --- Delimiter Configuration for list-like columns ---
# Check your CSV and set these to the correct characters!
DELIMITERS = {
    "inventors": ';',
    "keywords": ',',
    "cpc_classifications": ',',
    "references": ';',
    "use_case_examples": ','
}

# ==============================================================================
# --- SETUP AND HELPER FUNCTIONS ---
# ==============================================================================
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("spaCy model 'en_core_web_sm' not found. Please run 'python -m spacy download en_core_web_sm'")
    exit()

def get_driver():
    """Establishes a connection to the Neo4j database."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def check_services(driver):
    """Checks if Neo4j and Ollama services are running before starting."""
    # Check Neo4j
    try:
        driver.verify_connectivity()
        print("✅ Neo4j connection successful.")
    except Exception as e:
        print(f"❌ CRITICAL: Could not connect to Neo4j. Please check your credentials and database status.\nError: {e}")
        return False
        
    # Check Ollama
    if not ENABLE_LLM_LAYER:
        print("ℹ️ LLM Layer is disabled by configuration.")
        return True
    
    print("Checking connection to Ollama LLM service...")
    try:
        requests.post(OLLAMA_API_URL, json={"model": LLM_MODEL}, timeout=5)
        print("✅ Ollama connection successful.")
        return True
    except requests.RequestException:
        print(f"❌ CRITICAL: Could not connect to Ollama service at {OLLAMA_API_URL}")
        print("   Please ensure Ollama is running and the '{LLM_MODEL}' model is available (`ollama run {LLM_MODEL}`).")
        return False

def setup_database_constraints(driver):
    """Creates unique constraints in Neo4j for performance and data integrity."""
    print("Setting up database constraints...")
    with driver.session(database="neo4j") as session:
        constraints = [
            "CREATE CONSTRAINT patent_id_unique IF NOT EXISTS FOR (p:Patent) REQUIRE p.patent_id IS UNIQUE;",
            "CREATE CONSTRAINT inventor_name_unique IF NOT EXISTS FOR (i:Inventor) REQUIRE i.name IS UNIQUE;",
            "CREATE CONSTRAINT assignee_name_unique IF NOT EXISTS FOR (a:Assignee) REQUIRE a.name IS UNIQUE;",
            "CREATE CONSTRAINT entity_name_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS UNIQUE;",
            "CREATE CONSTRAINT domain_name_unique IF NOT EXISTS FOR (d:Domain) REQUIRE d.name IS UNIQUE;",
            "CREATE CONSTRAINT subdomain_name_unique IF NOT EXISTS FOR (sd:SubDomain) REQUIRE sd.name IS UNIQUE;",
            "CREATE CONSTRAINT usecase_name_unique IF NOT EXISTS FOR (uc:UseCase) REQUIRE uc.name IS UNIQUE;",
            "CREATE CONSTRAINT keyword_term_unique IF NOT EXISTS FOR (k:Keyword) REQUIRE k.term IS UNIQUE;",
            "CREATE CONSTRAINT classification_code_unique IF NOT EXISTS FOR (c:Classification) REQUIRE c.code IS UNIQUE;"
        ]
        for constraint in constraints:
            session.run(constraint)
    print("✅ Constraints are in place.")

def parse_list_string(text, delimiter):
    """Safely parses a delimited string into a list of stripped strings."""
    if not text or not isinstance(text, str):
        return []
    return [item.strip() for item in text.split(delimiter) if item.strip()]

# ==============================================================================
# --- CORE INGESTION LOGIC ---
# ==============================================================================

def ingest_layer1_structured(tx, patent_data, delimiters):
    """Layer 1: Ingests nodes and relationships from structured columns."""
    query = """
    // 1. MERGE Patent node with core properties
    MERGE (p:Patent {patent_id: $patent_id})
    SET p.title = $title,
        p.publication_date = $publication_date,
        p.abstract = $abstract,
        p.ai_generated_abstract = $ai_generated_abstract

    // 2. MERGE and connect Inventors
    WITH p
    UNWIND $inventors AS inventor_name
    MERGE (i:Inventor {name: inventor_name})
    MERGE (i)-[:AUTHORED]->(p)

    // 3. MERGE and connect Assignee (if it exists)
    WITH p
    WHERE $assignee_org IS NOT NULL AND $assignee_org <> ''
    MERGE (a:Assignee {name: $assignee_org})
    MERGE (a)-[:HOLDS]->(p)

    // 4. MERGE and connect Domain/SubDomain hierarchy (if they exist)
    WITH p
    WHERE $sub_domain IS NOT NULL AND $sub_domain <> ''
    MERGE (sd:SubDomain {name: $sub_domain})
    MERGE (p)-[:BELONGS_TO]->(sd)
    WITH p, sd
    WHERE $domain IS NOT NULL AND $domain <> ''
    MERGE (d:Domain {name: $domain})
    MERGE (sd)-[:PART_OF]->(d)

    // 5. MERGE and connect Use Cases (if they exist)
    WITH p
    UNWIND $use_cases AS use_case_name
    MERGE (uc:UseCase {name: use_case_name})
    MERGE (p)-[:APPLIES_TO]->(uc)

    // 6. MERGE and connect Keywords
    WITH p
    UNWIND $keywords AS keyword_term
    MERGE (k:Keyword {term: keyword_term})
    MERGE (p)-[:HAS_KEYWORD]->(k)

    // 7. MERGE and connect CPC Classifications
    WITH p
    UNWIND $cpc_classifications AS cpc_code
    MERGE (c:Classification {code: cpc_code, type: 'CPC'})
    MERGE (p)-[:HAS_CLASSIFICATION]->(c)

    // 8. MERGE and connect Patent Citations (References)
    WITH p
    UNWIND $references AS cited_patent_id
    MERGE (cited_p:Patent {patent_id: cited_patent_id})
    MERGE (p)-[:CITES]->(cited_p)
    """
    tx.run(query,
           patent_id=patent_data.get("patent_id"),
           title=patent_data.get("title"),
           publication_date=patent_data.get("publication_date"),
           abstract=patent_data.get("abstract"),
           ai_generated_abstract=patent_data.get("ai_generated_abstract"),
           inventors=parse_list_string(patent_data.get("inventors"), delimiters["inventors"]),
           assignee_org=patent_data.get("assignee_org"),
           domain=patent_data.get("domain"),
           sub_domain=patent_data.get("sub_domain"),
           use_cases=parse_list_string(patent_data.get("use_case_examples"), delimiters["use_case_examples"]),
           keywords=parse_list_string(patent_data.get("keywords"), delimiters["keywords"]),
           cpc_classifications=parse_list_string(patent_data.get("cpc_classifications"), delimiters["cpc_classifications"]),
           references=parse_list_string(patent_data.get("references"), delimiters["references"])
    )

def ingest_layer2_spacy(tx, patent_id, texts_to_process):
    """Layer 2: Extracts named entities using spaCy from a list of texts."""
    query = """
    MATCH (p:Patent {patent_id: $patent_id})
    MERGE (e:Entity {name: $ent_text})
    ON CREATE SET e.type = $ent_label
    MERGE (p)-[:MENTIONS]->(e)
    """
    for text in texts_to_process:
        if text and isinstance(text, str):
            doc = nlp(text)
            for ent in doc.ents:
                if ent.label_ in ['ORG', 'PRODUCT', 'GPE', 'PERSON', 'EVENT', 'LAW']:
                    tx.run(query, patent_id=patent_id, ent_text=ent.text, ent_label=ent.label_)

def ingest_layer3_llm(tx, texts_to_process):
    """Layer 3: Extracts typed relationships using an LLM from a list of texts."""
    query = """
    MERGE (h:Entity {name: $head})
    MERGE (t:Entity {name: $tail})
    MERGE (h)-[r:RELATION {type: $relation}]->(t)
    """
    for text in texts_to_process:
        if not text or not isinstance(text, str) or len(text.split()) < 15:
            continue
        
        prompt = f"""
        You are an expert patent analyst. Your task is to extract meaningful relationships between entities from the following patent abstract. The entities can be organizations, technologies, products, or concepts. Provide the output ONLY in a JSON format as a list of dictionaries, where each dictionary has "head", "relation", and "tail". The relation should be a concise verb phrase in uppercase.
        Example Text: "A novel system for secure communication using quantum key distribution. The technology, developed by Quantum Corp, ensures data integrity."
        Example Output:
        [
          {{"head": "Quantum Corp", "relation": "DEVELOPED", "tail": "quantum key distribution"}},
          {{"head": "quantum key distribution", "relation": "USED_FOR", "tail": "secure communication"}}
        ]
        ---
        Now, analyze this text and provide ONLY the JSON output:
        "{text}"
        """
        try:
            response = requests.post(
                OLLAMA_API_URL,
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False, "format": "json"},
                timeout=180
            )
            response.raise_for_status()
            response_data = response.json()
            extracted_json = json.loads(response_data.get('response', '[]'))
            
            if isinstance(extracted_json, list):
                for rel in extracted_json:
                    if isinstance(rel, dict) and all(k in rel for k in ['head', 'relation', 'tail']):
                        tx.run(query, head=rel['head'], relation=rel['relation'], tail=rel['tail'])
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            print(f"\n   [L3 WARNING] LLM processing failed for a row: {e}")
            continue

def process_row_in_transaction(tx, patent_data, delimiters):
    """Orchestrator function to run all layers for a single row within one transaction."""
    # Layer 1
    ingest_layer1_structured(tx, patent_data, delimiters)
    
    # Prepare texts for layers 2 and 3
    texts_for_nlp = [
        patent_data.get("abstract", ""),
        patent_data.get("ai_generated_abstract", "")
    ]
    patent_id = patent_data.get("patent_id")

    # Layer 2
    ingest_layer2_spacy(tx, patent_id, texts_for_nlp)
    
    # Layer 3
    if ENABLE_LLM_LAYER:
        ingest_layer3_llm(tx, texts_for_nlp)

def print_post_ingestion_queries():
    """Prints powerful Cypher queries to be run after the main ingestion is complete."""
    print("\n" + "="*80)
    print("🚀 Post-Ingestion Step: Creating Inferred Relationships")
    print("="*80)
    print("\nRun the following Cypher queries in your Neo4j Browser to create deeper, inferred relationships:")
    
    works_for_query = """
    // This query infers a :WORKS_FOR relationship between an Inventor and an Assignee
    // if they are associated on 2 or more patents.
    MATCH (i:Inventor)-[:AUTHORED]->(p:Patent)<-[:HOLDS]-(a:Assignee)
    WITH i, a, COUNT(p) AS patentCount
    WHERE patentCount >= 2
    MERGE (i)-[r:WORKS_FOR]->(a)
    SET r.inferredFromPatentCount = patentCount;
    """
    
    influenced_by_query = """
    // This query infers an :INFLUENCED_BY relationship between inventors
    // based on patent citations.
    MATCH (p1:Patent)-[:CITES]->(p2:Patent)
    MATCH (i1:Inventor)-[:AUTHORED]->(p1)
    MATCH (i2:Inventor)-[:AUTHORED]->(p2)
    WHERE i1 <> i2
    MERGE (i1)-[r:INFLUENCED_BY]->(i2);
    """
    
    print("\n--- Infer :WORKS_FOR Relationships ---")
    print(works_for_query)
    print("\n--- Infer :INFLUENCED_BY Relationships (based on citations) ---")
    print(influenced_by_query)
    print("\n" + "="*80)


# ==============================================================================
# --- MAIN EXECUTION ---
# ==============================================================================
def main():
    """Main function to run the entire ingestion pipeline."""
    print("🚀 Starting Full Patent Knowledge Graph Ingestion Pipeline")
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CRITICAL: The data file '{CSV_FILE_PATH}' was not found.")
        return

    driver = get_driver()
    
    if not check_services(driver):
        driver.close()
        return
        
    setup_database_constraints(driver)
    
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        df = df.fillna('')  # CRITICAL: Replace all NaN/NaT with empty strings
        
        total_rows = len(df)
        print(f"\nFound {total_rows} patents to process from '{CSV_FILE_PATH}'.")

        with driver.session(database="neo4j") as session:
            # Using tqdm for a progress bar
            for index, row in tqdm(df.iterrows(), total=total_rows, desc="Ingesting Patents"):
                patent_dict = row.to_dict()
                if not patent_dict.get("patent_id"):
                    print(f"\n[WARNING] Skipping row {index+1} due to missing 'patent_id'.")
                    continue
                
                # Execute all three layers in a single transaction for atomicity
                session.execute_write(process_row_in_transaction, patent_dict, DELIMITERS)

        print("\n✅ Main data ingestion complete.")
        print_post_ingestion_queries()

    except Exception as e:
        print(f"\n❌ An unexpected error occurred during the main process: {e}")
    finally:
        print("\n--- Pipeline Finished ---")
        driver.close()

if __name__ == "__main__":
    main()