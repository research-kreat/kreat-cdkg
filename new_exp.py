#
# REQUIREMENTS: pip install neo4j pandas numpy scikit-learn tqdm
#

from neo4j import GraphDatabase
import pandas as pd
import logging
import re
import numpy as np
import ast
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ----------------------------
# CONFIG
# ----------------------------
NEO4J_URI = "bolt://135.235.170.159:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "cHRE3FjhhxvkGNH"
BATCH_SIZE = 1000

# ----------------------------
# CONNECT
# ----------------------------
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    logger.info("✅ Successfully connected to Neo4j.")
except Exception as e:
    logger.error(f"❌ Could not connect to Neo4j. Please check your URI and credentials. Error: {e}")
    exit()

# ----------------------------
# TEXT PROCESSING & PARSING (UNCHANGED)
# ----------------------------
def extract_technologies_from_text(text):
    if pd.isna(text): return []
    text = str(text).lower()
    tech_patterns = [r'\b(ai|artificial intelligence|machine learning|ml|deep learning|neural network|cnn|rnn|lstm|transformer)\b', r'\b(blockchain|cryptocurrency|bitcoin|ethereum|smart contract|dlt|distributed ledger)\b', r'\b(iot|internet of things|sensor|rfid|nfc|bluetooth|wifi|zigbee|lpwan)\b', r'\b(cloud computing|aws|azure|google cloud|gcp|saas|paas|iaas|serverless|kubernetes)\b', r'\b(quantum computing|quantum|qbit|qubit|quantum algorithm)\b', r'\b(computer vision|image processing|opencv|pattern recognition|object detection)\b', r'\b(nlp|natural language processing|text mining|sentiment analysis|ner|language model)\b', r'\b(robotics|robot|autonomous|drone|uav|cobot)\b', r'\b(5g|6g|4g|lte|telecommunications|wireless|cellular|mimo)\b', r'\b(ar|vr|augmented reality|virtual reality|mixed reality|xr|metaverse)\b', r'\b(cybersecurity|encryption|firewall|iam|zero trust)\b', r'\b(3d printing|additive manufacturing)\b']
    technologies = set()
    for pattern in tech_patterns: technologies.update(re.findall(pattern, text))
    return list(technologies)

def extract_domains_from_text(text):
    if pd.isna(text): return []
    text = str(text).lower()
    domain_patterns = [r'\b(healthcare|medical|hospital|clinical|diagnosis|treatment|pharmaceutical|biotech)\b', r'\b(automotive|vehicle|car|transportation|traffic|navigation|autonomous driving)\b', r'\b(manufacturing|factory|industrial|production|assembly|quality control|supply chain)\b', r'\b(finance|banking|financial|trading|investment|payment|fintech|insurtech)\b', r'\b(agriculture|farming|crop|livestock|precision agriculture|agritech)\b', r'\b(energy|power|electricity|renewable|solar|wind|battery|smart grid)\b', r'\b(telecommunications|telecom|network|communication)\b', r'\b(retail|e-commerce|shopping|consumer|customer|marketplace)\b', r'\b(education|learning|training|university|school|edtech|e-learning)\b', r'\b(aerospace|aviation|satellite|space)\b']
    domains = set()
    for pattern in domain_patterns: domains.update(re.findall(pattern, text))
    return list(domains)

def extract_problems_from_text(text):
    if pd.isna(text): return []
    text = str(text).lower()
    problem_patterns = [r'\b(inefficiency|inefficient|slow|delay|bottleneck|latency)\b', r'\b(error|mistake|inaccuracy|unreliable|failure|fault)\b', r'\b(expensive|costly|high cost|resource intensive)\b', r'\b(complex|complexity|difficult|challenging)\b', r'\b(security risk|vulnerability|threat|breach|fraud)\b', r'\b(scalability|scale|limitation|constraint)\b', r'\b(manual|labor intensive|time consuming)\b', r'\b(outdated|legacy|obsolete|old)\b', r'\b(waste|pollution|emission|sustainability)\b']
    problems = set()
    for pattern in problem_patterns: problems.update(re.findall(pattern, text))
    return list(problems)

def parse_list_field(field, separators=[';', ',', '|']):
    if pd.isna(field) or field == '': return []
    field_str = str(field).strip()
    if not field_str: return []
    for sep in separators:
        if sep in field_str: return [x.strip() for x in field_str.split(sep) if x.strip()]
    return [field_str]

def parse_embedding(embedding_str):
    if pd.isna(embedding_str) or embedding_str == '': return None
    try:
        s = str(embedding_str).strip()
        if s.startswith('Array') or s.startswith('array'): return None
        if s.startswith('[') and s.endswith(']'): return np.array(ast.literal_eval(s), dtype=np.float32)
        if ',' in s: return np.array([float(v.strip()) for v in s.split(',')], dtype=np.float32)
        return None
    except (ValueError, SyntaxError):
        return None

def calculate_embedding_similarity(embeddings_list, similarity_threshold=0.75):
    if len(embeddings_list) < 2: return []
    valid_embeddings, valid_indices = [], []
    for i, (patent_id, embedding) in enumerate(embeddings_list):
        if embedding is not None and embedding.ndim == 1 and embedding.size > 0:
            valid_embeddings.append(embedding)
            valid_indices.append(patent_id)
    if len(valid_embeddings) < 2: return []
    try:
        embeddings_matrix = np.vstack(valid_embeddings)
        similarity_matrix = cosine_similarity(embeddings_matrix)
        similar_pairs = []
        n = len(valid_embeddings)
        for i in tqdm(range(n), desc="🔍 Calculating embedding similarities"):
            for j in range(i + 1, n):
                if similarity_matrix[i, j] >= similarity_threshold:
                    similar_pairs.append({'patent_1': valid_indices[i], 'patent_2': valid_indices[j], 'similarity': float(similarity_matrix[i, j])})
        logger.info(f"Found {len(similar_pairs)} embedding-similar patent pairs (threshold: {similarity_threshold}).")
        return similar_pairs
    except Exception as e:
        logger.error(f"Error calculating embedding similarity: {e}")
        return []

# ----------------------------
# GRAPH CREATION FUNCTIONS
# ----------------------------

def batch_create_patents(tx, patents_batch):
    """Processes a batch of patent data in a single, efficient transaction."""
    query = """
    UNWIND $patents as data
    MERGE (p:Patent {patent_id: data.patent_id}) 
    SET p.id = data.id, 
        p.title = data.title, 
        p.knowledge_type = data.knowledge_type, 
        p.publication_date = data.publication_date, 
        p.domain = data.domain, 
        p.patent_type = data.patent_type, 
        p.num_claims = data.num_claims, 
        p.summary = data.summary, 
        p.abstract = data.abstract, 
        p.ai_generated_abstract = data.ai_generated_abstract, 
        p.wipo_kind = data.wipo_kind, 
        p.foreign_citation_count = data.foreign_citation_count, 
        p.local_citation_count = data.local_citation_count, 
        p.has_embedding = data.has_embedding, 
        p.created_at = coalesce(p.created_at, datetime())
    WITH p, data WHERE data.embedding IS NOT NULL SET p.embedding = data.embedding
    WITH p, data
    FOREACH (tech IN data.technology_stack | MERGE (t:Technology {name: tech}) MERGE (p)-[:USES_TECHNOLOGY]->(t))
    FOREACH (kw IN data.keywords | MERGE (k:Keyword {name: kw}) MERGE (p)-[:HAS_KEYWORD]->(k))
    FOREACH (inv IN data.inventors | MERGE (i:Inventor {name: inv}) MERGE (p)-[:INVENTED_BY]->(i))
    FOREACH (org IN data.assignee_names | MERGE (o:Organization {name: org}) MERGE (p)-[:ASSIGNED_TO]->(o))
    FOREACH (ipc IN data.ipc_classifications | MERGE (ic:IPCClass {code: ipc}) MERGE (p)-[:CLASSIFIED_AS_IPC]->(ic))
    FOREACH (cpc IN data.cpc_classifications | MERGE (cc:CPCClass {code: cpc}) MERGE (p)-[:CLASSIFIED_AS_CPC]->(cc))
    FOREACH (uc_text IN data.use_cases | MERGE (uc:UseCase {description: uc_text}) MERGE (p)-[:HAS_USE_CASE]->(uc))
    FOREACH (ext_tech IN data.extracted_technologies | MERGE (t:Technology {name: ext_tech}) MERGE (p)-[:MENTIONS_TECHNOLOGY]->(t))
    FOREACH (ext_dom IN data.application_domains | MERGE (d:ApplicationDomain {name: ext_dom}) MERGE (p)-[:APPLIES_TO_DOMAIN]->(d))
    FOREACH (prob IN data.problems | MERGE (pr:Problem {name: prob}) MERGE (p)-[:ADDRESSES_PROBLEM]->(pr))
    FOREACH (val IN CASE WHEN data.sector IS NOT NULL AND data.sector <> '' THEN [1] ELSE [] END | MERGE (s:Sector {name: data.sector}) MERGE (p)-[:BELONGS_TO_SECTOR]->(s))
    FOREACH (val IN CASE WHEN data.sub_industry IS NOT NULL AND data.sub_industry <> '' THEN [1] ELSE [] END | MERGE (si:SubIndustry {name: data.sub_industry}) MERGE (p)-[:IN_SUB_INDUSTRY]->(si))
    FOREACH (val IN CASE WHEN data.function IS NOT NULL AND data.function <> '' THEN [1] ELSE [] END | MERGE (f:Function {name: data.function}) MERGE (p)-[:HAS_FUNCTION]->(f))
    FOREACH (val IN CASE WHEN data.taxonomy_domain IS NOT NULL AND data.taxonomy_domain <> '' THEN [1] ELSE [] END | MERGE (td:TaxonomyDomain {name: data.taxonomy_domain}) MERGE (p)-[:IN_TAXONOMY_DOMAIN]->(td))
    FOREACH (val IN CASE WHEN data.equivalent_function IS NOT NULL AND data.equivalent_function <> '' THEN [1] ELSE [] END | MERGE (ef:Function {name: data.equivalent_function}) MERGE (p)-[:HAS_EQUIVALENT_FUNCTION]->(ef))
    FOREACH (val IN CASE WHEN data.complementary_domain IS NOT NULL AND data.complementary_domain <> '' THEN [1] ELSE [] END | MERGE (cd:Domain {name: data.complementary_domain}) MERGE (p)-[:HAS_COMPLEMENTARY_DOMAIN]->(cd))
    FOREACH (val IN CASE WHEN data.complementary_sub_industry IS NOT NULL AND data.complementary_sub_industry <> '' THEN [1] ELSE [] END | MERGE (csi:SubIndustry {name: data.complementary_sub_industry}) MERGE (p)-[:HAS_COMPLEMENTARY_SUB_INDUSTRY]->(csi))
    FOREACH (val IN CASE WHEN data.adjacent_domain IS NOT NULL AND data.adjacent_domain <> '' THEN [1] ELSE [] END | MERGE (ad:Domain {name: data.adjacent_domain}) MERGE (p)-[:HAS_ADJACENT_DOMAIN]->(ad))
    FOREACH (val IN CASE WHEN data.adjacent_sub_industry IS NOT NULL AND data.adjacent_sub_industry <> '' THEN [1] ELSE [] END | MERGE (asi:SubIndustry {name: data.adjacent_sub_industry}) MERGE (p)-[:HAS_ADJACENT_SUB_INDUSTRY]->(asi))
    """
    tx.run(query, patents=patents_batch)

### ENHANCED: This function now adds a 'weight' to relationships to quantify their strength.
def create_multi_level_relationships(tx):
    """Create dense, inferred, and weighted relationships across the graph."""
    relationship_queries = [
        {'name': 'Inventor Collaboration Network', 'query': "MATCH (i1:Inventor)<-[:INVENTED_BY]-(p:Patent)-[:INVENTED_BY]->(i2:Inventor) WHERE elementId(i1) < elementId(i2) MERGE (i1)-[r:COLLABORATED_WITH]-(i2) ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1"},
        {'name': 'Technology-Problem Solution', 'query': "MATCH (t:Technology)<-[:USES_TECHNOLOGY|:MENTIONS_TECHNOLOGY]-(p:Patent)-[:ADDRESSES_PROBLEM]->(pr:Problem) MERGE (t)-[r:SOLVES]->(pr) ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1"},
        {'name': 'Function Equivalence (Weighted)', 'query': """
            MATCH (f1:Function)<-[:HAS_FUNCTION]-(p:Patent)-[:HAS_EQUIVALENT_FUNCTION]->(f2:Function) 
            WHERE elementId(f1) <> elementId(f2) 
            MERGE (f1)-[r:IS_EQUIVALENT_TO]-(f2)
            ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1
        """},
        {'name': 'Domain Complementarity (Weighted)', 'query': """
            MATCH (p:Patent)-[:HAS_COMPLEMENTARY_DOMAIN]->(cd:Domain) 
            WHERE p.domain IS NOT NULL AND p.domain <> '' AND p.domain <> cd.name
            MERGE (d:Domain {name: p.domain}) 
            MERGE (d)-[r:IS_COMPLEMENTARY_TO]-(cd)
            ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1
        """},
        {'name': 'Sub-Industry Complementarity (Weighted)', 'query': """
            MATCH (si1:SubIndustry)<-[:IN_SUB_INDUSTRY]-(p:Patent)-[:HAS_COMPLEMENTARY_SUB_INDUSTRY]->(si2:SubIndustry) 
            WHERE elementId(si1) <> elementId(si2) 
            MERGE (si1)-[r:IS_COMPLEMENTARY_TO]-(si2)
            ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1
        """},
        {'name': 'Domain Adjacency (Weighted)', 'query': """
            MATCH (p:Patent)-[:HAS_ADJACENT_DOMAIN]->(ad:Domain) 
            WHERE p.domain IS NOT NULL AND p.domain <> '' AND p.domain <> ad.name
            MERGE (d:Domain {name: p.domain}) 
            MERGE (d)-[r:IS_ADJACENT_TO]-(ad)
            ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1
        """},
        {'name': 'Sub-Industry Adjacency (Weighted)', 'query': """
            MATCH (si1:SubIndustry)<-[:IN_SUB_INDUSTRY]-(p:Patent)-[:HAS_ADJACENT_SUB_INDUSTRY]->(si2:SubIndustry) 
            WHERE elementId(si1) <> elementId(si2) 
            MERGE (si1)-[r:IS_ADJACENT_TO]-(si2)
            ON CREATE SET r.weight = 1 ON MATCH SET r.weight = r.weight + 1
        """}
    ]
    
    logger.info("🕸️ Creating multi-level inferred and weighted relationships...")
    for rel_config in tqdm(relationship_queries, desc="🕸️ Creating multi-level relationships"):
        try:
            tx.run(rel_config['query'])
        except Exception as e:
            logger.error(f"  ❌ Error creating '{rel_config['name']}' relationships: {e}")

def create_embedding_based_relationships(tx, similar_pairs_batch):
    if not similar_pairs_batch: return
    query = "UNWIND $pairs as pair MATCH (p1:Patent {patent_id: pair.patent_1}) MATCH (p2:Patent {patent_id: pair.patent_2}) MERGE (p1)-[r:SEMANTICALLY_SIMILAR]-(p2) SET r.similarity = pair.similarity, r.method = 'embedding_cosine'"
    tx.run(query, pairs=similar_pairs_batch)

def create_indexes(tx):
    indexes_and_constraints = [
        "CREATE CONSTRAINT patent_id_unique IF NOT EXISTS FOR (n:Patent) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT patent_patent_id_unique IF NOT EXISTS FOR (n:Patent) REQUIRE n.patent_id IS UNIQUE",
        "CREATE INDEX technology_name_idx IF NOT EXISTS FOR (n:Technology) ON (n.name)",
        "CREATE INDEX keyword_name_idx IF NOT EXISTS FOR (n:Keyword) ON (n.name)",
        "CREATE INDEX inventor_name_idx IF NOT EXISTS FOR (n:Inventor) ON (n.name)",
        "CREATE INDEX org_name_idx IF NOT EXISTS FOR (n:Organization) ON (n.name)",
        "CREATE INDEX problem_name_idx IF NOT EXISTS FOR (n:Problem) ON (n.name)",
        "CREATE INDEX domain_name_idx IF NOT EXISTS FOR (n:Domain) ON (n.name)",
        "CREATE INDEX app_domain_name_idx IF NOT EXISTS FOR (n:ApplicationDomain) ON (n.name)",
        "CREATE INDEX usecase_desc_idx IF NOT EXISTS FOR (n:UseCase) ON (n.description)",
        "CREATE INDEX sector_name_idx IF NOT EXISTS FOR (n:Sector) ON (n.name)",
        "CREATE INDEX function_name_idx IF NOT EXISTS FOR (n:Function) ON (n.name)",
        "CREATE INDEX subindustry_name_idx IF NOT EXISTS FOR (n:SubIndustry) ON (n.name)",
        "CREATE INDEX taxdomain_name_idx IF NOT EXISTS FOR (n:TaxonomyDomain) ON (n.name)",
        "CREATE INDEX cpc_code_idx IF NOT EXISTS FOR (n:CPCClass) ON (n.code)",
        "CREATE INDEX ipc_code_idx IF NOT EXISTS FOR (n:IPCClass) ON (n.code)"
    ]
    logger.info("🏗️ Creating indexes and constraints...")
    for query in indexes_and_constraints: tx.run(query)
    logger.info("✅ Indexes and constraints created.")

# ----------------------------
# MAIN PROCESSING
# ----------------------------
if __name__ == "__main__":
    try:
        csv_file = "cdkg.csv"
        logger.info(f"📖 Reading CSV file: {csv_file}...")
        df = pd.read_csv(csv_file, dtype={'patent_id': str})
        
        logger.info(f"Loaded {len(df)} raw records.")
        df.dropna(subset=['patent_id'], inplace=True)
        df['patent_id'] = df['patent_id'].str.strip()
        df = df[df['patent_id'] != '']
        df.drop_duplicates(subset=['patent_id'], keep='first', inplace=True)
        
        df = df.replace({np.nan: None})
        total_records = len(df)
        logger.info(f"🧮 Processing {total_records} unique patents after de-duplication.")

        patents_data, embeddings_list = [], []
        for _, row in tqdm(df.iterrows(), total=total_records, desc="🧮 Processing CSV rows"):
            patent_unique_id = str(row.get('_id'))
            patent_id_str = str(row.get('patent_id', ''))
            if not patent_id_str: continue

            full_text = str(row.get('full_text', '')) + ' ' + str(row.get('abstract', ''))
            embedding = parse_embedding(row.get('ai_embeddings'))
            
            patents_data.append({'id': patent_unique_id, 'patent_id': patent_id_str, 'title': row.get('title'), 'abstract': row.get('abstract'), 'ai_generated_abstract': row.get('ai_generated_abstract'), 'summary': row.get('summary'), 'publication_date': str(row.get('publication_date', '')), 'knowledge_type': row.get('knowledge_type'), 'domain': row.get('domain'), 'patent_type': row.get('patent_type'), 'num_claims': int(row.get('num_claims', 0)) if row.get('num_claims') is not None else 0, 'wipo_kind': row.get('wipo_kind'), 'foreign_citation_count': int(row.get('foreign_citation_count', 0)) if row.get('foreign_citation_count') is not None else 0, 'local_citation_count': int(row.get('local_citation_count', 0)) if row.get('local_citation_count') is not None else 0, 'has_embedding': embedding is not None, 'embedding': embedding, 'technology_stack': parse_list_field(row.get('technology_stack')), 'keywords': parse_list_field(row.get('keywords')), 'inventors': parse_list_field(row.get('inventors')), 'assignee_names': parse_list_field(row.get('assignee_names')), 'ipc_classifications': parse_list_field(row.get('ipc_classifications')), 'cpc_classifications': parse_list_field(row.get('cpc_classifications')), 'use_cases': parse_list_field(row.get('use_case_examples')), 'sector': row.get('sector'), 'sub_industry': row.get('sub_industry'), 'function': row.get('function'), 'taxonomy_domain': row.get('taxonomy_domain'), 'equivalent_function': row.get('equivalent_function'), 'complementary_domain': row.get('complementary_domain'), 'complementary_sub_industry': row.get('complementary_sub_industry'), 'adjacent_domain': row.get('adjacent_domain'), 'adjacent_sub_industry': row.get('adjacent_sub_industry'), 'extracted_technologies': extract_technologies_from_text(full_text), 'application_domains': extract_domains_from_text(full_text), 'problems': extract_problems_from_text(full_text)})
            
            if embedding is not None:
                embeddings_list.append((patent_id_str, embedding))
        
        similar_pairs = calculate_embedding_similarity(embeddings_list, similarity_threshold=0.85)

        with driver.session(database="neo4j") as session:
            logger.warning("Clearing existing database...")
            session.run("MATCH (n) DETACH DELETE n")
            
            session.execute_write(create_indexes)
            
            logger.info(f"🏗️ Creating {total_records} patent nodes in batches of {BATCH_SIZE}...")
            for i in tqdm(range(0, total_records, BATCH_SIZE), desc="🏗️ Creating patent nodes"):
                batch = patents_data[i:i + BATCH_SIZE]
                session.execute_write(batch_create_patents, batch)
            
            # This now creates the weighted relationships
            session.execute_write(create_multi_level_relationships)
            
            if similar_pairs:
                logger.info(f"🧠 Creating {len(similar_pairs)} semantic relationships in batches...")
                for i in tqdm(range(0, len(similar_pairs), BATCH_SIZE), desc="🧠 Creating semantic relationships"):
                    batch = similar_pairs[i:i + BATCH_SIZE]
                    session.execute_write(create_embedding_based_relationships, batch)

        logger.info("\n🎉 DENSE KNOWLEDGE GRAPH CREATION COMPLETE! 🎉")

    except FileNotFoundError:
        logger.error(f"❌ Error: The file '{csv_file}' was not found.")
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}", exc_info=True)
        raise
    finally:
        if 'driver' in locals() and driver:
            driver.close()
            logger.info("Neo4j driver closed.")