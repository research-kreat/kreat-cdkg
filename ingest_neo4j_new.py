import os
import glob
import pandas as pd
import numpy as np
import ijson
from neo4j import GraphDatabase
from tqdm import tqdm
from dotenv import load_dotenv
import re
from sentence_transformers import SentenceTransformer

load_dotenv()

# --- CONFIGURATION ---
# JSON File Path (Local)
JSON_FILE_PATH = "KG.json" 

NEO4J_URI = "bolt://135.235.170.159:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "cHRE3FjhhxvkGNH"

INDUSTRY_CSV = "/Users/user/Downloads/MASTER_INDUSTRY_TAXONOMY_COMPLETE_989_Functions.csv"
FUNCTION_CSV_DIR = "/Users/user/Downloads/CDKG - DATA/*.csv"

# Using MiniLM for speed/efficiency
EMBEDDING_MODEL_NAME = 'all-MiniLM-L6-v2' 
VECTOR_DIMENSIONS = 384
BATCH_SIZE = 500  # Process patents in chunks of 500

print(f"⏳ Loading Embedding Model ({EMBEDDING_MODEL_NAME})...")
model = SentenceTransformer(EMBEDDING_MODEL_NAME)

# --- GLOBAL CACHE ---
universal_embedding_cache = {}

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# --- HELPER FUNCTIONS ---

def clean_list(value, delimiter=None):
    if isinstance(value, list):
        return [str(x).strip() for x in value if x]
    
    if isinstance(value, str) and value:
        if delimiter is None:
            if '|' in value: delimiter = '|'
            elif ';' in value: delimiter = ';'
            else: delimiter = ','
            
        return [x.strip() for x in value.split(delimiter) if x.strip()]
    return []

def drop_all_indexes_and_constraints(session):
    print("🧨 Dropping all Indexes and Constraints...")
    
    # 1. Drop Constraints First (Constraints depend on indexes)
    constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
    for rec in constraints:
        try:
            session.run(f"DROP CONSTRAINT {rec['name']}")
            print(f"   - Dropped constraint: {rec['name']}")
        except Exception as e:
            print(f"   ! Failed to drop constraint {rec['name']}: {e}")

    # 2. Drop Explicit Indexes
    indexes = session.run("SHOW INDEXES YIELD name, type").data()
    for rec in indexes:
        # Don't try to drop internal LOOKUP indexes
        if rec['type'] == 'LOOKUP': 
            continue
            
        try:
            session.run(f"DROP INDEX {rec['name']}")
            print(f"   - Dropped index: {rec['name']}")
        except Exception as e:
            print(f"   ! Failed to drop index {rec['name']}: {e}")


def extract_problems_from_text(text):
    if not isinstance(text, str):
        return []
    text = text.lower()
    patterns = [
        r'\b(inefficiency|inefficient|slow|delay|bottleneck|latency|lag)\b',
        r'\b(error|mistake|inaccuracy|unreliable|failure|fault|defect|bug)\b',
        r'\b(expensive|costly|high cost|resource intensive|wasteful)\b',
        r'\b(complex|complexity|difficult|challenging|cumbersome)\b',
        r'\b(security|vulnerability|threat|breach|fraud|leak|attack)\b',
        r'\b(scalability|scale|limitation|constraint|capacity)\b',
        r'\b(manual|labor|time consuming|tedious)\b',
        r'\b(outdated|legacy|obsolete|old|incompatible)\b',
        r'\b(waste|pollution|emission|sustainability|toxic|hazard)\b',
        r'\b(overheat|thermal|cooling|heat|dissipation)\b',
        r'\b(noise|vibration|interference|signal|attenuation)\b'
    ]
    problems = set()
    for p in patterns:
        matches = re.findall(p, text)
        problems.update(matches)
    return list(problems)

def extract_properties(text):
    if not isinstance(text, str):
        return []
    pattern = r'(\d+(?:\.\d+)?)\s?([°]?[CcFf]\b|MPa\b|kPa\b|Pa\b|psi\b|V\b|kV\b|Hz\b|GHz\b|MHz\b|%|mm\b|cm\b|nm\b|kg\b|mg\b)'
    matches = re.findall(pattern, text)
    properties = []
    for val, unit in matches:
        clean_unit = unit.strip().replace('°', '')
        properties.append(f"{val}_{clean_unit}")
    return list(set(properties))

def get_safe_mongo_value(field_value):
    """Helper to handle MongoDB specific types like $oid or $date if they appear in JSON"""
    if isinstance(field_value, dict):
        if '$oid' in field_value: return str(field_value['$oid'])
        if '$date' in field_value: return str(field_value['$date'])
    return field_value

def normalize_columns_v2(df):
    mappings = {
        'function_category': ['Function Category', 'Category', 'Func_Cat'],
        'specific_function': ['Specific Function', 'Source Function', 'Function Name'],
        'universal_function': ['Universal Function Class', 'Universal Function', 'Universal Function Name'],
        'biology_mechanisms': ['Biology Mechanisms', 'Biological Mechanisms'],
        'physics_mechanisms': ['Physics/Engineering Mechanisms', 'Physics Mechanisms'],
        'cyber_physical_mechanisms': ['Cyber-Physical Mechanisms', 'Cyber Mechanisms'],
        'transfer_evidence': ['Transfer Evidence', 'Evidence'],
        'key_stakeholders': ['Key Stakeholders', 'Stakeholders']
    }
    new_data = {}
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for std_key, variations in mappings.items():
        found = False
        for v in variations:
            if v.lower() in cols_lower:
                new_data[std_key] = df[cols_lower[v.lower()]]
                found = True
                break
        if not found:
            new_data[std_key] = pd.Series([""] * len(df))
    return pd.concat([df, pd.DataFrame(new_data)], axis=1)


# --- PHASE 1: WORLD BUILDER ---

def build_industry_backbone(tx, row):
    # 1. Extract Core Taxonomy Fields
    sector = str(row.get('Sector', '')).strip()
    domain = str(row.get('Domain', '')).strip()
    sub_ind = str(row.get('Sub-Industry', '')).strip()
    func_name = str(row.get('Function', '')).strip() 
    univ_name = str(row.get('Universal Function', '')).strip() 
    
    if not sub_ind: return

    # 2. Build Hierarchy
    q_core = """
    MERGE (s:Sector {name: $sector})
    MERGE (d:Domain {name: $domain})
    MERGE (sub:SubIndustry {name: $sub_ind})
    SET sub.naics = $naics, 
        sub.classification = $classification, 
        sub.horizontal_connection = $horiz
    
    MERGE (s)-[:CONTAINS]->(d)
    MERGE (d)-[:CONTAINS]->(sub)
    MERGE (d)-[:BELONGS_TO_SECTOR]->(s)
    MERGE (sub)-[:BELONGS_TO_DOMAIN]->(d)
    """
    tx.run(q_core,
           sector=sector,
           domain=domain,
           sub_ind=sub_ind,
           naics=str(row.get('NAICS Code', '')),
           classification=str(row.get('Classification', '')),
           horiz=str(row.get('Horizontal Connection', '')))

    # 3. Ingest Function & Bridge
    if func_name and univ_name:
        context_text = f"Sector: {sector} | Domain: {domain} | SubIndustry: {sub_ind} | Function: {func_name} | Universal: {univ_name}"
        try:
            spec_emb = model.encode(context_text).tolist()
        except:
            spec_emb = []

        univ_emb = []
        if univ_name not in universal_embedding_cache:
            try:
                univ_emb = model.encode(f"Universal Design Function: {univ_name}").tolist()
                universal_embedding_cache[univ_name] = univ_emb
            except:
                universal_embedding_cache[univ_name] = []
        univ_emb = universal_embedding_cache[univ_name]

        q_func_bridge = """
        MATCH (sub:SubIndustry {name: $sub_ind})
        
        MERGE (f:SpecificFunction {name: $func_name})
        SET f.embedding = $spec_emb,
            f.context_text = $context,
            f.source = 'Master_Taxonomy'
            
        MERGE (u:UniversalFunction {name: $univ_name})
        SET u.embedding = $univ_emb
        
        MERGE (sub)-[:HAS_FUNCTION]->(f)
        MERGE (f)-[:CLASSIFIED_AS]->(u)
        MERGE (u)-[:INCLUDES_FUNCTION]->(f)
        MERGE (sub)-[:PERFORMS]->(u)
        """
        tx.run(q_func_bridge, 
               sub_ind=sub_ind, 
               func_name=func_name, 
               univ_name=univ_name, 
               spec_emb=spec_emb, 
               univ_emb=univ_emb,
               context=context_text)

    # 4. Adjacencies
    adj_configs = [
        ('Adjacent Domain 1', 'Adjacent Sub-Industry 1', 'Strength 1', 'IS_ADJACENT_TO'),
        ('Adjacent Domain 2', 'Adjacent Sub-Industry 2', 'Strength 2', 'IS_ADJACENT_TO'),
        ('Adjacent Domain 3', 'Adjacent Sub-Industry 3', 'Strength 3', 'IS_ADJACENT_TO'),
        ('Complementary Domain', 'Complementary Sub-Industry', 'Complementary Strength', 'IS_COMPLEMENTARY_TO'),
        ('Far-Off Domain', 'Far-Off Sub-Industry', 'Far-Off Strength', 'POTENTIAL_TRANSFER')
    ]
    
    for d_key, s_key, score_key, rel in adj_configs:
        tgt_dom = str(row.get(d_key, '')).strip()
        tgt_sub = str(row.get(s_key, '')).strip()
        try:
            score = float(row.get(score_key, 0))
        except:
            score = 0.5

        # A. Detailed Sub-Industry Link (Granular)
        if tgt_sub:
            q_rel = f"""
            MATCH (src:SubIndustry {{name: $src}})
            MERGE (tgt_d:Domain {{name: $tgt_d}})
            MERGE (tgt_s:SubIndustry {{name: $tgt_s}})
            MERGE (tgt_d)-[:CONTAINS]->(tgt_s)
            MERGE (src)-[r:{rel}]->(tgt_s) SET r.weight = $score
            MERGE (tgt_s)-[r2:{rel}]->(src) SET r2.weight = $score
            """
            tx.run(q_rel, src=sub_ind, tgt_d=tgt_dom, tgt_s=tgt_sub, score=score)
        
        # B. Domain Link (Broad)
        if tgt_dom:
            q_rel_sub_dom = f"""
            MATCH (src:SubIndustry {{name: $src}})
            MERGE (tgt_d:Domain {{name: $tgt_d}})
            MERGE (src)-[r:{rel}]->(tgt_d) SET r.weight = $score
            """
            tx.run(q_rel_sub_dom, src=sub_ind, tgt_d=tgt_dom, score=score)
            
            if rel == 'IS_ADJACENT_TO':
                    q_domain_highway = """
                    MATCH (src_d:Domain {name: $src_d})
                    MERGE (tgt_d:Domain {name: $tgt_d})
                    MERGE (src_d)-[r:IS_ADJACENT_TO]->(tgt_d)
                    SET r.source = 'Taxonomy_Aggregation'
                    """
                    tx.run(q_domain_highway, src_d=domain, tgt_d=tgt_dom)

def build_function_backbone(tx, row):
    # 1. Normalization
    spec_name = str(row.get('specific_function') or row.get('Specific Function') or "").strip()
    univ_name = str(row.get('universal_function') or row.get('Universal Function') or "").strip()
    cat_name = str(row.get('function_category') or row.get('Category') or row.get('Function Category') or "").strip()
    
    sector_name = str(row.get('Sector') or row.get('sector') or row.get('Domain') or "").strip()
    domain_name = str(row.get('Domain') or row.get('domain') or "").strip()

    if not spec_name:
        return

    # 2. Contextual Embedding
    context_text = f"Sector: {sector_name} | Domain: {domain_name} | Category: {cat_name} | Function: {spec_name} | Universal: {univ_name}"
    
    try:
        spec_embedding = model.encode(context_text).tolist()
    except Exception:
        spec_embedding = []

    # 3. Universal Embedding
    univ_embedding = []
    if univ_name:
        if univ_name not in universal_embedding_cache:
            try:
                univ_embedding = model.encode(f"Universal Design Function: {univ_name}").tolist()
                universal_embedding_cache[univ_name] = univ_embedding
            except:
                universal_embedding_cache[univ_name] = []
        univ_embedding = universal_embedding_cache[univ_name]

    q_func = """
    MERGE (f:SpecificFunction {name: $spec})
    SET f.embedding = $spec_emb,
        f.universal_function = $univ,
        f.sector = $sector,
        f.domain = $domain,
        f.context_text = $context
    
    MERGE (u:UniversalFunction {name: $univ})
    SET u.embedding = $univ_emb
    
    MERGE (f)-[:HAS_UNIVERSAL_FUNCTION]->(u)
    MERGE (u)-[:INCLUDES_FUNCTION]->(f)
    MERGE (f)-[:CLASSIFIED_AS]->(u)
    
    // Category Bridge
    FOREACH (ignoreMe IN CASE WHEN $cat <> "" THEN [1] ELSE [] END |
        MERGE (c:FunctionCategory {name: $cat})
        MERGE (f)-[:BELONGS_TO_CATEGORY]->(c)
        MERGE (c)-[:PART_OF_UNIVERSAL]->(u)
    )
    """
    
    tx.run(q_func, 
           spec=spec_name, 
           univ=univ_name, 
           cat=cat_name, 
           spec_emb=spec_embedding, 
           univ_emb=univ_embedding,
           sector=sector_name, 
           domain=domain_name, 
           context=context_text)

    # Mechanism Linking
    mechanisms = [
        ('biology_mechanisms', 'BioMechanism', 'MIMICS'),
        ('physics_mechanisms', 'PhyMechanism', 'UTILIZES'),
        ('cyber_physical_mechanisms', 'CyberPhyMechanism', 'OPERATES_VIA')
    ]
    for col, label, rel in mechanisms:
        items = clean_list(str(row.get(col, '')) or str(row.get(col.capitalize(), '')), '|')
        for item in items:
            name_part = item.split('(')[0].strip()
            if len(name_part) > 2:
                tx.run(
                    f"MATCH (f:SpecificFunction {{name: $spec}}) "
                    f"MERGE (m:{label} {{name: $name}}) "
                    f"MERGE (f)-[:{rel}]->(m)",
                    spec=spec_name, name=name_part
                )
    
    # Equivalent Domain Linking
    equivalent_domain = row.get('Equivalent Domain') or row.get('equivalent_domain') or ""
    if equivalent_domain:
        tx.run("""
        MATCH (spec:SpecificFunction {name: $spec})
        MERGE (d:Domain {name: $dom})
        MERGE (spec)-[r:IS_EQUIVALENT_TO]->(d)
        MERGE (d)-[r2:HAS_EQUIVALENT_FUNCTION]->(spec)
        SET r.layer = $layer, r.evidence = $evidence
        """, spec=spec_name, dom=equivalent_domain,
               layer=row.get('Equivalence Layer', ''), evidence=row.get('transfer_evidence', ''))

    # Stakeholders
    stakeholders_field = row.get('key_stakeholders') or row.get('Key Stakeholders') or ""
    for sh in clean_list(str(stakeholders_field), '|'):
        tx.run("MATCH (spec:SpecificFunction {name: $spec}) MERGE (s:Stakeholder {name: $name}) MERGE (s)-[:INVESTS_IN]->(spec)",
               spec=spec_name, name=sh)


# --- PHASE 2: PATENT BATCH INGESTOR (Replaces ingest_patent_dense) ---

# Cypher Query for Batch Processing
BATCH_INGEST_QUERY = """
UNWIND $batch AS row
MERGE (p:Patent {id: row.id})
SET p.title = row.title, 
    p.abstract = row.abstract, 
    p.publication_date = row.date,
    p.embedding = row.embedding, 
    p.num_claims = toInteger(row.num_claims), 
    p.patent_type = row.patent_type,
    p.patent_id_str = row.patent_id_str,
    p.sector = row.sector,  
    p.domain = row.domain,
    p.function_category = row.cat,
    p.country = row.country,
    p.pdf_link = row.pdf_link,
    p.url = row.url,
    p.relevance_score = toFloat(row.relevance_score),
    p.quality_score = toFloat(row.quality_score),
    p.ipc_classifications = row.ipc_list,
    p.cpc_classifications = row.cpc_list

// --- LINK TO SECTOR (Layer 1 Entry Point) ---
FOREACH (_ IN CASE WHEN row.sector <> 'Unclassified' THEN [1] ELSE [] END |
    MERGE (s:Sector {name: row.sector})
    MERGE (s)-[:HAS_PATENT]->(p)
    MERGE (p)-[:BELONGS_TO_SECTOR]->(s)
)

// --- LINK TO DOMAIN (Layer 1.5 Entry Point) ---
FOREACH (_ IN CASE WHEN row.domain <> 'Unclassified' THEN [1] ELSE [] END |
    MERGE (d:Domain {name: row.domain})
    MERGE (d)-[:HAS_PATENT]->(p)
    MERGE (p)-[:FOUND_IN_DOMAIN]->(d)
    // Ensure Sector -> Domain exists
    FOREACH (_2 IN CASE WHEN row.sector <> 'Unclassified' THEN [1] ELSE [] END |
        MERGE (s2:Sector {name: row.sector})
        MERGE (s2)-[:CONTAINS]->(d)
    )
)

// --- LINK TO FUNCTION ---
FOREACH (_ IN CASE WHEN row.func <> '' THEN [1] ELSE [] END |
    MERGE (f:SpecificFunction {name: row.func})
    MERGE (p)-[:USES_FUNCTION]->(f)
    MERGE (p)-[:IMPLEMENTS]->(f)
)

// --- LINK ENTITIES ---
FOREACH (n IN row.problems | MERGE (pr:Problem {name: n}) MERGE (p)-[:ADDRESSES_PROBLEM]->(pr))
FOREACH (n IN row.tech | MERGE (t:Technology {name: n}) MERGE (p)-[:USES_TECHNOLOGY]->(t))
FOREACH (n IN row.keywords | MERGE (k:Keyword {name: n}) MERGE (p)-[:HAS_KEYWORD]->(k))
FOREACH (n IN row.assignees | MERGE (o:Organization {name: n}) MERGE (p)-[:ASSIGNED_TO]->(o))
FOREACH (n IN row.properties | MERGE (prop:Property {name: n}) MERGE (p)-[:HAS_PARAMETER]->(prop))
FOREACH (n IN row.inventors | MERGE (i:Inventor {name: n}) MERGE (i)-[:INVENTED]->(p))
FOREACH (n IN row.use_cases | MERGE (uc:UseCase {name: n}) MERGE (p)-[:APPLIED_IN]->(uc))
FOREACH (n IN row.trends | MERGE (mt:MarketTrend {name: n}) MERGE (p)-[:ALIGNS_WITH_TREND]->(mt))
FOREACH (n IN row.cpc_groups | MERGE (cg:CPCGroup {name: n}) MERGE (p)-[:CLASSIFIED_AS]->(cg))
FOREACH (n IN row.cpc_subclasses | MERGE (cs:CPCSubclass {name: n}) MERGE (p)-[:IN_SUBCLASS]->(cs))
"""

def process_patent_batch(session, batch_docs):
    """
    Extracts data from a list of JSON docs and runs a single UNWIND transaction.
    Includes Type Casting to fix 'Decimal' errors from ijson.
    """
    clean_batch = []
    
    for doc in batch_docs:
        # 1. EXTRACT DATA
        tax = doc.get('taxonomy_data', {})
        ind = tax.get('industry', {})
        func = tax.get('function', {})

        # --- SWAP LOGIC ---
        raw_sector_value = str(ind.get('Sector') or 'Unclassified').strip()
        raw_domain_value = str(ind.get('Domain') or doc.get('domain') or 'Unclassified').strip()
        
        sector_name = raw_domain_value  # Swapped
        domain_name = raw_sector_value  # Swapped

        # IDs and Dates
        pid = get_safe_mongo_value(doc.get('_id')) or str(doc.get('patent_id', ''))
        patent_id_str = str(doc.get('patent_id', ''))
        pub_date = get_safe_mongo_value(doc.get('publication_date'))

        # Extract Function Info
        func_name = str(func.get('Specific Function') or func.get('standard_specific_function') or "").strip()
        cat_name = str(func.get('Function Category') or func.get('category') or "").strip()
        
        # Text Processing
        abstract_text = doc.get('abstract') or doc.get('summary') or doc.get('ai_generated_abstract') or ""
        full_text = f"{doc.get('title', '')} {abstract_text} {doc.get('full_text', '')}"
        
        extracted_problems = extract_problems_from_text(full_text)
        extracted_properties = extract_properties(full_text)
        
        # --- TYPE CASTING FIX (Decimal -> Float/Int) ---
        
        # 1. Embeddings: Convert list of Decimals to list of Floats
        raw_emb = doc.get('embedding') or doc.get('ai_embeddings')
        if raw_emb and hasattr(raw_emb, '__iter__'):
             # This list comprehension forces conversion of every element to float
             emb_list = [float(x) for x in raw_emb]
        else:
             emb_list = []

        # 2. Scalar Scores: Explicit conversion
        try:
            rel_score = float(doc.get('relevance_score', 0) or 0)
        except: rel_score = 0.0
            
        try:
            qual_score = float(doc.get('data_quality_score', 0) or 0)
        except: qual_score = 0.0

        try:
            n_claims = int(doc.get('num_claims', 0) or 0)
        except: n_claims = 0

        # Skip if embedding is missing or invalid dimension
        if not emb_list or len(emb_list) != VECTOR_DIMENSIONS:
            continue
        
        # Lists
        ipc_list = clean_list(doc.get('ipc_classifications'), ',')
        cpc_list = clean_list(doc.get('cpc_classifications'), ',')
        
        # Prepare item dictionary
        item = {
            'id': pid, 
            'patent_id_str': patent_id_str,
            'title': doc.get('title', 'Untitled'), 
            'abstract': abstract_text[:5000], 
            'date': str(pub_date), 
            'embedding': emb_list,  # <--- Now strictly floats
            'num_claims': n_claims, # <--- Now strictly int
            'patent_type': doc.get('patent_type', ''),
            'sector': sector_name, 
            'domain': domain_name,
            'country': doc.get('country', ''), 
            'pdf_link': doc.get('pdf_link', ''),
            'url': doc.get('source_url', ''), 
            'relevance_score': rel_score, # <--- Now strictly float
            'quality_score': qual_score,  # <--- Now strictly float
            'func': func_name, 
            'cat': cat_name,
            'ipc_list': ipc_list,
            'cpc_list': cpc_list,
            'problems': extracted_problems, 
            'properties': extracted_properties, 
            'tech': clean_list(doc.get('technology_stack') or "", ','),
            'keywords': clean_list(doc.get('keywords') or "", ','),
            'assignees': clean_list(doc.get('assignee_org') or doc.get('assignee_names'), ','), 
            'inventors': clean_list(doc.get('inventors'), ','),
            'use_cases': clean_list(doc.get('use_case_examples'), ';'), 
            'trends': clean_list(doc.get('market_trends'), ';'),
            'cpc_groups': clean_list(doc.get('cpc_group_title')), 
            'cpc_subclasses': clean_list(doc.get('cpc_subclass_title'))
        }

        clean_batch.append(item)

    # Execute Batch
    if clean_batch:
        try:
            session.run(BATCH_INGEST_QUERY, batch=clean_batch)
        except Exception as e:
            print(f"⚠️ Batch Error: {e}")
# --- PHASE 3: DENSIFICATION ---

def run_densification_protocols(session):
    print("🔗 Running Densification Protocols...")

    print("   - Creating Semantic Wormholes...")
    session.run("""
    MATCH (p1:Patent) WHERE p1.embedding IS NOT NULL
    CALL db.index.vector.queryNodes('patent_embeddings', 5, p1.embedding)
    YIELD node AS p2, score
    WHERE score > 0.88 AND elementId(p1) < elementId(p2) AND p1.domain <> p2.domain
    MERGE (p1)-[r:SEMANTICALLY_SIMILAR]-(p2) SET r.score = score
    """)

    print("   - Mapping Inventor Networks...")
    session.run("""
    MATCH (i1:Inventor)-[:INVENTED]->(p:Patent)<-[:INVENTED]-(i2:Inventor)
    WHERE elementId(i1) < elementId(i2)
    MERGE (i1)-[r:CO_INVENTED_WITH]-(i2)
    ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1
    """)


def create_function_similarity(session, score_threshold=0.85, batch_size=50, parallel=True):
    print("🔁 Creating FUNCTIONALLY_SIMILAR relationships (batched)...")
    apoc_query = f"""
    CALL apoc.periodic.iterate(
      "MATCH (f1:SpecificFunction) WHERE f1.embedding IS NOT NULL RETURN f1",
      "CALL db.index.vector.queryNodes('function_embeddings', 10, f1.embedding) YIELD node AS f2, score
       WHERE score > {score_threshold} AND elementId(f1) < elementId(f2) AND f1 <> f2
       MERGE (f1)-[r:FUNCTIONALLY_SIMILAR]-(f2)
       ON CREATE SET r.score = score, r.created_at = datetime()
       ON MATCH SET r.score = CASE WHEN score > r.score THEN score ELSE r.score END",
      {{batchSize: {batch_size}, parallel: {str(parallel).lower()}}}
    );
    """
    try:
        session.run(apoc_query)
    except Exception as e:
        print(f"⚠️ APOC Batched Job Failed: {e}")


# --- MAIN EXECUTION ---
def main():
    with driver.session() as session:
        drop_all_indexes_and_constraints(session)
        # 1. SETUP & INDEXES
        print("📐 Creating Indexes & Constraints...")
        indexes = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Domain) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Sector) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SpecificFunction) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Patent) REQUIRE n.id IS UNIQUE", 
            "CREATE INDEX IF NOT EXISTS FOR (n:FunctionCategory) ON (n.name)",
            f"CREATE VECTOR INDEX patent_embeddings IF NOT EXISTS FOR (p:Patent) ON (p.embedding) OPTIONS {{indexConfig: {{`vector.dimensions`: {VECTOR_DIMENSIONS}, `vector.similarity_function`: 'cosine'}}}}",
            f"CREATE VECTOR INDEX function_embeddings IF NOT EXISTS FOR (f:SpecificFunction) ON (f.embedding) OPTIONS {{indexConfig: {{`vector.dimensions`: {VECTOR_DIMENSIONS}, `vector.similarity_function`: 'cosine'}}}}",
            f"CREATE VECTOR INDEX universal_embeddings IF NOT EXISTS FOR (u:UniversalFunction) ON (u.embedding) OPTIONS {{indexConfig: {{`vector.dimensions`: {VECTOR_DIMENSIONS}, `vector.similarity_function`: 'cosine'}}}}"
        ]
        for idx in indexes:
            try:
                session.run(idx)
            except Exception as e:
                print(f"Index creation warning: {e}")

        # 2. WORLD BUILDER
        print("🏗️  Building World (Taxonomy)...")
        if os.path.exists(INDUSTRY_CSV):
            df = pd.read_csv(INDUSTRY_CSV).replace({np.nan: ""})
            for _, row in tqdm(df.iterrows(), total=len(df)):
                session.execute_write(build_industry_backbone, row)

        print("🧬 Building Function Details (With Embeddings)...")
        files = glob.glob(FUNCTION_CSV_DIR)
        for f in files:
            try:
                df = pd.read_csv(f).replace({np.nan: ""})
                df = normalize_columns_v2(df)
                for _, row in tqdm(df.iterrows(), total=len(df), leave=False):
                    session.execute_write(build_function_backbone, row)
            except Exception as e:
                print(f"Warning: failed to process {f}: {e}")

        # 3. JSON STREAMING INGESTION
        print(f"🚀 Streaming Patents from {JSON_FILE_PATH} (Low RAM Mode)...")
        if not os.path.exists(JSON_FILE_PATH):
            print(f"❌ Error: File {JSON_FILE_PATH} not found.")
            return

        batch_buffer = []
        total_ingested = 0

        # Open JSON file as a stream
        try:
            with open(JSON_FILE_PATH, 'rb') as f:
                # ijson.items yields one object at a time from the JSON array
                parser = ijson.items(f, 'item')
                
                for doc in tqdm(parser, desc="Ingesting Patents"):
                    # Basic Validation
                    if "taxonomy_data" not in doc:
                        continue
                        
                    batch_buffer.append(doc)

                    # When buffer fills, flush to Neo4j
                    if len(batch_buffer) >= BATCH_SIZE:
                        process_patent_batch(session, batch_buffer)
                        total_ingested += len(batch_buffer)
                        batch_buffer = [] # Clear RAM

                # Process remaining items in buffer
                if batch_buffer:
                    process_patent_batch(session, batch_buffer)
                    total_ingested += len(batch_buffer)
                    
        except Exception as e:
            print(f"❌ JSON Reading Error: {e}")

        print(f"✅ Ingestion Complete. Total Processed: {total_ingested}")

        # 4. DENSIFICATION
        run_densification_protocols(session)

        try:
            create_function_similarity(session, score_threshold=0.85, batch_size=50, parallel=True)
        except Exception as e:
            print(f"Function similarity job failed: {e}")

    print("✅ Complete! Graph built with Universal Bridges & Cross-Domain Intelligence.")


if __name__ == "__main__":
    main()