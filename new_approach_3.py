import os
import glob
import pandas as pd
import numpy as np
from pymongo import MongoClient
from neo4j import GraphDatabase
from tqdm import tqdm
from dotenv import load_dotenv
import re
load_dotenv()
# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COL = "temp"  # Your fully enriched collection

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  # UPDATE THIS

# We still read CSVs to build the 'World' first, ensuring complete connectivity
INDUSTRY_CSV = "/Users/user/Downloads/MASTER_INDUSTRY_TAXONOMY_COMPLETE_989_Functions.csv"
FUNCTION_CSV_DIR = "/Users/user/Downloads/CDKG - DATA/*.csv"

mongo_client = MongoClient(MONGO_URI)
mongo_col = mongo_client[MONGO_DB][MONGO_COL]
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- 3. HELPER FUNCTIONS ---
def clean_list(value, delimiter=';'):
    """Parses various formats into a clean list of strings."""
    if isinstance(value, list):
        return [str(x).strip() for x in value if x]
    if isinstance(value, str) and value:
        if delimiter in value:
            return [x.strip() for x in value.split(delimiter) if x.strip()]
        return [value.strip()]
    return []

def extract_problems_from_text(text):
    """Restoring the regex extraction logic for 'Problem' nodes."""
    if not isinstance(text, str): return []
    text = text.lower()
    patterns = [
        r'\b(inefficiency|inefficient|slow|delay|bottleneck|latency)\b', 
        r'\b(error|mistake|inaccuracy|unreliable|failure|fault)\b', 
        r'\b(expensive|costly|high cost|resource intensive)\b', 
        r'\b(complex|complexity|difficult|challenging)\b', 
        r'\b(security risk|vulnerability|threat|breach|fraud)\b', 
        r'\b(scalability|scale|limitation|constraint)\b', 
        r'\b(manual|labor intensive|time consuming)\b', 
        r'\b(outdated|legacy|obsolete|old)\b', 
        r'\b(waste|pollution|emission|sustainability)\b'
    ]
    problems = set()
    for p in patterns:
        matches = re.findall(p, text)
        problems.update(matches)
    return list(problems)

def normalize_columns_v2(df):
    """Maps CSV headers to internal keys."""
    mappings = {
        'function_category': ['Function Category', 'Category', 'Func_Cat'],
        'specific_function': ['Specific Function', 'Source Function', 'Function Name'],
        'universal_function': ['Universal Function Class', 'Universal Function'],
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

# --- 4. PHASE 1: WORLD BUILDER (High Fidelity) ---

def build_industry_backbone(tx, row):
    # A. Core Hierarchy
    q_core = """
    MERGE (s:Sector {name: $sector})
    MERGE (d:Domain {name: $domain})
    MERGE (sub:SubIndustry {name: $sub_ind})
    SET sub.naics = $naics, 
        sub.classification = $classification,
        sub.horizontal_connection = $horiz
    MERGE (s)-[:CONTAINS]->(d)-[:CONTAINS]->(sub)
    """
    tx.run(q_core, sector=str(row.get('Sector','')), domain=str(row.get('Domain','')), 
           sub_ind=str(row.get('Sub-Industry','')), naics=str(row.get('NAICS Code','')),
           classification=str(row.get('Classification','')), horiz=str(row.get('Horizontal Connection','')))

    # B. Universal Bridge
    if row.get('Universal Function'):
        tx.run("MATCH (sub:SubIndustry {name: $sub}) MERGE (u:UniversalFunction {name: $univ}) MERGE (sub)-[:PERFORMS]->(u)", 
               sub=row['Sub-Industry'], univ=row['Universal Function'])

    # C. The 5-Layer Adjacency Web
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
        score = row.get(score_key, 0)
        
        if not tgt_dom and not tgt_sub: continue

        # Link Sub-to-Sub (High Precision)
        if tgt_sub:
            q_rel = f"""
            MATCH (src:SubIndustry {{name: $src}})
            MERGE (tgt_d:Domain {{name: $tgt_d}})
            MERGE (tgt_s:SubIndustry {{name: $tgt_s}})
            MERGE (tgt_d)-[:CONTAINS]->(tgt_s)
            MERGE (src)-[r:{rel}]->(tgt_s)
            SET r.weight = toFloat($score)
            """
            tx.run(q_rel, src=row['Sub-Industry'], tgt_d=tgt_dom, tgt_s=tgt_sub, score=score)
        # Fallback Link Sub-to-Domain
        elif tgt_dom:
            q_rel = f"""
            MATCH (src:SubIndustry {{name: $src}})
            MERGE (tgt_d:Domain {{name: $tgt_d}})
            MERGE (src)-[r:{rel}]->(tgt_d)
            SET r.weight = toFloat($score)
            """
            tx.run(q_rel, src=row['Sub-Industry'], tgt_d=tgt_dom, score=score)


def build_function_backbone(tx, row):
    # A. Core Hierarchy
    q_func = """
    MERGE (cat:FunctionCategory {name: $f_cat})
    MERGE (spec:SpecificFunction {name: $spec_f})
    MERGE (univ:UniversalFunction {name: $univ_f})
    MERGE (cat)-[:INCLUDES]->(spec)
    MERGE (spec)-[:CLASSIFIED_AS]->(univ)
    """
    tx.run(q_func, f_cat=str(row.get('function_category','')), spec_f=str(row.get('specific_function','')), univ_f=str(row.get('universal_function','')))

    # B. Mechanism Trinity (Exploding Nodes)
    mechanisms = [
        ('biology_mechanisms', 'BioMechanism', 'MIMICS'),
        ('physics_mechanisms', 'PhyMechanism', 'UTILIZES'),
        ('cyber_physical_mechanisms', 'CyberPhyMechanism', 'OPERATES_VIA')
    ]
    for col, label, rel in mechanisms:
        items = clean_list(str(row.get(col, '')), '|')
        for item in items:
            name_part = item.split('(')[0].strip()
            tx.run(f"MATCH (spec:SpecificFunction {{name: $spec}}) MERGE (m:{label} {{name: $name}}) SET m.full_text=$full MERGE (spec)-[:{rel}]->(m)", 
                   spec=row['specific_function'], name=name_part, full=item)

    # C. Equivalence Logic
    if row.get('Equivalent Domain'):
        tx.run("""
        MATCH (spec:SpecificFunction {name: $spec})
        MERGE (d:Domain {name: $dom})
        MERGE (spec)-[r:IS_EQUIVALENT_TO]->(d)
        SET r.layer = $layer, r.evidence = $evidence
        """, spec=row['specific_function'], dom=row['Equivalent Domain'], 
             layer=row.get('Equivalence Layer',''), evidence=row.get('transfer_evidence',''))

    # D. Stakeholders
    for sh in clean_list(str(row.get('key_stakeholders','')), '|'):
        tx.run("MATCH (spec:SpecificFunction {name: $spec}) MERGE (s:Stakeholder {name: $name}) MERGE (s)-[:INVESTS_IN]->(spec)", 
               spec=row['specific_function'], name=sh)


# --- 5. PHASE 2: PATENT POPULATOR (All-Inclusive) ---

def ingest_patent_complete(tx, doc):
    tax = doc.get('taxonomy_data', {})
    ind = tax.get('industry', {})
    func = tax.get('function', {})

    # Generate problems list from text on the fly
    full_text = f"{doc.get('title','')} {doc.get('abstract','')}"
    extracted_problems = extract_problems_from_text(full_text)

    # 1. PATENT NODE
    q_patent = """
    MERGE (p:Patent {id: $id})
    SET p.title = $title, p.abstract = $abstract, p.publication_date = $date,
        p.embedding = $embedding, p.num_claims = toInteger($num_claims),
        p.patent_type = $patent_type
    """
    
    # 2. TAXONOMY LINKS (With WITH Fix)
    q_tax = """
    MATCH (p:Patent {id: $id})
    
    // Industry Link
    MATCH (sub:SubIndustry) WHERE sub.naics = $naics OR sub.name = $sub_name
    MERGE (p)-[r1:BELONGS_TO]->(sub)
    SET r1.score = toFloat($ind_score)

    WITH p

    // Function Link
    MATCH (spec:SpecificFunction {name: $spec_name})
    MERGE (p)-[r2:USES_FUNCTION]->(spec)
    SET r2.score = toFloat($func_score),
        r2.constraint_boundaries = $constraints,
        r2.key_transfer_logic = $logic
    """

    # 3. ENTITY LINKS (Restored + Problems)
    q_entities = """
    MATCH (p:Patent {id: $id})

    FOREACH (n IN $inventors | MERGE (i:Inventor {name: n}) MERGE (p)-[:INVENTED_BY]->(i))
    FOREACH (n IN $assignees | MERGE (o:Organization {name: n}) MERGE (p)-[:ASSIGNED_TO]->(o))
    FOREACH (n IN $keywords | MERGE (k:Keyword {name: n}) MERGE (p)-[:HAS_KEYWORD]->(k))
    FOREACH (n IN $cpc | MERGE (c:CPCClass {code: n}) MERGE (p)-[:CLASSIFIED_AS_CPC]->(c))
    FOREACH (n IN $tech | MERGE (t:Technology {name: n}) MERGE (p)-[:USES_TECHNOLOGY]->(t))
    FOREACH (n IN $use_cases | MERGE (u:UseCase {description: n}) MERGE (p)-[:HAS_USE_CASE]->(u))
    FOREACH (n IN $problems | MERGE (pr:Problem {name: n}) MERGE (p)-[:ADDRESSES_PROBLEM]->(pr))
    """

    # Params
    params = {
        'id': str(doc['_id']),
        'title': doc.get('title', 'Untitled'),
        'abstract': doc.get('abstract', ''),
        'date': str(doc.get('publication_date', '')),
        'embedding': doc.get('embedding') or doc.get('ai_embeddings'),
        'num_claims': doc.get('num_claims', 0),
        'patent_type': doc.get('patent_type', ''),

        # Taxonomy
        'naics': str(ind.get('NAICS Code', '000000')),
        'sub_name': str(ind.get('Sub-Industry', 'Unclassified')),
        'ind_score': float(ind.get('match_score', 0)),
        'spec_name': str(func.get('Specific Function') or func.get('standard_specific_function', 'Unclassified')),
        'func_score': float(func.get('match_score', 0)),
        'constraints': str(func.get('Constraint Boundaries', '')),
        'logic': str(func.get('Key Transfer Logic', '')),

        # Entities
        'inventors': clean_list(doc.get('inventors'), ','),
        'assignees': clean_list(doc.get('assignee_org') or doc.get('assignee_names'), ';'),
        'keywords': clean_list(doc.get('keywords'), ','),
        'cpc': clean_list(doc.get('cpc_classifications'), ','),
        'tech': clean_list(doc.get('technology_stack'), ','),
        'use_cases': clean_list(doc.get('use_case_examples'), ';'),
        'problems': extracted_problems
    }

    tx.run(q_patent, **params)
    if params['sub_name'] != 'Unclassified': tx.run(q_tax, **params)
    tx.run(q_entities, **params)


# --- 6. MAIN EXECUTION ---
def main():
    with driver.session() as session:
        # A. BUILD INDUSTRY
        print("🏗️  Building Industry Backbone...")
        if os.path.exists(INDUSTRY_CSV):
            df = pd.read_csv(INDUSTRY_CSV).replace({np.nan: ""})
            for _, row in tqdm(df.iterrows(), total=len(df)):
                session.execute_write(build_industry_backbone, row)
        
        # B. BUILD FUNCTION
        print("🧬 Building Function Backbone & Trinity...")
        files = glob.glob(FUNCTION_CSV_DIR)
        for f in files:
            try:
                df = pd.read_csv(f).replace({np.nan: ""})
                df = normalize_columns_v2(df)
                for _, row in tqdm(df.iterrows(), total=len(df), leave=False):
                    session.execute_write(build_function_backbone, row)
            except: pass

        # C. CREATE INDEXES
        print("📐 Creating Indexes...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (n:Inventor) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Organization) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Keyword) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Technology) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:BioMechanism) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:SpecificFunction) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:SubIndustry) ON (n.name)",
            "CREATE VECTOR INDEX patent_embeddings IF NOT EXISTS FOR (p:Patent) ON (p.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 384, `vector.similarity_function`: 'cosine'}}"
        ]
        for idx in indexes: session.run(idx)

        # D. INGEST PATENTS
        print("🚀 Ingesting Patents & Connecting Entities...")
        cursor = mongo_col.find({"taxonomy_data": {"$exists": True}})
        total = mongo_col.count_documents({"taxonomy_data": {"$exists": True}})
        
        for doc in tqdm(cursor, total=total):
            # Ensure embedding is list
            emb = doc.get('embedding') or doc.get('ai_embeddings')
            if not emb: continue
            if hasattr(emb, 'tolist'): doc['embedding'] = emb.tolist()
            else: doc['embedding'] = emb
            
            session.execute_write(ingest_patent_complete, doc)

    print("✅ Complete! Graph now contains World, Citizens, and all 21 Relationship types.")

if __name__ == "__main__":
    main()