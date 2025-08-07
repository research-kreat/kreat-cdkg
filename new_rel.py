# File: push_dense_kg_actual.py
# 
# REQUIREMENTS: pip install neo4j pandas numpy scikit-learn
#

from neo4j import GraphDatabase
import pandas as pd
import logging
import re
import numpy as np
import ast
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# CONFIG
# ----------------------------
NEO4J_URI = "bolt://135.235.170.159:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "cHRE3FjhhxvkGNH"

# ----------------------------
# CONNECT
# ----------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ----------------------------
# TEXT PROCESSING FOR DENSE EXTRACTION
# ----------------------------
def extract_technologies_from_text(text):
    """Extract technology mentions from text using pattern matching"""
    if pd.isna(text):
        return []
    
    text = str(text).lower()
    tech_patterns = [
        r'\b(ai|artificial intelligence|machine learning|ml|deep learning|neural network|cnn|rnn|lstm)\b',
        r'\b(blockchain|cryptocurrency|bitcoin|ethereum|smart contract)\b',
        r'\b(iot|internet of things|sensor|rfid|nfc|bluetooth|wifi)\b',
        r'\b(cloud computing|aws|azure|google cloud|saas|paas|iaas)\b',
        r'\b(quantum computing|quantum|qbit|quantum algorithm)\b',
        r'\b(computer vision|image processing|opencv|pattern recognition)\b',
        r'\b(nlp|natural language processing|text mining|sentiment analysis)\b',
        r'\b(robotics|robot|autonomous|drone|uav)\b',
        r'\b(5g|4g|lte|telecommunications|wireless|cellular)\b',
        r'\b(ar|vr|augmented reality|virtual reality|mixed reality)\b'
    ]
    
    technologies = set()
    for pattern in tech_patterns:
        matches = re.findall(pattern, text)
        technologies.update(matches)
    
    return list(technologies)

def extract_domains_from_text(text):
    """Extract application domains from text"""
    if pd.isna(text):
        return []
    
    text = str(text).lower()
    domain_patterns = [
        r'\b(healthcare|medical|hospital|clinical|diagnosis|treatment|pharmaceutical)\b',
        r'\b(automotive|vehicle|car|transportation|traffic|navigation)\b',
        r'\b(manufacturing|factory|industrial|production|assembly|quality control)\b',
        r'\b(finance|banking|financial|trading|investment|payment|fintech)\b',
        r'\b(agriculture|farming|crop|livestock|precision agriculture)\b',
        r'\b(energy|power|electricity|renewable|solar|wind|battery)\b',
        r'\b(telecommunications|telecom|network|communication|5g|wireless)\b',
        r'\b(retail|e-commerce|shopping|consumer|customer|marketplace)\b',
        r'\b(education|learning|training|university|school|e-learning)\b',
        r'\b(security|cybersecurity|encryption|authentication|privacy)\b'
    ]
    
    domains = set()
    for pattern in domain_patterns:
        matches = re.findall(pattern, text)
        domains.update(matches)
    
    return list(domains)

def extract_problems_from_text(text):
    """Extract problems/challenges from text"""
    if pd.isna(text):
        return []
    
    text = str(text).lower()
    problem_patterns = [
        r'\b(inefficiency|inefficient|slow|delay|bottleneck)\b',
        r'\b(error|mistake|inaccuracy|unreliable|failure)\b',
        r'\b(expensive|costly|high cost|resource intensive)\b',
        r'\b(complex|complexity|difficult|challenging)\b',
        r'\b(security risk|vulnerability|threat|breach)\b',
        r'\b(scalability|scale|limitation|constraint)\b',
        r'\b(manual|labor intensive|time consuming)\b',
        r'\b(outdated|legacy|obsolete|old)\b'
    ]
    
    problems = set()
    for pattern in problem_patterns:
        matches = re.findall(pattern, text)
        problems.update(matches)
    
    return list(problems)

def parse_list_field(field, separators=[';', ',', '|']):
    """Parse list fields with multiple possible separators"""
    if pd.isna(field) or field == '':
        return []
    
    field_str = str(field).strip()
    if not field_str:
        return []
    
    # Try each separator
    for sep in separators:
        if sep in field_str:
            return [x.strip() for x in field_str.split(sep) if x.strip()]
    
    # Single value
    return [field_str] if field_str else []

def parse_embedding(embedding_str):
    """Parse embedding string to numpy array"""
    if pd.isna(embedding_str) or embedding_str == '':
        return None
    
    try:
        embedding_str = str(embedding_str).strip()
        
        if embedding_str.startswith('[') and embedding_str.endswith(']'):
            embedding = ast.literal_eval(embedding_str)
            return np.array(embedding, dtype=np.float32)
        
        elif ',' in embedding_str:
            values = [float(x.strip()) for x in embedding_str.split(',')]
            return np.array(values, dtype=np.float32)
        
        elif ' ' in embedding_str:
            values = [float(x.strip()) for x in embedding_str.split()]
            return np.array(values, dtype=np.float32)
        
        else:
            logger.warning(f"Could not parse embedding: {embedding_str[:100]}...")
            return None
            
    except Exception as e:
        logger.warning(f"Error parsing embedding: {e}")
        return None

def calculate_embedding_similarity(embeddings_list, similarity_threshold=0.7):
    """Calculate cosine similarity between embeddings and return similar pairs"""
    if len(embeddings_list) < 2:
        return []
    
    valid_embeddings = []
    valid_indices = []
    
    for i, (patent_id, embedding) in enumerate(embeddings_list):
        if embedding is not None and len(embedding) > 0:
            valid_embeddings.append(embedding)
            valid_indices.append((i, patent_id))
    
    if len(valid_embeddings) < 2:
        logger.warning("Not enough valid embeddings for similarity calculation")
        return []
    
    try:
        embeddings_matrix = np.vstack(valid_embeddings)
        similarity_matrix = cosine_similarity(embeddings_matrix)
        
        similar_pairs = []
        n = len(valid_embeddings)
        
        for i in range(n):
            for j in range(i + 1, n):
                similarity_score = similarity_matrix[i, j]
                if similarity_score >= similarity_threshold:
                    patent_id_1 = valid_indices[i][1]
                    patent_id_2 = valid_indices[j][1]
                    similar_pairs.append({
                        'patent_1': patent_id_1,
                        'patent_2': patent_id_2,
                        'similarity': float(similarity_score)
                    })
        
        logger.info(f"Found {len(similar_pairs)} embedding-similar patent pairs (threshold: {similarity_threshold})")
        return similar_pairs
        
    except Exception as e:
        logger.error(f"Error calculating embedding similarity: {e}")
        return []

# ----------------------------
# CORRECTED CREATE ULTRA-DENSE GRAPH FUNCTIONS
# ----------------------------
def create_ultra_dense_patent(tx, patent_data):
    """Create patent with extremely dense relationships, including embedding."""
    
    query = """
    MERGE (p:Patent {id: $id})
    SET p.title = $title,
        p.abstract = $abstract,
        p.full_text = $full_text,
        p.domain = $domain,
        p.sub_domain = $sub_domain,
        p.knowledge_type = $knowledge_type,
        p.relevance_score = $relevance_score,
        p.data_quality_score = $data_quality_score,
        p.patent_type = $patent_type,
        p.num_claims = $num_claims,
        p.summary = $summary,
        p.country = $country,
        p.publication_date = $publication_date,
        p.has_embedding = $has_embedding,
        p.created_at = datetime()

    // Correctly set the embedding property if it exists
    WITH p
    WHERE $embedding IS NOT NULL
    SET p.embedding = $embedding
    
    // Remaining relationships
    FOREACH (tech IN $technology_stack | MERGE (t:Technology {name: tech}) MERGE (p)-[:USES_TECHNOLOGY {context: 'stack'}]->(t))
    FOREACH (tech IN $extracted_technologies | MERGE (t:Technology {name: tech}) MERGE (p)-[:MENTIONS_TECHNOLOGY {context: 'text_analysis'}]->(t))
    FOREACH (kw IN $keywords | MERGE (k:Keyword {name: kw}) MERGE (p)-[:HAS_KEYWORD]->(k))
    FOREACH (dom IN $application_domains | MERGE (d:ApplicationDomain {name: dom}) MERGE (p)-[:APPLIES_TO_DOMAIN]->(d))
    FOREACH (prob IN $problems | MERGE (pr:Problem {name: prob}) MERGE (p)-[:ADDRESSES_PROBLEM]->(pr))
    FOREACH (author IN $authors | MERGE (a:Author {name: author}) MERGE (p)-[:AUTHORED_BY]->(a))
    FOREACH (inv IN $inventors | MERGE (i:Inventor {name: inv}) MERGE (p)-[:INVENTED_BY]->(i))
    FOREACH (org IN $assignee_names | MERGE (o:Organization {name: org}) MERGE (p)-[:ASSIGNED_TO]->(o))
    FOREACH (ipc IN $ipc_classifications | MERGE (ic:IPCClass {code: ipc}) MERGE (p)-[:CLASSIFIED_AS_IPC]->(ic))
    FOREACH (cpc IN $cpc_classifications | MERGE (cc:CPCClass {code: cpc}) MERGE (p)-[:CLASSIFIED_AS_CPC]->(cc))
    FOREACH (uc IN $use_cases | MERGE (u:UseCase {name: uc}) MERGE (p)-[:HAS_USE_CASE]->(u))
    FOREACH (trend IN $market_trends | MERGE (mt:MarketTrend {name: trend}) MERGE (p)-[:RELATES_TO_TREND]->(mt))
    
    WITH p
    WHERE $publisher IS NOT NULL
    MERGE (pub:Publisher {name: $publisher})
    MERGE (p)-[:PUBLISHED_BY]->(pub)

    WITH p
    WHERE $journal_name IS NOT NULL
    MERGE (j:Journal {name: $journal_name})
    MERGE (p)-[:PUBLISHED_IN]->(j)
    """
    
    result = tx.run(query, **patent_data)
    return result.consume().counters

def create_multi_level_relationships(tx):
    """Create dense multi-level relationships across the entire graph"""
    
    relationship_queries = [
        {
            'name': 'Technology-Problem Solving',
            'query': """
            MATCH (p:Patent)-[:USES_TECHNOLOGY|MENTIONS_TECHNOLOGY]->(t:Technology)
            MATCH (p)-[:ADDRESSES_PROBLEM]->(pr:Problem)
            MERGE (t)-[:SOLVES_PROBLEM]->(pr)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Cross-Domain Technology Transfer',
            'query': """
            MATCH (p1:Patent)-[:USES_TECHNOLOGY]->(t:Technology)<-[:USES_TECHNOLOGY]-(p2:Patent)
            WHERE p1.domain <> p2.domain AND p1.id < p2.id
            MERGE (p1)-[:SHARES_TECHNOLOGY {technology: t.name, cross_domain: true}]->(p2)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Author-Technology Expertise',
            'query': """
            MATCH (a:Author)<-[:AUTHORED_BY]-(p:Patent)-[:USES_TECHNOLOGY]->(t:Technology)
            MERGE (a)-[:EXPERT_IN]->(t)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Organization-Domain Focus',
            'query': """
            MATCH (o:Organization)<-[:ASSIGNED_TO]-(p:Patent)-[:APPLIES_TO_DOMAIN]->(d:ApplicationDomain)
            MERGE (o)-[:FOCUSES_ON]->(d)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Keyword-Technology Correlation',
            'query': """
            MATCH (k:Keyword)<-[:HAS_KEYWORD]-(p:Patent)-[:USES_TECHNOLOGY]->(t:Technology)
            MERGE (k)-[:CORRELATES_WITH]->(t)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Problem-Solution Clustering',
            'query': """
                CALL apoc.periodic.iterate(
                'MATCH (pr:Problem) RETURN pr',
                'MATCH (p1:Patent)-[:ADDRESSES_PROBLEM]->(pr)
                MATCH (p2:Patent)-[:ADDRESSES_PROBLEM]->(pr)
                WHERE id(p1) < id(p2)
                MERGE (p1)-[:ADDRESSES_SAME_PROBLEM {problem: pr.name}]->(p2)',
                {batchSize: 500}
        )
        """
        },
        
        {
            'name': 'Classification Similarity',
            'query': """
            MATCH (p1:Patent)-[:CLASSIFIED_AS_CPC]->(c:CPCClass)<-[:CLASSIFIED_AS_CPC]-(p2:Patent)
            WHERE p1.id < p2.id
            MERGE (p1)-[:SIMILAR_CLASSIFICATION {cpc_class: c.code}]->(p2)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Use Case Overlap',
            'query': """
            MATCH (p1:Patent)-[:HAS_USE_CASE]->(u:UseCase)<-[:HAS_USE_CASE]-(p2:Patent)
            WHERE p1.id < p2.id
            MERGE (p1)-[:OVERLAPPING_USE_CASE {use_case: u.name}]->(p2)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Market Trend Connections',
            'query': """
            MATCH (p1:Patent)-[:RELATES_TO_TREND]->(mt:MarketTrend)<-[:RELATES_TO_TREND]-(p2:Patent)
            WHERE p1.id < p2.id
            MERGE (p1)-[:FOLLOWS_SAME_TREND {trend: mt.name}]->(p2)
            RETURN count(*) as created
            """
        },
        
        {
            'name': 'Inventor Collaboration Network',
            'query': """
            MATCH (i1:Inventor)<-[:INVENTED_BY]-(p:Patent)-[:INVENTED_BY]->(i2:Inventor)
            WHERE i1.name < i2.name
            WITH i1, i2, count(p) as collaboration_count
            MERGE (i1)-[:COLLABORATED_WITH {patents: collaboration_count}]->(i2)
            RETURN count(*) as created
            """
        }
    ]
    
    total_created = 0
    for rel_config in relationship_queries:
        result = tx.run(rel_config['query'])
        count = result.single()['created']
        total_created += count
        logger.info(f"✅ {rel_config['name']}: {count} relationships created")
    
    return total_created

def create_embedding_based_relationships(tx, similar_pairs):
    """Create embedding-based semantic similarity relationships"""
    if not similar_pairs:
        logger.warning("No embedding similarity pairs to process")
        return 0
    
    batch_size = 1000
    total_created = 0
    
    for i in range(0, len(similar_pairs), batch_size):
        batch = similar_pairs[i:i + batch_size]
        
        query = """
        UNWIND $pairs as pair
        MATCH (p1:Patent {id: pair.patent_1})
        MATCH (p2:Patent {id: pair.patent_2})
        WHERE NOT EXISTS((p1)-[:SEMANTICALLY_SIMILAR]-(p2))
        MERGE (p1)-[:SEMANTICALLY_SIMILAR {
            similarity_score: pair.similarity,
            method: 'embedding_cosine',
            created_at: datetime()
        }]->(p2)
        RETURN count(*) as created
        """
        
        result = tx.run(query, pairs=batch)
        batch_created = result.single()['created']
        total_created += batch_created
        
        logger.info(f"Created {batch_created} semantic similarity relationships (batch {i//batch_size + 1})")
    
    return total_created

def create_embedding_clusters(tx, similar_pairs, min_cluster_size=3):
    """Create embedding-based clusters for dense patent groupings"""
    if not similar_pairs:
        return 0
    
    similarity_graph = defaultdict(set)
    all_patents = set()
    
    for pair in similar_pairs:
        p1, p2 = pair['patent_1'], pair['patent_2']
        similarity_graph[p1].add(p2)
        similarity_graph[p2].add(p1)
        all_patents.add(p1)
        all_patents.add(p2)
    
    visited = set()
    clusters = []
    
    def dfs(patent, current_cluster):
        if patent in visited:
            return
        visited.add(patent)
        current_cluster.append(patent)
        for neighbor in similarity_graph[patent]:
            dfs(neighbor, current_cluster)
    
    for patent in all_patents:
        if patent not in visited:
            cluster = []
            dfs(patent, cluster)
            if len(cluster) >= min_cluster_size:
                clusters.append(cluster)
    
    logger.info(f"Found {len(clusters)} embedding-based clusters")
    
    total_created = 0
    for i, cluster in enumerate(clusters):
        cluster_id = f"embedding_cluster_{i}"
        
        query = """
        MERGE (c:EmbeddingCluster {id: $cluster_id})
        SET c.size = $cluster_size,
            c.created_at = datetime()
        
        WITH c
        UNWIND $patents as patent_id
        MATCH (p:Patent {id: patent_id})
        MERGE (p)-[:BELONGS_TO_CLUSTER]->(c)
        RETURN count(*) as created
        """
        
        result = tx.run(query, 
                       cluster_id=cluster_id, 
                       cluster_size=len(cluster),
                       patents=cluster)
        
        cluster_created = result.single()['created']
        total_created += cluster_created
        
        logger.info(f"Created cluster {cluster_id} with {len(cluster)} patents")
    
    return total_created

def create_indexes(tx):
    """Create comprehensive indexes"""
    indexes = [
        "CREATE INDEX patent_id_idx IF NOT EXISTS FOR (p:Patent) ON (p.id)",
        "CREATE INDEX patent_domain_idx IF NOT EXISTS FOR (p:Patent) ON (p.domain)",
        "CREATE INDEX technology_name_idx IF NOT EXISTS FOR (t:Technology) ON (t.name)",
        "CREATE INDEX keyword_name_idx IF NOT EXISTS FOR (k:Keyword) ON (k.name)",
        "CREATE INDEX author_name_idx IF NOT EXISTS FOR (a:Author) ON (a.name)",
        "CREATE INDEX inventor_name_idx IF NOT EXISTS FOR (i:Inventor) ON (i.name)",
        "CREATE INDEX org_name_idx IF NOT EXISTS FOR (o:Organization) ON (o.name)",
        "CREATE INDEX problem_name_idx IF NOT EXISTS FOR (p:Problem) ON (p.name)",
        "CREATE INDEX domain_name_idx IF NOT EXISTS FOR (d:ApplicationDomain) ON (d.name)",
        "CREATE INDEX usecase_name_idx IF NOT EXISTS FOR (u:UseCase) ON (u.name)"
    ]
    
    for index_query in indexes:
        try:
            tx.run(index_query)
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

# ----------------------------
# MAIN PROCESSING
# ----------------------------
if __name__ == "__main__":
    try:
        logger.info("📖 Reading CSV file...")
        df = pd.read_csv("mongo_full_export.csv")
        logger.info(f"Loaded {len(df)} records")

        logger.info("🧮 Processing embeddings...")
        embeddings_list = []
        for _, row in df.iterrows():
            patent_id = str(row['patent_id'])
            embedding = parse_embedding(row.get('embedding') or row.get('ai_embeddings'))
            embeddings_list.append((patent_id, embedding))
        
        logger.info("🔍 Calculating embedding similarities...")
        similar_pairs = calculate_embedding_similarity(embeddings_list, similarity_threshold=0.75)

        patents_data = []
        for _, row in df.iterrows():
            full_text = str(row.get('full_text', '')) + ' ' + str(row.get('abstract', ''))
            
            embedding = parse_embedding(row.get('embedding') or row.get('ai_embeddings'))
            has_embedding = embedding is not None

            patent_data = {
                'id': str(row['_id']),
                'title': str(row.get('title', '')),
                'abstract': str(row.get('abstract', '')),
                'full_text': str(row.get('full_text', '')),
                'domain': str(row.get('domain', '')),
                'sub_domain': str(row.get('sub_domain', '')),
                'knowledge_type': str(row.get('knowledge_type', '')),
                'relevance_score': float(row.get('relevance_score', 0.0)) if pd.notna(row.get('relevance_score')) else 0.0,
                'data_quality_score': float(row.get('data_quality_score', 0.0)) if pd.notna(row.get('data_quality_score')) else 0.0,
                'patent_type': str(row.get('patent_type', '')),
                'num_claims': int(row.get('num_claims', 0)) if pd.notna(row.get('num_claims')) else 0,
                'summary': str(row.get('summary', '')),
                'country': str(row.get('country', '')),
                'publication_date': str(row.get('publication_date', '')),
                'publisher': str(row.get('publisher', '')) if pd.notna(row.get('publisher')) else None,
                'journal_name': str(row.get('journal_name', '')) if pd.notna(row.get('journal_name')) else None,
                'has_embedding': has_embedding,
                'embedding': embedding, # Pass the embedding vector here
                'technology_stack': parse_list_field(row.get('technology_stack', '')),
                'keywords': parse_list_field(row.get('keywords', '')),
                'authors': parse_list_field(row.get('authors', '')),
                'inventors': parse_list_field(row.get('inventors', '')),
                'assignee_names': parse_list_field(row.get('assignee_names', '')),
                'ipc_classifications': parse_list_field(row.get('ipc_classifications', '')),
                'cpc_classifications': parse_list_field(row.get('cpc_classifications', '')),
                'use_cases': parse_list_field(row.get('use_case_examples', '')),
                'market_trends': parse_list_field(row.get('market_trends', '')),
                'extracted_technologies': extract_technologies_from_text(full_text),
                'application_domains': extract_domains_from_text(full_text),
                'problems': extract_problems_from_text(full_text)
            }
            patents_data.append(patent_data)

        with driver.session() as session:
            logger.info("🏗️  Creating indexes...")
            session.execute_write(create_indexes)
            
            logger.info("🏗️  Creating patent nodes and direct relationships...")
            for i, patent_data in enumerate(patents_data):
                session.execute_write(create_ultra_dense_patent, patent_data)
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(patents_data)} patents")
            
            logger.info("🕸️  Creating multi-level dense relationships...")
            total_multi_level = session.execute_write(create_multi_level_relationships)
            logger.info(f"Created {total_multi_level} multi-level relationships")
            
            if similar_pairs:
                logger.info("🧠 Creating embedding-based semantic relationships...")
                embedding_rels = session.execute_write(create_embedding_based_relationships, similar_pairs)
                logger.info(f"Created {embedding_rels} embedding-based relationships")
                
                logger.info("🎯 Creating embedding-based clusters...")
                cluster_rels = session.execute_write(create_embedding_clusters, similar_pairs)
                logger.info(f"Created {cluster_rels} cluster relationships")
            else:
                logger.warning("No embedding similarities found - skipping embedding-based relationships")

        with driver.session() as session:
            node_result = session.run("MATCH (n) RETURN labels(n)[0] as node_type, count(n) as count ORDER BY count DESC")
            logger.info("\n📊 FINAL GRAPH STATISTICS:")
            logger.info("=" * 40)
            logger.info("NODES:")
            total_nodes = 0
            for record in node_result:
                count = record['count']
                total_nodes += count
                logger.info(f"  {record['node_type']:20}: {count:6,}")
            
            rel_result = session.run("MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC")
            logger.info("\nRELATIONSHIPS:")
            total_rels = 0
            for record in rel_result:
                count = record['count']
                total_rels += count
                logger.info(f"  {record['rel_type']:30}: {count:6,}")
            
            logger.info("=" * 40)
            logger.info(f"TOTAL NODES: {total_nodes:,}")
            logger.info(f"TOTAL RELATIONSHIPS: {total_rels:,}")
            logger.info(f"DENSITY RATIO: {total_rels/total_nodes:.2f} relationships per node")

        logger.info("\n🎉 ULTRA-DENSE KNOWLEDGE GRAPH CREATED SUCCESSFULLY!")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise
    finally:
        driver.close()