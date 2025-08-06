from flask import Flask, request, jsonify, render_template_string
from neo4j import GraphDatabase
import json
from sentence_transformers import SentenceTransformer

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

EMBEDDING_MODEL_NAME = 'all-MiniLM-L6-v2'
INDEX_NAME = "patentAiEmbeddingIndex" 
VECTOR_DIMENSION = 384

# ==============================================================================
# --- FLASK APP AND MODEL INITIALIZATION ---
# ==============================================================================
app = Flask(__name__)

try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("✅ Successfully connected to Neo4j.")
except Exception as e:
    print(f"❌ CRITICAL: Could not connect to Neo4j.\nError: {e}")
    driver = None

try:
    print(f"Loading model: {EMBEDDING_MODEL_NAME}...")
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    print("✅ Model loaded.")
except Exception as e:
    print(f"❌ CRITICAL: Could not load embedding model.\nError: {e}")
    embedding_model = None

# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================

def get_embedding(text):
    if not embedding_model:
        return None
    try:
        return embedding_model.encode(text, convert_to_tensor=False).tolist()
    except Exception as e:
        print(f"[ERROR] Failed to embed text. Error: {e}")
        return None

def format_graph_for_visjs(records):
    nodes = {}
    edges = {}

    for record in records:
        for node_key in ['p', 'n', 'm']:
            node = record.get(node_key)
            if node:
                node_id = node.element_id
                if node_id not in nodes:
                    label = list(node.labels)[0] if node.labels else 'Node'
                    name_prop = next((prop for prop in ['title', 'name', 'term', 'code'] if prop in node), 'id')
                    name = node.get(name_prop, node_id)
                    nodes[node_id] = {
                        "id": node_id,
                        "label": str(name)[:20] + ('...' if len(str(name)) > 20 else ''),
                        "title": f"Type: {label}\nValue: {name}",
                        "group": label.lower(),
                        "patent_id": node.get('patent_id')
                    }

        for rel_key in ['r', 'r1', 'r2']:
            rel = record.get(rel_key)
            if rel:
                try:
                    edges[rel.element_id] = {
                        "from": rel.start_node.element_id,
                        "to": rel.end_node.element_id,
                        "label": rel.type
                    }
                except Exception as e:
                    print(f"[DEBUG] Skipping malformed edge: {e}")

    print(f"[DEBUG] Total nodes: {len(nodes)}")
    print(f"[DEBUG] Total edges: {len(edges)}")
    if edges:
        print("Sample edge:", list(edges.values())[0])
    return {"nodes": list(nodes.values()), "edges": list(edges.values())}

# ==============================================================================
# --- FLASK ROUTES ---
# ==============================================================================

@app.route('/')
def index():
    with open('index.html', 'r') as f:
        return render_template_string(f.read())

@app.route('/search', methods=['POST'])
def search():
    if not driver or not embedding_model:
        return jsonify({"error": "Backend not ready."}), 500

    data = request.get_json()
    query_text = data.get('query')
    if not query_text:
        return jsonify({"error": "Query cannot be empty."}), 400

    query_vector = get_embedding(query_text)
    if not query_vector:
        return jsonify({"error": "Failed to generate embedding."}), 500

    with driver.session(database="neo4j") as session:
        try:
            vector_query = """
            CALL db.index.vector.queryNodes($index_name, 5, $query_vector)
            YIELD node, score
            RETURN node, score
            """
            result_patents = session.run(vector_query, index_name=INDEX_NAME, query_vector=query_vector).data()
        except Exception as e:
            return jsonify({"error": f"Vector search failed: {e}"}), 500

        if not result_patents:
            return jsonify({"documents": [], "graph": {"nodes": [], "edges": []}})

        documents = [
            {
                "id": record['node'].get('patent_id'),
                "title": record['node'].get('title'),
                "abstract": record['node'].get('ai_generated_abstract') or record['node'].get('abstract'),
                "score": round(record['score'], 4)
            } for record in result_patents
        ]
        patent_ids = [doc['id'] for doc in documents]

        # 🚀 New Single Query: 2-hop neighborhood
        try:
            graph_query = """
            MATCH (p:Patent) WHERE p.patent_id IN $patent_ids
            OPTIONAL MATCH (p)-[r1]-(n)
            OPTIONAL MATCH (n)-[r2]-(m)
            RETURN p, r1, n, r2, m
            """
            records = list(session.run(graph_query, patent_ids=patent_ids))
            graph_data = format_graph_for_visjs(records)
        except Exception as e:
            return jsonify({"error": f"Graph query failed: {e}"}), 500

    return jsonify({"documents": documents, "graph": graph_data})

# ==============================================================================
# --- MAIN ---
# ==============================================================================

if __name__ == '__main__':
    if driver and embedding_model:
        print("\n🚀 Server running on http://localhost:5001")
        app.run(debug=True, port=5001)
    else:
        print("❌ Server not started due to config errors.")
