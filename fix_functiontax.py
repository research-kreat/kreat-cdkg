import os
import json
import requests
import time
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = "taxonomy_functions"

# Ollama Settings
OLLAMA_API_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "herald/phi3-128k:latest"

# Connect to Mongo
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION_NAME]

def generate_fields_ollama(record):
    """
    Sends a prompt to the local Ollama instance to generate missing data.
    """
    
    # 1. Create Context
    context = f"""
    Sector: {record.get('sector')}
    Category: {record.get('function_category')}
    Function Name: {record.get('specific_function')}
    Universal Function: {record.get('universal_function')}
    Biology Mechanisms: {record.get('biology_mechanisms')}
    """

    # 2. Construct Prompt (Optimized for Phi-3)
    # Phi-3 follows a standard Instruct format, but for JSON tasks, 
    # explicit constraints work best.
    prompt = f"""You are a taxonomy expert. 
    Context:
    {context}

    Task:
    Generate two concise fields for this record:
    1. "risk_assessment": A 1-sentence summary of potential risks (operational, financial, or safety).
    2. "regulatory_compliance": A 1-sentence summary of relevant compliance standards (ISO, FDA, OSHA, etc).

    Output Requirement:
    Return ONLY valid JSON. Do not include markdown ticks (```json).
    Example:
    {{
        "risk_assessment": "Potential for chemical spills requiring containment.",
        "regulatory_compliance": "Must adhere to OSHA and REACH standards."
    }}
    """

    # 3. Payload for Ollama
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",  # Forces Ollama to output valid JSON
        "options": {
            "temperature": 0.3, # Low temp for consistency
            "num_ctx": 2048     # Ensure context window is sufficient
        }
    }

    try:
        # 4. Send Request
        response = requests.post(OLLAMA_API_URL, json=payload, timeout=60)
        response.raise_for_status()
        
        # 5. Parse Response
        result = response.json()
        raw_text = result.get("response", "")
        
        # Try to parse the JSON string from the model
        return json.loads(raw_text)

    except json.JSONDecodeError:
        print(f"  ⚠️ JSON Parse Error. Model Output: {raw_text[:50]}...")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ Ollama Connection Error: {e}")
        return None

def run_enrichment():
    # Find records where EITHER field is null or empty
    query = {
        "$or": [
            {"risk_assessment": None},
            {"regulatory_compliance": None},
            {"risk_assessment": ""},
            {"regulatory_compliance": ""}
        ]
    }
    
    total_missing = col.count_documents(query)
    print(f"🚀 Found {total_missing} records needing enrichment via Ollama ({OLLAMA_MODEL})...")
    
    if total_missing == 0:
        print("✅ No records need enrichment.")
        return

    cursor = col.find(query)
    
    # Progress bar
    pbar = tqdm(total=total_missing, desc="Generating Data")
    
    updated_count = 0
    
    for doc in cursor:
        try:
            generated_data = generate_fields_ollama(doc)
            
            if generated_data:
                # Validate keys exist before updating
                risk = generated_data.get("risk_assessment")
                compliance = generated_data.get("regulatory_compliance")

                if risk and compliance:
                    col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "risk_assessment": risk,
                            "regulatory_compliance": compliance
                        }}
                    )
                    updated_count += 1
            
        except Exception as e:
            print(f"❌ Critical Error on doc {doc.get('_id')}: {e}")
        
        pbar.update(1)

    print(f"\n🎉 Enrichment Complete! Updated {updated_count} records.")

if __name__ == "__main__":
    # Simple check to see if Ollama is running
    try:
        requests.get("http://localhost:11434")
        run_enrichment()
    except requests.exceptions.ConnectionError:
        print("❌ Error: Could not connect to Ollama at http://localhost:11434")
        print("   Please ensure 'ollama serve' is running.")