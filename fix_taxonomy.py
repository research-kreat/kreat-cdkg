import os
import glob
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI") 
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = "taxonomy_functions"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
FUNCTION_CSV_DIR = "/Users/user/Downloads/CDKG - DATA/*.csv"

# --- THE GOLDEN MAPPING ---
# Key = The final field name in MongoDB
# Value = List of possible column names in the CSVs (Priority Order)
FIELD_MAPPING = {
    "sector": ["Sector"],
    
    "function_category": [
        "Category", 
        "Function Category", 
        "Sub-Industry", # In some files, sub-industry acts as the category
        "Subcategory"   # Used in Basic Sciences / Oil Gas
    ],
    
    "specific_function": [
        "Source Function", 
        "Specific Function"
    ],
    
    "universal_function": [
        "Universal Function Class"
    ],
    
    "equivalent_domain": [
        "Equivalent Domain"
    ],
    
    "equivalent_function": [
        "Equivalent Function",
        "Target Function" # Found in Basic Sciences / Oil Gas
    ],
    
    "equivalence_layer": [
        "Equivalence Layer",
        "Functional Equivalence" # Found in Basic Sciences / Oil Gas
    ],
    
    "transfer_evidence": ["Transfer Evidence"],
    
    "strength_score": ["Strength Score"],
    
    "constraint_boundaries": ["Constraint Boundaries"],
    
    "key_transfer_logic": ["Key Transfer Logic"],
    
    # Mechanism Fields
    "biology_mechanisms": ["Biology Mechanisms"],
    "cyber_physical_mechanisms": ["Cyber-Physical Mechanisms"],
    "physics_engineering_mechanisms": ["Physics/Engineering Mechanisms"],
    
    # Metadata Fields
    "stakeholders": ["Key Stakeholders", "Stakeholder Mapping"],
    "temporal_analysis": ["Temporal Evolution", "Temporal Analysis"],
    "regulatory_compliance": ["Regulatory & Compliance Mapping"],
    "risk_assessment": ["Risk Assessment"]
}

def clean_text(val):
    """Helper to clean strings and handle NaNs"""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s != "" and s.lower() != "nan" else None

def get_mapped_value(row, possible_columns):
    """
    Scans the row for any of the possible_columns. 
    Returns the first valid value found.
    """
    # Create a lowercase map of the row's keys for case-insensitive matching
    row_lower = {k.lower().strip(): v for k, v in row.items()}
    
    for col in possible_columns:
        clean_col = col.lower().strip()
        if clean_col in row_lower:
            val = clean_text(row_lower[clean_col])
            if val:
                return val
    return None

def ingest_consistent_taxonomy():
    # 1. Setup
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]
    
    print(f"⬇️ Loading Model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)

    # 2. Reset Collection
    print("🧹 Wiping existing collection to ensure consistency...")
    col.delete_many({})

    # 3. Process Files
    files = glob.glob(FUNCTION_CSV_DIR)
    if not files:
        print("❌ No files found.")
        return

    all_clean_records = []
    
    print(f"🚀 Processing {len(files)} files...")
    
    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            df = pd.read_csv(filepath)
            
            # Iterating rows
            for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Reading {filename[:15]}"):
                row_dict = row.to_dict()
                
                # --- BUILD THE GOLDEN RECORD ---
                clean_record = {}
                
                # Apply the mapping
                for mongo_key, csv_options in FIELD_MAPPING.items():
                    clean_record[mongo_key] = get_mapped_value(row_dict, csv_options)

                # --- HANDLING MISSING SECTOR ---
                # Some files like 'Basic_Sciences...' don't have a 'Sector' column.
                # We can infer it from the filename if it's missing.
                if not clean_record['sector']:
                    # Extract "Basic_Sciences" from "Basic_Sciences_Comprehensive_V2.csv"
                    clean_record['sector'] = filename.split('_')[0]

                # --- VALIDATION ---
                # If specific function is missing, the record is useless
                if not clean_record['specific_function']:
                    continue

                # --- EMBEDDING GENERATION ---
                # Create a rich text representation for the vector
                # We join specific function, universal class, mechanisms, and evidence
                text_parts = [
                    clean_record['specific_function'] or "",
                    clean_record['universal_function'] or "",
                    clean_record['biology_mechanisms'] or "",
                    clean_record['physics_engineering_mechanisms'] or "",
                    clean_record['transfer_evidence'] or ""
                ]
                signature_text = " ".join(text_parts).strip()
                
                if len(signature_text) < 5:
                    clean_record['embedding'] = None
                else:
                    clean_record['embedding'] = model.encode(signature_text).tolist()

                # Add metadata
                clean_record['signature_text'] = signature_text
                clean_record['source_file'] = filename

                all_clean_records.append(clean_record)

        except Exception as e:
            print(f"❌ Error in {filename}: {e}")

    # 4. Insert
    if all_clean_records:
        print(f"\n💾 Inserting {len(all_clean_records)} consistent records...")
        
        # Batch insert
        batch_size = 1000
        for i in range(0, len(all_clean_records), batch_size):
            col.insert_many(all_clean_records[i:i + batch_size])
            
        print("✅ Ingestion Complete.")
        
        # 5. Consistency Check
        sample = col.find_one()
        print("\n🔍 Sample Record Structure (Golden Schema):")
        print(list(sample.keys()))
    else:
        print("⚠️ No records processed.")

if __name__ == "__main__":
    ingest_consistent_taxonomy()