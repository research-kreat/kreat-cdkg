import os
import glob
import pandas as pd
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")  # Replace with your Atlas URI
DB_NAME = os.getenv("MONGO_DB")
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Paths to your CSVs
INDUSTRY_CSV_PATH = "/Users/user/Downloads/MASTER_INDUSTRY_TAXONOMY_COMPLETE_989_Functions.csv"
FUNCTION_CSV_DIR = "/Users/user/Downloads/CDKG - DATA/*.csv" # Glob pattern for all 14 files

# Connect to Mongo
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_industry = db["taxonomy_industries"]
col_function = db["taxonomy_functions"]

# Load Model
print(f"Loading embedding model: {EMBEDDING_MODEL}...")
model = SentenceTransformer(EMBEDDING_MODEL)

def normalize_columns(df, filename):
    """
    Standardizes column names across different CSV formats.
    """
    # Define mapping variations (Lower case for easier matching)
    column_mappings = {
        'function_category': ['function category', 'category', 'func_cat'],
        'specific_function': ['specific function', 'source function', 'function', 'function name'],
        'universal_function': ['universal function class', 'universal function', 'universal_class'],
        'biology_mechanisms': ['biology mechanisms', 'biological mechanisms', 'biology'],
        'physics_mechanisms': ['physics/engineering mechanisms', 'physics mechanisms', 'engineering mechanisms']
    }

    # Create a cleaner version of current columns
    current_cols = {c.lower().strip(): c for c in df.columns}
    
    normalized_data = {}
    
    # Try to find each required field
    for standard_name, variations in column_mappings.items():
        found_col = None
        for var in variations:
            if var in current_cols:
                found_col = current_cols[var]
                break
        
        if found_col:
            normalized_data[standard_name] = df[found_col]
        else:
            # If a critical column is missing, fill with empty strings to prevent crashes
            # valid for mechanisms, but we should log warning for core fields
            if standard_name in ['function_category', 'specific_function', 'universal_function']:
                 print(f"⚠️ Warning: Critical column '{standard_name}' not found in {os.path.basename(filename)}. Variations checked: {variations}")
            normalized_data[standard_name] = pd.Series([""] * len(df))

    # Return a new standardize DataFrame (keeping original data for other fields is tricky if we just want embeddings, 
    # but let's return a dataframe with standard names + original names to keep context)
    
    norm_df = pd.DataFrame(normalized_data)
    # Join with original df to keep extra metadata like 'Equivalent Domain' etc.
    return pd.concat([df, norm_df], axis=1)

def ingest_industry_taxonomy():
    print("\n--- Processing Industry Taxonomy ---")
    if not os.path.exists(INDUSTRY_CSV_PATH):
        print(f"Skipping Industry: File not found at {INDUSTRY_CSV_PATH}")
        return

    df = pd.read_csv(INDUSTRY_CSV_PATH)
    records = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Vectorizing Industries"):
        # Handle NaN values gracefully
        sector = str(row.get('Sector', '')).strip()
        domain = str(row.get('Domain', '')).strip()
        sub_ind = str(row.get('Sub-Industry', '')).strip()
        func = str(row.get('Function', '')).strip()
        univ = str(row.get('Universal Function', '')).strip()

        signature_text = f"{sector} {domain} {sub_ind} {func} {univ}"
        
        embedding = model.encode(signature_text).tolist()
        
        record = row.to_dict()
        # Clean NaNs in the record for Mongo
        record = {k: (v if pd.notna(v) else None) for k, v in record.items()}
        
        record['embedding'] = embedding
        record['signature_text'] = signature_text
        records.append(record)
    
    if records:
        col_industry.delete_many({}) 
        col_industry.insert_many(records)
        print(f"✅ Inserted {len(records)} industry records.")

def ingest_function_taxonomy():
    print("\n--- Processing Function Taxonomy (14 Files) ---")
    files = glob.glob(FUNCTION_CSV_DIR)
    
    if not files:
        print(f"Error: No CSV files found in {FUNCTION_CSV_DIR}")
        return

    total_inserted = 0
    col_function.delete_many({}) # Clear old data

    for filepath in files:
        filename = os.path.basename(filepath)
        print(f"Reading: {filename}")
        try:
            df = pd.read_csv(filepath)
            
            # Normalize columns before processing
            df_norm = normalize_columns(df, filepath)
            
            records = []
            for _, row in tqdm(df_norm.iterrows(), total=len(df_norm), desc="Vectorizing"):
                
                # Use the normalized column names
                f_cat = str(row['function_category']).strip()
                spec_f = str(row['specific_function']).strip()
                univ_f = str(row['universal_function']).strip()
                bio_mech = str(row['biology_mechanisms']).strip()
                phy_mech = str(row['physics_mechanisms']).strip()
                
                # Build signature
                signature_text = f"{f_cat} {spec_f} {univ_f} {bio_mech} {phy_mech}"
                
                embedding = model.encode(signature_text).tolist()
                
                # Create record: Use Original columns (from 'row') but add normalized for query ease
                record = row.to_dict()
                
                # Clean up the record (remove the duplicated normalized columns from the dict if you want, 
                # or keep them as standard keys. Keeping them is safer for querying).
                # Convert NaNs to None for Mongo
                record = {k: (v if pd.notna(v) else None) for k, v in record.items()}
                
                record['embedding'] = embedding
                record['signature_text'] = signature_text
                
                # Add standard keys explicitly for easier lookup later
                record['standard_specific_function'] = spec_f
                record['standard_function_category'] = f_cat
                record['standard_universal_function'] = univ_f
                
                records.append(record)
            
            if records:
                col_function.insert_many(records)
                total_inserted += len(records)
                
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

    print(f"✅ Inserted {total_inserted} function records across all files.")

if __name__ == "__main__":
    # Ensure your paths are correct relative to where you run this
    ingest_industry_taxonomy()
    ingest_function_taxonomy()
    print("\n🎉 Ingestion Complete.")