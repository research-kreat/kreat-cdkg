import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import ast

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Load CSV
df = pd.read_csv('KG.cleanedData.csv')

# Fill NaNs with empty string to avoid issues
df.fillna('', inplace=True)

# Combine relevant fields into one string per row
def build_text(row):
    return f"{row['knowledge_type']}\n{row['keywords']}\n{row['domain']}\n{row['sub_domain']}"

# Generate embeddings
tqdm.pandas(desc="Generating embeddings")
df['embedding'] = df.progress_apply(lambda row: model.encode(build_text(row)).tolist(), axis=1)

# Save to new CSV
df.to_csv('CDKG_data.csv', index=False)

print("✅ Embeddings generated and saved.csv")
