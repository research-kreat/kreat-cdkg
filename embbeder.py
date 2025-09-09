import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import ast

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Load CSV
df = pd.read_csv('../f5001_output.csv')

# Fill NaNs with empty string to avoid issues
df.fillna('', inplace=True)

# Combine all fields into one string per row for full embedding
def build_full_text(row):
    return f"{row['knowledge_type']}\n{row['keywords']}\n{row['domain']}\n{row['sub_domain']}\n{row['ai_generated_abstract']}\n{row['use_case_examples']}"

# Combine only abstract and use-case fields
def build_ai_text(row):
    return f"{row['ai_generated_abstract']}\n{row['use_case_examples']}"

# Enable progress bar
tqdm.pandas(desc="Generating embeddings")

# Generate full embedding (optional, keep or remove based on your needs)
df['embedding'] = df.progress_apply(lambda row: model.encode(build_full_text(row)).tolist(), axis=1)

# Generate embedding only for ai_generated_abstract and use_case_examples
df['ai_embeddings'] = df.progress_apply(lambda row: model.encode(build_ai_text(row)).tolist(), axis=1)

# Save to CSV
df.to_csv('f5k8k.csv', index=False)

print("✅ Embeddings generated and saved to f5k8k.csv")

