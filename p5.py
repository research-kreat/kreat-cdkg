import pandas as pd
import requests
import re

# Configuration
INPUT_FILE = "partn.csv"
OUTPUT_FILE = "final_part1.csv"
OLLAMA_MODEL = "herald/phi3-128k:latest"
OLLAMA_API_URL = "http://localhost:11434/api/generate"

# Unified schema
unified_columns = [
    "id", "knowledge_type", "title", "full_text", "publication_date", "updated_at",
    "local_url", "technology_stack", "keywords", "country", "references", "pdf_link",
    "source_url", "source_date", "domain", "sub_domain", "inventors", "assignee_names",
    "relevance_score", "data_quality_score", "patent_type", "num_claims", "summary",
    "patent_id", "assignee_org", "foreign_citation_count", "local_citation_count",
    "cpc_type", "wipo_kind", "cpc_class_title", "cpc_subclass_title",
    "cpc_group_title", "ipc_classifications", "cpc_classifications",
    "doi_url", "publisher", "journal_name", "journal_volume", "journal_issue",
    "journal_pages", "doi", "authors", "cited_by", "abstract",
    "ai_generated_abstract", "use_case_examples", "market_trends",
    "customer_behavior", "competitor_data"
]

# Mapping from input to unified schema
column_mapping = {
    "patent_id": "patent_id",
    "summary_text": "summary",
    "patent_type": "patent_type",
    "patent_date": "publication_date",
    "patent_title": "title",
    "wipo_kind": "wipo_kind",
    "num_claims": "num_claims",
    "assignee_names": "assignee_names",
    "disambig_assignee_organization": "assignee_org",
    "ipc": "ipc_classifications",
    "cpc_group": "cpc_classifications",
    "cpc_type": "cpc_type",
    "cpc_group_title": "cpc_group_title",
    "cpc_class_title": "cpc_class_title",
    "cpc_subclass_title": "cpc_subclass_title",
    "foreign_citation_count": "foreign_citation_count",
    "local_citation_count": "local_citation_count",
    "description_text": "full_text",
    "inventor_names": "inventors",
    "domains": "domain",
    "sub_domains": "sub_domain",
    "abstract": "abstract"
}

# Numbered field mapping
numbered_fields = {
    1: "knowledge_type",
    2: "technology_stack",
    3: "keywords",
    4: "domain",
    5: "sub_domain",
    6: "ai_generated_abstract",
    7: "use_case_examples"
}

# Function to call Ollama
def query_ollama(prompt: str) -> str:
    try:
        response = requests.post(OLLAMA_API_URL, json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        }, timeout=90)

        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

        return response.json().get("response", "")

    except Exception as e:
        print(f"❌ Ollama API error: {e}")
        return ""

# Function to parse numbered fields from AI response
def extract_fields_from_numbered_text(text: str) -> dict:
    results = {}
    for line in text.strip().splitlines():
        match = re.match(r"^\s*(\d+)\.\s*[^:]+:\s*(.+)$", line)
        if match:
            index = int(match.group(1))
            if index in numbered_fields:
                results[numbered_fields[index]] = match.group(2).strip()
    return results

# Load input
df_input = pd.read_csv(INPUT_FILE)

if df_input.empty:
    print("❌ Error: The input CSV is empty.")
    exit()

# Prepare output DataFrame with unified schema columns
df_output = pd.DataFrame(index=df_input.index)

# Apply column mappings
for old_col, new_col in column_mapping.items():
    if old_col in df_input.columns:
        df_output[new_col] = df_input[old_col].astype(str)

# Ensure all unified columns exist
for col in unified_columns:
    if col not in df_output.columns:
        df_output[col] = ""

# Process each row
for idx, row in df_output.iterrows():
    print(f"\n🔍 Processing row {idx + 1}/{len(df_output)}")

    abstract = row.get("abstract", "")[:3000]

    if not abstract.strip():
        print("⚠️ Skipping due to empty abstract.")
        continue

    prompt = f"""
Based on the following patent abstract, extract and return the following 7 fields, using the **exact format** shown below:

1. Knowledge Type: ...
2. Technology Stack: ...
3. Keywords: ...
4. Domain: ...
5. Sub-domain: ...
6. AI-generated Abstract: ...
7. Use-case Examples: ...

Patent Abstract:
{abstract}
"""

    print("📤 Sending prompt to Ollama...")
    response_text = query_ollama(prompt)

    print("📥 Response:")
    print(response_text)

    parsed_fields = extract_fields_from_numbered_text(response_text)

    for field, value in parsed_fields.items():
        df_output.at[idx, field] = value

# Ensure output column order
df_output = df_output[unified_columns]

# Save result
df_output.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Output saved to {OUTPUT_FILE}")
