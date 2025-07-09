import pandas as pd
import os

# 🔧 Replace with your actual file paths
csv_files = [
    #"final_partn.csv",
    "final_part1.csv",
    "final_part2.csv",
    "final_part3.csv",
    #"final_partn4.csv",
    #"final_partn5.csv",
    #"final_part6.csv",
]

# 📁 Output file
output_file = "finalpart_2.csv"

# ✅ Unified schema
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

# 🛠 Merge CSVs
merged_df = pd.DataFrame()

for file in csv_files:
    try:
        df = pd.read_csv(file, encoding='utf-8', on_bad_lines='skip')
        df = df[unified_columns]  # Ensure correct column order
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        print(f"✅ Merged: {file} ({len(df)} rows)")
    except FileNotFoundError:
        print(f"❌ File not found: {file}")
    except Exception as e:
        print(f"❌ Error processing {file}: {e}")

# 💾 Save final merged file
merged_df.to_csv(output_file, index=False, encoding='utf-8')
print(f"\n🎉 All files merged into: {output_file}")
print(f"📊 Total records: {len(merged_df)}")
