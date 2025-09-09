import os
import csv
from typing import List, Set


csv.field_size_limit(1_000_000_000)


def find_desktop_path() -> str:
    """
    Finds the path to the user's desktop directory in a cross-platform way.

    Returns:
        str: The absolute path to the desktop.
    """
    return os.path.join(os.path.expanduser('~'), 'Desktop/csv')

def merge_csv_files(desktop_path: str, output_filename: str) -> None:
    """
    Merges CSV files from the desktop, ensuring unique patent_id entries.

    Args:
        desktop_path (str): The path to the user's desktop.
        output_filename (str): The name of the file for the merged output.
    """
    print(f"Scanning for CSV files on your desktop at: {desktop_path}")

    # --- Configuration ---
    # The full schema as provided in the request. This helps validate files.
    EXPECTED_HEADER_SCHEMA = [
        '_id', 'knowledge_type', 'title', 'full_text', 'publication_date', 
        'updated_at', 'local_url', 'technology_stack', 'keywords', 'country', 
        'references', 'pdf_link', 'source_url', 'source_date', 'domain', 
        'inventors', 'assignee_names', 'relevance_score', 'data_quality_score', 
        'patent_type', 'num_claims', 'summary', 'patent_id', 'assignee_org', 
        'foreign_citation_count', 'local_citation_count', 'cpc_type', 
        'wipo_kind', 'cpc_class_title', 'cpc_subclass_title', 
        'cpc_group_title', 'ipc_classifications', 'cpc_classifications', 
        'doi_url', 'publisher', 'journal_name', 'journal_volume', 
        'journal_issue', 'journal_pages', 'doi', 'authors', 'cited_by', 
        'abstract', 'ai_generated_abstract', 'use_case_examples', 
        'market_trends', 'customer_behavior', 'competitor_data', 
        'adjacent_domain', 'adjacent_sub_industry', 'complementary_domain', 
        'complementary_sub_industry', 'sub_industry', 'taxonomy_domain', 
        'equivalent_function', 'function', 'sector', 'embedding', 'ai_embeddings'
    ]
    
    # The column name we use to check for uniqueness.
    UNIQUE_ID_COLUMN = 'patent_id'

    # --- Initialization ---
    processed_patent_ids: Set[str] = set()
    header_written: bool = False
    patent_id_index: int = -1
    total_rows_processed = 0
    total_duplicates_found = 0

    output_filepath = os.path.join(desktop_path, output_filename)
    
    # Find all files on the desktop that end with .csv
    try:
        all_files = os.listdir(desktop_path)
        csv_files = [f for f in all_files if f.lower().endswith('.csv') and f != output_filename]
    except FileNotFoundError:
        print(f"Error: Could not find the desktop path '{desktop_path}'.")
        return

    if not csv_files:
        print("No CSV files found on the desktop to merge.")
        return

    print(f"Found {len(csv_files)} CSV files to process: {', '.join(csv_files)}\n")

    # --- Processing ---
    try:
        # Open the output file in write mode with newline='' to prevent blank rows
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)

            for filename in csv_files:
                filepath = os.path.join(desktop_path, filename)
                print(f"--- Processing file: {filename} ---")
                
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as infile:
                        reader = csv.reader(infile)
                        
                        # Read and validate the header
                        header = next(reader)
                        
                        # Find the index of the 'patent_id' column
                        try:
                            current_patent_id_index = header.index(UNIQUE_ID_COLUMN)
                        except ValueError:
                            print(f"Warning: Skipping '{filename}' because it does not contain the required column '{UNIQUE_ID_COLUMN}'.")
                            continue

                        # Write the header to the output file only once, from the first valid file
                        if not header_written:
                            writer.writerow(header)
                            header_written = True
                            patent_id_index = current_patent_id_index
                            print("Header written to output file.")

                        # Process data rows
                        rows_in_file = 0
                        duplicates_in_file = 0
                        for row in reader:
                            total_rows_processed += 1
                            rows_in_file += 1
                            
                            if len(row) <= patent_id_index:
                                print(f"Warning: Skipping malformed row {rows_in_file+1} in '{filename}'.")
                                continue

                            patent_id = row[patent_id_index]
                            
                            # Deduplication check
                            if patent_id and patent_id not in processed_patent_ids:
                                processed_patent_ids.add(patent_id)
                                writer.writerow(row)
                            else:
                                total_duplicates_found += 1
                                duplicates_in_file += 1
                        
                        print(f"Finished '{filename}': Found {rows_in_file} data rows, skipped {duplicates_in_file} duplicates.")

                except StopIteration:
                     print(f"Warning: Skipping '{filename}' because it is empty.")
                except Exception as e:
                    print(f"An error occurred while processing {filename}: {e}")
            
            print("\n--- Merge Complete ---")
            print(f"Total unique patents found: {len(processed_patent_ids)}")
            print(f"Total rows processed across all files: {total_rows_processed}")
            print(f"Total duplicate rows skipped: {total_duplicates_found}")
            print(f"Merged data saved to: {output_filepath}")

    except IOError as e:
        print(f"Error writing to output file '{output_filepath}'. Please check permissions. Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    desktop = find_desktop_path()
    output_file = "merged_patents.csv"
    merge_csv_files(desktop, output_file)
