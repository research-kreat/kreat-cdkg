import os
import csv
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
csv.field_size_limit(1_000_000_000)  # Increase CSV field size limit for large files

def get_total_lines(filepath):
    """Efficiently count lines in a file for the progress bar."""
    with open(filepath, 'rb') as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read
        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)
    return lines

def filter_patents_fast_inplace():
    """
    A high-performance version of the filtering script that uses Python's
    native csv module to correctly handle complex CSVs with quoted newlines.
    """
    # --- 1. Load Configuration ---
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    input_csv_path = "../output_with_abstract.csv"
    temp_csv_path = f"{input_csv_path}.tmp"
    ids_file_path = "ids_to_remove.txt"  # The file with IDs

    if not os.path.exists(input_csv_path):
        print(f"Error: Input file '{input_csv_path}' not found.")
        return
    if not os.path.exists(ids_file_path):
        print(f"Error: ID file '{ids_file_path}' not found. Please generate it first.")
        return
        
    # --- 2. Load the IDs to remove from the text file ---
    print(f"Loading patent IDs from '{ids_file_path}'...")
    with open(ids_file_path, 'r') as f:
        # Use a set for extremely fast lookups
        processed_patent_ids = {line.strip() for line in f}
    print(f"Loaded {len(processed_patent_ids)} unique patent_ids to remove.")

    # --- 3. High-Performance CSV Processing ---
    print(f"\nFiltering '{input_csv_path}'...")
    processing_successful = False
    
    try:
        # Note: Counting physical lines might not perfectly match logical rows,
        # but it's the best we can do for a progress bar estimate.
        total_lines = get_total_lines(input_csv_path)
        print(f"Processing ~{total_lines} physical lines...")

        with open(input_csv_path, 'r', newline='', encoding='utf-8') as infile, \
             open(temp_csv_path, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Find the index of the 'patent_id' column from the header
            header = next(reader)
            writer.writerow(header)
            try:
                # Assuming patent_id is the first column as discussed
                patent_id_index = 0 
            except IndexError:
                print("Error: CSV header is empty or invalid.")
                return

            # Use tqdm for progress, iterating over the reader object
            # The total is an estimate, so the bar might not be perfectly smooth.
            for row in tqdm(reader, total=total_lines, desc="Filtering rows"):
                try:
                    # The core logic: check the ID at the specific index
                    if row[patent_id_index] not in processed_patent_ids:
                        writer.writerow(row)
                except IndexError:
                    # This can happen if there are malformed/empty rows
                    print(f"Warning: Skipping a malformed row: {row}")
            
        processing_successful = True

    except Exception as e:
        print(f"\nAn error occurred during processing: {e}")
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
    
    # --- 4. Replace Original File ---
    if processing_successful:
        print("\nProcessing completed successfully.")
        print(f"Replacing original file '{input_csv_path}'...")
        os.remove(input_csv_path)
        os.rename(temp_csv_path, input_csv_path)
        print("File replaced successfully.")

if __name__ == "__main__":
    filter_patents_fast_inplace()