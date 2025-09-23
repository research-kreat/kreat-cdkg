import pandas as pd
import os
import re
from tqdm import tqdm
import math

def rollup_embedding_columns(input_file, output_file, chunk_size=1000):
    """
    Reads a large CSV in chunks, finds columns representing flattened arrays
    (e.g., 'embedding[0]', 'embedding[1]'), and rolls them up into a single
    array column.

    Args:
        input_file (str): Path to the source CSV file.
        output_file (str): Path to save the processed CSV file.
        chunk_size (int): Number of rows to process per chunk for memory efficiency.
    """
    print(f"Starting the roll-up process for '{input_file}'...")

    # --- 1. Pre-scan the file to identify columns to roll up ---
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return

    print("Identifying embedding columns from the header...")
    # Read only the header to get column names without loading data
    header_df = pd.read_csv(input_file, nrows=0)
    all_columns = header_df.columns.tolist()

    # Find all columns that match the 'embedding[...]' pattern
    embedding_cols = [c for c in all_columns if c.startswith('embedding[')] or [c for c in all_columns if c.startswith('fullDescription_embedding[')]
    ai_embedding_cols = [c for c in all_columns if c.startswith('ai_embeddings[')]

    # This is crucial: sort the columns numerically to maintain the correct array order
    # For example, to ensure 'embedding[10]' comes after 'embedding[9]'
    try:
        embedding_cols.sort(key=lambda x: int(re.search(r'\[(\d+)\]', x).group(1)))
        ai_embedding_cols.sort(key=lambda x: int(re.search(r'\[(\d+)\]', x).group(1)))
    except (TypeError, AttributeError):
        print("Error: Could not parse numerical index from column names. Please check column format.")
        return

    if not embedding_cols and not ai_embedding_cols:
        print("No embedding columns found to roll up. Exiting.")
        return
        
    print(f"Found {len(embedding_cols)} 'embedding' columns to consolidate.")
    print(f"Found {len(ai_embedding_cols)} 'ai_embeddings' columns to consolidate.")

    # --- 2. Process the file in chunks ---
    print("\nProcessing the large CSV file in chunks...")
    
    # Calculate total chunks for an accurate progress bar
    try:
        total_rows = sum(1 for row in open(input_file, 'r', encoding='utf-8')) - 1
        total_chunks = math.ceil(total_rows / chunk_size)
    except Exception as e:
        print(f"Could not count rows for progress bar, will proceed without it. Error: {e}")
        total_chunks = None

    # Use an iterator to read the CSV chunk by chunk
    chunk_iterator = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)
    
    header_written = False

    for chunk in tqdm(chunk_iterator, total=total_chunks, desc="Rolling up chunks"):
        # For 'embedding' columns
        if embedding_cols:
            # This efficient operation takes all values from the embedding columns for each row
            # and converts them into a list, creating a new 'embedding' Series.
            chunk['embedding'] = chunk[embedding_cols].values.tolist()
            # Drop the original flat columns
            chunk.drop(columns=embedding_cols, inplace=True)

        # For 'ai_embeddings' columns
        if ai_embedding_cols:
            chunk['ai_embeddings'] = chunk[ai_embedding_cols].values.tolist()
            chunk.drop(columns=ai_embedding_cols, inplace=True)
            
        # Write the processed chunk to the new file
        if not header_written:
            # For the first chunk, write to a new file with the header
            chunk.to_csv(output_file, mode='w', header=True, index=False, encoding='utf-8')
            header_written = True
        else:
            # For subsequent chunks, append without the header
            chunk.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')

    print(f"\n✅ Success! Processing is complete.")
    print(f"The rolled-up data has been saved to '{output_file}'.")


if __name__ == "__main__":
    # --- Configuration ---
    source_csv_file = 'biomimicry.csv'
    destination_csv_file = 'biomimicry_final.csv'
    
    rollup_embedding_columns(source_csv_file, destination_csv_file)