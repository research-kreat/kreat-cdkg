import pandas as pd
import os

def split_csv(file_path, chunk_size=600, output_dir='chunks'):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load the CSV file
    df = pd.read_csv(file_path)

    # Drop 'summary' column if it exists
    df.drop(columns=['summary'], inplace=True, errors='ignore')

    # Calculate number of chunks
    num_chunks = (len(df) + chunk_size - 1) // chunk_size

    # Split and save
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk_df = df[start:end]
        chunk_df.to_csv(f'{output_dir}/chunk_{i+1}.csv', index=False)
        print(f"✅ Saved chunk {i+1} with {len(chunk_df)} rows.")

# Usage
split_csv('CDKG_final.csv')
