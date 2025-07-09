import pandas as pd
import csv
from collections import deque

# Raise the max field size (safe value for large text fields)
csv.field_size_limit(1_000_000_000)

# File paths
input_file = "partn.csv"
output_file = "partn_test.csv"

# Number of rows to extract from the end
NUM_LAST_ROWS = 10

try:
    print(f"🔄 Reading the last {NUM_LAST_ROWS} valid rows using a rolling buffer...")

    # Rolling buffer to keep only the last N rows
    last_rows = deque(maxlen=NUM_LAST_ROWS)

    # Read in chunks and store rows in the buffer
    for chunk in pd.read_csv(input_file, chunksize=10, on_bad_lines='skip', engine='python'):
        for _, row in chunk.iterrows():
            last_rows.append(row)

    # Convert deque to DataFrame
    df_last_n = pd.DataFrame(last_rows)

    # Save to CSV
    df_last_n.to_csv(output_file, index=False)

    print(f"✅ Successfully extracted last {NUM_LAST_ROWS} rows to: {output_file}")
    print(f"📊 Columns: {list(df_last_n.columns)}")
    print("\n🔍 Preview of last few rows:")
    print(df_last_n.tail().to_string())

except Exception as e:
    print(f"❌ An error occurred: {str(e)}")
