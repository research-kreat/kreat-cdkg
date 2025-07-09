import pandas as pd
import csv

# Increase max field size for abstract/full_text
csv.field_size_limit(1_000_000_000)

input_file = "D:\\ZYPTR_SCRPITS\\output_with_abstract.csv"
output_file = "partn.csv"

count = 1000   # Number of rows to extract for each mode

offset = 2000
buffer_size = offset + count + 10  # extra buffer for safety

try:
    print(f"🔄 Reading in chunks to extract rows for (offset: {offset})...")

    CHUNK_SIZE = 50000
    buffer = pd.DataFrame()

    chunks = pd.read_csv(
        input_file,
        chunksize=CHUNK_SIZE,
        encoding='utf-8',
        engine='python',
        on_bad_lines='skip'  # Skip rows with mismatched columns
    )

    for chunk in chunks:
        buffer = pd.concat([buffer, chunk])
        if len(buffer) > buffer_size:
            buffer = buffer.tail(buffer_size + 5)

    start_idx = -offset - count if offset > 0 else -count
    end_idx = -offset if offset > 0 else None

    df_result = buffer.iloc[start_idx:end_idx]

    df_result.to_csv(output_file, index=False)

    print(f"✅ Successfully extracted rows.")
    print(f"📥 Input: {input_file}")
    print(f"📤 Output: {output_file}")
    print(f"🧾 Records written: {len(df_result)}")
    print(f"📊 Columns: {list(df_result.columns)}")
    print("\n🔍 Preview:")
    print(df_result.head().to_string())

except FileNotFoundError:
    print(f"❌ File not found: {input_file}")
except pd.errors.EmptyDataError:
    print("❌ The CSV file is empty.")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
