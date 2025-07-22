import csv
from collections import deque
csv.field_size_limit(1_000_000_000)
input_file = "/Users/user/Downloads/output_with_abstract.csv"
output_file = "partn.csv"

count = 2000
offset = 2000
total_required = count + offset

# Store only the last (count + offset) rows
buffer = deque(maxlen=total_required)

try:
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            buffer.append(row)

    print(f"📦 Total buffered rows: {len(buffer)}")

    # Extract the desired slice
    result_rows = list(buffer)[-offset - count : -offset if offset else None]

    # Write to new CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)
        writer.writerows(result_rows)

    print(f"✅ Extracted {len(result_rows)} rows to {output_file}")

except Exception as e:
    print(f"❌ Error: {e}")
