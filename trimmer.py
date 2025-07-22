import csv

input_file = "partn.csv"
output_file = "partn_trimmed.csv"
csv.field_size_limit(1_000_000_000)
rows_to_trim = 5

try:
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        header = reader[0]
        data = reader[1:]  # exclude header

        if len(data) <= 2 * rows_to_trim:
            raise ValueError("Not enough data to trim safely.")

        trimmed_data = data[rows_to_trim : -rows_to_trim]

    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)
        writer.writerows(trimmed_data)

    print(f"✅ Trimmed {rows_to_trim} rows from start and end.")
    print(f"📥 Input: {input_file}")
    print(f"📤 Output: {output_file}")
    print(f"🧾 Rows written: {len(trimmed_data)}")

except Exception as e:
    print(f"❌ Error: {e}")
