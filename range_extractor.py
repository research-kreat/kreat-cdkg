import csv
csv.field_size_limit(1_000_000_000)
def extract_row_range(input_file, output_file, start_row, end_row):
    """
    Extract rows from start_row to end_row (inclusive) from input_file and save to output_file.
    start_row and end_row are 1-based (i.e., the first row is 1).
    """
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for i, row in enumerate(reader, start=1):
            if i < start_row:
                continue
            elif start_row <= i <= end_row:
                writer.writerow(row)
            else:
                break
        print(f"✅ Extracted rows {start_row} to {end_row} from {input_file} to {output_file}")

input_csv = 'strict_aerospace_filtered.csv'
output_csv = 'aerospace2.csv'
start = 0  # inclusive
end = 517    # inclusive

extract_row_range(input_csv, output_csv, start, end)
