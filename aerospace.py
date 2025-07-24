import csv
csv.field_size_limit(1_000_000_000)

AEROSPACE_CSV = '../aerospace_filtered.csv'
KG_CSV = '../KGcdkg.csv'
OUTPUT_CSV = 'filtered_output.csv'

def get_patent_ids_from_kg(kg_file):
    kg_patents = set()
    with open(kg_file, newline='', encoding='utf-8') as kgf:
        reader = csv.DictReader(kgf)
        for row in reader:
            pid = row.get("patent_id", "").strip()
            if pid:
                kg_patents.add(pid)
    return kg_patents

def filter_aerospace_csv(input_file, kg_patents, output_file):
    with open(input_file, newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for idx, row in enumerate(reader, start=1):
            patent_id = row.get("patent_id", "").strip()
            if patent_id in kg_patents:
                print(f"Matched row {idx}: {patent_id}")
                continue  # skip matched rows
            writer.writerow(row)

kg_patent_ids = get_patent_ids_from_kg(KG_CSV)
filter_aerospace_csv(AEROSPACE_CSV, kg_patent_ids, OUTPUT_CSV)
