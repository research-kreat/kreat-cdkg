import csv
csv.field_size_limit(1_000_000_000)

INPUT_CSV = 'aerospace_keyword_filtered.csv'
OUTPUT_CSV = 'strict_aerospace_filtered.csv'

moderate_keywords = [
    "aerospace", "spacecraft", "rocket", "missile", "satellite", "orbital",
    "launch vehicle", "thruster", "guidance system", "propellant",
    "aerodynamics", "turbofan", "hypersonic", "supersonic", "re-entry",
    "payload", "altitude control", "inertial navigation", "launchpad",
    "space propulsion"
]

# Normalize keywords for case-insensitive search
strict_keywords = [kw.lower() for kw in moderate_keywords]

def contains_strict_keyword(row):
    fields_to_check = ['title', 'abstract', 'summary', 'use_case_examples']
    for field in fields_to_check:
        text = row.get(field, '').lower()
        for keyword in strict_keywords:
            if keyword in text:
                return True
    return False

def apply_strict_filter(input_file, output_file):
    with open(input_file, newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        count = 0
        for row in reader:
            if contains_strict_keyword(row):
                writer.writerow(row)
                count += 1

        print(f"✅ Stricter filtering complete. Rows retained: {count}")

apply_strict_filter(INPUT_CSV, OUTPUT_CSV)
