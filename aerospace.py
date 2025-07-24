import csv
import requests
import time
csv.field_size_limit(1_000_000_000)
INPUT_CSV = '../KGcdkg.csv'
OUTPUT_CSV = 'output_strength_scores.csv'
OLLAMA_MODEL = "herald/phi3-128k:latest"
OLLAMA_API_URL = "http://localhost:11434/api/generate"

def ask_ollama(use_cases, tech_stack):
    prompt = f"""
Given the following use case examples and technology stack, rate from 0 to 100 how relevant this patent is to the aerospace domain.

Use Case Examples:
{use_cases}

Technology Stack:
{tech_stack}

Respond only with a number between 0 and 100 indicating the strength of relevance to aerospace.
"""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt.strip(),
        "stream": False
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        text = result.get("response", "").strip()

        # Try to extract a number from the response
        try:
            score = int(''.join(filter(str.isdigit, text)))
            return min(max(score, 0), 100)  # Clamp between 0 and 100
        except ValueError:
            print(f"⚠️ Invalid score returned: {text}")
            return None

    except Exception as e:
        print(f"❌ Error querying Ollama: {e}")
        return None


def process_csv(input_file, output_file):
    with open(input_file, newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=['patent_id', 'strength_score'])
        writer.writeheader()

        for i, row in enumerate(reader):
            patent_id = row.get("patent_id")
            use_cases = row.get("use_case_examples", "").strip()
            tech_stack = row.get("technology_stack", "").strip()

            if not patent_id:
                continue  # skip if patent_id is missing

            strength_score = ask_ollama(use_cases, tech_stack)
            print(f"[{i+1}] Patent ID: {patent_id} => Score: {strength_score}")

            writer.writerow({
                "patent_id": patent_id,
                "strength_score": strength_score if strength_score is not None else ""
            })

            time.sleep(0.5)  # prevent overload

process_csv(INPUT_CSV, OUTPUT_CSV)
