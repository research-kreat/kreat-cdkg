import json
import time
import urllib.request
import urllib.error
import sys


def preprocess_entry_for_prompt(entry, sector):
    """
    Creates a detailed, natural language string from a taxonomy entry
    to be used as context in the LLM prompt.
    """
    context = (
        f"This entry belongs to the '{sector}' sector, within the '{entry['domain']}' domain, "
        f"specifically focusing on '{entry['sub_industry']}'. "
        f"It is closely related to '{entry['adjacent_sub_industry']}' in the '{entry['adjacent_domain']}' domain, "
        f"and is complemented by '{entry['complementary_sub_industry']}' from the '{entry['complementary_domain']}' domain."
    )
    return context


def generate_prompt(context):
    """
    Generates the full prompt for the Ollama model.
    """
    prompt = (
        "Based on the following industrial taxonomy context, please generate a single, concise, descriptive sentence. "
        "This sentence should act as a definitive abstract or summary that captures the essence of the primary sub-industry, "
        "its function, and its relationship with its adjacent and complementary fields. The tone should be formal and encyclopedic.\n\n"
        f"CONTEXT: \"{context}\"\n\n"
        "ABSTRACT:"
    )
    return prompt


def call_ollama(prompt, model='herald/phi3-128k:latest', retries=3, delay=5):
    """
    Calls the local Ollama model API using Python's built-in urllib module.
    Assumes Ollama is running on http://localhost:11434.
    """
    api_url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    # Encode the payload to bytes
    data = json.dumps(payload).encode('utf-8')

    # Prepare the request
    req = urllib.request.Request(api_url, data = data, headers = {'Content-Type': 'application/json'})

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    response_body = response.read().decode('utf-8')
                    response_json = json.loads(response_body)
                    return response_json.get('response', '').strip()
                else:
                    print(f"Received non-200 status code: {response.status}. Attempt {attempt + 1} of {retries}.")
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            print(f"Error calling Ollama API: {e}. Attempt {attempt + 1} of {retries}.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}. Attempt {attempt + 1} of {retries}.")

        if attempt < retries - 1:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            print("Max retries reached. Returning an empty string.")
            return ""


def process_taxonomy_data(data):
    """
    Main function to process the entire taxonomy dataset.
    """
    mongo_ready_data = []
    for sector, entries in data.items():
        for entry in entries:
            # 1. Pre-process the entry to create a context string
            context_string = preprocess_entry_for_prompt(entry, sector)

            # 2. Generate the specific prompt for the LLM
            prompt = generate_prompt(context_string)

            print(f"--- Processing: {sector} -> {entry['sub_industry']} ---")
            print(f"Generated Prompt:\n{prompt}\n")

            # 3. Call the LLM to generate the abstract
            generated_abstract = call_ollama(prompt)

            if not generated_abstract:
                print(f"Failed to generate abstract for {entry['sub_industry']}. Skipping this entry.")
                continue

            print(f"Generated Abstract:\n{generated_abstract}\n")

            # 4. Create the final JSON object for MongoDB
            mongo_entry = {
                "sector": sector,
                "domain": entry["domain"],
                "sub_industry": entry["sub_industry"],
                "adjacent_domain": entry["adjacent_domain"],
                "adjacent_sub_industry": entry["adjacent_sub_industry"],
                "complementary_domain": entry["complementary_domain"],
                "complementary_sub_industry": entry["complementary_sub_industry"],
                "abstract": generated_abstract
            }
            mongo_ready_data.append(mongo_entry)
            print("-" * 50)

    return mongo_ready_data


if __name__ == "__main__":

    input_filename = 'industry_taxonomy.json'
    output_filename = 'taxonomy_with_abstracts.json'

    try:
        with open(input_filename, 'r', encoding = 'utf-8') as f:
            bidirectional_data = json.load(f)
        print(f"Successfully loaded data from {input_filename}")
    except FileNotFoundError:
        print(f"Error: The file '{input_filename}' was not found.")
        print("Please make sure the JSON file is in the same directory as the script.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: The file '{input_filename}' is not a valid JSON file.")
        sys.exit(1)

    final_data_for_mongo = process_taxonomy_data(bidirectional_data)

    print(f"\n\n--- FINAL DATA READY FOR MONGODB INSERTION ({len(final_data_for_mongo)} entries processed) ---")

    # Save the final data to a new file
    try:
        with open(output_filename, 'w', encoding = 'utf-8') as f:
            json.dump(final_data_for_mongo, f, indent = 2, ensure_ascii = False)
        print(f"\nSuccessfully saved processed data to {output_filename}")
    except IOError as e:
        print(f"Error saving data to {output_filename}: {e}")

