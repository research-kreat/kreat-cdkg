import json
import sys
from sentence_transformers import SentenceTransformer


def generate_embeddings(data, model_name='all-MiniLM-L6-v2'):
    """
    Generates sentence embeddings for each entry in the data using a specified model.

    Args:
        data (list): A list of dictionary objects, each containing an 'abstract'.
        model_name (str): The name of the sentence-transformer model to use.

    Returns:
        list: The list of dictionaries with an added 'embedding' field.
    """
    print(f"Loading sentence-transformer model: '{model_name}'...")
    try:
        model = SentenceTransformer(model_name)
    except Exception as e:
        print(f"Error loading the SentenceTransformer model: {e}")
        print("Please ensure you have run 'pip install sentence-transformers'.")
        sys.exit(1)

    print("Model loaded successfully.")

    # Extract all abstracts to be encoded
    # This allows for efficient batch processing
    abstracts = [entry.get('abstract', '') for entry in data]

    if not abstracts:
        print("No abstracts found in the data. Exiting.")
        return []

    print(f"Generating embeddings for {len(abstracts)} abstracts in a single batch...")

    # Generate embeddings in a batch
    embeddings = model.encode(abstracts, show_progress_bar = True)

    print("Embeddings generated successfully.")

    # Add the 'embedding' field to each entry
    for i, entry in enumerate(data):
        # Convert numpy array to a list to make it JSON serializable
        entry['embedding'] = embeddings[i].tolist()

    return data


if __name__ == "__main__":

    input_filename = 'taxonomy_with_abstracts.json'
    output_filename = 'taxonomy_with_embeddings.json'

    # --- Load Input Data ---
    try:
        with open(input_filename, 'r', encoding = 'utf-8') as f:
            taxonomy_data = json.load(f)
        print(f"Successfully loaded {len(taxonomy_data)} entries from {input_filename}")
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        print("Please make sure the file from the previous step is in the same directory.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: The file '{input_filename}' is not a valid JSON file.")
        sys.exit(1)

    # --- Generate Embeddings ---
    if taxonomy_data:
        enriched_data = generate_embeddings(taxonomy_data)
    else:
        enriched_data = []

    # --- Save Output Data ---
    if enriched_data:
        print(f"\n\n--- FINAL DATA READY FOR MONGODB INSERTION ({len(enriched_data)} entries processed) ---")

        try:
            with open(output_filename, 'w', encoding = 'utf-8') as f:
                json.dump(enriched_data, f, indent = 2, ensure_ascii = False)
            print(f"\nSuccessfully saved data with embeddings to {output_filename}")
        except IOError as e:
            print(f"Error saving data to {output_filename}: {e}")
    else:
        print("No data was processed, output file was not created.")

