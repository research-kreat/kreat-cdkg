import os
from collections import deque
import time
import csv
csv.field_size_limit(1_000_000_000)
# --- Configuration ---
# The large CSV file you want to process.
INPUT_FILENAME = "../output_with_abstract.csv"
# A new file where the result will be saved.
OUTPUT_FILENAME = "../patents_final_2024.csv"
# The number of rows you want to remove from the end of the file.
ROWS_TO_REMOVE = 4000
# --- End of Configuration ---


def create_dummy_file(filename, num_rows):
    """Creates a large dummy CSV file for testing purposes using the csv module."""
    print(f"Creating a dummy file '{filename}' with {num_rows:,} rows for demonstration...")
    # This schema is based on the one you provided to make the dummy file more realistic.
    header = [
        "id", "knowledge_type", "title", "full_text", "publication_date", "updated_at",
        "local_url", "technology_stack", "keywords", "country", "references", "pdf_link",
        "source_url", "source_date", "domain", "sub_domain", "inventors", "assignee_names",
        "relevance_score", "data_quality_score", "patent_type", "num_claims", "summary",
        "patent_id", "assignee_org", "foreign_citation_count", "local_citation_count",
        "cpc_type", "wipo_kind", "cpc_class_title", "cpc_subclass_title",
        "cpc_group_title", "ipc_classifications", "cpc_classifications",
        "doi_url", "publisher", "journal_name", "journal_volume", "journal_issue",
        "journal_pages", "doi", "authors", "cited_by", "abstract",
        "ai_generated_abstract", "use_case_examples", "market_trends",
        "customer_behavior", "competitor_data"
    ]
    try:
        # Use newline='' as recommended by the csv module documentation
        with open(filename, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)  # Write the header
            for i in range(num_rows):
                # Create a row with dummy data for each column
                row = [f"id_{i+1}"] + [f"value_{col}_{i+1}" for col in header[1:]]
                writer.writerow(row)  # Write the data row
        print("Dummy file created successfully.")
    except IOError as e:
        print(f"Error creating dummy file: {e}")
        exit(1)


def process_csv_with_rolling_buffer(input_path, output_path, lines_to_skip):
    """
    Reads a large CSV file row-by-row using the csv module and writes it
    to an output file, skipping the last `lines_to_skip` number of rows.
    """
    print("\nProcessing file with a rolling buffer (using csv module)...")
    start_time = time.time()

    try:
        # We don't set maxlen here; we'll manage the size manually.
        # This makes the logic clearer.
        buffer = deque()

        # Use newline='' for both reading and writing
        with open(input_path, "r", encoding="utf-8", newline='') as f_in, \
             open(output_path, "w", encoding="utf-8", newline='') as f_out:

            csv_reader = csv.reader(f_in)
            csv_writer = csv.writer(f_out)

            try:
                header = next(csv_reader)
                csv_writer.writerow(header)
            except StopIteration:
                print("Warning: Input file is empty.")
                return

            line_count = 1
            for row in csv_reader:
                # --- CORRECTED LOGIC (now with csv rows) ---
                # 1. Always add the new row (a list of strings) to the buffer first.
                buffer.append(row)

                # 2. If the buffer is now larger than the number of lines to skip,
                #    it means the oldest row is safe to be written.
                if len(buffer) > lines_to_skip:
                    # 3. Remove the oldest row from the left and write it.
                    row_to_write = buffer.popleft()
                    csv_writer.writerow(row_to_write)
                # --- END OF CORRECTION ---

                line_count += 1
                if line_count % 1_000_000 == 0:
                    print(f"  ...processed {line_count:,} lines")

        elapsed_time = time.time() - start_time
        print(f"\nProcessing complete in {elapsed_time:.2f} seconds.")
        print(f"Successfully removed the last {lines_to_skip:,} rows.")
        print(f"Result saved to: '{output_path}'")

        input_size = os.path.getsize(input_path) / (1024 * 1024)
        output_size = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Original file size: {input_size:.2f} MB")
        print(f"New file size: {output_size:.2f} MB")

    except FileNotFoundError:
        print(f"Error: The file '{input_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FILENAME):
        print(f"The input file '{INPUT_FILENAME}' was not found.")
        # Create a smaller dummy file for demonstration.
        create_dummy_file(INPUT_FILENAME, num_rows=10000)

    process_csv_with_rolling_buffer(
        input_path=INPUT_FILENAME,
        output_path=OUTPUT_FILENAME,
        lines_to_skip=ROWS_TO_REMOVE
    )


