import os
import csv
from typing import Set
csv.field_size_limit(1_000_000_000)
def filter_csv_by_ids(source_filepath: str, filter_filepath: str, output_filepath: str) -> None:
    """
    Removes rows from a source CSV file if their patent_id exists in a filter CSV file.

    Args:
        source_filepath (str): The full path to the main CSV file to be filtered.
        filter_filepath (str): The full path to the CSV file containing patent_ids to remove.
        output_filepath (str): The full path for the new, filtered CSV file.
    """
    # --- Configuration ---
    ID_COLUMN_NAME = 'patent_id'

    # --- Step 1: Load all patent_ids from the filter file into a set for fast lookups ---
    filter_filename = os.path.basename(filter_filepath)
    print(f"--- Step 1: Loading patent IDs to remove from '{filter_filename}' ---")
    ids_to_remove: Set[str] = set()
    try:
        with open(filter_filepath, 'r', encoding='utf-8', errors='ignore') as filter_file:
            reader = csv.reader(filter_file)
            try:
                header = next(reader)
                filter_id_index = header.index(ID_COLUMN_NAME)
            except StopIteration:
                print(f"Error: The filter file '{filter_filename}' is empty. Cannot proceed.")
                return
            except ValueError:
                print(f"Error: The filter file '{filter_filename}' must contain a '{ID_COLUMN_NAME}' column. Cannot proceed.")
                return

            for row in reader:
                if len(row) > filter_id_index and row[filter_id_index]:
                    ids_to_remove.add(row[filter_id_index])
        
        print(f"Successfully loaded {len(ids_to_remove)} unique patent IDs to use as a filter.\n")

    except FileNotFoundError:
        print(f"Error: The filter file was not found at the specified path: '{filter_filepath}'")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading '{filter_filename}': {e}")
        return

    # --- Step 2: Read the source file and write to the output file, skipping matching IDs ---
    source_filename = os.path.basename(source_filepath)
    print(f"--- Step 2: Filtering '{source_filename}' ---")
    rows_written = 0
    rows_removed = 0
    total_source_rows = 0
    
    try:
        with open(source_filepath, 'r', encoding='utf-8', errors='ignore') as source_file, \
             open(output_filepath, 'w', newline='', encoding='utf-8') as output_file:
            
            reader = csv.reader(source_file)
            writer = csv.writer(output_file)

            # Read and write the header to ensure the schema is identical
            try:
                source_header = next(reader)
                source_id_index = source_header.index(ID_COLUMN_NAME)
                writer.writerow(source_header) # Preserve the original header
            except StopIteration:
                print(f"Error: The source file '{source_filename}' is empty.")
                return
            except ValueError:
                print(f"Error: The source file '{source_filename}' must contain a '{ID_COLUMN_NAME}' column.")
                return
            
            # Process each data row
            for row in reader:
                total_source_rows += 1
                if len(row) > source_id_index:
                    patent_id = row[source_id_index]
                    
                    # If the patent_id is NOT in the removal set, write it to the new file
                    if patent_id not in ids_to_remove:
                        writer.writerow(row)
                        rows_written += 1
                    else:
                        rows_removed += 1
                else:
                    print(f"Warning: Skipping malformed row {total_source_rows + 1} in '{source_filename}'.")
        
        print("\n--- Filtering Complete ---")
        print(f"Original rows in '{source_filename}': {total_source_rows}")
        print(f"Rows removed: {rows_removed}")
        print(f"Rows kept and written to new file: {rows_written}")
        print(f"Filtered data saved successfully to: {output_filepath}")

    except FileNotFoundError:
        print(f"Error: The source file was not found at the specified path: '{source_filepath}'")
    except Exception as e:
        print(f"An unexpected error occurred while processing the files: {e}")


if __name__ == "__main__":
    # --- CONFIGURATION ---
    # IMPORTANT: Replace these placeholder paths with the ACTUAL, FULL file paths on your system.
    # Use raw strings (r"...") on Windows or forward slashes for cross-platform compatibility.

    # Example for Windows: r"C:\Users\YourUser\Documents\output_with_abstract.csv"
    # Example for macOS/Linux: "/home/youruser/documents/output_with_abstract.csv"

    # The main file you want to filter
    SOURCE_FILE_PATH = r"/Users/user/Downloads/output_with_abstract.csv"

    # The file containing the patent_ids that should be removed
    FILTER_FILE_PATH = r"/Users/user/Desktop/csv/merged_patents.csv"

    # The full path for the final, cleaned file
    OUTPUT_FILE_PATH = r"/Users/user/Downloads/updated_patents.csv"

    # --- EXECUTION ---
    # Check if paths have been changed from the default placeholders
    if "C:\\path\\to\\your\\" in SOURCE_FILE_PATH:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! PLEASE UPDATE THE FILE PATHS IN THE SCRIPT BEFORE RUNNING !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    else:
        filter_csv_by_ids(SOURCE_FILE_PATH, FILTER_FILE_PATH, OUTPUT_FILE_PATH)