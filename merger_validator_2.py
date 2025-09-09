import os
import csv
from typing import Set
csv.field_size_limit(1_000_000_000)
def find_desktop_path() -> str:
    """
    Finds the path to the user's desktop directory in a cross-platform way.

    Returns:
        str: The absolute path to the desktop.
    """
    return os.path.join(os.path.expanduser('~'), 'Desktop/csv2')

def count_unique_patent_ids(desktop_path: str) -> None:
    """
    Iterates through CSV files on the desktop, finds the 'patent_id' column
    regardless of schema, and counts the number of unique patent IDs.

    Args:
        desktop_path (str): The path to the user's desktop.
    """
    print(f"Scanning for CSV files on your desktop at: {desktop_path}")

    # --- Configuration ---
    # The column name we are looking for in each CSV file.
    UNIQUE_ID_COLUMN = 'patent_id'

    # --- Initialization ---
    unique_patent_ids: Set[str] = set()
    total_rows_scanned = 0
    files_with_id_column = 0
    processed_files_count = 0

    # Find all files on the desktop that end with .csv
    try:
        all_files = os.listdir(desktop_path)
        csv_files = [f for f in all_files if f.lower().endswith('.csv')]
    except FileNotFoundError:
        print(f"Error: Could not find the desktop path '{desktop_path}'.")
        return

    if not csv_files:
        print("No CSV files found on the desktop to process.")
        return

    print(f"Found {len(csv_files)} CSV files to process: {', '.join(csv_files)}\n")

    # --- Processing ---
    for filename in csv_files:
        filepath = os.path.join(desktop_path, filename)
        print(f"--- Processing file: {filename} ---")
        processed_files_count += 1
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as infile:
                reader = csv.reader(infile)
                
                # Read the header to find the patent_id column
                try:
                    header = next(reader)
                except StopIteration:
                    print(f"Warning: Skipping '{filename}' because it is empty.")
                    continue

                # Find the index of the 'patent_id' column for this specific file
                try:
                    patent_id_index = header.index(UNIQUE_ID_COLUMN)
                    files_with_id_column += 1
                    print(f"Found '{UNIQUE_ID_COLUMN}' column at index {patent_id_index}.")
                except ValueError:
                    print(f"Warning: Skipping '{filename}' as it does not contain the required column '{UNIQUE_ID_COLUMN}'.")
                    continue # Move to the next file

                # Process data rows for this file
                rows_in_file = 0
                for row in reader:
                    total_rows_scanned += 1
                    rows_in_file += 1
                    
                    # Check for malformed rows where the patent_id column might be out of bounds
                    if len(row) > patent_id_index:
                        patent_id = row[patent_id_index]
                        # Add non-empty patent_ids to the set for counting
                        if patent_id:
                            unique_patent_ids.add(patent_id)
                    else:
                        print(f"Warning: Skipping malformed row {rows_in_file+1} in '{filename}'.")
                
                print(f"Finished '{filename}': Scanned {rows_in_file} data rows.")

        except Exception as e:
            print(f"An error occurred while processing {filename}: {e}")
    
    # --- Final Report ---
    print("\n--- Count Complete ---")
    print(f"Processed {processed_files_count} total CSV files.")
    print(f"Scanned a total of {total_rows_scanned} rows across {files_with_id_column} files that contained the '{UNIQUE_ID_COLUMN}' column.")
    print(f"Found {len(unique_patent_ids)} unique patent IDs.")


if __name__ == "__main__":
    desktop = find_desktop_path()
    count_unique_patent_ids(desktop)

