import os
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv

# --- CONFIGURATION ---
# If you get an 'OutOfMemoryError', lower this value (e.g., 500, 100).
# If you have a very powerful server, you might try increasing it.
DEFAULT_BATCH_SIZE = 1000
# --- END CONFIGURATION ---

def load_credentials():
    """
    Loads Neo4j credentials from environment variables.
    It checks for the same variables used in your app.py.
    """
    print("Loading credentials from .env file...")
    load_dotenv()
    
    # Use the same environment variable names as in your app.py
    uri = os.getenv("NEO4J_URI3")
    user = os.getenv("NEO4J_USER3")
    password = os.getenv("NEO4J_PASSWORD3")

    if not all([uri, user, password]):
        print("\n--- ERROR ---")
        print("Missing one or more environment variables.")
        print("Please ensure NEO4J_URI2, NEO4J_USER2, and NEO4J_PASSWORD2 are set in your .env file.")
        return None
    
    print("Credentials loaded successfully.")
    return uri, user, password

def delete_all_data_batched(driver, batch_size):
    """
    Deletes all nodes and their relationships in batches to avoid
    memory overload or transaction timeouts.
    
    This version does NOT require the APOC plugin.
    It runs one transaction per batch.
    """
    total_deleted = 0
    start_time = time.time()
    
    # Use a session from the driver
    with driver.session() as session:
        print(f"Starting to delete all data in batches of {batch_size}...")
        
        while True:
            try:
                # This query finds a batch of nodes,
                # detaches (deletes) all their relationships,
                # deletes the nodes themselves,
                # and returns the number of nodes it just deleted.
                # This runs as a single transaction per batch.
                query = f"""
                MATCH (n)
                WITH n LIMIT {batch_size}
                DETACH DELETE n
                RETURN count(n) AS count
                """
                
                result = session.run(query)
                record = result.single()
                count = record["count"] if record else 0
                
                if count == 0:
                    # No more nodes were found, so we are done.
                    break
                
                total_deleted += count
                elapsed = time.time() - start_time
                print(f"  > Deleted {count} nodes. Total deleted: {total_deleted}. (Time: {elapsed:.2f}s)")
            
            except Exception as e:
                print(f"An error occurred during a batch delete: {e}")
                if "MemoryPoolOutOfMemoryError" in str(e):
                    print("\n--- MEMORY ERROR ---")
                    print(f"Batch size of {batch_size} is still too large for your server's memory.")
                    print(f"Try lowering the 'DEFAULT_BATCH_SIZE' value in this script (e.g., to {batch_size // 2}) and run again.")
                    print("----------------------\n")
                    break
                
                print("This could be a temporary issue. Retrying in 5 seconds...")
                time.sleep(5)
    
    end_time = time.time()
    print("\n--------------------------------------------------")
    print(f"Finished deleting all data.")
    print(f"Total nodes deleted: {total_deleted}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds.")
    print("--------------------------------------------------")

def main():
    """
    Main function to orchestrate the connection and deletion process.
    """
    try:
        creds = load_credentials()
        if not creds:
            return
        
        uri, user, password = creds
        
        print(f"\nConnecting to Neo4j at: {uri.split('//')[1]}")
        
        # Connect to the database
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            try:
                driver.verify_connectivity()
                print("Connection successful.")
            except Exception as e:
                print(f"\n--- CONNECTION FAILED ---")
                print(f"Could not connect to the database: {e}")
                print("Please check your credentials and network connection.")
                return

            # --- CRITICAL SAFETY CHECK ---
            print("\n" + "="*50)
            print("WARNING: YOU ARE ABOUT TO DELETE ALL DATA")
            print("         FROM THE NEO4J DATABASE.")
            print("         THIS OPERATION CANNOT BE UNDONE.")
            print("="*50 + "\n")
            
            confirm = input(f"To confirm, type 'DELETE': ")
            
            if confirm.strip() == "DELETE":
                print("\nConfirmation received. Starting deletion process...")
                # Pass the configurable batch size to the function
                delete_all_data_batched(driver, batch_size=DEFAULT_BATCH_SIZE)
                print("Database re-initialization complete.")
            else:
                print("\nOperation cancelled. No data was deleted.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

