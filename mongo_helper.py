import os
import sys
from pymongo import MongoClient
from tqdm import tqdm


def get_mongo_client(uri):
    """Establishes connection to MongoDB."""
    try:
        client = MongoClient(uri)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("MongoDB connection successful.")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Please ensure your MongoDB instance is running and the URI is correct.")
        sys.exit(1)


def delete_fields(client, db_name, collection_name, fields_to_delete):
    """
    Deletes one or more specified fields from all documents in a collection.

    Args:
        client (MongoClient): The active MongoDB client.
        db_name (str): The name of the database.
        collection_name (str): The name of the collection to modify.
        fields_to_delete (list): A list of strings with the field names to remove.
    """
    db = client[db_name]
    collection = db[collection_name]

    if not fields_to_delete:
        print("No fields specified to delete.")
        return

    print(f"Preparing to DELETE the following fields from '{collection_name}': {', '.join(fields_to_delete)}")

    # Create the payload for the $unset operation
    unset_payload = {field: "" for field in fields_to_delete}

    try:
        # Confirm with the user before making a destructive change
        confirm = input(
            f"Are you sure you want to DELETE these {len(fields_to_delete)} fields from ALL documents in '{collection_name}'? This action cannot be undone. (yes/no): ")
        if confirm.lower() == 'yes':
            result = collection.update_many({}, {"$unset": unset_payload})
            print(
                f"Operation complete. Matched {result.matched_count} documents and modified {result.modified_count} documents.")
        else:
            print("Operation cancelled by user.")
    except Exception as e:
        print(f"An error occurred while trying to delete fields: {e}")


def rename_field(client, db_name, collection_name, old_name, new_name):
    """
    Renames a single field in all documents of a collection.

    Args:
        client (MongoClient): The active MongoDB client.
        db_name (str): The name of the database.
        collection_name (str): The name of the collection to modify.
        old_name (str): The current name of the field.
        new_name (str): The new name for the field.
    """
    db = client[db_name]
    collection = db[collection_name]

    if not old_name or not new_name:
        print("Old and new field names must be specified.")
        return

    print(f"Preparing to RENAME field '{old_name}' to '{new_name}' in collection '{collection_name}'.")

    # Create the payload for the $rename operation
    rename_payload = {old_name: new_name}

    try:
        # Confirm with the user before making a destructive change
        confirm = input(
            f"Are you sure you want to RENAME '{old_name}' to '{new_name}' for ALL documents in '{collection_name}'? This action cannot be undone. (yes/no): ")
        if confirm.lower() == 'yes':
            # We add a filter to only affect documents where the old field exists
            result = collection.update_many({old_name: {"$exists": True}}, {"$rename": rename_payload})
            print(
                f"Operation complete. Matched {result.matched_count} documents and modified {result.modified_count} documents.")
        else:
            print("Operation cancelled by user.")
    except Exception as e:
        print(f"An error occurred while trying to rename the field: {e}")


if __name__ == "__main__":
    # IMPORTANT: Replace with your MongoDB connection details
    MONGO_URI = "mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net"
    DB_NAME = "KG"  # Replace with your database name

    mongo_client = get_mongo_client(MONGO_URI)

    if mongo_client:
        # --- HELPER FUNCTION 1: Delete fields from a collection ---
        # To use this, uncomment the lines below and fill in the details.
        #fields_to_remove = ["function_match_score", "equivalent_domain", "taxonomy_match_score"]
        #delete_fields(mongo_client, DB_NAME, 'cdkg', fields_to_remove)

        # --- HELPER FUNCTION 2: Rename a field in a collection ---
        # To use this, uncomment the lines below and fill in the details.
        old_field_name = "function_sector"
        new_field_name = "sector"
        rename_field(mongo_client, DB_NAME, 'cdkg', old_field_name, new_field_name)

        mongo_client.close()
        print("MongoDB connection closed.")
