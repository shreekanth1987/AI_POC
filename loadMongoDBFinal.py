import json
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import os
import glob     # NEW: For finding files matching a pattern
import shutil   # NEW: For moving files

# --- Helper Function to Load Configuration ---
def load_config(file_path):
    """Loads configuration settings from a JSON file."""
    try:
        # Check if the file exists before attempting to open it
        if not os.path.exists(file_path):
            print(f"Error: Configuration file not found at '{file_path}'.")
            return None
            
        with open(file_path, 'r') as f:
            config = json.load(f)
        print(f"Successfully loaded configuration from '{file_path}'.")
        return config
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{file_path}'. Please check the file content.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading config: {e}")
        return None

# --- Main MongoDB Upload Function (Modified to return status) ---
def upload_json_to_mongodb(file_path, mongo_uri, db_name, collection_name):
    """
    Connects to MongoDB, reads a single line-delimited JSON file, 
    and inserts documents. Returns True on success, False otherwise.
    """
    client = None
    documents_to_insert = []
    
    # --- File Reading and Parsing ---
    try:
        # 1. Read the file line by line
        with open(file_path, 'r') as f:
            for line_number, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                try:
                    # 2. Parse the line into a Python dictionary (JSON object)
                    document = json.loads(line)
                    documents_to_insert.append(document)
                except json.JSONDecodeError as e:
                    # Log a warning but continue processing the file
                    print(f"Warning: Could not parse line {line_number + 1} in '{os.path.basename(file_path)}'. Skipping. Error: {e}")
        
        if not documents_to_insert:
            print(f"No valid JSON documents found in '{os.path.basename(file_path)}'. Skipping upload.")
            return False

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found during reading.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during file reading: {e}")
        return False
        
    # --- MongoDB Insertion ---
    try:
        # 3. Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        print(f"  Attempting to insert {len(documents_to_insert)} documents into '{collection_name}'...")
        
        # 4. Insert all documents into the collection
        result = collection.insert_many(documents_to_insert)
        
        print(f"  Successfully inserted {len(result.inserted_ids)} documents.")
        return True # Return True on successful insertion
        
    except PyMongoError as e:
        print(f"  A MongoDB error occurred for file '{os.path.basename(file_path)}': {e}")
        return False
    except Exception as e:
        print(f"  An unexpected error occurred during insertion: {e}")
        return False
    finally:
        # 5. Close the MongoDB connection
        if client:
            client.close()

# --- New Batch Processing and File Movement Function (Orchestrator) ---
def process_ingestion_with_file_move(base_dir, mongo_uri, db_name, collection_name):
    """
    Scans the source directory for JSON files, uploads them, and moves 
    them to the DONE directory upon successful insertion.
    """
    # Define Source and Destination folders based on the base path
    SOURCE_DIR = os.path.join(base_dir, "TO")
    DONE_DIR = os.path.join(base_dir, "DONE")
    
    print(f"\n🚀 Starting ingestion process from: {SOURCE_DIR}")
    print(f"📁 Files will be moved to: {DONE_DIR}")
    print("-" * 50)
    
    # 1. Ensure the DONE directory exists
    os.makedirs(DONE_DIR, exist_ok=True)

    # 2. Find all JSON files in the source directory
    file_paths = glob.glob(os.path.join(SOURCE_DIR, "*.json"))

    if not file_paths:
        print(f"ℹ️ No JSON files found in {SOURCE_DIR}. Exiting.")
        return

    total_files_processed = 0
    total_files_successful = 0

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        print(f"\nProcessing file: {file_name}")
        
        # 3. Attempt to upload the file
        success = upload_json_to_mongodb(file_path, mongo_uri, db_name, collection_name)
        
        total_files_processed += 1

        if success:
            total_files_successful += 1
            
            # 4. Move the file to the DONE folder
            destination_path = os.path.join(DONE_DIR, file_name)
            try:
                shutil.move(file_path, destination_path)
                print(f"  ✅ File moved to DONE: {destination_path}")
            except Exception as e:
                print(f"❌ Error moving file '{file_name}': {e}")
        else:
            print(f"  ⚠️ File upload failed. Keeping file in 'TO' folder: {file_name}")

    print("-" * 50)
    print(f"✨ Ingestion Complete: {total_files_successful}/{total_files_processed} files successfully processed and moved.")


# --- MongoDB Retrieval Function (Latest Completed Jobs) ---
def retrieve_and_print_json_getlatest(mongo_uri, db_name, collection_name):
    """
    Connects to MongoDB, retrieves only documents where eventType = COMPLETE,
    gets the latest entry for each unique job.name based on eventTime,
    and prints results using the Aggregation Framework.
    """
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        print(f"Connected to MongoDB database '{db_name}' and collection '{collection_name}'")

        # Aggregation pipeline:
        pipeline = [
            # 1. Filter only COMPLETE events
            {"$match": {"eventType": "COMPLETE"}},
            
            # 2. Sort by eventTime descending (latest first). Critical for $group.
            {"$sort": {"eventTime": -1}},
            
            # 3. Group by job.name and take the first document (which is the latest)
            {"$group": {
                "_id": "$job.name",
                "latestEvent": {"$first": "$$ROOT"}
            }},
            
            # 4. Replace root to get back the original document structure
            {"$replaceRoot": {"newRoot": "$latestEvent"}},
            
            # 5. Remove MongoDB's _id field
            {"$project": {"_id": 0}},
            
            # Optional: Sort by eventTime again for final output readability
            {"$sort": {"eventTime": -1}}
        ]

        completed_events = list(collection.aggregate(pipeline))

        if not completed_events:
            print("No COMPLETE events found.")
            return

        print(f"Retrieved {len(completed_events)} unique jobs with latest COMPLETE events")
        print("-" * 60)

        # Print each JSON, using default=str to correctly handle datetime objects
        for doc in completed_events:
            print(json.dumps(doc, default=str)) 

        print("-" * 60)
        return completed_events

    except PyMongoError as e:
        print(f"MongoDB Retrieval Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during retrieval: {e}")

    finally:
        # Close the connection whether the try block succeeded or failed
        if client:
            client.close()
            print("🔌 Connection closed.")
            
# --- MongoDB Deletion Function ---
def delete_mongodb_entries(mongo_uri, db_name, collection_name, query_filter=None):
    """
    Connects to MongoDB and deletes documents matching the specified query_filter.
    """
    client = None
    if query_filter is None:
        query_filter = {} 
        print("⚠️ Warning: No specific filter provided. Preparing to delete ALL documents.")

    try:
        # 1. Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        print(f"✅ Connected to MongoDB database '{db_name}' and collection '{collection_name}' for deletion.")
        
        # 2. Execute deletion
        print(f"Attempting to delete documents matching filter: {query_filter}")
        result = collection.delete_many(query_filter)
        
        # 3. Report results
        if result.deleted_count > 0:
            print(f"✅ Successfully deleted {result.deleted_count} document(s).")
        else:
            print(f"ℹ️ No documents found matching the filter to delete.")
            
        return result.deleted_count

    except PyMongoError as e:
        print(f"❌ MongoDB Deletion Error: {e}")
        return 0
    except Exception as e:
        print(f"❌ An unexpected error occurred during deletion: {e}")
        return 0

    finally:
        # 4. Close the connection
        if client:
            client.close()
            print("🔌 Connection closed.")

# --- Execution Block ---
if __name__ == "__main__":
    
    CONFIG_FILE = "mongoConfig.JSON" # The name of your config file

    # 1. Load configuration details
    config = load_config(CONFIG_FILE)
    
    if config:
        # 2. Safely retrieve values from the loaded configuration
        try:
            MONGODB_URI = config["MONGODB_URI"]
            DATABASE_NAME = config["DATABASE_NAME"]
            COLLECTION_NAME = config["COLLECTION_NAME"]
            
            # NEW: Retrieve the base ingestion path from the config file
            INGESTION_BASE_PATH = config["INGESTION_BASE_PATH"]
            
            # --- Choose which operation to run here (Only Ingestion is active) ---
            
            # OPTION 1: UPLOAD DATA (Now uses batch processing and file move)
            print("\n--- Starting MongoDB UPLOAD with File Movement ---")
            process_ingestion_with_file_move(INGESTION_BASE_PATH, MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)

            # OPTION 2: RETRIEVE LATEST COMPLETED DATA
            # print("\n--- Starting MongoDB RETRIEVAL (Latest Completed Jobs) ---")
            # retrieve_and_print_json_getlatest(MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)

            # OPTION 3: DELETE DATA
            # print("\n--- Starting MongoDB DELETION ---")
            # deletion_filter = {"eventType": "START"}
            # # deletion_filter = {} # Use this filter to delete ALL documents!

            # deleted_count = delete_mongodb_entries(
            #     MONGODB_URI, 
            #     DATABASE_NAME, 
            #     COLLECTION_NAME, 
            #     query_filter=deletion_filter
            # )
            # print(f"Total documents affected: {deleted_count}")

        except KeyError as e:
            # Handle cases where a required key is missing in the JSON file
            print(f"Error: Missing required key '{e}' in {CONFIG_FILE}. Check your configuration file.")