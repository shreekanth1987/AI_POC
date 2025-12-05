import json
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import os

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

# --- Main MongoDB Upload Function ---
def upload_json_to_mongodb(file_path, mongo_uri, db_name, collection_name):
    """
    Connects to MongoDB, reads a file containing line-delimited JSON objects, 
    and inserts each object as a separate document into a specified collection.
    """
    client = None
    try:
        # 1. Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        print(f"Successfully connected to MongoDB database '{db_name}'.")

        documents_to_insert = []
        
        # 2. Read the file line by line
        with open(file_path, 'r') as f:
            for line_number, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                try:
                    # 3. Parse the line into a Python dictionary (JSON object)
                    document = json.loads(line)
                    documents_to_insert.append(document)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line {line_number + 1} as JSON. Skipping. Error: {e}")
        
        if not documents_to_insert:
            print(f"No valid JSON documents found in '{file_path}'. Exiting.")
            return

        # 4. Insert all documents into the collection
        print(f"Attempting to insert {len(documents_to_insert)} documents into '{collection_name}'...")
        
        # Use insert_many for efficiency
        result = collection.insert_many(documents_to_insert)
        
        print(f"Successfully inserted {len(result.inserted_ids)} documents.")
        print(f"Collection: {db_name}.{collection_name}")
        
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except PyMongoError as e:
        print(f"A MongoDB error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 5. Close the MongoDB connection
        if client:
            client.close()
            print("Connection closed.")

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
    If no filter is provided (query_filter={}), all documents in the collection will be deleted.
    """
    client = None
    if query_filter is None:
        # Default to an empty query which matches all documents
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
    
    CONFIG_FILE = "mongoConfig.JSON" # Define the name of your config file

    # 1. Load configuration details
    config = load_config(CONFIG_FILE)
    
    if config:
        # 2. Safely retrieve values from the loaded configuration
        try:
            MONGODB_URI = config["MONGODB_URI"]
            DATABASE_NAME = config["DATABASE_NAME"]
            COLLECTION_NAME = config["COLLECTION_NAME"]
            JSON_FILE_PATH = config["JSON_FILE_PATH"] # Only needed for upload

            # --- Choose which operation to run here ---
            
            # OPTION 1: UPLOAD DATA
            # print("\n--- Starting MongoDB UPLOAD ---")
            # upload_json_to_mongodb(JSON_FILE_PATH, MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)

            # OPTION 2: RETRIEVE LATEST COMPLETED DATA (CURRENTLY ACTIVE)
            print("\n--- Starting MongoDB RETRIEVAL (Latest Completed Jobs) ---")
            retrieve_and_print_json_getlatest(MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)

            # OPTION 3: DELETE DATA (Example: Delete all documents with eventType 'START')
            print("\n--- Starting MongoDB DELETION ---")
            deletion_filter = {"eventType": "START"}
            # deletion_filter = {} # Use this filter to delete ALL documents!

            deleted_count = delete_mongodb_entries(
                MONGODB_URI, 
                DATABASE_NAME, 
                COLLECTION_NAME, 
                query_filter=deletion_filter
            )
            print(f"Total documents affected: {deleted_count}")

        except KeyError as e:
            # Handle cases where a required key is missing in the JSON file
            print(f"Error: Missing required key '{e}' in {CONFIG_FILE}. Check your configuration file.")