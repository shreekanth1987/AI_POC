import sqlite3
import pandas as pd
import logging
from datetime import datetime
import os
import json
import sys
from typing import Dict, Any, List

# --- GLOBAL CONFIGURATION ---
FOLDERS = ["BRONZE" ] #, "SILVER", "GOLD"]
JSON_FILE_PATH = "sqllite_config.json"
TIER_KEYWORDS = ["bronze"] # , "silver", "gold"]
OUTPUT_CSV_FILE = "table_metadata.csv" # Added for metadata function

# =============================================================================
# --- HELPER FUNCTIONS (Part 2: Table Name Generation) ---
# =============================================================================

def derive_custom_table_name(filename):
    """
    Extracts the table name starting from the tier keyword and includes
    the next two underscore-separated words, effectively stopping after the 3rd word.
    Example: 'ailab_bronze_eres_flight_data1.csv' -> 'bronze_eres_flight'
    """
    if not filename:
        return None

    base_name, _ = os.path.splitext(filename)
    base_name_lower = base_name.lower()

    start_index = -1
    for tier in TIER_KEYWORDS:
        if tier in base_name_lower:
            start_index = base_name_lower.find(tier)
            break

    if start_index != -1:
        custom_name = base_name[start_index:]
        parts = custom_name.split('_')
        final_parts = parts[:3]

        if final_parts:
            return "_".join(final_parts)
        return custom_name
    
    return base_name


def process_files_for_tier(file_list_or_single_file, tier_key, table_dict):
    """Processes filenames to dynamically create table entries for JSON config."""
    if isinstance(file_list_or_single_file, str):
        files_to_process = [file_list_or_single_file]
    elif file_list_or_single_file is None:
        files_to_process = []
    else:
        files_to_process = file_list_or_single_file

    tier_generated_names = []
    
    for file_name in files_to_process:
        if file_name:
            table_name = derive_custom_table_name(file_name)

            if tier_key == "silver" and table_name.lower().startswith("silver"):
                table_name = "silver" + table_name[6:]

            db_file_name = f"{table_name}.db"
            table_dict[table_name] = db_file_name
            tier_generated_names.append(table_name)

    return tier_generated_names


# =============================================================================
# --- HELPER FUNCTION (Part 3: Data Loading) ---
# =============================================================================

def process_data_load(tier_name, folder, csv_file, db_file, table_name):
    """Loads a single CSV file into a specified SQLite table. Uses 'replace' for all tiers."""
    full_csv_path = os.path.join(folder, csv_file)

    logging.info(f"\n--- Processing {tier_name.upper()} File: {csv_file} ---")
    logging.info(f"Source Path: {full_csv_path}")
    logging.info(f"Target DB: {db_file}")
    logging.info(f"Target Table: {table_name}") 

    if not os.path.exists(full_csv_path):
        logging.error(f"ERROR: CSV file not found at {full_csv_path}. Skipping load.")
        return

    try:
        df = pd.read_csv(full_csv_path)
        logging.info(f"   Successfully read {len(df)} rows from CSV.")

        conn = sqlite3.connect(db_file)
        
        # 🟢 CORRECTION: Use 'replace' for all tiers (Bronze, Silver, Gold) 
        # since each file loads into its own unique, corresponding table.
        if_exists_strategy = 'replace' 
        
        df.to_sql(table_name, conn, if_exists=if_exists_strategy, index=False)

        conn.commit()
        conn.close()
        logging.info(f"Data successfully loaded into table '{table_name}' in {db_file}. Strategy: {if_exists_strategy.upper()}")

    except Exception as e:
        logging.error(f"ERROR during {tier_name} data loading for {csv_file}: {e}")

# =============================================================================
# --- METADATA EXTRACTION FUNCTION (NEWLY ADDED) ---
# =============================================================================

def extract_and_compile_metadata(json_path: str = JSON_FILE_PATH) -> pd.DataFrame:
    """
    Reads SQLite database paths and table names from a JSON config file,
    connects to each database, extracts table metadata, and compiles it
    into a single Pandas DataFrame with the required additional columns.

    This function implements the following features:
    1. Tags are set to the corresponding database tier (e.g., #BRONZE).
    2. It checks for an existing metadata CSV file, loads it, updates it
       with the latest data (overwriting old entries for the same table),
       and then writes the combined result back.

    Args:
        json_path: The path to the SQLite configuration JSON file.

    Returns:
        A pandas DataFrame containing the compiled metadata, or an empty
        DataFrame if an error occurs.
    """
    print(f"\n--- Starting Metadata Extraction from {json_path} ---")

    # 1. Load Configuration
    try:
        with open(json_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {json_path}. Aborting metadata extraction.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print(f"ERROR: Failed to decode JSON from {json_path}. Aborting metadata extraction.")
        return pd.DataFrame()

    db_map: Dict[str, str] = config.get('databases', {})
    table_configs: Dict[str, Dict[str, str]] = {
        'bronze': config.get('bronze_tables', {}),
        'silver': config.get('silver_tables', {}),
        'gold': config.get('gold_tables', {}),
    }

    # Ensure db_map uses the actual file names created by the ingestion process
    db_map = {
        'bronze_data': 'bronze_data.db',
        'silver': 'silver_data.db',
        #'gold': 'gold.db',
    }

    all_metadata: List[Dict[str, Any]] = []

    # 2. Iterate through Tiers (Bronze, Silver, Gold)
    for tier, db_file_key in db_map.items():
        print(f"Processing Tier: {tier.upper()} (DB File: {db_file_key})")
        
        if not os.path.exists(db_file_key):
            print(f"WARNING: Database file '{db_file_key}' not found. Skipping {tier} tier.")
            continue

        conn = None
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(db_file_key)
            cursor = conn.cursor()

            # Query the sqlite_master table to get table metadata
            query = """
            SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
            cursor.execute(query)
            master_tables = cursor.fetchall()
            
            # The list of expected tables from the configuration for this tier (used for 'certified' flag)
            expected_tables = set(table_configs.get(tier, {}).keys())

            print(f"Found {len(master_tables)} tables in {db_file_key}.")

            for table_name, create_sql in master_tables:
                
                is_expected = table_name in expected_tables
                
                # 3. Create a dictionary for the row data
                row_data = {
                    'schema': tier.lower(),
                    'object_name': table_name,
                    'owner': 'Data_Pipeline_User',
                    'certified': 'Yes' if is_expected else 'No',
                    # Tags now only contain the tier hashtag as requested
                    'tags': f"#{tier.upper()}",
                    'create_sql': create_sql # Internal field for tracing
                }
                
                all_metadata.append(row_data)

        except sqlite3.Error as e:
            print(f"ERROR: SQLite error connecting to or querying {db_file_key}: {e}")
        finally:
            if conn:
                conn.close()

    # 4. Compile and Export/Update CSV
    if not all_metadata:
        print("No new metadata records were extracted.")
        return pd.DataFrame()

    # Create the DataFrame for the newly extracted data
    df_new_metadata = pd.DataFrame(all_metadata)
    
    # Define the columns for the final output CSV
    final_columns = ['schema', 'object_name', 'owner', 'certified', 'tags']
    
    # Ensure only the requested columns are kept and in the correct order
    df_new_output = df_new_metadata[final_columns]
    
    # Check for existing CSV and merge data
    df_combined = df_new_output
    if os.path.exists(OUTPUT_CSV_FILE):
        print(f"Existing file '{OUTPUT_CSV_FILE}' found. Loading and merging data...")
        try:
            df_existing = pd.read_csv(OUTPUT_CSV_FILE)
            
            # Combine existing and new data. Duplicates are identified by ['schema', 'object_name'].
            # Using 'keep=last' ensures the newest (just extracted) data for a table overwrites the old.
            df_combined = pd.concat([df_existing, df_new_output], ignore_index=True)
            
            df_combined.drop_duplicates(
                subset=['schema', 'object_name'], 
                keep='last', 
                inplace=True
            )
            print(f"Merged {len(df_existing)} existing records with {len(df_new_output)} new records.")
            
        except pd.errors.EmptyDataError:
            print(f"WARNING: Existing CSV '{OUTPUT_CSV_FILE}' was empty. Starting fresh.")
        except Exception as e:
            print(f"ERROR reading existing CSV file: {e}. Writing only new data.")
            df_combined = df_new_output

    try:
        # Write the resulting combined DataFrame to a single CSV file
        df_combined.to_csv(OUTPUT_CSV_FILE, index=False)
        print(f"\nSUCCESS: Compiled metadata written to {OUTPUT_CSV_FILE}")
        print("--- Head of Final Output Data ---")
        print(df_combined.head())
        print("---------------------------------")
        return df_combined
    except Exception as e:
        print(f"ERROR: Failed to write output CSV file: {e}")
        return df_combined

# --- Constants ---
# Define the database files and their corresponding schema/tier names
DATABASE_MAP = {
    'bronze': 'bronze_data.db',
    'silver': 'silver_data.db',
    # Add 'gold': 'gold_data.db' if a gold file is introduced
}

# The name of the output CSV file
OUTPUT_CSV_FILE = 'database_metadata.csv'

# The columns required in the final output CSV
FINAL_COLUMNS = ['schema', 'object_name', 'owner', 'certified', 'tags']
# -----------------

def extract_and_compile_metadata_simple() -> pd.DataFrame:
    """
    Connects to the hardcoded SQLite database files ('bronze_data.db', 'silver_data.db'),
    extracts metadata for all tables in each, and compiles it into a Pandas DataFrame.

    It creates a CSV file (database_metadata.csv) and updates it if it already exists,
    overwriting old entries for the same table.

    Returns:
        A pandas DataFrame containing the compiled metadata.
    """
    print("\n--- Starting Simple Metadata Extraction ---")
    all_metadata: List[Dict[str, Any]] = []

    # 1. Iterate through Databases and Extract Metadata
    for tier, db_file_key in DATABASE_MAP.items():
        print(f"Processing Tier: {tier.upper()} (DB File: {db_file_key})")

        if not os.path.exists(db_file_key):
            print(f"WARNING: Database file '{db_file_key}' not found. Skipping {tier} tier.")
            continue

        conn = None
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(db_file_key)
            cursor = conn.cursor()

            # Query the sqlite_master table to get table metadata
            query = """
            SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
            cursor.execute(query)
            master_tables = cursor.fetchall()
            
            print(f"Found {len(master_tables)} tables in {db_file_key}.")

            for table_name, create_sql in master_tables:
                
                # 2. Create a dictionary for the row data
                row_data = {
                    'schema': tier.lower(),
                    'object_name': table_name,
                    'owner': 'Data_Pipeline_User',
                    # Simplified: since we no longer read a config for 'expected' tables,
                    # we will mark all found tables as 'certified'. 
                    'certified': 'Yes', 
                    # Tags now only contain the tier hashtag
                    'tags': f"#{tier.upper()}",
                }
                all_metadata.append(row_data)

        except sqlite3.Error as e:
            print(f"ERROR: SQLite error connecting to or querying {db_file_key}: {e}")
        finally:
            if conn:
                conn.close()

    # 3. Compile and Export/Update CSV
    if not all_metadata:
        print("No new metadata records were extracted.")
        return pd.DataFrame()

    # Create the DataFrame for the newly extracted data
    df_new_metadata = pd.DataFrame(all_metadata)
    
    # Ensure only the requested columns are kept and in the correct order
    df_new_output = df_new_metadata[FINAL_COLUMNS]
    
    df_combined = df_new_output
    if os.path.exists(OUTPUT_CSV_FILE):
        print(f"Existing file '{OUTPUT_CSV_FILE}' found. Loading and merging data...")
        try:
            df_existing = pd.read_csv(OUTPUT_CSV_FILE)
            
            # Combine and remove duplicates, keeping the newest data (from the current run)
            df_combined = pd.concat([df_existing, df_new_output], ignore_index=True)
            df_combined.drop_duplicates(
                subset=['schema', 'object_name'], 
                keep='last', 
                inplace=True
            )
            print(f"Merged existing records with {len(df_new_output)} new records.")
            
        except pd.errors.EmptyDataError:
            print(f"WARNING: Existing CSV '{OUTPUT_CSV_FILE}' was empty. Starting fresh.")
        except Exception as e:
            print(f"ERROR reading existing CSV file: {e}. Writing only new data.")
            df_combined = df_new_output # Fallback to just the new data

    try:
        # Write the resulting combined DataFrame to a single CSV file
        df_combined.to_csv(OUTPUT_CSV_FILE, index=False)
        print(f"\nSUCCESS: Compiled metadata written to {OUTPUT_CSV_FILE}")
        print("--- Head of Final Output Data ---")
        print(df_combined.head())
        print("---------------------------------")
        return df_combined
    except Exception as e:
        print(f"ERROR: Failed to write output CSV file: {e}")
        return df_combined
    
# =============================================================================
# --- MAIN EXECUTION ORCHESTRATORS ---
# =============================================================================

def run_part1_fetch_files():
    """Part 1: Reads all CSV files from tier folders and updates the JSON config file."""
    print("\n--- Part 1: Fetching Filenames ---")
    
    csv_files = {}
    for folder in FOLDERS:
        try:
            files = [f for f in os.listdir(folder) if f.endswith(".csv")]
            csv_files[folder] = files
        except FileNotFoundError:
            print(f"Warning: Folder '{folder}' not found. Skipping file fetching for this folder.")
            csv_files[folder] = []

    file_config = {}
    if csv_files["BRONZE"]:
        file_config["bronze_csv_file"] = csv_files["BRONZE"][0]
    # file_config["silver_csv_files_list"] = csv_files.get("SILVER", [])
    # file_config["gold_csv_files_list"] = csv_files.get("GOLD", [])

    try:
        with open(JSON_FILE_PATH, 'r') as f:
            config_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config_data = {}

    existing_files = config_data.get("files", {})
    existing_files.update(file_config)
    config_data["files"] = existing_files

    print(f"Writing updated filenames to {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, 'w') as f:
        json.dump(config_data, f, indent=4)

    print(f"Part 1: Filenames updated in {JSON_FILE_PATH}.")


def run_part2_create_tables():
    """Part 2: Reads filenames from JSON and creates dynamic table configurations."""
    print("\n--- Part 2: Creating Dynamic Table Configuration ---")
    
    try:
        print(f"Reading existing configuration from {JSON_FILE_PATH}...")
        with open(JSON_FILE_PATH, 'r') as f:
            config_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Error: Could not read or decode JSON file at {JSON_FILE_PATH}. Aborting Part 2.")
        return 
    
    file_config = config_data.get("files", {})

    bronze_file = file_config.get("bronze_csv_file")
    # silver_files_list = file_config.get("silver_csv_files_list", []) 
    # gold_files_list = file_config.get("gold_csv_files_list", [])     

    bronze_tables = {}
    # silver_tables = {}
    # gold_tables = {}

    bronze_names = process_files_for_tier(bronze_file, "bronze", bronze_tables)
    # silver_names = process_files_for_tier(silver_files_list, "silver", silver_tables)
    # gold_names = process_files_for_tier(gold_files_list, "gold", gold_tables)

    config_data["bronze_tables"] = bronze_tables
    # config_data["silver_tables"] = silver_tables
    # config_data["gold_tables"] = gold_tables

    if "tables" in config_data:
        del config_data["tables"]

    print(f"Writing updated nested table configuration back to {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, 'w') as f:
        json.dump(config_data, f, indent=4)

    print(f"\nPart 2: Configuration file updated successfully.")
    print(f" - Bronze Table(s) Generated: **{', '.join(bronze_names) if bronze_names else 'N/A'}**")
    # print(f" - Silver Table(s) Generated: **{', '.join(silver_names) if silver_names else 'N/A'}**")
    # print(f" - Gold Table(s) Generated: **{', '.join(gold_names) if gold_names else 'N/A'}**")


def load_data_from_csv_to_db():
    """Part 3: Loads data using the 1:1 file-to-table mapping (no schema required)."""
    
    # 1. Load Configuration from JSON
    try:
        with open(JSON_FILE_PATH, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"FATAL ERROR: Could not load configuration file {JSON_FILE_PATH}. Aborting. Error: {e}")
        return

    # --- 2. Setup Logging ---
    try:
        log_file = config['logging']['log_file']
    except KeyError:
        log_file = "default_db_operations.log"
        print(f"Warning: 'logging' key not found in config. Using default log file: {log_file}")
    
    if not logging.getLogger('').handlers:
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filemode='a'
        )
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    
    logging.info(f"--- Data Loading Process Started ---")
    logging.info(f"Configuration loaded successfully from {JSON_FILE_PATH}.")


    # --- 3. Extract Configuration Values ---
    
    bronze_filename = config['files'].get('bronze_csv_file')
    # silver_files = config['files'].get('silver_csv_files_list', [])
    # gold_files = config['files'].get('gold_csv_files_list', [])
    
    db_map = {
        "bronze": config['databases']['bronze_database_file']
    } 
    # ,
    #     "silver": config['databases']['silver_database_file'],
    #     "gold": config['databases']['gold_database_file'],
    # }
    
    # Bronze table name is derived from Part 2
    try:
        bronze_table_name = list(config['bronze_tables'].keys())[0]
    except (IndexError, KeyError):
        bronze_table_name = None


    # --- 4. Execute Data Loading ---
    
    # A. Load Bronze Data (Single File)
    logging.info("\n--- EXECUTING BRONZE LOAD ---")
    if bronze_filename and bronze_table_name:
        process_data_load(
            tier_name="bronze",
            folder="BRONZE",
            csv_file=bronze_filename,
            db_file=db_map["bronze"],
            table_name=bronze_table_name # Uses derived table name
        )
    else:
        logging.info("Skipping Bronze load: No bronze file or table name found.")


    # B. Load Silver Data (Multiple Files -> Multiple Tables)
    # logging.info("\n--- EXECUTING SILVER LOAD (Files map to respective tables) ---")
    # if silver_files:
    #     for silver_file in silver_files:
    #         # 🟢 DYNAMIC MAPPING: Recalculate the table name based on the filename
    #         target_table = derive_custom_table_name(silver_file)
            
    #         if target_table:
    #             process_data_load(
    #                 tier_name="silver",
    #                 folder="SILVER",
    #                 csv_file=silver_file,
    #                 db_file=db_map["silver"],
    #                 table_name=target_table # Uses unique table name per file
    #             )
    #         else:
    #              logging.warning(f"Skipping Silver file '{silver_file}': Could not map to a target table name.")
    # else:
    #     logging.info("Skipping Silver load: No silver files found.")


    # C. Load Gold Data (Multiple Files -> Multiple Tables)
    # logging.info("\n--- EXECUTING GOLD LOAD (Files map to respective tables) ---")
    # if gold_files:
    #     for gold_file in gold_files:
    #         # 🟢 DYNAMIC MAPPING: Recalculate the table name based on the filename
    #         target_table = derive_custom_table_name(gold_file)
            
    #         if target_table:
    #             process_data_load(
    #                 tier_name="gold",
    #                 folder="GOLD",
    #                 csv_file=gold_file,
    #                 db_file=db_map["gold"],
    #                 table_name=target_table # Uses unique table name per file
    #             )
    #         else:
    #              logging.warning(f"Skipping Gold file '{gold_file}': Could not map to a target table name.")
    # else:
    #     logging.info("Skipping Gold load: No gold files found.")
    
    # logging.info("\n--- Data Loading Process Finished ---")


def main():
    """Orchestrates the entire data pipeline: Fetching files, creating tables, and loading data."""
    # run_part1_fetch_files()
    # run_part2_create_tables() 
    # load_data_from_csv_to_db()
    # extract_and_compile_metadata()
    extract_and_compile_metadata_simple()

if __name__ == "__main__":
    main()