import json
import os
import sys
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging file
LOG_FILE = None

def setup_log_file():
    """Setup pipeline_timestamp log file in logs folder with timestamp"""
    global LOG_FILE
    logs_folder = "../logs"
    os.makedirs(logs_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    LOG_FILE = os.path.join(logs_folder, f"pipeline_{timestamp}.log")
    return LOG_FILE

def log_message(message):
    """Append message to pipeline_timestamp log file"""
    global LOG_FILE
    if LOG_FILE:
        try:
            with open(LOG_FILE, "a") as f:
                f.write(message + "\n")
        except Exception as e:
            print(f"❌ Error writing to log file: {e}")

def log_line_break():
    """Add a line break to the log file"""
    global LOG_FILE
    if LOG_FILE:
        try:
            with open(LOG_FILE, "a") as f:
                f.write("\n")
        except Exception as e:
            print(f"❌ Error writing to log file: {e}")

# Connect to PostgreSQL database
def connect_to_db():
    """Connect to PostgreSQL database using environment variables"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "pdc"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        print("✅ Connected to PostgreSQL database 'pdc'")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Error connecting to database: {error}")
        return None

# Helper function to recursively remove @ from all keys
def remove_at_prefix(obj):
    """Recursively remove @ prefix from all dictionary keys"""
    if isinstance(obj, dict):
        return {k.lstrip("@"): remove_at_prefix(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_at_prefix(item) for item in obj]
    else:
        return obj

# Load mapping file and build lookup cache
def load_mapping():
    """Load the EB code mappings from mapping.json and build lookup cache"""
    with open("mapping.json", "r") as f:
        mapping = json.load(f)
    
    # Pre-build lookup cache for faster access
    # This avoids repeated dictionary lookups during processing
    lookup_cache = {}
    for eb_field, codes_dict in mapping.items():
        lookup_cache[eb_field] = {}
        for code, description in codes_dict.items():
            # Normalize code to string for consistent lookup
            lookup_cache[eb_field][str(code)] = description
    
    return lookup_cache

# Extract member ID and payer info from JSON
def extract_patient_info(data):
    """Extract patient member ID and payer info from the JSON structure"""
    try:
        member_id = data["ISA"]["GS"]["ST"]["HL"]["HL"]["HL"]["NM1"]["@NM109"]
        return member_id
    except KeyError:
        return None

# Get EB data location (handle both HL3 and HL4)
def get_eb_list(data):
    """Extract EB list from JSON, handling both HL3 and HL4 structures"""
    try:
        # Try HL4 structure first
        eb_list = data["ISA"]["GS"]["ST"]["HL"]["HL"]["HL"]["HL"]["EB"]
        return eb_list
    except (KeyError, TypeError):
        try:
            # Fall back to HL3 structure
            eb_list = data["ISA"]["GS"]["ST"]["HL"]["HL"]["HL"]["EB"]
            return eb_list
        except (KeyError, TypeError):
            return None

# Map EB code values to their descriptions
def map_eb_codes(eb_entry, lookup_cache):
    """Map EB codes to their descriptions using pre-built lookup cache"""
    mapped_entry = {}
    
    # Process all fields in the EB entry
    for key, value in eb_entry.items():
        if key == "MSG":
            # Keep MSG as-is (convert dict to list if needed)
            if isinstance(value, list):
                mapped_entry[key] = value
            elif isinstance(value, dict):
                mapped_entry[key] = [value]
            else:
                mapped_entry[key] = value
        elif key.startswith("@EB"):
            # Map EB codes using pre-built cache
            eb_field = key[1:]  # Remove @ prefix: "@EB03" → "EB03"
            clean_key = key.lstrip("@")  # Remove @ for output key
            
            # Check if this EB field has mappings
            if eb_field not in lookup_cache:
                # No mapping available, keep original value
                mapped_entry[clean_key] = value
                continue
            
            field_mapping = lookup_cache[eb_field]
            value_str = str(value).strip()
            
            # Handle multi-code values (e.g., "UC^86")
            if "^" in value_str:
                codes = value_str.split("^")
                # Use list comprehension for better performance
                # Keep original code if not found in mapping
                mapped_values = [
                    field_mapping.get(code.strip(), code.strip()) 
                    for code in codes
                ]
                mapped_entry[clean_key] = ", ".join(mapped_values)
            else:
                # Single code value - direct lookup, keep original if not found
                mapped_entry[clean_key] = field_mapping.get(value_str, value_str)
        else:
            # Keep other fields as-is, but remove @ from all nested keys
            mapped_entry[key] = remove_at_prefix(value)
    
    # Finally, remove @ prefix from all remaining keys
    return remove_at_prefix(mapped_entry)

# Insert data into database
def insert_into_db(conn, member_id, data_records):
    """Insert processed EB records into eb_blocks table"""
    if not conn or not data_records:
        return 0
    
    try:
        cursor = conn.cursor()
        inserted_count = 0
        
        for record in data_records:
            insert_query = sql.SQL(
                "INSERT INTO eb_blocks (member_id, data) VALUES (%s, %s)"
            )
            cursor.execute(insert_query, (member_id, json.dumps(record["data"])))
            inserted_count += 1
        
        conn.commit()
        cursor.close()
        return inserted_count
    
    except Exception as e:
        print(f"❌ Error inserting into database: {str(e)}")
        if conn:
            conn.rollback()
        return 0

# Process a single JSON file
def process_json_file(file_path, lookup_cache):
    """Process a single JSON file and extract EB data"""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        member_id = extract_patient_info(data)
        eb_list = get_eb_list(data)
        
        if not eb_list:
            print(f"⚠️ No EB data found in {os.path.basename(file_path)}")
            return []
        
        # Ensure EB is a list
        if isinstance(eb_list, dict):
            eb_list = [eb_list]
        
        # Process each EB entry
        results = []
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        for idx, eb_entry in enumerate(eb_list, 1):
            if not isinstance(eb_entry, dict):
                continue
            
            # Map codes using optimized lookup
            mapped_data = map_eb_codes(eb_entry, lookup_cache)
            
            # Create output record
            record = {
                "id": idx,
                "member_id": member_id,
                "inserted_at": timestamp,
                "data": mapped_data
            }
            
            results.append(record)
        
        return results
    
    except Exception as e:
        print(f"❌ Error processing {os.path.basename(file_path)}: {str(e)}")
        return []

# Main processing function
def main():
    """Main function to process all JSON files in data folder"""
    
    # Setup logging
    setup_log_file()
    
    # Check database connection
    print("Checking database connection...")
    conn = connect_to_db()
    if not conn:
        print("❌ Failed to connect to database. Exiting.")
        return
    
    # Load mapping once and build lookup cache
    print("Loading and building mapping cache...")
    lookup_cache = load_mapping()
    
    # Get all JSON files from data folder
    data_folder = "../data"
    output_folder = "../output_data"
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Get all JSON files
    json_files = [f for f in os.listdir(data_folder) if f.endswith(".json")]
    
    print(f"Found {len(json_files)} JSON files to process\n")
    
    total_db_records = 0
    
    # Process each file
    for json_file in json_files:
        file_path = os.path.join(data_folder, json_file)
        print(f"Processing: {json_file}")
        log_message(f"Processing: {json_file}")
        
        # Extract member ID from filename for reference
        results = process_json_file(file_path, lookup_cache)
        
        if results:
            # Insert into database
            member_id = results[0].get("member_id")
            db_inserted = insert_into_db(conn, member_id, results)
            total_db_records += db_inserted
            
            # Create output filename
            output_filename = f"{json_file.replace('.json', '')}_processed.json"
            output_path = os.path.join(output_folder, output_filename)
            
            # Write output
            with open(output_path, "w") as f:
                json.dump(results, f, indent=2)
            
            log_msg_1 = f"✅ Saved: {output_filename} ({len(results)} EB records)"
            log_msg_2 = f"✅ Inserted into DB: {db_inserted} records"
            
            print(log_msg_1)
            print(log_msg_2)
            log_message(log_msg_1)
            log_message(log_msg_2)
            log_line_break()
        else:
            print(f"⚠️ No data extracted from {json_file}\n")
    
    print(f"📊 Processing complete! Total DB records inserted: {total_db_records}")
    conn.close()

if __name__ == "__main__":
    # Check if a specific file was provided as argument
    if len(sys.argv) > 1:
        # Setup logging
        setup_log_file()
        
        # Process single file
        specific_file = sys.argv[1]
        data_folder = "../data"
        output_folder = "../output_data"
        
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Check if file exists
        file_path = os.path.join(data_folder, specific_file)
        if not os.path.exists(file_path):
            print(f"❌ File not found: {specific_file}")
            print(f"   Expected path: {file_path}")
            sys.exit(1)
        
        # Connect to database
        print("Checking database connection...")
        conn = connect_to_db()
        if not conn:
            print("❌ Failed to connect to database. Exiting.")
            sys.exit(1)
        
        # Load mapping and process single file
        print(f"Loading and building mapping cache...")
        lookup_cache = load_mapping()
        
        print(f"\nProcessing single file: {specific_file}")
        log_message(f"Processing: {specific_file}")
        
        results = process_json_file(file_path, lookup_cache)
        
        if results:
            # Insert into database
            member_id = results[0].get("member_id")
            db_inserted = insert_into_db(conn, member_id, results)
            
            # Create output filename
            output_filename = f"{specific_file.replace('.json', '')}.json"
            output_path = os.path.join(output_folder, output_filename)
            
            # Write output
            with open(output_path, "w") as f:
                json.dump(results, f, indent=2)
            
            log_msg_1 = f"✅ Saved: {output_filename} ({len(results)} EB records)"
            log_msg_2 = f"✅ Inserted into DB: {db_inserted} records"
            
            print(f"✅ Successfully processed: {specific_file}")
            print(log_msg_1)
            print(log_msg_2)
            log_message(log_msg_1)
            log_message(log_msg_2)
            log_line_break()
        else:
            print(f"⚠️ No data extracted from {specific_file}")
        
        conn.close()
    else:
        # Process all files if no argument provided
        main()




# Process all files (default behavior)
# python script.py

# Process a specific file
# python script.py "FILENAME.json"

# Example with actual filename
# python script.py "AETNA-Cobia-271.json"