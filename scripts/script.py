import json
import os
import sys
from datetime import datetime
from pathlib import Path

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
                # No mapping available, set to null
                mapped_entry[clean_key] = None
                continue
            
            field_mapping = lookup_cache[eb_field]
            value_str = str(value).strip()
            
            # Handle multi-code values (e.g., "UC^86")
            if "^" in value_str:
                codes = value_str.split("^")
                # Use list comprehension for better performance
                # Set to None if code is not found in mapping
                mapped_values = [
                    field_mapping.get(code.strip(), None) 
                    for code in codes
                ]
                # Filter out None values and join
                mapped_values = [v for v in mapped_values if v is not None]
                mapped_entry[clean_key] = ", ".join(mapped_values) if mapped_values else None
            else:
                # Single code value - direct lookup, None if not found
                mapped_entry[clean_key] = field_mapping.get(value_str, None)
        else:
            # Keep other fields as-is, but remove @ from all nested keys
            mapped_entry[key] = remove_at_prefix(value)
    
    # Finally, remove @ prefix from all remaining keys
    return remove_at_prefix(mapped_entry)

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
    
    # Process each file
    for json_file in json_files:
        file_path = os.path.join(data_folder, json_file)
        print(f"Processing: {json_file}")
        
        # Extract member ID from filename for reference
        results = process_json_file(file_path, lookup_cache)
        
        if results:
            # Create output filename
            output_filename = f"{json_file.replace('.json', '')}_processed.json"
            output_path = os.path.join(output_folder, output_filename)
            
            # Write output
            with open(output_path, "w") as f:
                json.dump(results, f, indent=2)
            
            print(f"✅ Saved: {output_filename} ({len(results)} EB records)\n")
        else:
            print(f"⚠️ No data extracted from {json_file}\n")

if __name__ == "__main__":
    # Check if a specific file was provided as argument
    if len(sys.argv) > 1:
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
        
        # Load mapping and process single file
        print(f"Loading and building mapping cache...")
        lookup_cache = load_mapping()
        
        print(f"\nProcessing single file: {specific_file}")
        results = process_json_file(file_path, lookup_cache)
        
        if results:
            # Create output filename
            output_filename = f"{specific_file.replace('.json', '')}.json"
            output_path = os.path.join(output_folder, output_filename)
            
            # Write output
            with open(output_path, "w") as f:
                json.dump(results, f, indent=2)
            
            print(f"✅ Successfully processed: {specific_file}")
            print(f"✅ Output saved to: {output_filename}")
            print(f"   Records: {len(results)} EB entries")
        else:
            print(f"⚠️ No data extracted from {specific_file}")
    else:
        # Process all files if no argument provided
        main()




# Process all files (default behavior)
# python script.py

# Process a specific file
# python script.py "FILENAME.json"

# Example with actual filename
# python script.py "AETNA-Cobia-271.json"