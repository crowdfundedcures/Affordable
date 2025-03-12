import os
import json
import duckdb

# Define paths
DATA_DIR = "data/202409XX/molecule"  # Change this to your actual directory path
DUCKDB_PATH = "bio_data.duck.db"
TEMP_TSV_PATH = "data_tmp/temp_data.tsv"  # Changed file extension to .tsv
NULL = '<NULL>'  # Define NULL value "string" in temporary TSV file

# Initialize DuckDB connection
con = duckdb.connect(DUCKDB_PATH)

# Create a list to store parsed data
data_list = [[
    "id",
    "canonicalSmiles",
    "inchiKey",
    "drugType",
    "blackBoxWarning",
    "name",
    "yearOfFirstApproval",
    "maximumClinicalTrialPhase",
    "hasBeenWithdrawn",
    "isApproved",
    "tradeNames",
    "synonyms",
    "crossReferences",
    "childChemblIds",
    "linkedDiseases",
    "linkedTargets",
    "description",
]]

# Iterate through files in the directory
for filename in os.listdir(DATA_DIR):
    if filename.startswith("part-") and filename.endswith(".json"):  # Adjust based on file format
        file_path = os.path.join(DATA_DIR, filename)
        
        try:
            # Read NDJSON (Newline Delimited JSON)
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.replace('\t', ' ')
                    record = json.loads(line)
                    # {
                    #     "id": str,
                    #     "canonicalSmiles": str,
                    #     "inchiKey": str,
                    #     "drugType": str,
                    #     "blackBoxWarning": bool,
                    #     "name": str,
                    #     "yearOfFirstApproval": int,
                    #     "maximumClinicalTrialPhase": float,
                    #     "hasBeenWithdrawn": bool,
                    #     "isApproved": bool,
                    #     "tradeNames": [str, ...],
                    #     "synonyms": [str, ...],
                    #     "crossReferences": {
                    #         "PubChem": [str, ...],
                    #         "Wikipedia": [str, ...],
                    #         ...
                    #     },
                    #     "childChemblIds": [str, ...],
                    #     "linkedDiseases": {
                    #         "rows": [str, ...],
                    #         "count": int
                    #     },
                    #     "linkedTargets": {
                    #         "rows": [str, ...],
                    #         "count": int
                    #     },
                    #     "description": str
                    # }

                    # Extract relevant fields
                    data_list.append([
                        record["id"],
                        record.get("canonicalSmiles", NULL),
                        record.get("inchiKey", NULL),
                        record.get("drugType", NULL),
                        record.get("blackBoxWarning", NULL),
                        record.get("name", NULL),
                        record.get("yearOfFirstApproval", NULL),
                        record.get("maximumClinicalTrialPhase", NULL),
                        record.get("hasBeenWithdrawn", NULL),
                        record.get("isApproved", NULL),
                        json.dumps(record.get("tradeNames", []), sort_keys=True),  # Store as JSON string
                        json.dumps(record.get("synonyms", []), sort_keys=True),  # Store as JSON string
                        json.dumps(record.get("crossReferences", {}), sort_keys=True),  # Store as JSON string
                        json.dumps(record.get("childChemblIds", []), sort_keys=True),  # Store as JSON string
                        json.dumps(record.get("linkedDiseases", {}).get("rows", []), sort_keys=True),  # Store as JSON string
                        json.dumps(record.get("linkedTargets", {}).get("rows", []), sort_keys=True),  # Store as JSON string
                        record.get("description", NULL),
                    ])

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Save to tsv file
with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\n'.join('\t'.join(map(str, row)) for row in data_list))

# Copy data into DuckDB with strict mode disabled
con.execute(f"""
    COPY tbl_molecules FROM '{TEMP_TSV_PATH}'
    (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
""")

# Verify data import
con.sql("SELECT * FROM tbl_molecules LIMIT 20").show()
print(f'Total: {con.execute("SELECT count(*) FROM tbl_molecules").fetchone()[0]} rows')

# Cleanup
con.close()
os.remove(TEMP_TSV_PATH)

print("Data successfully imported into DuckDB as TSV.")
