
import os
import json

import duckdb
from tqdm import tqdm



data_dir = "data_tmp"

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

files = [filename for filename in os.listdir(data_dir) if filename.startswith("target_disease_") and filename.endswith(".json")]

for filename in tqdm(files):
    file_path = os.path.join(data_dir, filename)
    
    with open(file_path, "r", encoding="utf-8") as f:
        associated_diseases_list = json.load(f)

    # Extract target_id
    target_id = filename.replace("target_disease_", "").replace(".json", "")

    for row in associated_diseases_list:
        disease_id = row['disease']['id']

        q = 'INSERT OR IGNORE INTO tbl_disease_target VALUES ($disease_id, $target_id)'
        params = {'disease_id': disease_id, 'target_id': target_id}
        con.execute(q, params)


con.sql("SELECT * FROM tbl_diseases LIMIT 10").show()
print(f'tbl_diseases: {con.execute("SELECT count(*) FROM tbl_diseases").fetchone()[0]} rows')
print(f'tbl_disease_target: {con.execute("SELECT count(*) FROM tbl_disease_target").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")
