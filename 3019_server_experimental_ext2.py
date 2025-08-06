import os
import json
from threading import Thread
import duckdb
from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import numpy as np
from typing import List, Dict, Optional
from pydantic import BaseModel
from hashlib import sha256
import hashlib
import secrets
import datetime as dt



STATUS_NUM = {
    'Active, not recruiting': 4,
    'Completed': 5,
    'Enrolling by invitation': 3,
    'Not yet recruiting': 1,
    'Recruiting': 2,
    'Suspended': 0,
    'Terminated': 0,
    'Unknown status': 0,
    'Withdrawn': 0,
    'N/A': 0,
}

management_db_path = "management.duck.db"
bio_data_db_path = "bio_data.duck.db"


# Database Initialization
management_conn = duckdb.connect(management_db_path)

management_conn.execute("""
    CREATE TABLE IF NOT EXISTS evidence (
        disease_id TEXT NOT NULL,
        reference_drug_id TEXT NOT NULL,
        replacement_drug_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        action_type TEXT NOT NULL,
        mechanism_of_action TEXT NOT NULL,
        refs TEXT[],
        PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id, target_id)
    )
""")
management_conn.execute("""
    CREATE TABLE IF NOT EXISTS ivpe_table (
        similarity FLOAT NOT NULL,
        disease_id TEXT NOT NULL,
        disease_name TEXT NOT NULL,
        reference_drug_id TEXT NOT NULL,
        reference_drug_name TEXT NOT NULL,
        replacement_drug_id TEXT NOT NULL,
        replacement_drug_name TEXT NOT NULL,
        global_patient_population TEXT NOT NULL DEFAULT 'N/A',
        cost_difference TEXT NOT NULL DEFAULT 'N/A',
        evidence TEXT NOT NULL,
        annual_cost_reduction TEXT NOT NULL DEFAULT 'N/A',
        is_active BOOLEAN NOT NULL DEFAULT 0,
        PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id)
    )
""")
management_conn.execute("""
    CREATE TABLE IF NOT EXISTS pfs_table (
        similarity FLOAT NOT NULL,
        disease_id TEXT NOT NULL,
        disease_name TEXT NOT NULL,
        reference_drug_id TEXT NOT NULL,
        reference_drug_name TEXT NOT NULL,
        replacement_drug_id TEXT NOT NULL,
        replacement_drug_name TEXT NOT NULL,
        global_patient_population TEXT NOT NULL DEFAULT 'N/A',
        estimated_qaly_impact TEXT NOT NULL DEFAULT 'N/A',
        evidence TEXT NOT NULL,
        annual_cost TEXT NOT NULL DEFAULT 'N/A',
        is_active BOOLEAN NOT NULL DEFAULT 0,
        PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id)
    )
""")
management_conn.execute("""
    CREATE TABLE IF NOT EXISTS ai_logs (
        disease_id TEXT NOT NULL,
        reference_drug_id TEXT NOT NULL,
        replacement_drug_id TEXT NOT NULL,
        field_name TEXT NOT NULL,
        datetime TEXT NOT NULL,
        log TEXT
    )
""")
management_conn.close()


bio_data_conn = duckdb.connect(bio_data_db_path, read_only=True)
ALL_DISEASES = bio_data_conn.execute('SELECT id, name FROM tbl_diseases').fetchall()
ALL_SUBSTANCES = bio_data_conn.execute('SELECT ChEMBL_id, name, tradeNames FROM tbl_substances').fetchall()
bio_data_conn.close()

last_result = {}
last_calculation_thread: Thread = None
last_calculation_pair = None
last_calculation_progress: float = None # from 0 to 1

with open('users.txt', encoding='utf-8') as f:
    USERS = dict(line.split(maxsplit=1) for line in f.read().strip().split('\n'))
 
active_auth_tokens = dict()

# OAuth2 authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
app = FastAPI(title="Affordable API", description="API for molecular similarity, target data, and management system", version="1.2")

# Static files and templates for admin UI
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Authentication Middleware

def get_current_user(request: Request):
    token = request.cookies.get("token")  # ✅ Get token from the cookie

    if not token:
        raise HTTPException(status_code=401, detail="No authentication token provided")

    # Validate token against stored tokens
    username = active_auth_tokens.get(token)

    if not username:
        raise HTTPException(status_code=401, detail="Invalid token or user not authenticated")

    return username  # ✅ Return the authenticated username




@app.get("/", response_class=RedirectResponse)
def root():
    return RedirectResponse(url="/login")


# Secure Token Generation
def generate_token(username):
    random_string = secrets.token_hex(16)  # Generate random 16-byte string
    hashed_token = hashlib.sha256(f"{username}{random_string}".encode()).hexdigest()
    return hashed_token


@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if USERS.get(form_data.username) != form_data.password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    new_token = generate_token(form_data.username)  # ✅ Generate secure token

    # Remove old token
    for token, username in list(active_auth_tokens.items()):
        if username == form_data.username:
            active_auth_tokens.pop(token)

    # ✅ Store new token
    active_auth_tokens[new_token] = form_data.username

    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="token", 
        value=new_token, 
        httponly=True,  # ✅ Secure against XSS
        secure=True,  # ✅ Use secure cookies
        samesite="Lax",  # ✅ Allow cross-page access
        path="/"  # ✅ Global access
    )
    return response






@app.get("/diseases", response_model=List[List], dependencies=[Depends(get_current_user)])
def get_diseases():
    return ALL_DISEASES

@app.get("/substances", response_model=List[List], dependencies=[Depends(get_current_user)])
def get_substances():
    return ALL_SUBSTANCES


# Serve HTML Pages
@app.get("/admin", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/management", response_class=HTMLResponse, dependencies=[Depends(get_current_user)])
def management_page(request: Request):
    return templates.TemplateResponse("management.html", {"request": request})


def similar_substances(disease_id: str, chembl_id: str, top_k: int, save: bool = False):
    if save:
        global last_result
        global last_calculation_progress
        global last_calculation_pair
        last_result = {}
        last_calculation_progress = 0.0
        last_calculation_pair = (disease_id, chembl_id)

    bio_data_conn = duckdb.connect(bio_data_db_path, read_only=True)
    disease = bio_data_conn.execute('SELECT * FROM tbl_diseases WHERE id = ?', [disease_id]).fetchone()
    if not disease:
        raise HTTPException(status_code=404, detail="Disease not found")
    disease_columns = [desc[0] for desc in bio_data_conn.description]
    disease = dict(zip(disease_columns, disease))

    # Get all target IDs associated with the disease
    target_query = """
        SELECT DISTINCT target_id FROM tbl_disease_target WHERE disease_id = ?
    """
    target_ids = bio_data_conn.execute(target_query, [disease_id]).fetchall()

    if not target_ids:
        raise HTTPException(status_code=404, detail="No targets found for this disease")

    target_ids = {tid[0] for tid in target_ids}  # Convert to set

    vec_ref = bio_data_conn.execute('SELECT * FROM tbl_vector_array WHERE ChEMBL_id = ?', [chembl_id]).fetchone()
    if not vec_ref:
        raise HTTPException(status_code=404, detail="ChEMBL ID not found in dataset")
    vec_ref = vec_ref[1:]
    vector_features = tuple(column[0] for column in bio_data_conn.description[1:])
    mask = np.array([1 if feature in target_ids else 0 for feature in vector_features], dtype=np.float32)
    vec_ref = np.array(vec_ref, dtype=np.float32) * mask
    vec_ref_norm = np.linalg.norm(vec_ref)

    total = bio_data_conn.execute("SELECT count(*) FROM tbl_vector_array").fetchone()[0]
    all_vectors = bio_data_conn.execute("SELECT * FROM tbl_vector_array")

    similarities = []
    for i in range(total):
        other_chembl_id, *vec = all_vectors.fetchone()
        vec = np.array(vec, dtype=np.float32) * mask  # Apply mask to each vector
        norm_product = vec_ref_norm * np.linalg.norm(vec)

        similarity = np.dot(vec_ref, vec) / (norm_product + 0) if norm_product > 0 else 0  # Avoid division by zero
        similarity = float(similarity)  # Convert to float for JSON serialization
        similarity = round(similarity, 6)

        if similarity > 0:
            similarities.append({"ChEMBL ID": other_chembl_id, "Similarity": similarity})

        if save:
            last_calculation_progress = 0.9 * (i+1) / total

    # Sort results by similarity
    ranked_results = sorted(similarities, key=lambda x: x["Similarity"], reverse=True)

    for i, row in enumerate(ranked_results):
        chembl_id1 = row['ChEMBL ID']
        query = "SELECT COALESCE(name, 'N/A'), isApproved FROM tbl_substances WHERE chembl_id = ?"
        molecule_name, is_approved = bio_data_conn.execute(query, [chembl_id1]).fetchone()

        query = "SELECT * FROM tbl_knownDrugsAggregated WHERE drugId = ? and diseaseId = ?"
        known_drugs_aggregated = [{column[0]: json.loads(value) if column[0] == 'urls' else value for column, value in zip(bio_data_conn.description, row)}
                                  for row in bio_data_conn.execute(query, [chembl_id1, disease_id]).fetchall()]
        if known_drugs_aggregated:
            is_url_available = any(row['urls'] for row in known_drugs_aggregated)
            max_phase = max(row['phase'] for row in known_drugs_aggregated)
            max_status_for_max_phase = max((row['status'] for row in known_drugs_aggregated if row['phase'] == max_phase), key=lambda x: STATUS_NUM.get(x, 0))
            if max_status_for_max_phase is None:
                max_status_for_max_phase = 'N/A'
            status_num = STATUS_NUM[max_status_for_max_phase]
        else:
            known_drugs_aggregated = []
            is_url_available = False
            max_phase = 0
            max_status_for_max_phase = 'N/A'
            status_num = 0

        ranked_results[i]['Molecule Name'] = molecule_name
        ranked_results[i]['isUrlAvailable'] = is_url_available
        ranked_results[i]['isApproved'] = is_approved
        ranked_results[i]['phase'] = max_phase
        ranked_results[i]['status'] = max_status_for_max_phase
        ranked_results[i]['status_num'] = status_num
        ranked_results[i]['fld_knownDrugsAggregated'] = known_drugs_aggregated

        if save:
            last_calculation_progress = 0.9 + 0.09 * (i+1) / len(ranked_results)

    bio_data_conn.close()

    reference_drug = next(row for row in ranked_results if row['ChEMBL ID'] == chembl_id)

    # ------------ isApproved OR isUrlAvailable ------------------
    results_top_k_lvl1 = [row for row in ranked_results if (row['isUrlAvailable'] or row['isApproved']) and row['ChEMBL ID'] != chembl_id]
    results_top_k_lvl1.sort(key=lambda x: [-x['Similarity'], -x['isApproved'], -x['isUrlAvailable'], -x['phase'], -x['status_num'], x['ChEMBL ID']])

    if len(results_top_k_lvl1) > top_k - 1:
        ref_similarity = results_top_k_lvl1[top_k - 1]["Similarity"]
        results_top_k_lvl1 = [row for row in results_top_k_lvl1 if row['Similarity'] >= ref_similarity]

    # ------------ not isApproved AND not isUrlAvailable ------------------
    results_top_k_lvl2 = [row for row in ranked_results if not row['isUrlAvailable'] and not row['isApproved'] and row['ChEMBL ID'] != chembl_id]
    results_top_k_lvl2.sort(key=lambda x: [-x['Similarity'], -x['phase'], -x['status_num'], x['ChEMBL ID']])

    if len(results_top_k_lvl2) > top_k - 1:
        ref_similarity = results_top_k_lvl2[top_k - 1]["Similarity"]
        results_top_k_lvl2 = [row for row in results_top_k_lvl2 if row['Similarity'] >= ref_similarity]

    res = {'disease': disease, 'reference_drug': reference_drug, 'similar_drugs_primary': results_top_k_lvl1, 'similar_drugs_secondary': results_top_k_lvl2}
    if save:
        last_result = res
        last_calculation_progress = 1.0
    return res


@app.get("/disease_chembl_similarity/{disease_id}/{chembl_id}", response_model=Dict)
def get_disease_chembl_similarity(disease_id: str, chembl_id: str, top_k: int = Query(10, ge=1, le=100)):
    """Retrieve top-k similar substances for a given disease and ChEMBL ID."""
    return similar_substances(disease_id, chembl_id, top_k)


@app.post("/calculate_similar_substances/{disease_id}/{chembl_id}", response_model=Dict, dependencies=[Depends(get_current_user)])
def calculate_similar_substances(disease_id: str, chembl_id: str, top_k: int = Query(10, ge=1, le=100)):
    global last_calculation_thread
    if last_calculation_thread is None or not last_calculation_thread.is_alive():
        last_calculation_thread = Thread(target=similar_substances, args=(disease_id, chembl_id, top_k, True), daemon=True)
        last_calculation_thread.start()
        return {"success": True, "message": "Calculation started"}
    else:
        raise HTTPException(status_code=400, detail="Please wait until the current calculation process is completed")


def extract_evidence(disease_id: str, reference_drug_id: str, replacement_drug_id: str):
    q = f'''
    SELECT DISTINCT a.target_id
    FROM tbl_disease_target dt
    JOIN tbl_actions a ON dt.target_id = a.target_id
    WHERE dt.disease_id = ? AND a.ChEMBL_id = ?
    '''
    bio_data_conn = duckdb.connect(bio_data_db_path, read_only=True)
    target_ids = bio_data_conn.execute(q, [disease_id, reference_drug_id]).fetchall()

    if not target_ids:
        bio_data_conn.close()
        raise HTTPException(status_code=404, detail="No targets found for this disease")

    target_ids = [row[0] for row in target_ids]

    placeholders = ','.join(['?']*len(target_ids))
    q = f'''
    SELECT a.target_id,
        a.actionType,
        a.mechanismOfAction,
        r.ref_source,
        r.ref_data
    FROM tbl_actions a
    LEFT JOIN tbl_refs r ON a.action_id = r.action_id
    WHERE a.ChEMBL_id = ? AND a.target_id IN ({placeholders})
    '''
    rows = bio_data_conn.execute(q, [replacement_drug_id, *target_ids]).fetchall()
    bio_data_conn.close()

    res = {}
    for target_id, action_type, mechanism_of_action, ref_source, ref_data in rows:
        k = (target_id, action_type, mechanism_of_action)
        if k not in res:
            res[k] = {
                'target_id': target_id,
                'action_type': action_type,
                'mechanism_of_action': mechanism_of_action,
                'refs': set()
            }
        if ref_source:
            res[k]['refs'].update(ref_data)
    for k in res:
        res[k]['refs'] = list(res[k]['refs'])
    res = sorted(res.values(), key=lambda x: (x['action_type'] == 'UNIDENTIFIED', x['target_id']))

    return res


@app.get("/evidence/{disease_id}/{reference_drug_id}/{replacement_drug_id}", response_model=List)
def get_evidence(disease_id: str, reference_drug_id: str, replacement_drug_id: str):
    try:
        management_conn = duckdb.connect(management_db_path, read_only=True)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    rows = management_conn.execute('''
        SELECT target_id, action_type, mechanism_of_action, refs
        FROM evidence
        WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ?''',
        [disease_id, reference_drug_id, replacement_drug_id]).fetchall()
    columns = [desc[0] for desc in management_conn.description]
    rows = [dict(zip(columns, row)) for row in rows]
    management_conn.close()
    if rows:
        return sorted(rows, key=lambda x: (x['action_type'] == 'UNIDENTIFIED', x['target_id']))
    else:
        return extract_evidence(disease_id, reference_drug_id, replacement_drug_id)


class CalculationStatus(BaseModel):
    is_running: bool
    progress: str

@app.get("/calculation_status", response_model=CalculationStatus, dependencies=[Depends(get_current_user)])
def get_calculation_status():
    progress = f'[{last_calculation_pair[0]} - {last_calculation_pair[1]}] progress: {last_calculation_progress * 100:.1f} %' if last_calculation_progress is not None else ''
    return {"is_running": last_calculation_thread is not None and last_calculation_thread.is_alive(), "progress": progress}


@app.get("/last_calculation_result", response_model=Dict, dependencies=[Depends(get_current_user)])
def get_last_calculation_result():
    global last_result
    return last_result


class IVPEEntryFullModel(BaseModel):
    similarity: float|int
    disease_id: str
    disease_name: str
    reference_drug_id: str
    reference_drug_name: str
    replacement_drug_id: str
    replacement_drug_name: str
    global_patient_population: Optional[str] = None
    cost_difference: Optional[str] = None
    evidence: str
    annual_cost_reduction: Optional[str] = None
    is_active: Optional[bool] = None

class PFSEntryFullModel(BaseModel):
    similarity: float|int
    disease_id: str
    disease_name: str
    reference_drug_id: str
    reference_drug_name: str
    replacement_drug_id: str
    replacement_drug_name: str
    global_patient_population: Optional[str] = None
    estimated_qaly_impact: Optional[str] = None
    evidence: str
    annual_cost: Optional[str] = None
    is_active: Optional[bool] = None

@app.get("/table_ivpe", response_model=List[IVPEEntryFullModel])
def get_table_ivpe():
    try:
        management_conn = duckdb.connect(management_db_path, read_only=True)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    rows = management_conn.execute('SELECT * FROM ivpe_table').fetchall()
    columns = [desc[0] for desc in management_conn.description]
    management_conn.close()
    rows = [dict(zip(columns, row)) for row in rows]
    rows = sorted(rows, key=lambda row: -row['similarity'])
    return rows

@app.get("/table_pfs", response_model=List[PFSEntryFullModel])
def get_table_pfs():
    try:
        management_conn = duckdb.connect(management_db_path, read_only=True)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    rows = management_conn.execute('SELECT * FROM pfs_table').fetchall()
    columns = [desc[0] for desc in management_conn.description]
    management_conn.close()
    rows = [dict(zip(columns, row)) for row in rows]
    rows = sorted(rows, key=lambda row: -row['similarity'])
    return rows

@app.put("/table_ivpe", response_model=Dict, dependencies=[Depends(get_current_user)])
def add_entry_to_table_ivpe(entry: IVPEEntryFullModel):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("BEGIN TRANSACTION")
    try:
        management_conn.execute("""
            INSERT INTO ivpe_table (
                similarity,
                disease_id,
                disease_name,
                reference_drug_id,
                reference_drug_name,
                replacement_drug_id,
                replacement_drug_name,
                evidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", 
            [entry.similarity,
            entry.disease_id,
            entry.disease_name,
            entry.reference_drug_id,
            entry.reference_drug_name,
            entry.replacement_drug_id,
            entry.replacement_drug_name,
            entry.evidence])
    except duckdb.ConstraintException:
        management_conn.close()
        raise HTTPException(status_code=400, detail="Already exists")

    evidence_list = extract_evidence(entry.disease_id, entry.reference_drug_id, entry.replacement_drug_id)
    evidence_list = [[entry.disease_id, entry.reference_drug_id, entry.replacement_drug_id, row['target_id'], row['action_type'], row['mechanism_of_action'], row['refs']] for row in evidence_list]

    # debug
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - before executemany")

    management_conn.executemany("""
        INSERT OR IGNORE INTO evidence (
            disease_id,
            reference_drug_id,
            replacement_drug_id,
            target_id,
            action_type,
            mechanism_of_action,
            refs
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)""", 
        evidence_list)

    management_conn.execute("COMMIT")

    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - after executemany")

    management_conn.close()

    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - after close")

    return {"success": True, "message": "entry was added successfully"}


@app.put("/table_pfs", response_model=Dict, dependencies=[Depends(get_current_user)])
def add_entry_to_table_pfs(entry: PFSEntryFullModel):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("BEGIN TRANSACTION")
    try:
        management_conn.execute("""
            INSERT INTO pfs_table (
                similarity,
                disease_id,
                disease_name,
                reference_drug_id,
                reference_drug_name,
                replacement_drug_id,
                replacement_drug_name,
                evidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", 
            [entry.similarity,
            entry.disease_id,
            entry.disease_name,
            entry.reference_drug_id,
            entry.reference_drug_name,
            entry.replacement_drug_id,
            entry.replacement_drug_name,
            entry.evidence])
    except duckdb.ConstraintException:
        management_conn.close()
        raise HTTPException(status_code=400, detail="Already exists")

    evidence_list = extract_evidence(entry.disease_id, entry.reference_drug_id, entry.replacement_drug_id)
    evidence_list = [[entry.disease_id, entry.reference_drug_id, entry.replacement_drug_id, row['target_id'], row['action_type'], row['mechanism_of_action'], row['refs']] for row in evidence_list]

    # debug
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - before executemany")

    management_conn.executemany("""
        INSERT OR IGNORE INTO evidence (
            disease_id,
            reference_drug_id,
            replacement_drug_id,
            target_id,
            action_type,
            mechanism_of_action,
            refs
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)""", 
        evidence_list)

    management_conn.execute("COMMIT")

    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - after executemany")

    management_conn.close()

    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {entry.disease_id} - {entry.reference_drug_id} - {entry.replacement_drug_id} - {entry.evidence} - after close")

    return {"success": True, "message": "entry was added successfully"}


@app.delete("/table_ivpe/{disease_id}/{reference_drug_id}/{replacement_drug_id}", response_model=Dict, dependencies=[Depends(get_current_user)])
def update_entry_in_table_ivpe(disease_id: str, reference_drug_id: str, replacement_drug_id: str):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("""
        DELETE FROM ivpe_table
        WHERE disease_id = ?
        AND reference_drug_id = ?
        AND replacement_drug_id = ?""", 
        [disease_id, reference_drug_id, replacement_drug_id])
    if not management_conn.execute('SELECT * FROM pfs_table WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ?', [disease_id, reference_drug_id, replacement_drug_id]).fetchone():
        management_conn.execute("""
            DELETE FROM evidence
            WHERE disease_id = ?
            AND reference_drug_id = ?
            AND replacement_drug_id = ?""", 
            [disease_id, reference_drug_id, replacement_drug_id])
    management_conn.close()

    return {"success": True, "message": "entry was deleted successfully"}


@app.delete("/table_pfs/{disease_id}/{reference_drug_id}/{replacement_drug_id}", response_model=Dict, dependencies=[Depends(get_current_user)])
def update_entry_in_table_pfs(disease_id: str, reference_drug_id: str, replacement_drug_id: str):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("""
        DELETE FROM pfs_table
        WHERE disease_id = ?
        AND reference_drug_id = ?
        AND replacement_drug_id = ?""", 
        [disease_id, reference_drug_id, replacement_drug_id])
    if not management_conn.execute('SELECT * FROM ivpe_table WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ?', [disease_id, reference_drug_id, replacement_drug_id]).fetchone():
        management_conn.execute("""
            DELETE FROM evidence
            WHERE disease_id = ?
            AND reference_drug_id = ?
            AND replacement_drug_id = ?""", 
            [disease_id, reference_drug_id, replacement_drug_id])
    management_conn.close()

    return {"success": True, "message": "entry was deleted successfully"}

class IVPEEntryUpdateModel(BaseModel):
    disease_id: str
    disease_name: str
    reference_drug_id: str
    reference_drug_name: str
    replacement_drug_id: str
    replacement_drug_name: str
    global_patient_population: str
    cost_difference: str
    evidence: str
    annual_cost_reduction: str
    is_active: bool

class PFSEntryUpdateModel(BaseModel):
    disease_id: str
    disease_name: str
    reference_drug_id: str
    reference_drug_name: str
    replacement_drug_id: str
    replacement_drug_name: str
    global_patient_population: str
    estimated_qaly_impact: str
    evidence: str
    annual_cost: str
    is_active: bool

@app.post("/table_ivpe", response_model=Dict, dependencies=[Depends(get_current_user)])
def update_entry_in_table_ivpe(entry: IVPEEntryUpdateModel):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("""
        UPDATE ivpe_table 
        SET disease_name = ?,
            reference_drug_name = ?,
            replacement_drug_name = ?,
            global_patient_population = ?,
            cost_difference = ?,
            evidence = ?,
            annual_cost_reduction = ?,
            is_active = ?
        WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ?""", 
        [
            entry.disease_name,
            entry.reference_drug_name,
            entry.replacement_drug_name,
            entry.global_patient_population,
            entry.cost_difference,
            entry.evidence,
            entry.annual_cost_reduction,
            entry.is_active,

            entry.disease_id,
            entry.reference_drug_id,
            entry.replacement_drug_id
        ])
    management_conn.close()

    return {"success": True, "message": "entry was added successfully"}

@app.post("/table_pfs", response_model=Dict, dependencies=[Depends(get_current_user)])
def update_entry_in_table_pfs(entry: PFSEntryUpdateModel):
    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    management_conn.execute("""
        UPDATE pfs_table 
        SET disease_name = ?,
            reference_drug_name = ?,
            replacement_drug_name = ?,
            global_patient_population = ?,
            estimated_qaly_impact = ?,
            evidence = ?,
            annual_cost = ?,
            is_active = ?
        WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ?""", 
        [
            entry.disease_name,
            entry.reference_drug_name,
            entry.replacement_drug_name,
            entry.global_patient_population,
            entry.estimated_qaly_impact,
            entry.evidence,
            entry.annual_cost,
            entry.is_active,

            entry.disease_id,
            entry.reference_drug_id,
            entry.replacement_drug_id
        ])
    management_conn.close()

    return {"success": True, "message": "entry was added successfully"}


@app.get("/ask_ai/{disease_id}/{reference_drug_id}/{replacement_drug_id}/{field_name}", response_model=Dict, dependencies=[Depends(get_current_user)])
def ask_ai(disease_id: str, reference_drug_id: str, replacement_drug_id: str, field_name: str):
    try:
        bio_data_conn = duckdb.connect(bio_data_db_path, read_only=True)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    disease_name = bio_data_conn.execute('SELECT name FROM tbl_diseases WHERE id = ?', [disease_id]).fetchone()[0]
    reference_drug_name = bio_data_conn.execute('SELECT name FROM tbl_substances WHERE ChEMBL_id = ?', [reference_drug_id]).fetchone()[0]
    replacement_drug_name = bio_data_conn.execute('SELECT name FROM tbl_substances WHERE ChEMBL_id = ?', [replacement_drug_id]).fetchone()[0]
    bio_data_conn.close()

    if field_name == 'global_patient_population':
        value = f'{field_name}, {disease_name}, {reference_drug_name}, {replacement_drug_name}' #TODO
        ai_log = 'test_log'
    elif field_name == 'cost_difference':
        value = f'{field_name}, {disease_name}, {reference_drug_name}, {replacement_drug_name}' #TODO
        ai_log = 'test_log'
    elif field_name == 'estimated_qaly_impact':
        value = f'{field_name}, {disease_name}, {reference_drug_name}, {replacement_drug_name}' #TODO
        ai_log = 'test_log'
    elif field_name == 'annual_cost':
        value = f'{field_name}, {disease_name}, {reference_drug_name}, {replacement_drug_name}' #TODO
        ai_log = 'test_log'

    try:
        management_conn = duckdb.connect(management_db_path)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    try:
        management_conn.execute("""
            INSERT INTO ai_logs (
                disease_id,
                reference_drug_id,
                replacement_drug_id,
                field_name,
                datetime,
                log
            )
            VALUES (?, ?, ?, ?, ?, ?)""", 
            [disease_id,
            reference_drug_id,
            replacement_drug_id,
            field_name,
            dt.datetime.now().isoformat(sep=' ', timespec='seconds'),
            ai_log])
    except duckdb.ConstraintException:
        raise HTTPException(status_code=400, detail="Already exists")
    finally:
        management_conn.close()

    return {"success": True, "value": value}


@app.get("/ai_logs/{disease_id}/{reference_drug_id}/{replacement_drug_id}/{field_name}", response_model=Dict, dependencies=[Depends(get_current_user)])
def ask_ai(disease_id: str, reference_drug_id: str, replacement_drug_id: str, field_name: str):
    try:
        management_conn = duckdb.connect(management_db_path, read_only=True)
    except duckdb.ConnectionException:
        raise HTTPException(status_code=500, detail="Server busy")
    rows = management_conn.execute("""
        SELECT datetime, log
        FROM ai_logs
        WHERE disease_id = ? AND reference_drug_id = ? AND replacement_drug_id = ? AND field_name = ?
        ORDER BY datetime DESC""", 
        [disease_id, reference_drug_id, replacement_drug_id, field_name]).fetchall()
    management_conn.close()
    return {"success": True, "logs": rows}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)

