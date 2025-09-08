
import duckdb


def _apply_migration(management_db_path: str, migration_name: str, sql_query_list: list[str]):
    print(f'Apply migration "{migration_name}" ...')
    with duckdb.connect(management_db_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        for q in sql_query_list:
            conn.execute(q)
        conn.execute('INSERT INTO db_migrations (name) VALUES (?)', [migration_name])
        conn.execute('COMMIT')
    print(f'Apply migration "{migration_name}" - success')


def apply_migrations(management_db_path):
    migrations = []

    # Init
    migration_name = '2025-09-08_21-44-03_init'
    sql_query_list = [
        """CREATE TABLE IF NOT EXISTS evidence (
            disease_id TEXT NOT NULL,
            reference_drug_id TEXT NOT NULL,
            replacement_drug_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            action_type TEXT NOT NULL,
            mechanism_of_action TEXT NOT NULL,
            refs TEXT[],
            PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id, target_id)
        )""",
        """CREATE TABLE IF NOT EXISTS ivpe_table (
            similarity FLOAT NOT NULL,
            disease_id TEXT NOT NULL,
            disease_name TEXT NOT NULL,
            reference_drug_id TEXT NOT NULL,
            reference_drug_name TEXT NOT NULL,
            replacement_drug_id TEXT NOT NULL,
            replacement_drug_name TEXT NOT NULL,
            patient_population TEXT NOT NULL DEFAULT 'N/A',
            cost_difference TEXT NOT NULL DEFAULT 'N/A',
            evidence TEXT NOT NULL,
            annual_cost_reduction TEXT NOT NULL DEFAULT 'N/A',
            approval_likelihood TEXT NOT NULL DEFAULT 'N/A',
            is_active BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id)
        )""",
        """CREATE TABLE IF NOT EXISTS pfs_table (
            similarity FLOAT NOT NULL,
            disease_id TEXT NOT NULL,
            disease_name TEXT NOT NULL,
            reference_drug_id TEXT NOT NULL,
            reference_drug_name TEXT NOT NULL,
            replacement_drug_id TEXT NOT NULL,
            replacement_drug_name TEXT NOT NULL,
            patient_population TEXT NOT NULL DEFAULT 'N/A',
            estimated_qaly_impact TEXT NOT NULL DEFAULT 'N/A',
            evidence TEXT NOT NULL,
            annual_cost TEXT NOT NULL DEFAULT 'N/A',
            cost_per_qaly TEXT NOT NULL DEFAULT 'N/A',
            total_qaly_impact TEXT NOT NULL DEFAULT 'N/A',
            rank TEXT NOT NULL DEFAULT 'N/A',
            approval_likelihood TEXT NOT NULL DEFAULT 'N/A',
            is_active BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY(disease_id, reference_drug_id, replacement_drug_id)
        )""",
        """CREATE TABLE IF NOT EXISTS ai_logs (
            disease_id TEXT NOT NULL,
            reference_drug_id TEXT NOT NULL,
            replacement_drug_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            datetime TEXT NOT NULL,
            log TEXT
        )""",
    ]
    migrations.append((migration_name, sql_query_list))

    # Create column rank in ivpe_table
    migration_name = '2025-09-08_21-58-00_column_rank_for_ivpe_table'
    sql_query_list = [
        """ALTER TABLE ivpe_table ADD COLUMN rank TEXT DEFAULT 'N/A'""",
        """ALTER TABLE ivpe_table ALTER COLUMN rank SET NOT NULL;""",
    ]
    migrations.append((migration_name, sql_query_list))

    # --------------------
    # Apply all migrations
    # --------------------
    with duckdb.connect(management_db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS db_migrations (
                name TEXT NOT NULL,
                PRIMARY KEY(name)
            )
        """)
        applied_migrations = [row[0] for row in conn.execute('SELECT name FROM db_migrations').fetchall()]

    for migration_name, sql_query_list in migrations:
        if migration_name in applied_migrations:
            continue
        _apply_migration(management_db_path, migration_name, sql_query_list, applied_migrations)
