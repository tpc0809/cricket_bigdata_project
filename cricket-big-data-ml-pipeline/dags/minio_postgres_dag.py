import logging
import time
import threading
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import xxhash
import os
import psutil
import gc

# ========================
# CONFIGURATION CONSTANTS
# ========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 10,  # We'll set 10 here, then skip further retries if manual (see on_failure_callback)
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=12),
    'max_retry_delay': timedelta(hours=2)
}

TARGET_UNIQUE_RECORDS = 111111111
PARQUET_BATCH_SIZE = 250000
POSTGRES_BATCH_SIZE = 100000
HEARTBEAT_INTERVAL = 20

# MinIO config
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "cricket-data"
MINIO_FILE = "cricket_dataset.parquet"

# PostgreSQL config
POSTGRES_CONN_ID = "postgres_default"
TARGET_TABLE = "cricket_data_partitioned"
STATE_TABLE = "dag_state_tracking"
HASHES_TABLE = "processed_record_hashes"

# Optional local file paths
PROCESSED_PARQUET_FILE = "/opt/airflow/processed_unique_records.parquet"
PICKLE_FILE_PATH = "/opt/airflow/xcom_data/parquet_metadata.pkl"

# Global counters (for final logging)
total_unique_records_global = {'value': 0}
total_duplicate_records_global = {'value': 0}


# =======================
# HELPER FUNCTIONS
# =======================

def is_manual_trigger(context: dict) -> bool:
    """
    Check if the current run is manually triggered.
    Called *within* a running task or callback, not at parse time.
    """
    dag_run = context.get('dag_run')
    return (dag_run and dag_run.run_type == 'manual')

def no_retry_if_manual(context: dict):
    """
    on_failure_callback to skip further retries if this is a manual run.
    Sets the current try number to the max tries, effectively stopping retries.
    """
    if is_manual_trigger(context):
        ti = context['task_instance']
        ti.max_tries = ti.try_number  # Force no further retries

def get_engine():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return pg_hook.get_sqlalchemy_engine()

def ensure_state_tracking_table():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                task TEXT PRIMARY KEY,
                last_processed_row_group INTEGER DEFAULT 0,
                last_processed_chunk INTEGER DEFAULT 0,
                total_unique_records BIGINT DEFAULT 0,
                total_duplicate_records BIGINT DEFAULT 0,
                status TEXT DEFAULT 'PENDING'
            );
        """))
    logging.debug(f"Ensured state tracking table '{STATE_TABLE}' exists.")

def ensure_processed_hashes_table():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {HASHES_TABLE} (
                record_hash TEXT PRIMARY KEY
            );
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_record_hash ON {HASHES_TABLE} USING BTREE (record_hash);
        """))
    logging.debug(f"Ensured processed hashes table '{HASHES_TABLE}' and index exist.")

def get_state_tracking_data(task_name: str):
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT last_processed_row_group, last_processed_chunk, total_unique_records, total_duplicate_records
            FROM {STATE_TABLE} WHERE task=:task
        """), {'task': task_name}).fetchone()

    if result:
        return {
            'last_processed_row_group': result[0],
            'last_processed_chunk': result[1],
            'total_unique_records': result[2],
            'total_duplicate_records': result[3],
        }
    return {
        'last_processed_row_group': 0,
        'last_processed_chunk': 0,
        'total_unique_records': 0,
        'total_duplicate_records': 0
    }

def update_dag_state(task_name: str,
                     last_processed_row_group: int,
                     last_processed_chunk: int,
                     cumulative_unique_records: int,
                     cumulative_duplicate_records: int):
    """
    Store the current state for resuming. Also push row group/chunk into XCom if needed.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {STATE_TABLE} (task, last_processed_row_group, last_processed_chunk, total_unique_records, total_duplicate_records)
            VALUES (:task, :rg, :ch, :uniq, :dup)
            ON CONFLICT (task) DO UPDATE
            SET last_processed_row_group = EXCLUDED.last_processed_row_group,
                last_processed_chunk = EXCLUDED.last_processed_chunk,
                total_unique_records = EXCLUDED.total_unique_records,
                total_duplicate_records = EXCLUDED.total_duplicate_records;
        """), {
            "task": task_name,
            "rg": last_processed_row_group,
            "ch": last_processed_chunk,
            "uniq": cumulative_unique_records,
            "dup": cumulative_duplicate_records
        })

def insert_batch_to_postgres(table_name: str, records_to_insert: list):
    """
    Insert a batch of records into the target table using COPY.
    Expects a list of dicts that match the table's columns (no extra columns).
    """
    if not records_to_insert:
        logging.debug(f"No records to insert into {table_name}.")
        return

    df = pd.DataFrame(records_to_insert)
    # Ensure we do not include record_hash if present
    if "record_hash" in df.columns:
        df = df.drop(columns=["record_hash"])

    engine = get_engine()
    quoted_cols = [f'"{col}"' for col in df.columns]
    columns_str = ", ".join(quoted_cols)

    with engine.begin() as conn:
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV"
        conn.connection.cursor().copy_expert(sql, buf)

    logging.debug(f"Inserted batch of {len(df)} records into {table_name}.")

def insert_batch_to_postgres_hashes(table_name: str, hashes: list):
    """
    Insert a batch of record_hashes into the processed_record_hashes table using COPY.
    Expects a list of dicts: [{'record_hash': 'xxxx'}, ...]
    """
    if not hashes:
        logging.debug("No hashes to insert.")
        return

    df = pd.DataFrame(hashes, columns=["record_hash"])

    engine = get_engine()
    with engine.begin() as conn:
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        sql = f"COPY {table_name} (record_hash) FROM STDIN WITH CSV"
        conn.connection.cursor().copy_expert(sql, buf)

    logging.debug(f"Inserted batch of {len(df)} hashes into {table_name}.")

def keep_airflow_alive(task_instance):
    """
    Sends a heartbeat every HEARTBEAT_INTERVAL seconds to avoid Airflow marking the task as zombie.
    """
    def heartbeat():
        while task_instance.state in ("running", "up_for_retry"):
            time.sleep(HEARTBEAT_INTERVAL)
            # In Airflow 2.x, there is no direct 'task_instance.heartbeat()',
            # so this is more of a placeholder if you have a custom solution.
            # At least logging something can keep the logs alive.
            logging.info("...heartbeat...")

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()


# =======================
# DAG DEFINITION
# =======================
@dag(
    dag_id='minio_postgres_dag',
    default_args=default_args,
    description='DAG that avoids parse-time context usage; loads 111,111,111 unique records from Parquet in MinIO to Postgres.',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_tasks=1,
    tags=['minio', 'postgres', 'deduplication', 'resume', 'manual-trigger']
)
def minio_postgres_etl_dag():

    # ---------------
    # CLEAR PREVIOUS
    # ---------------
    def clear_previous_data_callable(**context):
        """
        Clears data only if it's a manual trigger.
        Truncates the target table and processed hashes, resets the state table.
        """
        logger = logging.getLogger(__name__)
        ensure_state_tracking_table()
        ensure_processed_hashes_table()

        if is_manual_trigger(context):
            logger.info("Manual trigger detected. Clearing previous data...")
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE}"))
                conn.execute(text(f"TRUNCATE TABLE {HASHES_TABLE}"))
                conn.execute(text(f"""
                    INSERT INTO {STATE_TABLE} (task, last_processed_row_group, last_processed_chunk, total_unique_records, total_duplicate_records, status)
                    VALUES ('process_all_row_groups', 0, 0, 0, 0, 'PENDING')
                    ON CONFLICT (task) DO UPDATE
                    SET last_processed_row_group=0,
                        last_processed_chunk=0,
                        total_unique_records=0,
                        total_duplicate_records=0,
                        status='PENDING';
                """))
            # Optionally remove local caches
            if os.path.exists(PROCESSED_PARQUET_FILE):
                os.remove(PROCESSED_PARQUET_FILE)
            if os.path.exists(PICKLE_FILE_PATH):
                os.remove(PICKLE_FILE_PATH)
            logger.info("✅ Previous data cleared.")
        else:
            logger.info("Not a manual trigger. Skipping data clearing.")

        # Mark step complete
        update_dag_state('clear_previous_data', 0, 0, 0, 0)

    clear_data = PythonOperator(
        task_id='clear_previous_data',
        python_callable=clear_previous_data_callable,
        provide_context=True,
        on_failure_callback=no_retry_if_manual,  # skip further retries if manual
        retries=10  # maximum if scheduled
    )

    # ---------------
    # FETCH METADATA
    # ---------------
    @task(
        task_id="fetch_parquet_metadata_task",
        on_failure_callback=no_retry_if_manual,
        retries=3,
        retry_delay=timedelta(seconds=10),
        provide_context=True
    )
    def fetch_parquet_metadata_task_op(**context):
        """
        Fetch the Parquet file from MinIO (just metadata: row group count).
        Return list of row group indices for downstream tasks.
        """
        logger = logging.getLogger(__name__)
        logger.info("Fetching Parquet metadata from MinIO...")

        client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        try:
            response = client.get_object(Bucket=MINIO_BUCKET, Key=MINIO_FILE)
            file_data = response['Body'].read()
        except Exception as e:
            logger.error(f"❌ Error fetching Parquet from MinIO: {e}", exc_info=True)
            raise

        parquet_file = pq.ParquetFile(io.BytesIO(file_data))
        num_row_groups = parquet_file.num_row_groups
        logger.info(f"✅ Found {num_row_groups} row groups in the Parquet file.")

        return list(range(num_row_groups))

    # -------------------------
    # PROCESS ALL ROW GROUPS
    # -------------------------
    @task(
        task_id="process_all_row_groups",
        on_failure_callback=no_retry_if_manual,
        retries=10,  # For scheduled runs
        provide_context=True
    )
    def process_all_row_groups_op(row_group_indices, **context):
        """
        Reads each row group in chunks, deduplicates, and inserts into Postgres.
        Resumes from last known row group/chunk if previously failed.
        """
        logger = logging.getLogger(__name__)
        ti = context['ti']
        if not is_manual_trigger(context):
            # Keep airflow alive only if it's a potentially long scheduled run
            keep_airflow_alive(ti)

        # Get last state
        state_data = get_state_tracking_data('process_all_row_groups')
        start_rg = state_data['last_processed_row_group']
        start_chunk = state_data['last_processed_chunk']
        cumulative_unique = state_data['total_unique_records']
        cumulative_duplicates = state_data['total_duplicate_records']

        logger.info(f"Resuming from RowGroup={start_rg}, Chunk={start_chunk}, "
                    f"Already Unique={cumulative_unique}, Duplicates={cumulative_duplicates}")

        # Fetch entire parquet file once
        client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        response = client.get_object(Bucket=MINIO_BUCKET, Key=MINIO_FILE)
        file_data = response['Body'].read()
        parquet_file = pq.ParquetFile(io.BytesIO(file_data))

        for rg_index in row_group_indices[start_rg:]:
            logger.info(f"🚀 Processing Row Group {rg_index}")
            t0 = time.time()
            df_rg = parquet_file.read_row_group(rg_index).to_pandas()
            total_in_group = len(df_rg)

            for chunk_index, start_idx in enumerate(range(0, total_in_group, PARQUET_BATCH_SIZE)):
                if rg_index == start_rg and chunk_index < start_chunk:
                    logger.debug(f"Skipping chunk {chunk_index} in row group {rg_index} (already processed).")
                    continue

                end_idx = min(start_idx + PARQUET_BATCH_SIZE, total_in_group)
                df_chunk = df_rg.iloc[start_idx:end_idx].copy().astype(str)

                # In-chunk dedup
                before_in_chunk = len(df_chunk)
                df_chunk = df_chunk.drop_duplicates(keep='first')
                in_chunk_duplicates = before_in_chunk - len(df_chunk)

                # Compute record hashes
                df_chunk["record_hash"] = df_chunk.apply(
                    lambda row: xxhash.xxh64(str(row.to_dict())).hexdigest(), axis=1
                )
                # Deduplicate by hash within the chunk
                before_hash_dedup = len(df_chunk)
                df_chunk = df_chunk.drop_duplicates(subset=["record_hash"], keep='first')
                hash_dedup = before_hash_dedup - len(df_chunk)

                # Filter out already processed hashes
                engine = get_engine()
                existing_hashes = set()
                if not df_chunk.empty:
                    all_hashes = df_chunk["record_hash"].tolist()
                    placeholders = ", ".join(f"'{h}'" for h in all_hashes)
                    query = f"SELECT record_hash FROM {HASHES_TABLE} WHERE record_hash IN ({placeholders})"
                    with engine.connect() as conn:
                        rows = conn.execute(text(query)).fetchall()
                    existing_hashes = {r[0] for r in rows}

                before_filter = len(df_chunk)
                df_chunk = df_chunk[~df_chunk["record_hash"].isin(existing_hashes)]
                already_processed_count = before_filter - len(df_chunk)

                # Check how many we can still insert before hitting TARGET_UNIQUE_RECORDS
                remaining_to_target = TARGET_UNIQUE_RECORDS - cumulative_unique
                if remaining_to_target <= 0:
                    logger.info("Target unique records reached. Stopping.")
                    break
                if len(df_chunk) > remaining_to_target:
                    df_chunk = df_chunk.iloc[:remaining_to_target]

                # Update counters
                new_unique = len(df_chunk)
                chunk_duplicates = (in_chunk_duplicates + hash_dedup + already_processed_count)
                cumulative_unique += new_unique
                cumulative_duplicates += chunk_duplicates

                logger.info(
                    f"RowGroup={rg_index}, Chunk={chunk_index}: "
                    f"In-chunk duplicates={in_chunk_duplicates}, Hash dedup={hash_dedup}, "
                    f"Already processed={already_processed_count}, New unique={new_unique}, "
                    f"Total unique so far={cumulative_unique}, Duplicates so far={cumulative_duplicates}"
                )

                # Prepare for insertion
                records_to_insert = df_chunk.drop(columns=["record_hash"]).to_dict("records")
                hash_records = [{"record_hash": h} for h in df_chunk["record_hash"].tolist()]

                # Insert in smaller batches
                for i in range(0, len(records_to_insert), POSTGRES_BATCH_SIZE):
                    sub_records = records_to_insert[i:i+POSTGRES_BATCH_SIZE]
                    sub_hashes = hash_records[i:i+POSTGRES_BATCH_SIZE]
                    insert_batch_to_postgres(TARGET_TABLE, sub_records)
                    insert_batch_to_postgres_hashes(HASHES_TABLE, sub_hashes)

                # Update state
                update_dag_state(
                    'process_all_row_groups',
                    rg_index,  # current row group
                    chunk_index + 1,  # next chunk to process
                    cumulative_unique,
                    cumulative_duplicates
                )

                if cumulative_unique >= TARGET_UNIQUE_RECORDS:
                    logger.info("🎯 Exactly reached target unique records. Stopping further processing.")
                    break

                del df_chunk
                gc.collect()

            # Move to next row group
            update_dag_state(
                'process_all_row_groups',
                rg_index + 1,  # next row group
                0,
                cumulative_unique,
                cumulative_duplicates
            )
            duration = time.time() - t0
            logger.info(f"Finished row group {rg_index} in {duration:.2f}s.")

            if cumulative_unique >= TARGET_UNIQUE_RECORDS:
                break

        total_unique_records_global['value'] = cumulative_unique
        total_duplicate_records_global['value'] = cumulative_duplicates
        logger.info(
            f"✅ All row groups processed or target reached. "
            f"Final unique={cumulative_unique}, duplicates={cumulative_duplicates}"
        )

    # ---------------
    # FINALIZE TASK
    # ---------------
    @task(
        task_id="finalize_load",
        on_failure_callback=no_retry_if_manual,
        retries=0,  # Typically no need to retry final logging
        provide_context=True
    )
    def finalize_load_op(**context):
        logger = logging.getLogger(__name__)
        unique_loaded = total_unique_records_global['value']
        duplicates_skipped = total_duplicate_records_global['value']
        logger.info(
            f"✅ Final load complete. Unique records inserted={unique_loaded}, "
            f"Duplicates skipped={duplicates_skipped}"
        )
        # Optionally mark in state table
        update_dag_state('finalize_load', 0, 0, unique_loaded, duplicates_skipped)

    # ---------------
    # OPTIONAL REINDEX
    # ---------------
    def reindex_hashes_table_callable(**context):
        logger = logging.getLogger(__name__)
        engine = get_engine()
        try:
            with engine.begin() as conn:
                conn.execute(text(f"LOCK TABLE {HASHES_TABLE} IN ACCESS EXCLUSIVE MODE;"))
                conn.execute(text(f"REINDEX TABLE {HASHES_TABLE};"))
            logger.info(f"Reindex on '{HASHES_TABLE}' completed.")
        except Exception as e:
            logger.error(f"❌ Error reindexing: {e}", exc_info=True)
            raise
        # We can also update DAG state if desired
        current_unique = total_unique_records_global['value']
        current_duplicates = total_duplicate_records_global['value']
        update_dag_state('reindex_hashes_table', 0, 0, current_unique, current_duplicates)

    reindex_hashes_table_task = PythonOperator(
        task_id='reindex_hashes_table_task',
        python_callable=reindex_hashes_table_callable,
        provide_context=True,
        on_failure_callback=no_retry_if_manual,
        retries=0
    )

    # ---------------------------
    # TASK DEPENDENCIES / FLOW
    # ---------------------------
    parquet_metadata = fetch_parquet_metadata_task_op()
    process_data = process_all_row_groups_op(parquet_metadata)
    finalize = finalize_load_op()

    clear_data >> parquet_metadata >> process_data >> finalize >> reindex_hashes_table_task


minio_postgres_dag = minio_postgres_etl_dag()