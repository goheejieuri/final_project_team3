import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import get_current_context
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from google.cloud import bigquery
import gcsfs
import os

DUCKDB_CONN_ID = "my_local_duckdb_conn"
GCP_CONN_ID = "gcp_conn"
tables_to_combine = {
    "staging_point_history": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_pointhistory.parquet",
    "staging_userquestion_record": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_userquestionrecord.parquet"
}
BQ_PROJECT = "my-projectcodeit"
BQ_DATASET = "final_project"
BQ_TABLE   = "vote_point"

# GCS 파일 시스템 등록
def register_gcs(conn):
    fs = gcsfs.GCSFileSystem() 
    conn.register_filesystem(fs)

default_args = dict(
    owner = 'olozl',
    email = ['olozl@airflow.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="1_vote_point_dag",
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250812'],
    default_args = default_args,
    catchup=False
):
    # 1. GCS에서 데이터를 가져와서 하루치 데이터 DuckDB 테이블에 저장
    @task
    def extract_till_today(uri, staging_table_name, ts_col="created_at"):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        try:
            register_gcs(conn)

            # DAG 실행일
            ctx = get_current_context()
            run_day = ctx["logical_date"].in_timezone("Asia/Seoul").date().isoformat()
            next_day = (ctx["logical_date"].in_timezone("Asia/Seoul").date() + pendulum.duration(days=1)).isoformat()

            # 추출한 하루치 데이터 DuckDB view (staging_table_name)에 저장
            conn.execute(f"""
                CREATE OR REPLACE VIEW {staging_table_name} AS
                SELECT
                    *,
                    DATE({ts_col}) AS ds
                FROM read_parquet('{uri}')
                WHERE {ts_col} < TIMESTAMP '{next_day}'
            """)
        finally:
            conn.close()
        return run_day

    # 2. DuckDB 테이블에 있는거 1차 가공하고 1차 가공하고 parquet 파일 임시 저장
    @task
    def transform_part1(run_day, t1, t2):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        os.makedirs("/opt/airflow/tmp", exist_ok=True)
        out_path = f"/opt/airflow/tmp/vote_point_df_{run_day}.parquet"
        try:
            register_gcs(conn)

            query = f"""
                WITH ph AS (
                    SELECT
                        user_question_record_id,
                        user_id AS point_owner,
                        LIST(delta_point ORDER BY created_at) FILTER (WHERE delta_point IS NOT NULL) AS delta_points
                    FROM {t1}
                    GROUP BY 1, 2
                ),
                uqr AS (
                    SELECT * FROM {t2} WHERE ds <= ?
                ),
                joined AS (
                    -- case 1: user_id와 매칭
                    SELECT uqr.*, ph.delta_points, uqr.user_id AS point_owner, 'user_id' AS join_case
                    FROM uqr
                    INNER JOIN ph
                    ON ph.user_question_record_id = uqr.id
                    AND ph.point_owner = uqr.user_id
                    UNION ALL
                    -- case 2: chosen_user_id와 매칭
                    SELECT uqr.*, ph.delta_points, uqr.chosen_user_id AS point_owner, 'chosen_user_id' AS join_case
                    FROM uqr
                    INNER JOIN ph
                    ON ph.user_question_record_id = uqr.id
                    AND ph.point_owner = uqr.chosen_user_id
                )
                
                SELECT * FROM joined
            """
            conn.execute(f"COPY ({query}) TO '{out_path.replace("'", "''")}' (FORMAT PARQUET, OVERWRITE TRUE)", [run_day])
        finally:
            conn.close()
        return out_path

    create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset',
            gcp_conn_id=GCP_CONN_ID,
            project_id=BQ_PROJECT,
            dataset_id=BQ_DATASET,
            location='asia-northeast3',
            if_exists='ignore'
        )

    # 3. Parquet 파일 Bigquery에 적재
    @task
    def load_to_bigquery(parquet_path):
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location='asia-northeast3')
        client = hook.get_client(project_id=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        with open(parquet_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {parquet_path} -> {table_id}")
        return table_id


    # 파이프라인 연결
    table1 = "staging_point_history"
    table2 = "staging_userquestion_record"

    t_extract_ph = extract_till_today.override(task_id="extract_point_history")(tables_to_combine[table1], table1)
    t_extract_uqr = extract_till_today.override(task_id="extract_userquestion_record")(tables_to_combine[table2], table2)

    t_transform = transform_part1(t_extract_ph, table1, table2)
    t_load = load_to_bigquery(t_transform)
    t_extract_ph >> t_extract_uqr >> t_transform >> create_dataset >> t_load
