import pendulum
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import get_current_context
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from utils import register_gcs, nlp_modeling, advanced_questions
from airflow_plugins import (
    DUCKDB_CONN_ID, GCP_CONN_ID, tables_to_combine,
    BQ_PROJECT, BQ_DATASET
)

BQ_TABLE   = "final_question"

default_args = dict(
    owner = 'olozl',
    email = ['olozl@airflow.com'],
    email_on_failure = False,
    retries = 3
    )


with DAG(
    dag_id="2_question_metrics_dag",
    start_date=pendulum.datetime(2023, 4, 27, tz='Asia/Seoul'),
    schedule="30 11 * * *", # cron 표현식
    tags = ['20250813'],
    default_args = default_args,
    catchup=False
):  
    # 1. GCS와 BigQuery에서 데이터를 가져와서 과거 데이터 DuckDB 테이블에 저장
    @task
    def extract_till_today(staging_table_name, uri, ts_col="created_at", from_bq=False):
        ddb = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = ddb.get_conn()
        try:
            register_gcs(conn)

            ctx = get_current_context()
            run_day_dt = ctx["logical_date"].in_timezone("Asia/Seoul").date()
            run_day = run_day_dt.isoformat()
            next_day = (run_day_dt + pendulum.duration(days=1)).isoformat()

            if from_bq:
                bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location="asia-northeast3")
                client = bq_hook.get_client(project_id=BQ_PROJECT)

                query = f"""
                    SELECT *
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.{uri}`
                    WHERE {ts_col} < DATETIME '{next_day} 00:00:00'
                """
                df = client.query(query).to_dataframe(create_bqstorage_client=True)

                src = f"__src_{staging_table_name}"
                conn.register(src, df)

                conn.execute(f"""
                    CREATE OR REPLACE TABLE {staging_table_name} AS
                    SELECT
                        *,
                        CAST({ts_col} AS TIMESTAMP) AS ts,
                        DATE(CAST({ts_col} AS TIMESTAMP)) AS ds
                    FROM {src}
                    WHERE CAST({ts_col} AS TIMESTAMP) < TIMESTAMP '{next_day} 00:00:00'
                """)
            else:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE {staging_table_name} AS
                    SELECT
                        *,
                        CAST({ts_col} AS TIMESTAMP) AS ts,
                        DATE(CAST({ts_col} AS TIMESTAMP)) AS ds
                    FROM read_parquet('{uri}')
                    WHERE CAST({ts_col} AS TIMESTAMP) < TIMESTAMP '{next_day} 00:00:00'
                """)

            return run_day
        finally:
            conn.close()

    # 2. DuckDB에서 데이터 불러와서 데이터 변환하고 parquet 파일로 저장
    @task
    def transform_part2(run_day, t1, t2):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        os.makedirs("/opt/airflow/tmp", exist_ok=True)
        out_path = f"/opt/airflow/tmp/categorized_question_{run_day}.parquet"
        try:
            register_gcs(conn)
            
            vote_point_df = conn.execute(f""" SELECT * FROM {t2} """).df()
            question_df = conn.execute(f""" SELECT * FROM {t1} """).df()
            questions = question_df.question_text.unique()

            # 질문 데이터 카테고리화
            category_df = nlp_modeling(questions)

            # 질문 데이터 확장
            final_df = advanced_questions(question_df, category_df, vote_point_df)

            conn.register("final_df", final_df)
            safe_path = out_path.replace("'", "''")
            conn.execute(f"""
                COPY (SELECT * FROM final_df)
                TO '{safe_path}'
                (FORMAT PARQUET, OVERWRITE 1)
            """)
        finally:
            conn.close()
        return out_path

    # 3. parquet 파일을 읽어서 지표 설정 후 BigQuery에 저장
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
    table1 = "staging_question"
    table2 = "staging_vote_point"

    t_extract_q = extract_till_today.override(task_id="extract_question")(table1, tables_to_combine[table1], ts_col="created_at_piece", from_bq=True)
    t_extract_vp = extract_till_today.override(task_id="extract_vote_point")(table2, tables_to_combine[table2])

    t_transform = transform_part2(t_extract_vp, table1, table2)
    t_load = load_to_bigquery(t_transform)

    t_extract_q >> t_extract_vp >> t_transform >> t_load
