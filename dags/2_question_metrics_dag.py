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
    # 1. GCS에서 데이터를 가져와서 과거 데이터 DuckDB 테이블에 저장
    @task
    def extract_till_today(uri, staging_table_name, ts_col="created_at"):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        try:
            register_gcs(conn)

            # # DAG 실행일
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

    t_extract_q = extract_till_today.override(task_id="extract_question")(tables_to_combine[table1], table1, ts_col="created_at_piece")
    t_extract_vp = extract_till_today.override(task_id="extract_vote_point")(tables_to_combine[table2], table2)

    t_transform = transform_part2(t_extract_q, table1, table2)
    t_load = load_to_bigquery(t_transform)

    t_extract_q >> t_extract_vp >> t_transform >> t_load
