import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import get_current_context
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import gcsfs
import os

DUCKDB_CONN_ID = "my_local_duckdb_conn"
tables_to_combine = {
    "staging_point_history": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_pointhistory.parquet",
    "staging_userquestion_record": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_userquestionrecord.parquet"
}

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
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250812'],
    default_args = default_args,
    catchup=False
):
    # 1. GCS에서 데이터를 가져와서 하루치 데이터 DuckDB 테이블에 저장
    @task
    def extract_one_day(uri, staging_table_name):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        try:
            register_gcs(conn)

            # DAG 실행일
            ctx = get_current_context()
            # run_day = ctx["logical_date"].in_timezone("Asia/Seoul").date().isoformat()
            run_day = "2023-05-01"  # 테스트용으로 고정된 날짜 사용

            # 추출한 하루치 데이터 DuckDB 테이블 (staging_table_name)에 저장
            conn.execute(f"CREATE TABLE IF NOT EXISTS {staging_table_name} AS SELECT * FROM read_parquet(?) LIMIT 0", [uri])
            conn.execute(f"ALTER TABLE {staging_table_name} ADD COLUMN IF NOT EXISTS ds DATE")

            ts_col = "created_at"
            conn.execute(f"DELETE FROM {staging_table_name} WHERE ds = ?", [run_day])
            conn.execute(f"""
                INSERT INTO {staging_table_name}
                SELECT
                *, DATE({ts_col}) AS ds
                FROM read_parquet(?)
                WHERE DATE({ts_col}) = ?
            """, [uri, run_day])
        finally:
            conn.close()
        return run_day

    # 2. DuckDB 테이블에 있는거 1차 가공하고 1차 가공하고 DuckDB vote_point_df 테이블에 저장
    @task
    def transform_part1(run_day, t1, t2):
        hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = hook.get_conn()
        merged_table_name = "vote_point_df"
        try:
            register_gcs(conn)

            query = f"""
                WITH ph AS (
                    SELECT
                        user_question_record_id,
                        user_id AS point_owner,
                        LIST(delta_point) AS delta_points
                    FROM {t1}
                    WHERE ds = ?
                    GROUP BY user_question_record_id, point_owner
                ),
                uqr AS (
                    SELECT * FROM {t2} WHERE ds = ?
                ),
                joined AS (
                    -- case 1: user_id와 매칭
                    SELECT uqr.*, ph.delta_points, uqr.user_id AS point_owner, 'user_id' AS join_case
                    FROM uqr
                    LEFT JOIN ph
                    ON ph.user_question_record_id = uqr.id
                    AND ph.point_owner = uqr.user_id
                    UNION ALL
                    -- case 2: chosen_user_id와 매칭
                    SELECT uqr.*, ph.delta_points, uqr.chosen_user_id AS point_owner, 'chosen_user_id' AS join_case
                    FROM uqr
                    LEFT JOIN ph
                    ON ph.user_question_record_id = uqr.id
                    AND ph.point_owner = uqr.chosen_user_id
                )
                
                SELECT * FROM joined
            """
            conn.execute(f"CREATE TABLE IF NOT EXISTS {merged_table_name} AS SELECT * FROM (" + query + ") LIMIT 0", [run_day, run_day])
            conn.execute(f"DELETE FROM {merged_table_name} WHERE ds = ?", [run_day])
            conn.execute(f"INSERT INTO {merged_table_name} "+ query, [run_day, run_day])
        finally:
            conn.close()
        return merged_table_name

    # # 3. DuckDB에 있는 데이터 Bigquery에 적재
    # # TODO: task3
    # @task
    # def load_to_bigquery(table_name, run_day):
    #     hook = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
    #     conn = hook.get_conn()
    #     register_gcs(conn)

    #     vote_point_df = conn.execute(f"""
    #         SELECT * FROM {table_name} WHERE ds = ?
    #     """, [run_day]).fetchdf()
    #     # TODO: DuckDB에서 BigQuery로 데이터 적재
    #     # conn.execute("COPY vote_point_df TO 'gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/vote_point_df.parquet' (FORMAT PARQUET);")
    #     print(vote_point_df.iloc[0])


    # 파이프라인 연결
    table1 = "staging_point_history"
    table2 = "staging_userquestion_record"

    t_extract_ph = extract_one_day.override(task_id="extract_point_history")(tables_to_combine[table1], table1)
    t_extract_uqr = extract_one_day.override(task_id="extract_userquestion_record")(tables_to_combine[table2], table2)

    t_transform = transform_part1(t_extract_ph, table1, table2)
    t_load = load_to_bigquery(t_transform, t_extract_ph)
    t_extract_ph >> t_extract_uqr >> t_transform >> t_load
