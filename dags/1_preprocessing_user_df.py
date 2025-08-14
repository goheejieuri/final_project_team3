from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.airflow_func_user_df import GCS_to_duckDB, process_data, duckDB_to_bq
import pendulum


default_args = dict(
  owner = 'sohee',
  email = ['sohee@airflow.com'],
  email_on_failure = False,
  retries = 3
)

with DAG(
  dag_id='1_user_dag',
  start_date=pendulum.datetime(2023, 3, 29, tz='Asia/Seoul'),
  schedule='0 1 * * *',
  default_args=default_args,
  catchup=False,
  tags=['20250813'],
  max_active_runs=1
) as dag:
  
  # Task 1: GCS에서 DuckDB로 데이터 로드
  gcs_to_duckdb_task = PythonOperator(
    task_id='gcs_to_duckdb',
    python_callable=GCS_to_duckDB,
  )
  
  # Task 2: 데이터 처리
  process_task = PythonOperator(
    task_id='process_user_data',
    python_callable=process_data,
  )
  
  # Task 3: DuckDB에서 BigQuery로 업로드
  duckdb_to_bq_task = PythonOperator(
    task_id='duckdb_to_bigquery',
    python_callable=duckDB_to_bq,
  )
  
  # Task 의존성 설정
  gcs_to_duckdb_task >> process_task >> duckdb_to_bq_task