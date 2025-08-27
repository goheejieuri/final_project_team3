from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.airflow_func_total_user_df import GCS_to_duckDB, bq_to_duckDB, processing_data, duckDB_to_bq
import pendulum


default_args = dict(
  owner = 'sohee',
  email = ['sohee@airflow.com'],
  email_on_failure = False,
  retries = 3
)

with DAG(
  dag_id='2_total_user_df_dag',
  start_date=pendulum.datetime(2023, 3, 29, tz='Asia/Seoul'),
  schedule='0 2 * * *',
  default_args=default_args,
  catchup=False, # 하루에 한개씩 하고 싶으면 이 부분을 true로 바꿔야함
  tags=['20250813'],
  max_active_runs=1
) as dag:
  
  # Task 1: GCS에서 DuckDB로 데이터 로드
  gcs_to_duckdb_task = PythonOperator(
    task_id='gcs_to_duckdb',
    python_callable=GCS_to_duckDB,
  )
  
  # Task 2: bq에서 DcukDB로 데이터 로드
  bq_to_duckdb_task = PythonOperator(
    task_id='bq_to_duckdb',
    python_callable=bq_to_duckDB,
  )
    
  # Task 3: 데이터 처리
  process_task = PythonOperator(
    task_id='process_question_data',
    python_callable=processing_data,
  )
  
  # Task 4: DuckDB에서 BigQuery로 업로드
  duckdb_to_bq_task = PythonOperator(
    task_id='duckdb_to_bigquery',
    python_callable=duckDB_to_bq,
  )
  
  # Task 의존성 설정
  gcs_to_duckdb_task >> bq_to_duckdb_task >> process_task >> duckdb_to_bq_task