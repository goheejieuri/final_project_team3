import duckdb
import pandas as pd
import json
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from airflow.sdk import get_current_context

LOCAL_DUCKDB_CONN_ID = "my_local_duckdb_conn"

# DuckDB 파일 경로 (새로 생성)
DUCKDB_PATH = "/opt/airflow/mydb.duckdb"

def GCS_to_duckDB():
  # DuckDB 연결 (직접 연결)
  conn = duckdb.connect(DUCKDB_PATH)
  
  # 기존 테이블 확인
  existing_tables = conn.execute("SHOW TABLES").fetchall()
  print(f"기존 테이블들: {existing_tables}")
  
  # GCS Hook에서 credentials 가져오기
  gcs_hook = GCSHook(gcp_conn_id="gcs_connection")
  credentials = gcs_hook.get_credentials()
  
  # GCS 경로들
  base_path = 'gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/'
  
  files = {
    'polls_questionset.parquet': 'polls_questionset_df',
    'polls_questionpiece.parquet': 'polls_questionpiece_df', 
    'polls_question.parquet': 'polls_question_df '
  }
  
  # GCS에서 바로 pandas로 읽어서 DuckDB에 저장
  for file_name, table_name in files.items():
    gcs_file_path = f'{base_path}{file_name}'
    
    # pandas로 GCS에서 직접 읽기
    df = pd.read_parquet(gcs_file_path, storage_options={'token': credentials})
    print(f"{file_name} 로드 완료: {len(df)} rows")
    
    # DuckDB에 저장
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    
  conn.close()
  print("GCS → DuckDB 로드 완료")

def processing_data():
  
  # DuckDB 연결 (직접 연결)
  conn = duckdb.connect(DUCKDB_PATH)
  
  # # Airflow context에서 실행 날짜 가져오기
  # context = get_current_context()
  # date = context['ds']  # YYYY-MM-DD 형식
  # print(f"Process data - Airflow 실행 날짜: {date}")
    
  # DuckDB에서 데이터 불러오기
  polls_questionset_df = conn.execute("SELECT * FROM polls_questionset_df").fetchdf()
  polls_questionpiece_df = conn.execute("SELECT * FROM polls_questionpiece_df").fetchdf()
  polls_question_df = conn.execute("SELECT * FROM polls_question_df").fetchdf()
  print(f"polls_questionset_df: {len(polls_questionset_df)} rows, polls_questionpiece_df: {len(polls_questionpiece_df)} rows, polls_question_df: {len(polls_question_df)} rows")
  
  # polls_questionset_df정규화 대상 copy
  df = polls_questionset_df.copy()

  # question_piece_id_list 컬럼 정규화 (explode 사용)
  df['question_piece_id_list'] = df['question_piece_id_list'].apply(eval)  # 문자열로 저장된 리스트를 진짜 리스트로 변환
  normalized_df = df.explode('question_piece_id_list')[['id', 'question_piece_id_list']]

  # 열 이름 바꾸기
  normalized_df = normalized_df.rename(columns={'question_piece_id_list': 'question_piece_id'})

  # 정규화 후 df랑 기존 df 병합
  merged_df = normalized_df.merge(
      df[['id', 'opening_time', 'status', 'created_at', 'user_id']],
      how='left',
      on='id'
  )

  # polls_questionpiece_df 병합
  question_df = merged_df.merge(
      polls_questionpiece_df, 
      how='left', 
      left_on='question_piece_id', 
      right_on='id',
      suffixes=('_set','_piece'))

  # polls_question_df  질문내용과 병합
  question_df2 = question_df.merge(
    polls_question_df,
    how='left',
    left_on='question_id',
    right_on='id'
  )

  # null 제거
  question_df_notnull = question_df2.dropna()
  question_df_notnull

  # 컬럼 정리하기
  final_question_df = question_df_notnull[['id_set', 'question_piece_id', 'opening_time', 'status',
                                            'created_at_set', 'user_id', 'is_voted', 'is_skipped', 'created_at_piece',
                                            'question_id',  'question_text']]

  # 처리된 데이터를 DuckDB에 저장
  conn.execute("CREATE OR REPLACE TABLE processed_question_data AS SELECT * FROM final_question_df")
  print("데이터 처리 완료")
  
  conn.close()
  
def duckDB_to_bq():
  # DuckDB 연결 (직접 연결) - DuckDBHook 사용 안함!
  conn = duckdb.connect(DUCKDB_PATH)
  
  # 처리된 데이터 읽기
  question_df = conn.execute("SELECT * FROM processed_question_data").fetchdf()
  
  # BigQuery 업로드
  bq_hook = BigQueryHook(gcp_conn_id="gbq_connection", use_legacy_sql=False)
  client = bq_hook.get_client()
  
  table_id = "my-projectcodeit.final_project.question_df"
  
  job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND", # 기존 데이터를 유지하고 새 데이터를 추가
    autodetect=True
  )
  
  job = client.load_table_from_dataframe(question_df, table_id, job_config=job_config)
  job.result()
  
  conn.close()
  print(f"데이터 업로드 완료: {len(question_df)} rows")