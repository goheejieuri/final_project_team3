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
    'accounts_user.parquet': 'user_df',
    'accounts_attendance.parquet': 'attendance_df', 
    'accounts_group.parquet': 'group_df',
    'accounts_school.parquet': 'school_df'
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
  
def fast_len(x):
  if x == '[]' or pd.isna(x):
      return 0
  try:
    return len(json.loads(x))
  except:
    return 0
  
def process_data():
  # DuckDB 연결 (직접 연결)
  conn = duckdb.connect(DUCKDB_PATH)
  
  # Airflow context에서 실행 날짜 가져오기
  context = get_current_context()
  date = context['ds']  # YYYY-MM-DD 형식
  print(f"Process data - Airflow 실행 날짜: {date}")
    
  # DuckDB에서 해당 날짜 데이터만 읽기
  user_df = conn.execute(f"""
      SELECT * FROM user_df 
      WHERE DATE(created_at) = '{date}'
    """).fetchdf()
  print(f"user_df 날짜 필터링: {len(user_df)} rows (날짜: {date})")
    
  # 나머지 테이블들 (created_at 없음)
  attendance_df = conn.execute("SELECT * FROM attendance_df").fetchdf()
  group_df = conn.execute("SELECT * FROM group_df").fetchdf()
  school_df = conn.execute("SELECT * FROM school_df").fetchdf()
  print(f"attendance_df: {len(attendance_df)} rows, group_df: {len(group_df)} rows, school_df: {len(school_df)} rows")
  
  # 데이터 처리
  # user_df에 attendance_df merge하기
  merged_df_1 = user_df.merge(attendance_df[['attendance_date_list', 'user_id']], how='left', left_on='id', right_on='user_id').drop(columns=['user_id'])
  
  # fast_len 사용
  merged_df_1['friend_count'] = merged_df_1['friend_id_list'].apply(fast_len)
  merged_df_1['block_user_count'] = merged_df_1['block_user_id_list'].apply(fast_len)
  merged_df_1['hide_user_id_count'] = merged_df_1['hide_user_id_list'].apply(fast_len)
  merged_df_1['attendance_date_count'] = merged_df_1['attendance_date_list'].apply(fast_len)
  
  # 이상치 제거
  merged_df_1 = merged_df_1[(
    (merged_df_1['is_superuser'] == 0) &
    (merged_df_1['is_staff'] == 0))]
  
  # id를 user_id로 컬럼명 변경
  merged_df_1 = merged_df_1.rename(columns={'id': 'user_id'})

  # user_df에 group_df, school_df merge하기
  merged_df_2 = merged_df_1.merge(group_df, how='left', left_on='group_id', right_on='id')
  merged_df_3 = merged_df_2.merge(school_df, how='left', left_on='school_id', right_on='id')

  # 불필요한 컬럼 제거
  merged_df_3 = merged_df_3.drop(columns=['is_superuser', 'is_staff', 'friend_id_list', 'block_user_id_list', 'hide_user_id_list', 'attendance_date_list', 'id_x', 'id_y'])

  # null값 제거
  merged_df_3 = merged_df_3.dropna()
  
  # 처리된 데이터를 DuckDB에 저장
  conn.execute("CREATE OR REPLACE TABLE processed_data AS SELECT * FROM merged_df_3")
  print("데이터 처리 완료")
  
  conn.close()
  
def duckDB_to_bq():
  # DuckDB 연결 (직접 연결) - DuckDBHook 사용 안함!
  conn = duckdb.connect(DUCKDB_PATH)
  
  # 처리된 데이터 읽기
  merged_df_3 = conn.execute("SELECT * FROM processed_data").fetchdf()
  
  # BigQuery 업로드
  bq_hook = BigQueryHook(gcp_conn_id="gbq_connection", use_legacy_sql=False)
  client = bq_hook.get_client()
  
  table_id = "my-projectcodeit.final_project.user_df"
  
  job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND", # 기존 데이터를 유지하고 새 데이터를 추가
    autodetect=True
  )
  
  job = client.load_table_from_dataframe(merged_df_3, table_id, job_config=job_config)
  job.result()
  
  conn.close()
  print(f"데이터 업로드 완료: {len(merged_df_3)} rows")