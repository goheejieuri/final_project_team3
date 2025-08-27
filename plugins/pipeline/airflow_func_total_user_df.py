import duckdb
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from airflow.sdk import get_current_context
from google.oauth2 import service_account

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
    'accounts_pointhistory.parquet': 'pointhistory_df',
    'accounts_userquestionrecord.parquet': 'userquestionrecord_df', 
    'event_receipts.parquet': 'event_receipts_df',
    'accounts_paymenthistory.parquet': 'paymenthistory_df',
    'accounts_failpaymenthistory.parquet': 'failpaymenthistory_df',
    'accounts_timelinereport.parquet': 'timelinereport_df'
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
  
  
def bq_to_duckDB():
  # DuckDB 연결 (직접 연결)
  conn = duckdb.connect(DUCKDB_PATH)
  
  # 기존 테이블 확인
  existing_tables = conn.execute("SHOW TABLES").fetchall()
  print(f"기존 테이블들: {existing_tables}")
  
  # BigQuery Hook에서 credentials 가져오기
  bq_hook = BigQueryHook(gcp_conn_id="gbq_connection")
  credentials = bq_hook.get_credentials() 
  project_id = bq_hook.project_id
  
  # BigQuery 클라이언트 생성
  bq_client = bigquery.Client(credentials=credentials, project=project_id)
  
  df = pd.read_gbq(
    query ="SELECT * FROM final_project.user_df",
    project_id=project_id,
    credentials=credentials,
    location='asia-northeast3'
  )
  
  print(f"BigQuery에서 user_df 로드 완료: {len(df)} rows")
  
  # DuckDB에 저장
  conn.execute("CREATE OR REPLACE TABLE user_df AS SELECT * FROM df")
  
  conn.close()
  print("BigQuery → DuckDB 로드 완료")
  
  
def processing_data():
  # DuckDB 연결 (직접 연결)
  conn = duckdb.connect(DUCKDB_PATH)
  
  # DuckDB에서 데이터 불러오기
  pointhistory_df = conn.execute("SELECT * FROM pointhistory_df").fetchdf()
  userquestionrecord_df = conn.execute("SELECT * FROM userquestionrecord_df").fetchdf()
  event_receipts_df = conn.execute("SELECT * FROM event_receipts_df").fetchdf()
  paymenthistory_df = conn.execute("SELECT * FROM paymenthistory_df").fetchdf()
  failpaymenthistory_df = conn.execute("SELECT * FROM failpaymenthistory_df").fetchdf()
  timelinereport_df = conn.execute("SELECT * FROM timelinereport_df").fetchdf()
  user_df = conn.execute("SELECT * FROM user_df").fetchdf()
  print(f"""pointhistory_df: {len(pointhistory_df)} rows, userquestionrecord_df: {len(userquestionrecord_df)} rows, event_receipts_df: {len(event_receipts_df)} rows, 
          paymenthistory_df: {len(paymenthistory_df)} rows, failpaymenthistory_df: {len(failpaymenthistory_df)} rows, timelinereport_df: {len(timelinereport_df)} rows,
          user_df: {len(user_df)} rows""")
  
  # user_df 복사본 생성
  user_df_ver3 = user_df.copy()

  # school_type과 grade를 합쳐서 school_grade 컬럼 생성
  user_df_ver3['school_grade'] = user_df_ver3.apply(
      lambda row: f"{row['school_type']}_{int(row['grade'])}" if pd.notna(row['grade']) else None,
      axis=1
  )

  # 포인트 사용/획득 데이터프레임 생성
  use_df = pointhistory_df[pointhistory_df['delta_point'] < 0]
  get_df = pointhistory_df[pointhistory_df['delta_point'] > 0]

  use_user_df = use_df['user_id'].value_counts().reset_index(name='pointuse_cnt')
  get_user_df = get_df['user_id'].value_counts().reset_index(name='pointget_cnt')

  # 투표참여 횟수 데이터프레임 생성
  vote_df = userquestionrecord_df['user_id'].value_counts().reset_index(name='vote_cnt')
  chosen_df = userquestionrecord_df['chosen_user_id'].value_counts().reset_index(name='chosen_cnt')
  chosen_df.rename(columns = {'chosen_user_id':'user_id'}, inplace=True)

  # 이벤트 참여횟수 데이터프레임 생성
  event_df = event_receipts_df['user_id'].value_counts().reset_index(name='event_cnt')

  # 구매관련 데이터프레임 생성
  buy_df = paymenthistory_df['user_id'].value_counts().reset_index(name='buy_cnt')
  buyfail_df = failpaymenthistory_df['user_id'].value_counts().reset_index(name='buyfail_cnt')

  # 친구 신고 관련 데이터프레임 생성
  friendreport_df = timelinereport_df['user_id'].value_counts().reset_index(name='friendreport_cnt')

  # 데이터 합치기
  # 기준 DataFrame 복사
  merged_df = user_df_ver3.copy()

  # 기존 컬럼 목록 저장
  original_columns = set(merged_df.columns)

  # 병합 대상 DataFrame 리스트
  df_list = [use_user_df, get_user_df, vote_df, chosen_df, event_df, buy_df, buyfail_df, friendreport_df]

  # merge 실행
  for df in df_list:
      merged_df = merged_df.merge(df, on='user_id', how='left')

  # 새로 생긴 컬럼만 골라서 fillna(0)
  new_columns = list(set(merged_df.columns) - original_columns)
  merged_df[new_columns] = merged_df[new_columns].fillna(0)

  # 주소 데이터 정제
  addresses  = merged_df['address'].unique()

  # 전체 시도 이름 + 줄임말 모두 대응
  province_mapping = {
      '서울': '서울특별시', '서울특별시': '서울특별시',
      '부산': '부산광역시', '부산광역시': '부산광역시',
      '대구': '대구광역시', '대구광역시': '대구광역시',
      '인천': '인천광역시', '인천광역시': '인천광역시',
      '광주': '광주광역시', '광주광역시': '광주광역시',
      '대전': '대전광역시', '대전광역시': '대전광역시',
      '울산': '울산광역시', '울산광역시': '울산광역시',
      '세종': '세종특별자치시', '세종특별자치시': '세종특별자치시',
      '경기': '경기도', '경기도': '경기도',
      '강원': '강원도', '강원도': '강원도',
      '충북': '충청북도', '충청북도': '충청북도',
      '충남': '충청남도', '충청남도': '충청남도',
      '전북': '전라북도', '전라북도': '전라북도',
      '전남': '전라남도', '전라남도': '전라남도',
      '경북': '경상북도', '경상북도': '경상북도',
      '경남': '경상남도', '경상남도': '경상남도',
      '제주': '제주특별자치도', '제주특별자치도': '제주특별자치도'
  }

  def extract_province(address):
      for short, full in province_mapping.items():
          if short in address:
              return full
      return None

  # 적용
  merged_df['province'] = merged_df['address'].apply(extract_province)

  # 필요없는 컬럼 제외
  merged_df.drop(columns=['group_id', 'grade', 'class_num', 'school_id', 'school_type', 'address'], inplace=True)

  # null값 제거
  processed_user_data = merged_df.dropna(subset=['province']).copy()
  
  # 처리된 데이터를 DuckDB에 저장
  conn.execute("CREATE OR REPLACE TABLE processed_user_data AS SELECT * FROM processed_user_data")
  conn.close()  # ✅ 연결 닫기 추가
  print("데이터 처리 완료")
  
  
def duckDB_to_bq():
  # DuckDB 연결 (직접 연결) - DuckDBHook 사용 안함!
  conn = duckdb.connect(DUCKDB_PATH)
  
  # 처리된 데이터 읽기
  total_user_df = conn.execute("SELECT * FROM processed_user_data").fetchdf()
  
  # BigQuery 업로드
  bq_hook = BigQueryHook(gcp_conn_id="gbq_connection", use_legacy_sql=False)
  client = bq_hook.get_client()
  
  table_id = "my-projectcodeit.final_project.total_user_df"
  
  job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND", # 기존 데이터를 유지하고 새 데이터를 추가
    autodetect=True
  )
  
  job = client.load_table_from_dataframe(total_user_df, table_id, job_config=job_config)
  job.result()
  
  conn.close()
  print(f"데이터 업로드 완료: {len(total_user_df)} rows")