DUCKDB_CONN_ID = "my_local_duckdb_conn"
GCP_CONN_ID = "gcp_conn"
tables_to_combine = {
    "staging_point_history": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_pointhistory.parquet",
    "staging_userquestion_record": "gs://sprintda07-gohee-bucket/final_project/최종프로젝트_데이터(parquet)/votes/accounts_userquestionrecord.parquet",
    "staging_question": "question_df",
    "staging_vote_point": "vote_point",
}
BQ_PROJECT = "my-projectcodeit"
BQ_DATASET = "final_project"