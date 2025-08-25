import pendulum
import os, json, urllib, time
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.email import EmailOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from google.cloud import bigquery
from utils import register_gcs, nlp_modeling, advanced_questions
from airflow_plugins import (
    DUCKDB_CONN_ID, GCP_CONN_ID, tables_to_combine,
    BQ_PROJECT, BQ_DATASET
)

BQ_TABLE   = "final_question"
REPORT_BASE = "https://lookerstudio.google.com/reporting/65840f2b-fd39-4a07-a47b-e0f97c3d6520"
MAIL_TO = ["olozl1228@gmail.com", "mseungy13@gmail.com", "sohee1801@gmail.com"]

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
    # 1. BigQuery에서 데이터를 가져와서 과거 데이터 DuckDB 테이블에 저장
    @task
    def extract_till_today(staging_table_name, uri, ts_col="created_at"):
        ddb = DuckDBHook(duckdb_conn_id=DUCKDB_CONN_ID)
        conn = ddb.get_conn()
        try:
            register_gcs(conn)

            ctx = get_current_context()
            run_day_dt = ctx["logical_date"].in_timezone("Asia/Seoul").date()
            run_day = run_day_dt.isoformat()
            next_day = (run_day_dt + pendulum.duration(days=1)).isoformat()

            bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location="asia-northeast3")
            client = bq_hook.get_client(project_id=BQ_PROJECT)

            query = f"""
                SELECT *
                FROM `{BQ_PROJECT}.{BQ_DATASET}.{uri}`
                WHERE DATE({ts_col}) < DATE '{next_day}'
            """
            job = client.query(query)
            arrow_tbl = job.to_arrow()  
            src = f"__src_{staging_table_name}"
            conn.register(src, arrow_tbl)

            conn.execute(f"""
                CREATE OR REPLACE TABLE {staging_table_name} AS
                SELECT
                    *,
                    CAST({ts_col} AS TIMESTAMP) AS ts,
                    DATE(CAST({ts_col} AS TIMESTAMP)) AS ds
                FROM {src}
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

    # 4. 시각화한 리포트 PDF로 출력
    @task
    def export_lookerstudio_pdf(report_url: str) -> str:
        from playwright.sync_api import sync_playwright

        run_day = get_current_context()["logical_date"].in_timezone("Asia/Seoul").date().isoformat()
        out_dir = "/opt/airflow/tmp"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/looker_report_{run_day}.pdf"

        def build_params_json_for_date(d: str) -> str:
            candidates = [
                ("start_date", "end_date"),
                ("ds0.start_date", "ds0.end_date"),
                ("ds3.start_date", "ds3.end_date"),
                ("ds4.start_date", "ds4.end_date"),
                ("ds11.start_date", "ds11.end_date"),
                ("param.start_date", "param.end_date"),
            ]
            payload = {}
            for s_key, e_key in candidates:
                payload[s_key] = d
                payload[e_key] = d
            return json.dumps(payload, separators=(",", ":"))

        def append_params_to_url(url: str, params_json: str) -> str:
            p = urllib.parse.urlparse(url)
            q = [(k, v) for (k, v) in urllib.parse.parse_qsl(p.query, keep_blank_values=True) if k != "params"]
            q.append(("params", params_json))
            new_query = urllib.parse.urlencode(q)
            return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))

        params_json = build_params_json_for_date(run_day)
        final_url = append_params_to_url(report_url, params_json)

        # 다운로드 버튼 찾아 클릭
        DOWNLOAD_DIRECT_SELECTORS = [
            "button.share-dl-button",
            "button:has-text('다운로드')", "button[aria-label*='다운로드']",
            "button:has-text('Download')", "button[aria-label*='Download']",
            "[role='menuitem']:has-text('다운로드')", "[role='menuitem']:has-text('Download')",
            "div[role='menuitem']:has-text('다운로드')", "div[role='menuitem']:has-text('Download')",
        ]
        MENU_OPENERS = [
            "button.split-button-menu-button.mat-mdc-menu-trigger",
            "button.mat-mdc-menu-trigger[aria-haspopup='menu']",
            "button[aria-label*='더보기']", "button[aria-label*='옵션']",
            "button[aria-label*='More']", "button[aria-label*='Menu']",
        ]
        MENU_PANEL = ".mat-mdc-menu-panel, .mdc-menu-surface--open, [role='menu']"

        def try_click_any(page, selectors, timeout_ms=8000, force=False):
            deadline = time.time() + timeout_ms / 1000.0
            while time.time() < deadline:
                for sel in selectors:
                    loc = page.locator(sel).first
                    try:
                        if loc.count() and loc.is_visible():
                            try:
                                loc.click(timeout=1500, force=force)
                                return True, sel
                            except Exception:
                                try:
                                    loc.scroll_into_view_if_needed(timeout=500)
                                except Exception:
                                    pass
                                try:
                                    loc.click(timeout=1500, force=True)
                                    return True, sel
                                except Exception:
                                    pass
                    except Exception:
                        pass
                page.wait_for_timeout(200)
            return False, None

        def trigger_download(page):
            ok, _ = try_click_any(page, DOWNLOAD_DIRECT_SELECTORS, timeout_ms=6000, force=False)
            if ok:
                return True
            ok, _ = try_click_any(page, MENU_OPENERS, timeout_ms=8000, force=False)
            if ok:
                for _ in range(20):
                    if page.locator(MENU_PANEL).first.count():
                        break
                    page.wait_for_timeout(150)
                ok2, _ = try_click_any(page, DOWNLOAD_DIRECT_SELECTORS, timeout_ms=6000, force=False)
                if ok2:
                    return True
            ok, _ = try_click_any(page, ["button.mat-mdc-menu-trigger", "button[aria-haspopup='menu']"], timeout_ms=4000, force=True)
            if ok:
                ok2, _ = try_click_any(page, DOWNLOAD_DIRECT_SELECTORS, timeout_ms=6000, force=True)
                if ok2:
                    return True
            return False

        with sync_playwright() as p:
            b = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"])
            c = b.new_context(viewport={"width": 1920, "height": 1080}, locale="ko-KR", accept_downloads=True)
            c.set_default_timeout(15000)
            page = c.new_page()
            page.set_default_timeout(15000)

            page.goto(final_url, wait_until="domcontentloaded", timeout=180_000)
            page.wait_for_function("document.readyState==='complete'", timeout=180_000)

            STABLE_CANDIDATES = [
                "div.report-viewer",
                "button.mat-mdc-menu-trigger",
                "button.split-button-menu-button.mat-mdc-menu-trigger",
                "button.share-dl-button",
                "canvas", "svg",
            ]
            def ready_enough():
                for sel in STABLE_CANDIDATES:
                    loc = page.locator(sel).first
                    try:
                        if loc.count() and loc.is_visible():
                            return True
                    except Exception:
                        pass
                return False

            for _ in range(60):
                if ready_enough():
                    break
                page.wait_for_timeout(250)
            page.wait_for_timeout(800)

            if not trigger_download(page):
                try:
                    ss = f"{out_dir}/looker_dl_fail_{run_day}.png"
                    page.screenshot(path=ss, full_page=True)
                except Exception:
                    pass
                raise RuntimeError("리포트에서 '다운로드'를 찾지 못했습니다.")

            with page.expect_download(timeout=180_000) as dl:
                try_click_any(page, ["button:has-text('다운로드')", "button:has-text('Download')"], timeout_ms=6000, force=False)
            d = dl.value
            d.save_as(out_path)

            b.close()
        return out_path
        
    # 파이프라인 연결
    table1 = "staging_question"
    table2 = "staging_vote_point"

    t_extract_q = extract_till_today.override(task_id="extract_question")(table1, tables_to_combine[table1], ts_col="created_at_piece")
    t_extract_vp = extract_till_today.override(task_id="extract_vote_point")(table2, tables_to_combine[table2])

    t_transform = transform_part2(t_extract_vp, table1, table2)
    t_load = load_to_bigquery(t_transform)

    t_export = export_lookerstudio_pdf.override(task_id="export_lookerstudio_pdf")(REPORT_BASE)

    # 5. PDF파일 이메일로 전송
    t_email = EmailOperator(
        task_id="email_report_pdf",
        conn_id="smtp_default",
        to=MAIL_TO,
        subject="Looker Studio 리포트 스냅샷 - {{ ds }}",
        html_content="""
        <p>Looker Studio 리포트 스냅샷입니다. ({{ ds }})</p>
        """,
        files=["{{ ti.xcom_pull(task_ids='export_lookerstudio_pdf') }}"],
    )

    t_extract_q >> t_extract_vp >> t_transform >> t_load >> t_export >> t_email