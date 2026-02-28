"""
dags/stock_market_ingestion.py
Airflow DAG — Bronze / Silver / Gold pipeline using yfinance
Schedule: weekdays at 23:00 UTC
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os, sys
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

ENV = {
    "S3_BUCKET_RAW":       "projectalphav",
    "S3_BUCKET_PROCESSED": "projectalphav",
    "S3_BUCKET_CURATED":   "projectalphav",
    "S3_PREFIX_RAW":       "portfolio-raw",
    "S3_PREFIX_PROCESSED": "portfolio-processed",
    "S3_PREFIX_CURATED":   "portfolio-curated",
    "MINIO_ENDPOINT":      "",
    "AWS_DEFAULT_REGION":  "us-east-1",
}

def set_env():
    for k, v in ENV.items():
        os.environ[k] = v

def run_bronze(**context):
    set_env()
    from pipeline.bronze_ingestion import run
    result = run(run_date=context["ds"], period="5y")
    if result["failed"]:
        raise Exception(f"Bronze failed: {result['failed']}")
    print(f"Bronze OK: {len(result['success'])} tickers")

def run_silver(**context):
    set_env()
    from pipeline.silver_transform import run
    result = run(run_date=context["ds"])
    if result["failed"]:
        raise Exception(f"Silver failed: {result['failed']}")
    print(f"Silver OK: {len(result['success'])} tickers")

def run_gold(**context):
    set_env()
    from pipeline.gold_aggregations import run
    result = run(run_date=context["ds"])
    print(f"Gold OK: {result.get('stats', {})}")

with DAG(
    dag_id="stock_market_ingestion",
    description="Equity pipeline: yfinance -> Bronze -> Silver -> Gold -> S3",
    schedule_interval="0 23 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["equity", "bronze", "silver", "gold", "s3"],
) as dag:

    bronze = PythonOperator(task_id="bronze_ingestion", python_callable=run_bronze)
    silver = PythonOperator(task_id="silver_transform", python_callable=run_silver)
    gold   = PythonOperator(task_id="gold_aggregations", python_callable=run_gold)
    notify = BashOperator(task_id="notify_success", bash_command='echo "Pipeline done for {{ ds }}"')

    bronze >> silver >> gold >> notify
