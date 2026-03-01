from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="stock_market_ingestion",
    description="Equity pipeline: yfinance -> Bronze -> Silver -> Gold -> S3",
    schedule_interval="0 23 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["equity", "bronze", "silver", "gold", "s3"],
) as dag:

    base = """
cd /opt/airflow && \
export AWS_ACCESS_KEY_ID=$(airflow variables get AWS_ACCESS_KEY_ID) && \
export AWS_SECRET_ACCESS_KEY=$(airflow variables get AWS_SECRET_ACCESS_KEY) && \
export AWS_DEFAULT_REGION=us-east-1 && \
export S3_BUCKET_RAW=projectalphav && \
export S3_BUCKET_PROCESSED=projectalphav && \
export S3_BUCKET_CURATED=projectalphav && \
export S3_PREFIX_RAW=portfolio-raw && \
export S3_PREFIX_PROCESSED=portfolio-processed && \
export S3_PREFIX_CURATED=portfolio-curated && \
"""

    bronze = BashOperator(task_id="bronze_ingestion", bash_command=base + "/usr/local/bin/python /opt/airflow/pipeline/bronze_ingestion.py --date {{ ds }}")
    silver = BashOperator(task_id="silver_transform", bash_command=base + "/usr/local/bin/python /opt/airflow/pipeline/silver_transform.py --date {{ ds }}")
    gold   = BashOperator(task_id="gold_aggregations", bash_command=base + "/usr/local/bin/python /opt/airflow/pipeline/gold_aggregations.py --date {{ ds }}")
    notify = BashOperator(task_id="notify_success", bash_command='echo "Pipeline completed for {{ ds }}"')

    bronze >> silver >> gold >> notify
