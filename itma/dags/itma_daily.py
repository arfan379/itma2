"""
ITMA Daily Orchestration DAG (itma_daily.py)
Automates daily batch ingestion, simulation, Kafka streaming consumption,
dbt medallion transformations, and quality testing.
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add project root to sys.path so modules can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# Path biner dbt. Di image Airflow ITMA diisi lewat ENV DBT_BIN (lihat Dockerfile.airflow);
# kalau tidak ada, dipakai perintah "dbt" dari PATH.
DBT = "${DBT_BIN:-dbt}"

default_args = {
    "owner": "itma-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_ingest_gtfs(**context):
    """Pulls TransJakarta official GTFS feed and writes into Bronze."""
    from src.ingestion.api_extractor import run_gtfs_ingestion
    metrics = run_gtfs_ingestion()
    context["ti"].xcom_push(key="gtfs_metrics", value=metrics)


def task_simulate_delays(**context):
    """Generates synthetic actual arrivals for scheduled trips on current service date."""
    from src.simulator.delay_simulator import run_delay_simulation
    ds = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
    metrics = run_delay_simulation(service_date=ds)
    context["ti"].xcom_push(key="delay_metrics", value=metrics)


def task_produce_telemetry(**context):
    """Simulates road segment traffic telemetry and emits events to Kafka."""
    from src.simulator.telemetry_producer import run_telemetry_simulation
    ds = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
    metrics = run_telemetry_simulation(service_date=ds, publish_kafka=True)
    context["ti"].xcom_push(key="telemetry_metrics", value=metrics)


def task_consume_telemetry(**context):
    """Consumes Kafka topic events and upserts into raw.raw_traffic."""
    from src.ingestion.kafka_consumer import consume_messages
    metrics = consume_messages(max_records=50000, poll_timeout_ms=5000)
    context["ti"].xcom_push(key="consumer_metrics", value=metrics)


with DAG(
    dag_id="itma_daily",
    default_args=default_args,
    description="ITMA End-to-End Daily Pipeline: Ingestion -> Simulation -> Stream Ingestion -> dbt Medallion",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["itma", "transjakarta", "medallion", "mobility"],
) as dag:

    ingest_gtfs = PythonOperator(
        task_id="ingest_gtfs",
        python_callable=task_ingest_gtfs,
    )

    run_delay_simulator = PythonOperator(
        task_id="run_delay_simulator",
        python_callable=task_simulate_delays,
    )

    run_telemetry_producer = PythonOperator(
        task_id="run_telemetry_producer",
        python_callable=task_produce_telemetry,
    )

    run_kafka_consumer = PythonOperator(
        task_id="run_kafka_consumer",
        python_callable=task_consume_telemetry,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd /opt/airflow/transform && {DBT} deps --profiles-dir .",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd /opt/airflow/transform && {DBT} seed --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd /opt/airflow/transform && {DBT} run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd /opt/airflow/transform && {DBT} test --profiles-dir .",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd /opt/airflow/transform && {DBT} docs generate --profiles-dir .",
    )

    # Linear dependencies
    ingest_gtfs >> run_delay_simulator
    run_delay_simulator >> run_telemetry_producer >> run_kafka_consumer
    run_kafka_consumer >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs_generate
