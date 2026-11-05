from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from src.pipeline.config import CONFIG
from src.pipeline.data_pipeline import build_file_key
from src.pipeline.data_pipeline import main as run_data_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_FILES_DIR = PROJECT_ROOT / "data" / "metadata" / "processed_files"


def discover_pending_raw_files() -> list[Path]:
    candidates: list[Path] = []
    for pattern in CONFIG.source_patterns:
        candidates.extend(RAW_DIR.glob(pattern))
    unique_files = sorted({path.resolve(): path for path in candidates}.values(), key=lambda item: item.name.lower())

    if not PROCESSED_FILES_DIR.exists():
        return unique_files

    parquet_files = list(PROCESSED_FILES_DIR.rglob("*.parquet"))
    if not parquet_files:
        return unique_files

    try:
        processed_df = pd.read_parquet(parquet_files)
        processed_keys = set(processed_df["file_key"].astype(str).tolist()) if "file_key" in processed_df.columns else set()
    except Exception:
        processed_keys = set()

    return [path for path in unique_files if build_file_key(path) not in processed_keys]


def raw_files_available() -> bool:
    pending = discover_pending_raw_files()
    if not pending:
        raise AirflowSkipException("No new raw files are waiting for processing.")
    return True


def inspect_raw_files() -> int:
    pending_files = discover_pending_raw_files()
    print(f"Detected {len(pending_files)} new raw files for processing.")
    for path in pending_files[:10]:
        print(f" - {path.name}")
    return len(pending_files)


def validate_pipeline_outputs() -> None:
    metrics_files = list((PROJECT_ROOT / "data" / "metadata" / "metrics").rglob("*.parquet"))
    if not metrics_files:
        raise FileNotFoundError("Pipeline completed but no metrics parquet files were written.")
    print(f"Validated pipeline outputs with {len(metrics_files)} metrics parquet files present.")


default_args = {
    "owner": "pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@email.com"],
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def run_pipeline_job() -> None:
    run_data_pipeline()


def build_pipeline_task(task_id: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_pipeline_job,
    )


with DAG(
    dag_id="lakehouse_pipeline_scheduled",
    default_args=default_args,
    description="Scheduled daily Spark-to-Delta pipeline run",
    schedule="0 5 * * *",
    start_date=datetime(2026, 3, 13),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["lakehouse", "spark", "scheduled"],
) as scheduled_dag:
    inspect_files = PythonOperator(
        task_id="inspect_raw_files",
        python_callable=inspect_raw_files,
    )

    run_pipeline = build_pipeline_task("run_spark_pipeline")

    validate_outputs = PythonOperator(
        task_id="validate_pipeline_outputs",
        python_callable=validate_pipeline_outputs,
    )

    inspect_files >> run_pipeline >> validate_outputs


with DAG(
    dag_id="lakehouse_pipeline_event_driven",
    default_args=default_args,
    description="Event-driven Spark-to-Delta pipeline triggered by new raw files",
    schedule="* * * * *",
    start_date=datetime(2026, 3, 13),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["lakehouse", "spark", "event-driven"],
) as event_driven_dag:
    wait_for_file = PythonOperator(
        task_id="wait_for_new_file",
        python_callable=raw_files_available,
    )

    inspect_event_files = PythonOperator(
        task_id="inspect_raw_files",
        python_callable=inspect_raw_files,
    )

    run_event_pipeline = build_pipeline_task("run_spark_pipeline")

    validate_event_outputs = PythonOperator(
        task_id="validate_pipeline_outputs",
        python_callable=validate_pipeline_outputs,
    )

    wait_for_file >> inspect_event_files >> run_event_pipeline >> validate_event_outputs
