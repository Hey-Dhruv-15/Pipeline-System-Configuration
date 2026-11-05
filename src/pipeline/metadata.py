from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ensure_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def read_processed_files(spark: SparkSession, processed_files_dir: str) -> set[str]:
    try:
        rows = spark.read.parquet(processed_files_dir).select("file_key").distinct().collect()
        return {row["file_key"] for row in rows}
    except Exception:
        return set()


def write_processed_files(
    spark: SparkSession,
    processed_files_dir: str,
    processed_entries: list[tuple[str, str, int, str]],
    run_id: str,
) -> None:
    if not processed_entries:
        return

    df = spark.createDataFrame(
        [(file_name, file_key, file_size, file_modified_at, run_id) for file_name, file_key, file_size, file_modified_at in processed_entries],
        ["file_name", "file_key", "file_size", "file_modified_at", "run_id"],
    ).withColumn("processed_at", F.current_timestamp())
    df.write.mode("append").parquet(processed_files_dir)


def write_lineage(
    spark: SparkSession,
    lineage_dir: str,
    dataset: str,
    source_stage: str,
    target_stage: str,
    run_id: str,
) -> None:
    lineage_df = spark.createDataFrame(
        [(dataset, source_stage, target_stage, run_id)],
        ["dataset", "source_stage", "target_stage", "run_id"],
    ).withColumn("recorded_at", F.current_timestamp())
    lineage_df.write.mode("append").parquet(lineage_dir)


def write_metrics(
    spark: SparkSession,
    metrics_dir: str,
    run_id: str,
    run_status: str,
    records_processed: int,
    rejected_records: int,
    processed_files: int,
    runtime_seconds: float,
) -> None:
    metrics_df = spark.createDataFrame(
        [(run_id, run_status, records_processed, rejected_records, processed_files, runtime_seconds)],
        ["run_id", "run_status", "records_processed", "rejected_records", "processed_files", "runtime_seconds"],
    ).withColumn("recorded_at", F.current_timestamp())
    metrics_df.write.mode("append").parquet(metrics_dir)


def write_run_status(
    spark: SparkSession,
    run_status_dir: str,
    run_id: str,
    status: str,
    message: str,
) -> None:
    run_df = spark.createDataFrame(
        [(run_id, status, message)],
        ["run_id", "status", "message"],
    ).withColumn("recorded_at", F.current_timestamp())
    run_df.write.mode("append").parquet(run_status_dir)


def write_alert_event(
    spark: SparkSession,
    alerts_dir: str,
    run_id: str,
    alert_type: str,
    severity: str,
    message: str,
    delivery_state: str,
) -> None:
    alert_df = spark.createDataFrame(
        [(run_id, alert_type, severity, message, delivery_state)],
        ["run_id", "alert_type", "severity", "message", "delivery_state"],
    ).withColumn("recorded_at", F.current_timestamp())
    alert_df.write.mode("append").parquet(alerts_dir)


def write_parser_runs(df: DataFrame, parser_runs_dir: str) -> None:
    if df.rdd.isEmpty():
        return
    df.withColumn("recorded_at", F.current_timestamp()).write.mode("append").parquet(parser_runs_dir)


def write_quality_events(
    spark: SparkSession,
    quality_events_dir: str,
    run_id: str,
    event_type: str,
    record_count: int,
    details: str,
) -> None:
    quality_df = spark.createDataFrame(
        [(run_id, event_type, record_count, details)],
        ["run_id", "event_type", "record_count", "details"],
    ).withColumn("recorded_at", F.current_timestamp())
    quality_df.write.mode("append").parquet(quality_events_dir)


def append_stage(df: DataFrame, output_path: str, data_format: str, mode: str = "append") -> None:
    writer = (
        df.write.mode(mode)
        .option("maxRecordsPerFile", 50000)
        .format(data_format)
    )
    if data_format == "delta":
        writer = writer.option("mergeSchema", "true")
    writer.save(output_path)
