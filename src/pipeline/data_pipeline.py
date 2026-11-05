from __future__ import annotations

import shutil
import time
import uuid
from pathlib import Path

from pyspark.sql import functions as F

from src.pipeline.alerts import send_webhook_alert
from src.pipeline.config import CONFIG
from src.pipeline.metadata import (
    append_stage,
    ensure_directories,
    read_processed_files,
    write_alert_event,
    write_lineage,
    write_metrics,
    write_parser_runs,
    write_processed_files,
    write_quality_events,
    write_run_status,
)
from src.pipeline.parsers import build_parser_run_records, parse_unstructured_records
from src.pipeline.quality import EXPECTED_COLUMNS, apply_self_healing, split_valid_and_rejects, standardize_schema
from src.pipeline.spark import build_spark


def build_file_key(path: Path) -> str:
    stats = path.stat()
    return f"{path.name}:{stats.st_size}:{int(stats.st_mtime)}"


def discover_new_files(processed_files: set[str]) -> list[Path]:
    candidates: list[Path] = []
    for pattern in CONFIG.source_patterns:
        candidates.extend(CONFIG.raw_dir.glob(pattern))
    candidates = sorted({path.resolve(): path for path in candidates}.values(), key=lambda item: item.name.lower())
    return [path for path in candidates if build_file_key(path) not in processed_files]


def export_dataframe_json(df, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if df.rdd.isEmpty():
        output_file.write_text("[]\n", encoding="utf-8")
        return

    tmp_dir = output_file.parent / f"._tmp_{output_file.stem}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)

    df.coalesce(1).write.mode("overwrite").json(str(tmp_dir))

    part_files = list(tmp_dir.glob("part-*.json"))
    if not part_files:
        output_file.write_text("[]\n", encoding="utf-8")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    if output_file.exists():
        output_file.unlink()

    part_files[0].replace(output_file)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def export_healed_run(run_id: str, healed_df, rejects_df, processed_entries: list[tuple[str, str, int, str]]) -> None:
    run_dir = CONFIG.healed_exports_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    export_dataframe_json(
        healed_df.select("id", "name", "age", "run_id", "processed_time"),
        run_dir / "silver.json",
    )

    if not rejects_df.rdd.isEmpty():
        export_dataframe_json(rejects_df, run_dir / "rejects.json")

    manifest_rows = [
        {
            "file_name": file_name,
            "file_key": file_key,
            "file_size": file_size,
            "file_modified_at": file_modified_at,
            "run_id": run_id,
        }
        for file_name, file_key, file_size, file_modified_at in processed_entries
    ]
    manifest_path = run_dir / "manifest.json"
    import json

    manifest_path.write_text(json.dumps(manifest_rows, indent=2), encoding="utf-8")


def build_gold_table(silver_df):
    return silver_df.groupBy("age").agg(
        F.count("*").alias("customer_count"),
        F.countDistinct("id").alias("distinct_customer_ids"),
    )


def build_customer_segments(silver_df):
    total_customers = silver_df.select(F.countDistinct("id").alias("customer_base"))
    segmented = (
        silver_df.withColumn(
            "customer_segment",
            F.when(F.col("age") < 18, F.lit("pediatric_care"))
            .when(F.col("age") < 30, F.lit("young_adult_monitoring"))
            .when(F.col("age") < 45, F.lit("general_adult_care"))
            .when(F.col("age") < 60, F.lit("priority_chronic_followup"))
            .otherwise(F.lit("senior_risk_observation")),
        )
        .groupBy("customer_segment")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("id").alias("distinct_customers"),
            F.round(F.avg("age"), 1).alias("average_age"),
        )
        .crossJoin(total_customers)
        .withColumn(
            "share_of_customers",
            F.round((F.col("distinct_customers") / F.col("customer_base")) * F.lit(100.0), 1),
        )
        .withColumn(
            "business_story",
            F.when(
                F.col("customer_segment") == "priority_chronic_followup",
                F.lit("Requires proactive care coordination and repeat monitoring."),
            )
            .when(
                F.col("customer_segment") == "general_adult_care",
                F.lit("Represents the core inpatient and outpatient care volume."),
            )
            .when(
                F.col("customer_segment") == "young_adult_monitoring",
                F.lit("Monitoring focused on recovery, observation, and follow-up."),
            )
            .otherwise(F.lit("Needs age-specific clinical oversight and operational planning.")),
        )
        .drop("customer_base")
    )
    return segmented


def build_executive_summary(spark: SparkSession, run_id: str, processed_files: int, records_processed: int, rejected_records: int):
    acceptance_rate = round((records_processed / max(records_processed + rejected_records, 1)) * 100, 1)
    records_saved = records_processed
    headline = (
        "Stable patient intake with manageable validation pressure"
        if acceptance_rate >= 90
        else "Data quality intervention protected downstream clinical analytics"
    )
    message = (
        f"The pipeline prepared {records_saved} analytics-ready patient records across {processed_files} new files "
        f"while isolating {rejected_records} invalid records."
    )
    summary_df = spark.createDataFrame(
        [
            (
                run_id,
                headline,
                message,
                records_processed,
                rejected_records,
                acceptance_rate,
                "Clinical operations and monitoring dashboards are ready for downstream review.",
            )
        ],
        [
            "run_id",
            "headline",
            "business_message",
            "records_processed",
            "rejected_records",
            "acceptance_rate",
            "recommended_action",
        ],
    ).withColumn("processed_time", F.current_timestamp())
    return summary_df


def emit_alert(spark: SparkSession, run_id: str, alert_type: str, severity: str, message: str) -> None:
    delivery_state = send_webhook_alert(
        CONFIG.alert_webhook_url,
        {
            "run_id": run_id,
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
        },
    )
    write_alert_event(spark, str(CONFIG.alerts_dir), run_id, alert_type, severity, message, delivery_state)


def main() -> None:
    ensure_directories(
        [
            CONFIG.raw_dir,
            CONFIG.bronze_dir,
            CONFIG.silver_dir,
            CONFIG.gold_dir,
            CONFIG.gold_segments_dir,
            CONFIG.gold_summary_dir,
            CONFIG.reject_dir,
            CONFIG.healed_exports_dir,
            CONFIG.lineage_dir,
            CONFIG.metrics_dir,
            CONFIG.processed_files_dir,
            CONFIG.run_status_dir,
            CONFIG.alerts_dir,
            CONFIG.observability_dir,
            CONFIG.parser_runs_dir,
            CONFIG.quality_events_dir,
            CONFIG.checkpoint_dir,
        ]
    )

    spark = build_spark()
    run_id = f"run_{uuid.uuid4().hex[:10]}"
    start_time = time.perf_counter()
    records_processed = 0
    rejected_records = 0
    processed_entries: list[tuple[str, str, int, str]] = []
    run_status = "STARTED"

    try:
        write_run_status(spark, str(CONFIG.run_status_dir), run_id, run_status, "Pipeline run started")

        processed_files = read_processed_files(spark, str(CONFIG.processed_files_dir))
        new_files = discover_new_files(processed_files)
        if not new_files:
            runtime_seconds = time.perf_counter() - start_time
            write_metrics(spark, str(CONFIG.metrics_dir), run_id, "NO_OP", 0, 0, 0, runtime_seconds)
            write_run_status(spark, str(CONFIG.run_status_dir), run_id, "NO_OP", "No new files to process")
            emit_alert(spark, run_id, "pipeline_idle", "info", "No new files detected for the current scheduled run.")
            print("No new files to process.")
            return

        for path in new_files:
            stats = path.stat()
            processed_entries.append((path.name, build_file_key(path), stats.st_size, str(int(stats.st_mtime))))

        bronze_df = (
            spark.read.text([str(path) for path in new_files])
            .withColumnRenamed("value", "raw_payload")
            .filter(F.length(F.trim(F.col("raw_payload"))) > 0)
            .filter(~F.col("raw_payload").rlike(r"(?i)^\s*id\s*,\s*name\s*,\s*age\s*$"))
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
        )
        parsed_df = parse_unstructured_records(bronze_df)
        bronze_stage_df = parsed_df.select(
            "id",
            "name",
            "age",
            "raw_payload",
            "source_type",
            "source_extension",
            "parser_version",
            "source_file",
            "ingested_at",
            "run_id",
        )
        bronze_stage_df.cache()
        append_stage(bronze_stage_df, str(CONFIG.bronze_dir), CONFIG.data_format)
        write_lineage(spark, str(CONFIG.lineage_dir), "raw_files", "landing", "bronze", run_id)

        write_parser_runs(build_parser_run_records(parsed_df), str(CONFIG.parser_runs_dir))
        standardized_df = standardize_schema(parsed_df).withColumn("run_id", F.lit(run_id))
        valid_df, rejects_df = split_valid_and_rejects(standardized_df)
        healed_df = apply_self_healing(valid_df).withColumn("run_id", F.lit(run_id))
        healed_df.cache()
        rejects_df.cache()
        missing_name_count = standardized_df.filter(F.col("name").isNull() | (F.length(F.trim(F.col("name"))) == 0)).count()
        missing_age_count = valid_df.filter(F.col("age").isNull()).count()
        duplicate_count = valid_df.count() - healed_df.count()
        write_quality_events(
            spark,
            str(CONFIG.quality_events_dir),
            run_id,
            "schema_standardized",
            standardized_df.count(),
            f"Expected columns enforced: {', '.join(EXPECTED_COLUMNS)}",
        )
        write_quality_events(
            spark,
            str(CONFIG.quality_events_dir),
            run_id,
            "missing_values_filled",
            missing_name_count + missing_age_count,
            "Blank patient names replaced with defaults and null ages imputed using the run median during self-healing.",
        )
        append_stage(healed_df, str(CONFIG.silver_dir), CONFIG.data_format)
        write_lineage(spark, str(CONFIG.lineage_dir), "customer_records", "bronze", "silver", run_id)

        rejected_records = rejects_df.count()
        if rejected_records > 0:
            append_stage(rejects_df, str(CONFIG.reject_dir), CONFIG.data_format)
            write_quality_events(
                spark,
                str(CONFIG.quality_events_dir),
                run_id,
                "corrupted_records_isolated",
                rejected_records,
                "Records with missing ids or invalid ages were quarantined in the reject layer.",
            )
        if duplicate_count > 0:
            write_quality_events(
                spark,
                str(CONFIG.quality_events_dir),
                run_id,
                "duplicates_removed",
                duplicate_count,
                "Duplicate patient records were removed during self-healing.",
            )

        gold_df = build_gold_table(healed_df).withColumn("run_id", F.lit(run_id)).withColumn(
            "processed_time", F.current_timestamp()
        )
        append_stage(gold_df, str(CONFIG.gold_dir), CONFIG.data_format)
        customer_segments_df = build_customer_segments(healed_df).withColumn("run_id", F.lit(run_id)).withColumn(
            "processed_time", F.current_timestamp()
        )
        append_stage(customer_segments_df, str(CONFIG.gold_segments_dir), CONFIG.data_format)
        records_processed = healed_df.count()
        executive_summary_df = build_executive_summary(
            spark,
            run_id,
            len(new_files),
            records_processed=records_processed,
            rejected_records=rejected_records,
        )
        append_stage(executive_summary_df, str(CONFIG.gold_summary_dir), CONFIG.data_format)
        write_lineage(spark, str(CONFIG.lineage_dir), "customer_segment_story", "silver", "gold", run_id)
        write_lineage(spark, str(CONFIG.lineage_dir), "executive_summary", "silver", "gold", run_id)
        write_lineage(spark, str(CONFIG.lineage_dir), "customer_analytics", "silver", "gold", run_id)
        write_processed_files(spark, str(CONFIG.processed_files_dir), processed_entries, run_id)

        try:
            export_healed_run(run_id, silver_df, rejects_df, processed_entries)
        except Exception as export_exc:
            # Exports are for UI convenience; do not fail the pipeline run if export fails.
            write_alert_event(spark, str(CONFIG.alerts_dir), run_id, "WARN", "healed_export_failed", str(export_exc))

        reject_rate = rejected_records / max(records_processed + rejected_records, 1)
        if reject_rate >= float(CONFIG.alert_reject_rate_threshold):
            emit_alert(
                spark,
                run_id,
                "high_reject_rate",
                "warning",
                f"Reject rate reached {reject_rate:.1%}, exceeding threshold {float(CONFIG.alert_reject_rate_threshold):.0%}.",
            )

        runtime_seconds = time.perf_counter() - start_time
        run_status = "SUCCESS"
        write_metrics(
            spark,
            str(CONFIG.metrics_dir),
            run_id,
            run_status,
            records_processed,
            rejected_records,
            len(new_files),
            runtime_seconds,
        )
        write_run_status(spark, str(CONFIG.run_status_dir), run_id, run_status, "Pipeline completed successfully")
        emit_alert(
            spark,
            run_id,
            "pipeline_success",
            "info",
            f"Pipeline run completed successfully with {records_processed} valid rows and {rejected_records} rejects.",
        )
        print(
            f"Completed {run_id} in {runtime_seconds:.2f}s with "
            f"{records_processed} valid rows and {rejected_records} rejects."
        )
    except Exception as exc:
        runtime_seconds = time.perf_counter() - start_time
        write_metrics(
            spark,
            str(CONFIG.metrics_dir),
            run_id,
            "FAILED",
            records_processed,
            rejected_records,
            len(processed_entries),
            runtime_seconds,
        )
        write_run_status(spark, str(CONFIG.run_status_dir), run_id, "FAILED", str(exc))
        emit_alert(
            spark,
            run_id,
            "pipeline_failure",
            "critical",
            f"Pipeline run failed: {exc}",
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
