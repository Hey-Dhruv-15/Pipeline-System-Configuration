import os
from dataclasses import dataclass
from pathlib import Path


def _parse_source_patterns() -> tuple[str, ...]:
    raw_value = os.getenv("PIPELINE_SOURCE_PATTERNS", "*.csv,*.json,*.log,*.txt")
    patterns = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    return patterns or ("*.csv",)


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path = Path(__file__).resolve().parents[2]
    data_root: Path = Path(os.getenv("PIPELINE_DATA_ROOT", project_root / "data"))
    demo_data_dir: Path = project_root / "demo_data"
    raw_dir: Path = data_root / os.getenv("PIPELINE_RAW_DIR", "raw")
    bronze_dir: Path = data_root / os.getenv("PIPELINE_BRONZE_DIR", "bronze")
    silver_dir: Path = data_root / os.getenv("PIPELINE_SILVER_DIR", "silver")
    gold_dir: Path = data_root / os.getenv("PIPELINE_GOLD_DIR", "gold")
    gold_segments_dir: Path = gold_dir / "customer_segments"
    gold_summary_dir: Path = gold_dir / "executive_summary"
    reject_dir: Path = data_root / os.getenv("PIPELINE_REJECT_DIR", "reject")
    metadata_dir: Path = data_root / os.getenv("PIPELINE_METADATA_DIR", "metadata")
    healed_exports_dir: Path = data_root / os.getenv("PIPELINE_HEALED_EXPORTS_DIR", "healed_exports")
    observability_dir: Path = metadata_dir / "observability"
    lineage_dir: Path = metadata_dir / "lineage"
    metrics_dir: Path = metadata_dir / "metrics"
    processed_files_dir: Path = metadata_dir / "processed_files"
    run_status_dir: Path = metadata_dir / "run_status"
    alerts_dir: Path = metadata_dir / "alerts"
    parser_runs_dir: Path = observability_dir / "parser_runs"
    quality_events_dir: Path = observability_dir / "quality_events"
    data_format: str = os.getenv("PIPELINE_DATA_FORMAT", "parquet")
    app_name: str = os.getenv("PIPELINE_APP_NAME", "self_healing_lakehouse_pipeline")
    source_pattern: str = os.getenv("PIPELINE_SOURCE_PATTERN", "*.csv")
    source_patterns: tuple[str, ...] = _parse_source_patterns()
    checkpoint_dir: Path = metadata_dir / "checkpoints"
    shuffle_partitions: str = os.getenv("PIPELINE_SHUFFLE_PARTITIONS", "4")
    max_records_per_file: str = os.getenv("PIPELINE_MAX_RECORDS_PER_FILE", "50000")
    alert_webhook_url: str = os.getenv("PIPELINE_ALERT_WEBHOOK_URL", "")
    alert_reject_rate_threshold: str = os.getenv("PIPELINE_ALERT_REJECT_RATE_THRESHOLD", "0.15")
    backend_generation_interval_seconds: int = int(os.getenv("PIPELINE_BACKEND_GENERATION_INTERVAL_SECONDS", "20"))
    backend_batch_size: int = int(os.getenv("PIPELINE_BACKEND_BATCH_SIZE", "3"))


CONFIG = PipelineConfig()
