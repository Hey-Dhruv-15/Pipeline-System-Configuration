from __future__ import annotations

import math
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.pipeline.config import CONFIG
from src.pipeline.repository import load_pipeline_snapshot


APP_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIST = APP_ROOT / "frontend-dist"
DAG_FILE = APP_ROOT / "dags" / "lakehouse_pipeline_dag.py"

ESSENTIAL_DATASET_ORDER = [
    "metrics",
    "processed_files",
    "run_status",
    "alerts",
    "parser_runs",
    "quality_events",
    "rejects",
    "gold_summary",
    "gold_age",
    "gold_segments",
]


def titleize(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def build_file_key(path: Path) -> str:
    stats = path.stat()
    return f"{path.name}:{stats.st_size}:{int(stats.st_mtime)}"


def count_parquet_rows(path: Path) -> int:
    try:
        parquet_files = list(path.rglob("*.parquet"))
    except (FileNotFoundError, OSError):
        # Spark (and similar writers) can create/delete temp directories while we scan.
        # Treat transient scan issues as "no rows" rather than crashing /api/snapshot.
        return 0
    if not parquet_files:
        return 0
    try:
        frame = pd.read_parquet(parquet_files)
        return int(len(frame))
    except Exception:
        return 0


def has_any_parquet(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        for root, dirs, files in os.walk(path):
            dirs[:] = [d for d in dirs if d not in {"_temporary", "_delta_log"} and not d.startswith(".")]
            for name in files:
                if name.endswith(".parquet"):
                    return True
    except (FileNotFoundError, OSError):
        return False
    return False


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    updated = frame.copy()
    for column in updated.columns:
        lowered = column.lower()
        if any(token in lowered for token in ["_time", "_date", "recorded_at", "processed_at", "created_at", "updated_at", "timestamp"]):
            updated[column] = pd.to_datetime(updated[column], errors="coerce")
    return updated


def infer_time_column(frame: pd.DataFrame) -> str | None:
    for column in frame.columns:
        if pd.api.types.is_datetime64_any_dtype(frame[column]):
            return column
    for column in frame.columns:
        lowered = column.lower()
        if any(token in lowered for token in ["time", "date", "recorded", "processed", "created", "updated", "timestamp"]):
            return column
    return None


def serialize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        timestamp = value.tz_localize("UTC") if value.tzinfo is None else value.tz_convert("UTC")
        return timestamp.isoformat().replace("+00:00", "Z")
    if isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return round(value, 2)
    if isinstance(value, (int, str, bool)):
        return value
    return str(value)


def compute_quality_score(metrics_df: pd.DataFrame) -> dict[str, float]:
    if metrics_df.empty:
        return {"score": 100.0, "reliability": 100.0, "acceptance": 100.0, "throughput": 100.0}

    total_runs = max(len(metrics_df), 1)
    success_rate = (metrics_df["run_status"].eq("SUCCESS").sum() / total_runs) * 100 if "run_status" in metrics_df else 100.0
    processed = float(metrics_df["records_processed"].sum()) if "records_processed" in metrics_df else 0.0
    rejected = float(metrics_df["rejected_records"].sum()) if "rejected_records" in metrics_df else 0.0
    acceptance = (processed / max(processed + rejected, 1.0)) * 100.0
    avg_runtime = float(metrics_df["runtime_seconds"].mean()) if "runtime_seconds" in metrics_df else 0.0
    throughput = min(100.0, max(35.0, 100.0 - (avg_runtime * 2.1)))
    score = round((success_rate * 0.45) + (acceptance * 0.4) + (throughput * 0.15), 1)
    return {
        "score": score,
        "reliability": round(success_rate, 1),
        "acceptance": round(acceptance, 1),
        "throughput": round(throughput, 1),
    }


def list_backend_files() -> list[Path]:
    candidates: list[Path] = []
    for pattern in CONFIG.source_patterns:
        candidates.extend(CONFIG.raw_dir.glob(pattern))
    return sorted({path.resolve(): path for path in candidates}.values(), key=lambda item: item.name.lower())


def pending_backend_files(processed_df: pd.DataFrame) -> int:
    backend_files = list_backend_files()
    if processed_df.empty or "file_key" not in processed_df.columns:
        return len(backend_files)
    processed_keys = set(processed_df["file_key"].astype(str).tolist())
    return sum(1 for path in backend_files if build_file_key(path) not in processed_keys)


def file_type_distribution() -> dict[str, int]:
    distribution: dict[str, int] = {}
    for path in list_backend_files():
        extension = path.suffix.lower() or "unknown"
        distribution[extension] = distribution.get(extension, 0) + 1
    return dict(sorted(distribution.items(), key=lambda item: item[0]))


def latest_run_status(run_status_df: pd.DataFrame) -> tuple[str, str]:
    if run_status_df.empty or "status" not in run_status_df.columns:
        return "N/A", "N/A"
    time_column = infer_time_column(run_status_df)
    ordered = run_status_df.sort_values(time_column) if time_column else run_status_df
    latest = ordered.iloc[-1]
    return str(latest.get("run_id", "N/A")), str(latest.get("status", "N/A"))


def load_dag_metadata() -> dict[str, str]:
    metadata = {
        "event_dag_id": "N/A",
        "scheduled_dag_id": "N/A",
        "event_schedule": "N/A",
        "sensor_task": "N/A",
        "job_task": "run_spark_pipeline",
    }
    if not DAG_FILE.exists():
        return metadata
    text = DAG_FILE.read_text(encoding="utf-8")
    patterns = {
        "scheduled_dag_id": r'dag_id="([^"]+)"[\s\S]*?description="Scheduled daily',
        "event_dag_id": r'dag_id="([^"]+)"[\s\S]*?description="Event-driven',
        "event_schedule": r'description="Event-driven[\s\S]*?schedule="([^"]+)"',
        "sensor_task": r'task_id="([^"]+)"[\s\S]*?python_callable=raw_files_available',
        "job_task": r'def build_pipeline_task\(task_id: str\)[\s\S]*?build_pipeline_task\("([^"]+)"\)',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.DOTALL)
        if match:
            metadata[key] = match.group(1)
    return metadata


def build_event_feed(snapshot: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    for path in list_backend_files():
        events.append(
            {
                "time": None,
                "source": "raw",
                "message": f"Observed backend file {path.name}",
            }
        )

    dataset_specs = [
        ("processed_files", lambda row: f"Processed {row.get('file_name', 'file')}"),
        ("run_status", lambda row: f"{row.get('status', 'status')} {row.get('run_id', '')}".strip()),
        ("alerts", lambda row: f"{row.get('severity', 'alert')} {row.get('message', '')}".strip()),
        ("quality_events", lambda row: f"{row.get('event_type', 'quality')} {row.get('record_count', '')}".strip()),
        ("parser_runs", lambda row: f"Parser handled {row.get('records_seen', '')} records from {row.get('source_extension', '')}".strip()),
    ]

    for dataset_name, formatter in dataset_specs:
        frame = snapshot.get(dataset_name, pd.DataFrame())
        if frame.empty:
            continue
        time_column = infer_time_column(frame)
        ordered = frame.sort_values(time_column, ascending=False) if time_column else frame
        for _, row in ordered.head(8).iterrows():
            events.append(
                {
                    "time": serialize_value(row.get(time_column)) if time_column else None,
                    "source": titleize(dataset_name),
                    "message": formatter(row),
                }
            )

    sortable = pd.to_datetime(pd.Series([event["time"] for event in events]), errors="coerce", utc=True)
    indexed_events = list(enumerate(events))

    min_value = pd.Timestamp.min.value

    def sort_key_for(ts: Any) -> int:
        if pd.isna(ts):
            return min_value
        if isinstance(ts, pd.Timestamp):
            return ts.value
        coerced = pd.to_datetime(ts, errors="coerce", utc=True)
        return coerced.value if not pd.isna(coerced) else min_value

    indexed_events.sort(key=lambda item: sort_key_for(sortable.iloc[item[0]]), reverse=True)
    return [event for _, event in indexed_events[:24]]


def build_activity_feed(snapshot: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    activity = build_event_feed(snapshot)
    return activity[:12]


def dataset_preview(frame: pd.DataFrame, limit: int = 8) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    preview = frame.head(limit).copy()
    for column in preview.columns:
        if pd.api.types.is_datetime64_any_dtype(preview[column]):
            preview[column] = preview[column].apply(serialize_value)
    return [
        {column: serialize_value(value) for column, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]


def dataset_summary(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    time_column = infer_time_column(frame)
    latest = None
    if time_column and not frame.empty:
        latest_series = frame[time_column].dropna()
        if not latest_series.empty:
            latest = serialize_value(latest_series.max())

    metrics: dict[str, Any] = {
        "rows": int(len(frame)),
        "columns": int(len(frame.columns)),
    }

    for column in frame.columns:
        if column in {"run_id", "message", "details", "raw_payload"}:
            continue
        series = frame[column].dropna()
        if series.empty:
            continue
        if pd.api.types.is_numeric_dtype(series):
            metrics[titleize(column)] = serialize_value(series.sum())
            break
        if series.nunique() < len(series) and series.nunique() > 0:
            metrics[titleize(column)] = int(series.nunique())
            break

    preview_frame = frame
    if time_column and not frame.empty:
        try:
            preview_frame = frame.sort_values(time_column, ascending=False)
        except Exception:
            preview_frame = frame

    return {
        "name": name,
        "title": titleize(name),
        "latest": latest,
        "metrics": metrics,
        "columns": frame.columns.tolist(),
        "preview": dataset_preview(preview_frame),
    }


def bronze_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    processed_df = snapshot["processed_files"]
    parser_runs_df = snapshot["parser_runs"]
    latest_sources: list[dict[str, Any]] = []
    if not processed_df.empty:
        latest_sources = [
            {
                "file_name": row.get("file_name"),
                "source_type": Path(str(row.get("file_name", ""))).suffix.lstrip(".") or "unknown",
            }
            for row in processed_df.tail(8).to_dict(orient="records")
        ]
    return {
        "files_ingested": int(processed_df["file_name"].nunique()) if "file_name" in processed_df.columns and not processed_df.empty else 0,
        "raw_records_stored": int(parser_runs_df["records_seen"].sum()) if "records_seen" in parser_runs_df.columns and not parser_runs_df.empty else 0,
        "table": latest_sources,
    }


def self_healing_summary(snapshot: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    quality_df = snapshot["quality_events"]
    event_labels = {
        "missing_values_filled": "Missing values filled",
        "duplicates_removed": "Duplicates removed",
        "schema_standardized": "Schema standardized",
        "corrupted_records_isolated": "Corrupted records isolated",
    }
    totals = {key: 0 for key in event_labels}
    if not quality_df.empty and "event_type" in quality_df.columns and "record_count" in quality_df.columns:
        grouped = quality_df.groupby("event_type")["record_count"].sum().to_dict()
        for key in totals:
            totals[key] = int(grouped.get(key, 0))
    return [{"key": key, "label": label, "count": totals[key]} for key, label in event_labels.items()]


def silver_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    metrics_df = snapshot["metrics"]
    total_processed = int(metrics_df["records_processed"].sum()) if "records_processed" in metrics_df.columns and not metrics_df.empty else 0
    total_rejected = int(metrics_df["rejected_records"].sum()) if "rejected_records" in metrics_df.columns and not metrics_df.empty else 0
    total = max(total_processed + total_rejected, 1)
    return {
        "clean_records": total_processed,
        "acceptance_rate": round((total_processed / total) * 100, 1),
        "reject_rate": round((total_rejected / total) * 100, 1),
        "preview": dataset_preview(snapshot["gold_age"], limit=5),
    }


def reject_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    rejects_df = snapshot["rejects"]
    return {
        "count": int(len(rejects_df)),
        "preview": dataset_preview(rejects_df, limit=6),
    }


def gold_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {
        "age_distribution": dataset_preview(snapshot["gold_age"], limit=8),
        "segments": dataset_preview(snapshot["gold_segments"], limit=8),
    }


def metadata_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    parser_runs_df = snapshot["parser_runs"]
    distribution = {}
    if not parser_runs_df.empty and "source_extension" in parser_runs_df.columns and "records_seen" in parser_runs_df.columns:
        grouped = parser_runs_df.groupby("source_extension")["records_seen"].sum().to_dict()
        distribution = {str(key): int(value) for key, value in grouped.items()}
    return {
        "parser_activity": distribution,
        "run_status": dataset_preview(snapshot["run_status"], limit=6),
        "processed_files": dataset_preview(snapshot["processed_files"], limit=6),
    }


def airflow_summary(snapshot: dict[str, pd.DataFrame]) -> dict[str, Any]:
    metadata = load_dag_metadata()
    run_status_df = snapshot["run_status"]
    latest_status = latest_run_status(run_status_df)[1]
    trigger_reason = "new files detected" if pending_backend_files(snapshot["processed_files"]) > 0 else "awaiting new files"
    return {
        "dag": metadata["event_dag_id"],
        "schedule": metadata["event_schedule"],
        "trigger_condition": "new raw files detected",
        "last_run": latest_status,
        "trigger_reason": trigger_reason,
        "next_check": "within 1 minute",
    }


def business_summary(snapshot: dict[str, pd.DataFrame]) -> str:
    metrics_df = snapshot["metrics"]
    processed_df = snapshot["processed_files"]
    records_processed = int(metrics_df["records_processed"].sum()) if "records_processed" in metrics_df.columns and not metrics_df.empty else 0
    rejected_records = int(metrics_df["rejected_records"].sum()) if "rejected_records" in metrics_df.columns and not metrics_df.empty else 0
    files = int(processed_df["file_name"].nunique()) if "file_name" in processed_df.columns and not processed_df.empty else 0
    return (
        f"The system processed {files} hospital files, produced {records_processed} analytics-ready records, "
        f"and quarantined {rejected_records} invalid records for downstream safety."
    )


def stage_status(snapshot: dict[str, pd.DataFrame]) -> list[dict[str, str]]:
    statuses = []
    pending = pending_backend_files(snapshot["processed_files"])
    latest_status = latest_run_status(snapshot["run_status"])[1]
    statuses.append({"stage": "Backend Simulator", "status": "running" if list_backend_files() else "idle"})
    statuses.append({"stage": "Raw Zone", "status": "processing" if pending else "running"})
    statuses.append({"stage": "Airflow Event DAG", "status": "error" if latest_status == "FAILED" else "running"})
    statuses.append({"stage": "Spark Processing", "status": "processing" if latest_status == "STARTED" else ("error" if latest_status == "FAILED" else "running")})
    statuses.append({"stage": "Bronze", "status": "running" if has_any_parquet(CONFIG.bronze_dir) else "idle"})
    statuses.append({"stage": "Silver", "status": "running" if has_any_parquet(CONFIG.silver_dir) else "idle"})
    statuses.append({"stage": "Gold", "status": "running" if has_any_parquet(CONFIG.gold_dir) else "idle"})
    statuses.append({"stage": "Metadata", "status": "running" if has_any_parquet(CONFIG.metadata_dir) else "idle"})
    return statuses


def metrics_series(metrics_df: pd.DataFrame) -> list[dict[str, Any]]:
    if metrics_df.empty:
        return []
    time_column = infer_time_column(metrics_df)
    ordered = metrics_df.sort_values(time_column) if time_column else metrics_df
    series: list[dict[str, Any]] = []
    for _, row in ordered.tail(20).iterrows():
        series.append(
            {
                "time": serialize_value(row.get(time_column)) if time_column else None,
                "records_processed": serialize_value(row.get("records_processed")),
                "rejected_records": serialize_value(row.get("rejected_records")),
                "runtime_seconds": serialize_value(row.get("runtime_seconds")),
            }
        )
    return series


def build_snapshot_payload(use_demo_seed: bool = False) -> dict[str, Any]:
    snapshot = load_pipeline_snapshot(use_demo_seed=use_demo_seed)
    snapshot = {name: normalize_frame(frame) for name, frame in snapshot.items()}
    active = {name: frame for name, frame in snapshot.items() if not frame.empty and name in ESSENTIAL_DATASET_ORDER}

    metrics_df = snapshot["metrics"]
    processed_df = snapshot["processed_files"]
    run_status_df = snapshot["run_status"]
    alerts_df = snapshot["alerts"]

    run_id, status = latest_run_status(run_status_df)
    quality = compute_quality_score(metrics_df)

    return {
        "app_name": CONFIG.app_name,
        "mode": "live" if has_any_parquet(CONFIG.metrics_dir) else "demo",
        "data_root": str(Path(os.getenv("PIPELINE_DATA_ROOT", CONFIG.data_root))),
        "source_patterns": list(CONFIG.source_patterns),
        "overview": {
            "run_id": run_id,
            "status": status,
            "processed_files": int(processed_df["file_name"].nunique()) if not processed_df.empty and "file_name" in processed_df.columns else 0,
            "alerts": int(len(alerts_df)),
            "quality_score": quality["score"],
            "pending_files": pending_backend_files(processed_df),
        },
        "quality": quality,
        "health_gauge": quality["score"],
        "raw_monitor": {
            "incoming_files": [path.name for path in list_backend_files()[-12:]][::-1],
            "new_files_detected": pending_backend_files(processed_df),
            "pending_files": pending_backend_files(processed_df),
            "file_types": file_type_distribution(),
        },
        "airflow": airflow_summary(snapshot),
        "pipeline_flow": stage_status(snapshot),
        "bronze": bronze_summary(snapshot),
        "self_healing": self_healing_summary(snapshot),
        "silver": silver_summary(snapshot),
        "reject_quarantine": reject_summary(snapshot),
        "gold": gold_summary(snapshot),
        "metadata_monitoring": metadata_summary(snapshot),
        "business_summary": business_summary(snapshot),
        "metric_series": metrics_series(metrics_df),
        "events": build_event_feed(snapshot),
        "activity_feed": build_activity_feed(snapshot),
        "alerts": dataset_preview(alerts_df.sort_values(infer_time_column(alerts_df), ascending=False) if not alerts_df.empty and infer_time_column(alerts_df) else alerts_df, limit=8),
        "datasets": [dataset_summary(name, frame) for name, frame in active.items()],
    }


def _find_run_id_from_exports(file_name: str, *, max_runs: int = 50) -> str | None:
    exports_root = Path(CONFIG.healed_exports_dir)
    if not exports_root.exists():
        return None

    run_dirs = [p for p in exports_root.iterdir() if p.is_dir()]

    def mtime(item: Path) -> float:
        try:
            return item.stat().st_mtime
        except OSError:
            return -1.0

    run_dirs.sort(key=mtime, reverse=True)
    for run_dir in run_dirs[: max(int(max_runs), 1)]:
        manifest = run_dir / "manifest.json"
        if not manifest.exists():
            continue
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        for row in payload:
            if isinstance(row, dict) and str(row.get("file_name", "")) == file_name:
                return str(row.get("run_id") or run_dir.name)
    return None


def _data_root() -> Path:
    return Path(os.getenv("PIPELINE_DATA_ROOT", CONFIG.data_root)).resolve()


def _resolve_under_data_root(relative_path: str) -> Path:
    root = _data_root()
    candidate = (root / relative_path).resolve()
    if candidate == root:
        return candidate
    if root not in candidate.parents:
        raise HTTPException(status_code=400, detail="Invalid path (must be under PIPELINE_DATA_ROOT).")
    return candidate


def _collect_recent_files(path: Path, *, suffix: str, limit: int = 50) -> list[Path]:
    if not path.exists():
        return []
    candidates: list[Path] = []

    def onerror(_: OSError) -> None:
        return

    for root, dirs, files in os.walk(path, topdown=True, onerror=onerror):
        dirs[:] = [d for d in dirs if d not in {"_temporary", "_delta_log"} and not d.startswith(".")]
        for name in files:
            if not name.endswith(suffix):
                continue
            candidates.append(Path(root) / name)

    def mtime(item: Path) -> float:
        try:
            return item.stat().st_mtime
        except (FileNotFoundError, OSError):
            return -1.0

    candidates.sort(key=mtime, reverse=True)
    return candidates[:limit]


def _frame_to_records(frame: pd.DataFrame, *, offset: int = 0, limit: int = 1000) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe_offset = max(int(offset), 0)
    safe_limit = max(min(int(limit), 5000), 0)
    sliced = frame.iloc[safe_offset : safe_offset + safe_limit].copy()
    return [{col: serialize_value(val) for col, val in row.items()} for row in sliced.to_dict(orient="records")]


def _load_table_from_path(path: Path, *, columns: list[str] | None = None, max_files: int = 8) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.is_file() and path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path, columns=columns)
        except Exception:
            return pd.DataFrame()
    if path.is_file() and path.suffix.lower() == ".json":
        try:
            return pd.read_json(path)
        except ValueError:
            return pd.DataFrame()
    if path.is_dir():
        parquet_files = _collect_recent_files(path, suffix=".parquet", limit=int(max_files))
        if parquet_files:
            try:
                return pd.read_parquet(parquet_files, columns=columns)
            except Exception:
                return pd.DataFrame()
    return pd.DataFrame()


def _load_healed_export_records(run_id: str, name: str, limit: int) -> list[dict[str, Any]] | None:
    base = Path(CONFIG.healed_exports_dir) / run_id / f"{name}.json"
    if not base.exists():
        return None
    try:
        payload = json.loads(base.read_text(encoding="utf-8"))
    except Exception:
        return None

    records: list[dict[str, Any]]
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
        records = payload["records"]
    else:
        records = []

    safe_limit = max(min(int(limit), 2000), 0)
    sliced = records[:safe_limit]
    # normalize keys/values
    return [{str(k): v for k, v in row.items()} for row in sliced if isinstance(row, dict)]


app = FastAPI(title="Lakehouse Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/snapshot")
def snapshot(use_demo_seed: bool = False) -> dict[str, Any]:
    return build_snapshot_payload(use_demo_seed=use_demo_seed)

@app.get("/api/file-trace")
def file_trace(
    file_name: str = Query(..., description="Raw source filename (basename)"),
    limit: int = Query(200, ge=1, le=2000),
    include_bronze: bool = Query(False, description="Include bronze_records (slower)."),
) -> dict[str, Any]:
    if "/" in file_name or "\\" in file_name:
        raise HTTPException(status_code=400, detail="file_name must be a base filename.")

    raw_path = CONFIG.raw_dir / file_name
    raw_text: str | None = None
    if raw_path.exists() and raw_path.is_file():
        try:
            raw_text = raw_path.read_text(encoding="utf-8", errors="replace")[:20000]
        except Exception:
            raw_text = None

    snapshot = load_pipeline_snapshot(use_demo_seed=False)
    processed_df = normalize_frame(snapshot.get("processed_files", pd.DataFrame()))

    run_id: str | None = None
    processed_meta: dict[str, Any] | None = None

    if not processed_df.empty and "file_name" in processed_df.columns:
        matches = processed_df[processed_df["file_name"].astype(str) == file_name]
        if not matches.empty:
            time_column = infer_time_column(matches)
            ordered = matches.sort_values(time_column, ascending=False) if time_column else matches
            row = ordered.iloc[0].to_dict()
            processed_meta = {key: serialize_value(value) for key, value in row.items()}
            value = row.get("run_id")
            run_id = str(value) if value is not None else None

    if run_id is None:
        run_id = _find_run_id_from_exports(file_name)

    if run_id is None:
        raise HTTPException(status_code=404, detail="File has not been processed yet.")

    bronze_source = "skipped"
    bronze_records: list[dict[str, Any]] = []
    if include_bronze:
        bronze_source = "parquet_scan"
        bronze_frame = normalize_frame(_load_table_from_path(CONFIG.bronze_dir, columns=["id", "name", "age", "raw_payload", "source_type", "source_extension", "parser_version", "source_file", "ingested_at", "run_id"], max_files=12))
        if not bronze_frame.empty and "source_file" in bronze_frame.columns:
            mask = bronze_frame["source_file"].astype(str).str.contains(re.escape(file_name), na=False)
            bronze_records = _frame_to_records(bronze_frame[mask], offset=0, limit=limit)

    silver_source = "healed_exports"
    silver_records = _load_healed_export_records(run_id, "silver", limit)
    if silver_records is None:
        silver_source = "parquet_scan"
        silver_frame = normalize_frame(_load_table_from_path(CONFIG.silver_dir, columns=["id", "name", "age", "run_id", "processed_time"], max_files=12))
        silver_records = []
        if not silver_frame.empty and "run_id" in silver_frame.columns:
            mask = silver_frame["run_id"].astype(str) == run_id
            silver_records = _frame_to_records(silver_frame[mask], offset=0, limit=limit)

    rejects_source = "healed_exports"
    reject_records = _load_healed_export_records(run_id, "rejects", limit)
    if reject_records is None:
        rejects_source = "parquet_scan"
        rejects_frame = normalize_frame(_load_table_from_path(CONFIG.reject_dir, columns=["id", "name", "age", "run_id", "reject_reason"], max_files=12))
        reject_records = []
        if not rejects_frame.empty and "run_id" in rejects_frame.columns:
            mask = rejects_frame["run_id"].astype(str) == run_id
            reject_records = _frame_to_records(rejects_frame[mask], offset=0, limit=limit)

    return {
        "file_name": file_name,
        "run_id": run_id,
        "processed_file": processed_meta,
        "raw_text": raw_text,
        "bronze_source": bronze_source,
        "bronze_records": bronze_records,
        "silver_source": silver_source,
        "rejects_source": rejects_source,
        "silver_records": silver_records,
        "reject_records": reject_records,
    }



@app.get("/api/datasets")
def datasets(use_demo_seed: bool = False) -> dict[str, Any]:
    snapshot = load_pipeline_snapshot(use_demo_seed=use_demo_seed)
    available = [{"name": name, "rows": int(len(frame)), "columns": int(len(frame.columns))} for name, frame in snapshot.items()]
    available.sort(key=lambda item: item["name"])
    return {"mode": "demo" if use_demo_seed else "live", "datasets": available}


@app.get("/api/datasets/{dataset_name}")
def dataset_rows(
    dataset_name: str,
    use_demo_seed: bool = False,
    offset: int = Query(0, ge=0),
    limit: int = Query(500, ge=0, le=5000),
    latest_first: bool = True,
    download: bool = False,
) -> JSONResponse:
    snapshot = load_pipeline_snapshot(use_demo_seed=use_demo_seed)
    if dataset_name not in snapshot:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_name}")

    frame = normalize_frame(snapshot[dataset_name])
    time_column = infer_time_column(frame)
    if latest_first and time_column and not frame.empty:
        try:
            frame = frame.sort_values(time_column, ascending=False)
        except Exception:
            pass

    payload = {
        "name": dataset_name,
        "columns": frame.columns.tolist(),
        "records": _frame_to_records(frame, offset=offset, limit=limit),
    }
    headers = {}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="{dataset_name}.json"'
    return JSONResponse(payload, headers=headers)


@app.get("/api/preprocessed")
def preprocessed_index() -> dict[str, Any]:
    root = _data_root()
    layers = {
        "raw": CONFIG.raw_dir,
        "bronze": CONFIG.bronze_dir,
        "silver": CONFIG.silver_dir,
        "gold": CONFIG.gold_dir,
        "reject": CONFIG.reject_dir,
        "metadata": CONFIG.metadata_dir,
    }
    items: list[dict[str, Any]] = []
    for name, path in layers.items():
        resolved = Path(path).resolve()
        recent = _collect_recent_files(resolved, suffix=".parquet", limit=25)
        items.append(
            {
                "layer": name,
                "path": str(resolved),
                "exists": resolved.exists(),
                "recent_parquet": [str(p.relative_to(root)) for p in recent if root in p.parents],
            }
        )
    return {"data_root": str(root), "layers": items}


@app.get("/api/preprocessed/file")
def preprocessed_file_as_json(
    path: str = Query(..., description="Path relative to PIPELINE_DATA_ROOT (data root)."),
    offset: int = Query(0, ge=0),
    limit: int = Query(500, ge=0, le=5000),
    download: bool = False,
) -> JSONResponse:
    target = _resolve_under_data_root(path)
    frame = normalize_frame(_load_table_from_path(target))
    if frame.empty:
        raise HTTPException(status_code=404, detail="No readable tabular data found at that path.")

    payload = {
        "path": str(target),
        "relative_path": path,
        "columns": frame.columns.tolist(),
        "records": _frame_to_records(frame, offset=offset, limit=limit),
    }
    headers = {}
    if download:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(path).name or "preprocessed")
        headers["Content-Disposition"] = f'attachment; filename="{safe_name}.json"'
    return JSONResponse(payload, headers=headers)


if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")


@app.get("/")
def root() -> FileResponse:
    return FileResponse(FRONTEND_DIST / "index.html")


@app.get("/{full_path:path}")
def spa_fallback(full_path: str) -> FileResponse:
    if full_path.startswith("api/"):
        return FileResponse(FRONTEND_DIST / "index.html")
    return FileResponse(FRONTEND_DIST / "index.html")
