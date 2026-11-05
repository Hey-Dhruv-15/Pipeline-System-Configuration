from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from src.pipeline.config import CONFIG


def _read_json_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_json(path)
    except ValueError:
        return pd.DataFrame()


def _collect_recent_parquet_files(path: Path, limit: int = 25) -> list[Path]:
    if not path.exists():
        return []

    candidates: list[Path] = []

    def onerror(_: OSError) -> None:
        return

    for root, dirs, files in os.walk(path, topdown=True, onerror=onerror):
        # Skip Spark/Delta internal directories and hidden folders to avoid transient/racy paths.
        dirs[:] = [d for d in dirs if d not in {"_temporary", "_delta_log"} and not d.startswith(".")]
        for name in files:
            if not name.endswith(".parquet"):
                continue
            candidates.append(Path(root) / name)

    def mtime(item: Path) -> float:
        try:
            return item.stat().st_mtime
        except (FileNotFoundError, OSError):
            return -1.0

    candidates.sort(key=mtime, reverse=True)
    return candidates[:limit]


def read_table(path: Path, demo_file: str | None = None, use_demo_seed: bool = False) -> pd.DataFrame:
    try:
        # The pipeline often writes Delta-style directories (with _delta_log) and Spark temp folders
        # (with _temporary). A naive rglob() can be slow and can race with writers; prefer a small,
        # recent subset of non-internal parquet files for UI snapshots.
        parquet_files = _collect_recent_parquet_files(path, limit=25)
        if parquet_files:
            return pd.read_parquet(parquet_files)
    except Exception:
        pass

    if use_demo_seed and demo_file:
        return _read_json_frame(CONFIG.demo_data_dir / demo_file)
    return pd.DataFrame()


def load_pipeline_snapshot(use_demo_seed: bool = False) -> dict[str, pd.DataFrame]:
    return {
        "metrics": read_table(CONFIG.metrics_dir, "metrics.json", use_demo_seed),
        "lineage": read_table(CONFIG.lineage_dir, "lineage.json", use_demo_seed),
        "processed_files": read_table(CONFIG.processed_files_dir, "processed_files.json", use_demo_seed),
        "run_status": read_table(CONFIG.run_status_dir, "run_status.json", use_demo_seed),
        "alerts": read_table(CONFIG.alerts_dir, "alerts.json", use_demo_seed),
        "parser_runs": read_table(CONFIG.parser_runs_dir, None, use_demo_seed),
        "quality_events": read_table(CONFIG.quality_events_dir, None, use_demo_seed),
        "rejects": read_table(CONFIG.reject_dir, "rejects.json", use_demo_seed),
        "gold_age": read_table(CONFIG.gold_dir, "gold_age_distribution.json", use_demo_seed),
        "gold_segments": read_table(CONFIG.gold_segments_dir, "gold_customer_segments.json", use_demo_seed),
        "gold_summary": read_table(CONFIG.gold_summary_dir, "gold_executive_summary.json", use_demo_seed),
    }
