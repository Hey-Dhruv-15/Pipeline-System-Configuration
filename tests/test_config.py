import os
from pathlib import Path
from src.pipeline.config import PipelineConfig, _parse_source_patterns


def test_default_config_initialization():
    config = PipelineConfig()
    assert config.project_root.exists()
    assert config.data_format in ("parquet", "delta")
    assert config.app_name == "self_healing_lakehouse_pipeline"
    assert config.shuffle_partitions == "4"
    assert config.backend_batch_size == 3
    assert config.backend_generation_interval_seconds == 20


def test_parse_source_patterns_default():
    patterns = _parse_source_patterns()
    assert "*.csv" in patterns
    assert "*.json" in patterns


def test_parse_source_patterns_custom(monkeypatch):
    monkeypatch.setenv("PIPELINE_SOURCE_PATTERNS", "*.log, *.txt, *.tsv")
    patterns = _parse_source_patterns()
    assert patterns == ("*.log", "*.txt", "*.tsv")


def test_custom_data_root_override(tmp_path, monkeypatch):
    custom_root = tmp_path / "custom_data"
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(custom_root))
    monkeypatch.setenv("PIPELINE_DATA_FORMAT", "delta")

    config = PipelineConfig(data_root=custom_root, data_format="delta")
    assert config.data_root == custom_root
    assert config.raw_dir == custom_root / "raw"
    assert config.silver_dir == custom_root / "silver"
    assert config.gold_dir == custom_root / "gold"
    assert config.data_format == "delta"

