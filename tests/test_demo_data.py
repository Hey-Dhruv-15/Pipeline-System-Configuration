import json
from pathlib import Path


def test_demo_data_directory_files(demo_data_dir: Path):
    assert demo_data_dir.exists()
    required_files = [
        "alerts.json",
        "gold_age_distribution.json",
        "gold_customer_segments.json",
        "gold_executive_summary.json",
        "lineage.json",
        "metrics.json",
        "processed_files.json",
        "rejects.json",
        "run_status.json",
    ]
    for filename in required_files:
        filepath = demo_data_dir / filename
        assert filepath.exists(), f"Demo file {filename} missing"
        with filepath.open("r", encoding="utf-8") as f:
            data = json.load(f)
            assert isinstance(data, list), f"Expected list in {filename}"
