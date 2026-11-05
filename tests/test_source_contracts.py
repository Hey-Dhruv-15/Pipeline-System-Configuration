import json
from pathlib import Path


def test_source_contracts_file_exists(contracts_path: Path):
    assert contracts_path.exists()


def test_source_contracts_structure(contracts_path: Path):
    with contracts_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert "sources" in data
    assert "default_parser_version" in data
    sources = data["sources"]

    # Verify all expected raw file formats are mapped
    expected_extensions = [".csv", ".json", ".log", ".txt"]
    for ext in expected_extensions:
        assert ext in sources, f"Missing contract specification for {ext}"
        spec = sources[ext]
        assert "source_type" in spec
        assert "id_aliases" in spec
        assert "name_aliases" in spec
        assert "age_aliases" in spec
        assert isinstance(spec["id_aliases"], list)
        assert len(spec["id_aliases"]) > 0
