from pathlib import Path
import pytest

@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]

@pytest.fixture
def demo_data_dir(project_root: Path) -> Path:
    return project_root / "demo_data"

@pytest.fixture
def contracts_path(project_root: Path) -> Path:
    return project_root / "src" / "pipeline" / "source_contracts.json"

