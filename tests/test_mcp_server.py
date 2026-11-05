from src.mcp_server.server import mcp, _serialise_records


def test_mcp_server_initialization():
    assert mcp.name == "lakehouse-control-room"


def test_serialise_records_limit():
    records = [{"id": i} for i in range(20)]
    assert len(_serialise_records(records, 5)) == 5
    assert len(_serialise_records(records, 25)) == 20
