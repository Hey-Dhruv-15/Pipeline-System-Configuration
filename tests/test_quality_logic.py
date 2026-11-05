import re
import pytest
from src.pipeline.quality import EXPECTED_COLUMNS, NULL_LIKE_TOKENS


def test_expected_columns_definition():
    assert EXPECTED_COLUMNS == ["id", "name", "age"]


def test_null_like_tokens_coverage():
    expected_tokens = ("null", "none", "n/a", "na", "undefined", "nan")
    for token in expected_tokens:
        assert token in NULL_LIKE_TOKENS


def test_integer_regex_pattern_logic():
    pattern = re.compile(r"(-?\d+)")
    assert pattern.search("ID-12345").group(1) == "-12345"
    assert pattern.search("Age: 42 years").group(1) == "42"
    assert pattern.search("0099").group(1) == "0099"
    assert pattern.search("none") is None


def test_null_token_filtering_logic():
    raw_values = ["Null", "NONE", "n/a", "N/A", "undefined", "Valid Name", ""]
    cleaned = []
    for val in raw_values:
        trimmed = val.strip()
        if not trimmed or trimmed.lower() in NULL_LIKE_TOKENS:
            cleaned.append(None)
        else:
            cleaned.append(trimmed)

    assert cleaned == [None, None, None, None, None, "Valid Name", None]

