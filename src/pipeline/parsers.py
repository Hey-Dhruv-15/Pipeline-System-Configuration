from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CONTRACT_PATH = Path(__file__).with_name("source_contracts.json")


def load_source_contracts() -> dict[str, Any]:
    with CONTRACT_PATH.open(encoding="utf-8") as contract_file:
        return json.load(contract_file)


SOURCE_CONTRACTS = load_source_contracts()


def _non_empty_trimmed(expression):
    return F.when(F.length(F.trim(expression)) > 0, F.trim(expression))


def _escaped_alias(alias: str) -> str:
    return alias.replace(" ", r"\s+")


def _alias_extract(payload_column, aliases: list[str], quoted: bool = False):
    patterns = []
    for alias in aliases:
        escaped = _escaped_alias(alias)
        if quoted:
            patterns.append(F.regexp_extract(payload_column, rf'(?i)"{escaped}"\s*:\s*"([^"]+)"', 1))
        else:
            patterns.append(F.regexp_extract(payload_column, rf"(?i)\b{escaped}\b\s*[:=]\s*['\"]?([^,|;\n]+)", 1))
    return [_non_empty_trimmed(pattern) for pattern in patterns]


def parse_unstructured_records(bronze_df: DataFrame) -> DataFrame:
    contracts = SOURCE_CONTRACTS["sources"]
    default_version = SOURCE_CONTRACTS["default_parser_version"]
    extension = F.lower(F.regexp_extract(F.col("source_file"), r"(\.[A-Za-z0-9]+)$", 1))

    source_type = (
        F.when(extension == ".csv", F.lit(contracts[".csv"]["source_type"]))
        .when(extension == ".json", F.lit(contracts[".json"]["source_type"]))
        .when(extension == ".log", F.lit(contracts[".log"]["source_type"]))
        .when(extension == ".txt", F.lit(contracts[".txt"]["source_type"]))
        .otherwise(F.lit("unknown_source"))
    )

    payload = F.col("raw_payload")
    id_aliases = sorted({alias for source in contracts.values() for alias in source["field_aliases"]["id"]})
    name_aliases = sorted({alias for source in contracts.values() for alias in source["field_aliases"]["name"]})
    age_aliases = sorted({alias for source in contracts.values() for alias in source["field_aliases"]["age"]})

    id_candidates = _alias_extract(payload, id_aliases) + [
        _non_empty_trimmed(F.regexp_extract(payload, r'(?i)"patient_id"\s*:\s*([0-9]+)', 1)),
        _non_empty_trimmed(F.regexp_extract(payload, r'(?i)"id"\s*:\s*([0-9]+)', 1)),
        _non_empty_trimmed(F.regexp_extract(payload, r"^\s*([0-9]+)\s*,", 1)),
    ]
    name_candidates = _alias_extract(payload, name_aliases, quoted=False) + _alias_extract(payload, name_aliases, quoted=True) + [
        _non_empty_trimmed(F.regexp_extract(payload, r"^\s*[0-9]+\s*,\s*([^,]+)\s*,", 1)),
    ]
    age_candidates = _alias_extract(payload, age_aliases) + [
        _non_empty_trimmed(F.regexp_extract(payload, r'(?i)"age"\s*:\s*(-?[0-9]+)', 1)),
        _non_empty_trimmed(F.regexp_extract(payload, r"^\s*[0-9]+\s*,\s*[^,]+\s*,\s*(-?[0-9]+)", 1)),
    ]

    return (
        bronze_df.withColumn("source_extension", extension)
        .withColumn("source_type", source_type)
        .withColumn("parser_version", F.lit(default_version))
        .withColumn("id", F.coalesce(*id_candidates).cast("int"))
        .withColumn("name", F.coalesce(*name_candidates))
        .withColumn("age", F.coalesce(*age_candidates).cast("int"))
    )


def build_parser_run_records(parsed_df: DataFrame) -> DataFrame:
    return parsed_df.groupBy("run_id", "source_type", "source_extension", "parser_version").agg(
        F.count("*").alias("records_seen"),
        F.sum(F.when(F.col("id").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("missing_id_records"),
        F.sum(F.when(F.col("age").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("missing_age_records"),
        F.sum(F.when(F.col("name").isNull() | (F.length(F.trim(F.col("name"))) == 0), F.lit(1)).otherwise(F.lit(0))).alias(
            "missing_name_records"
        ),
    )
