from pyspark.sql import DataFrame
from pyspark.sql import functions as F


EXPECTED_COLUMNS = ["id", "name", "age"]
NULL_LIKE_TOKENS = ("null", "none", "n/a", "na", "undefined", "nan")


def _null_if_blank_or_token(column: str) -> F.Column:
    raw = F.trim(F.col(column).cast("string"))
    lowered = F.lower(raw)
    return F.when(
        raw.isNull() | (F.length(raw) == 0) | lowered.isin(*NULL_LIKE_TOKENS),
        F.lit(None),
    ).otherwise(raw)


def _extract_int(value: F.Column) -> F.Column:
    extracted = F.regexp_extract(value, r"(-?\d+)", 1)
    return F.when(F.length(extracted) == 0, F.lit(None)).otherwise(extracted.cast("int"))


def standardize_schema(df: DataFrame) -> DataFrame:
    missing_columns = [column for column in EXPECTED_COLUMNS if column not in df.columns]
    for column in missing_columns:
        df = df.withColumn(column, F.lit(None).cast("string"))

    cleaned = df.select(*EXPECTED_COLUMNS)
    cleaned = cleaned.withColumn("id", _extract_int(_null_if_blank_or_token("id")))
    cleaned = cleaned.withColumn("name", _null_if_blank_or_token("name"))
    cleaned = cleaned.withColumn("age", _extract_int(_null_if_blank_or_token("age")))
    return cleaned


def split_valid_and_rejects(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid_condition = F.col("id").isNull() | (F.col("age") < 0)
    reject_reason = (
        F.when(F.col("id").isNull(), F.lit("missing_id"))
        .when(F.col("age") < 0, F.lit("negative_age"))
        .otherwise(F.lit("unknown_validation_failure"))
    )
    rejects = df.filter(invalid_condition).withColumn("reject_reason", reject_reason)
    valid = df.filter(~invalid_condition)
    return valid, rejects


def apply_self_healing(df: DataFrame) -> DataFrame:
    # Fill missing names and ages (age uses an approximate median when available).
    age_source = df.filter(F.col("age").isNotNull())
    if age_source.limit(1).count() > 0:
        median_age = int(age_source.approxQuantile("age", [0.5], 0.01)[0])
    else:
        median_age = 0

    healed = (
        df.withColumn("name", F.coalesce(F.col("name"), F.lit("unknown")))
        .withColumn("name", F.when(F.length(F.col("name")) == 0, F.lit("unknown")).otherwise(F.col("name")))
        .withColumn("age", F.coalesce(F.col("age"), F.lit(median_age)))
        .dropDuplicates(["id", "name", "age"])
        .withColumn("processed_time", F.current_timestamp())
    )
    return healed
