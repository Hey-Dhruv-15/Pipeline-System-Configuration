from __future__ import annotations

from pyspark.sql import SparkSession

from src.pipeline.config import CONFIG


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(CONFIG.app_name)
        .config("spark.sql.shuffle.partitions", CONFIG.shuffle_partitions)
        .config("spark.sql.files.maxRecordsPerFile", CONFIG.max_records_per_file)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.speculation", "false")
    )

    if CONFIG.data_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    return builder.getOrCreate()
