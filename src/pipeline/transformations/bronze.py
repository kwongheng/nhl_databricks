"""Bronze: raw, append-only streaming tables loaded from the landing volume with Auto Loader.

Replaces the Fabric landing->bronze notebook: Auto Loader tracks which files it has read
(so no manifest or file hashing), and records new/changed columns itself. Every column is
kept as text (no type guessing); typing happens in silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import DATASETS


def define_bronze(ds):
    @dp.table(
        name=ds.bronze_table,
        comment=f"Raw {ds.source_file} from the Kaggle NHL dataset (all columns as text).",
    )
    def _bronze():
        raw = (
            spark.readStream.format("cloudFiles")  # noqa: F821  (spark is provided by the pipeline)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            # the landing job overwrites a file only when its content changed
            .option("cloudFiles.allowOverwrites", "true")
            .load(ds.landing_dir)
        )
        return (
            raw.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "_row_hash",
                F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in ds.hash_columns]), 256),
            )
        )


for _ds in DATASETS:
    define_bronze(_ds)
