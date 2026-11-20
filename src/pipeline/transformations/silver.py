"""Silver: typed, validated, de-duplicated tables (SCD Type 1).

For each table in ``utilities.config.SILVER_TABLES`` this defines
  1. a temporary view that casts columns, drops foreign-key orphans and enforces the
     data-quality expectations (a violation stops the update), and
  2. a streaming target table fed by AUTO CDC, which keeps one row per business key
     (newest bronze record wins).

AUTO CDC replaces the Fabric helpers read_new_bronze_rows, keep_latest_per_key,
validate_primary_keys and write_to_silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import SILVER_TABLES, silver


def _expectations(t):
    """SQL conditions that must hold for every row; any violation fails the update."""
    rules = {f"{c}_not_null": f"{c} IS NOT NULL" for c in t.not_null}
    for column, values in t.allowed_values.items():
        quoted = ", ".join(f"'{v}'" for v in values)
        rules[f"{column}_valid"] = f"{column} IS NOT NULL AND {column} IN ({quoted})"
    rules.update(t.checks)
    return rules


def define_silver(t):
    view_name = f"clean_{t.name}"
    rules = _expectations(t)

    @dp.temporary_view(name=view_name)
    @dp.expect_all_or_fail(rules)
    def _clean():
        df = spark.readStream.table(t.bronze_table)  # noqa: F821

        # types (TRY_CAST: 'NA' and other junk become NULL), plus the technical columns
        df = df.select(
            *[F.expr(f"try_cast(`{c}` AS {typ})").alias(c) for c, typ in t.columns.items()],
            F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
            "_row_hash",
        )

        for column, value in t.fill_null.items():
            df = df.withColumn(column, F.coalesce(F.col(column), F.lit(value)))

        # referential integrity against already-cleaned silver tables (orphans dropped)
        for fk in t.foreign_keys:
            ref = spark.read.table(silver(fk.ref_table)).select(fk.ref_column)  # noqa: F821
            if len(fk.keys) == 1:
                df = df.join(ref, on=fk.keys[0], how="left_semi")
            else:
                df = df.join(ref, df[fk.keys[0]] == ref[fk.keys[1]], how="left_semi")
        return df

    dp.create_streaming_table(
        name=t.silver_table,
        comment=f"Cleaned {t.name}: typed, validated, one row per {', '.join(t.primary_keys)}.",
    )
    dp.create_auto_cdc_flow(
        target=t.silver_table,
        source=view_name,
        keys=list(t.primary_keys),
        # newest ingestion wins; the hash makes ties deterministic
        sequence_by=F.struct("_bronze_ingested_at", "_row_hash"),
        stored_as_scd_type=1,
    )


for _t in SILVER_TABLES:
    define_silver(_t)
