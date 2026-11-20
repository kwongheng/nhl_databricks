# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up the `NHL_data` catalog
# MAGIC Creates the Unity Catalog catalog, the `bronze` / `silver` / `gold` / `landing` schemas and the
# MAGIC `landing.files` volume that holds the downloaded CSV files. Safe to run again.
# MAGIC
# MAGIC Unity Catalog stores catalog names in lower case, so `NHL_data` is created as `nhl_data`.
# MAGIC
# MAGIC **Needs:** permission to create catalogs on the metastore. If your workspace has no default
# MAGIC storage, enter a managed location (a cloud storage path you have an external location for) in the
# MAGIC `managed_location` widget; otherwise leave it empty.

# COMMAND ----------

dbutils.widgets.text("managed_location", "", "Managed location (optional)")
managed_location = dbutils.widgets.get("managed_location").strip()

CATALOG = "nhl_data"

# COMMAND ----------

location_clause = f" MANAGED LOCATION '{managed_location}'" if managed_location else ""
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}{location_clause} COMMENT 'NHL data pipeline (Kaggle NHL game data)'")

for schema in ("landing", "bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# Files downloaded from Kaggle land here: /Volumes/nhl_data/landing/files/<dataset>/<file>.csv
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.files COMMENT 'Raw CSV files from Kaggle'")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
