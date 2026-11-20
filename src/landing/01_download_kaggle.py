# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Download the Kaggle dataset into the landing volume
# MAGIC Downloads `martinellis/nhl-game-data` and copies each configured CSV to
# MAGIC `/Volumes/nhl_data/landing/files/<dataset>/<file>.csv`.
# MAGIC A file is written **only when its content changed** (SHA-256), so Auto Loader in the pipeline
# MAGIC does not re-read unchanged files.
# MAGIC
# MAGIC **Needs a Databricks secret scope** called `kaggle` with keys `username` and `key`
# MAGIC (see the README).

# COMMAND ----------

# MAGIC %pip install --quiet kagglehub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
import os
import shutil
import sys
from pathlib import Path

# Reuse the dataset list from the pipeline config (one source of truth).
sys.path.append(os.path.abspath("../pipeline"))
from utilities.config import DATASETS, KAGGLE_DATASET  # noqa: E402

os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get("kaggle", "username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get("kaggle", "key")

# COMMAND ----------

import kagglehub

download_path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
available = {p.name: p for p in download_path.rglob("*") if p.is_file()}
print(f"Downloaded {KAGGLE_DATASET} to {download_path} ({len(available)} files)")

# COMMAND ----------


def sha256_of(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def has_data_rows(path: Path) -> bool:
    """True if the CSV has a header plus at least one data row."""
    with path.open("rb") as f:
        return sum(1 for _ in zip(range(2), f)) >= 2


results, failed = [], []
for ds in DATASETS:
    source = available.get(ds.source_file)
    if source is None or not has_data_rows(source):
        failed.append(ds.source_file)
        continue

    target_dir = Path(ds.landing_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / ds.source_file

    if target.exists() and sha256_of(target) == sha256_of(source):
        results.append((ds.source_file, "unchanged"))
        continue

    tmp = target.with_suffix(".csv.tmp")  # write then rename so Auto Loader never sees a partial file
    shutil.copyfile(source, tmp)
    os.replace(tmp, target)
    results.append((ds.source_file, "written"))

for name, status in results:
    print(f"{status:>9}  {name}")

if failed:
    raise RuntimeError(f"Missing or empty files in the Kaggle dataset: {failed}")
