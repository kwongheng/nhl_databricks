# NHL Data Pipeline on Databricks (Lakeflow Spark Declarative Pipelines)

This is a port of https://github.com/kwongheng/JDE10_final_project on Microsoft Fabric to Databricks.
The main aim to practise attaching a git repo to Databricks and deploy the DAB bundle from repo (or from CLI if you wish), running the pipeline and comparing the difference in structure and coding style for the same datasets in Databricks. Noted that on Microsoft Fabric I was using a Free Trial account with many limitations, in Databricks a lot of the logic is reduced due to autoloader and cdc-flow.

Databricks version of the NHL pipeline. It loads the public [NHL game data from Kaggle](https://www.kaggle.com/datasets/martinellis/nhl-game-data) into a new Unity Catalog catalog, **`NHL_data`**, refines it through bronze → silver → gold Delta tables, and produces team and player forecasts.

Unity Catalog stores catalog names in lower case, so `NHL_data` is created and referenced as `nhl_data`.

## What runs

A Lakeflow Job (`nhl_etl`) with three tasks:

```
setup_catalog → land_files → run_pipeline
```

| Task | What it does |
|------|--------------|
| `setup_catalog` | Notebook `src/setup/00_setup_catalog.py`: creates the `nhl_data` catalog, the `landing`, `bronze`, `silver`, `gold` schemas and the `landing.files` volume. |
| `land_files` | Notebook `src/landing/01_download_kaggle.py`: downloads the Kaggle dataset and copies the six CSVs into the volume. A file is rewritten only if its content changed. |
| `run_pipeline` | The declarative pipeline in `src/pipeline/transformations/`. Databricks works out the table order itself. |

Inside the pipeline:

| Layer | Tables | How |
|-------|--------|-----|
| Bronze | `game`, `team_info`, `player_info`, `game_goalie_stats`, `game_skater_stats`, `game_teams_stats` | Auto Loader streaming tables. Raw data, every column kept as text. |
| Silver | the same six, cleaned | Cast to proper types (`try_cast`, so `NA` becomes null), foreign-key orphans dropped, data-quality expectations (a violation fails the update), then `AUTO CDC` keeps one row per key (newest wins). |
| Silver star | `dim_team`, `fact_team_game`, `fact_team_champion`, `fact_skater_game`, `fact_goalie_game` | Materialized views. |
| Gold | `team_season`, `skater_season`, `goalie_season`, `champion_validation_history`, `skater_validation_history`, `team_projection_2021_raw`, `championship_odds_2021`, `best_player_forecast_2021`, `actual_2020_21_results` | Materialized views. |

## Project layout

```
nhl-databricks/
├── databricks.yml                     bundle: workspace + environment
├── resources/
│   ├── nhl_pipeline.yml               the declarative pipeline (serverless)
│   └── nhl_job.yml                    the 3-task job
└── src/
    ├── setup/00_setup_catalog.py      notebook
    ├── landing/01_download_kaggle.py  notebook
    └── pipeline/
        ├── transformations/           pipeline source files (all executed)
        │   ├── bronze.py  silver.py  star.py  gold.py
        └── utilities/                 helper modules, imported (not executed as sources)
            ├── config.py              catalog, datasets, silver rules (edit this to add a dataset)
            ├── star_logic.py          star-schema builders
            └── gold_logic.py          rollups and forecast builders
```

## Prerequisites

- A Databricks workspace with **Unity Catalog** and **serverless** compute, and permission to create a catalog (or a managed location you can use, see below).
- The **Databricks CLI** on your PC: `winget install Databricks.DatabricksCLI`, then `databricks auth login --host https://<your-workspace-url>`.
- A **Kaggle** account and API key.

## Set up and run (Databricks CLI)

1. **Store the Kaggle credentials** as secrets (used by the download notebook):
   ```powershell
   databricks secrets create-scope kaggle
   databricks secrets put-secret kaggle username --string-value "<your-kaggle-username>"
   databricks secrets put-secret kaggle key --string-value "<your-kaggle-key>"
   ```
2. **Set your workspace URL** in `databricks.yml` (`host:`).
3. **Create the catalog first.** The pipeline can only be deployed once the catalog exists. In the workspace SQL editor, run:
   ```sql
   CREATE CATALOG IF NOT EXISTS nhl_data;
   ```
   If your workspace has no default storage this fails with a storage-root error. Add `MANAGED LOCATION '<cloud path you have an external location for>'`. (The job's `setup_catalog` task, `00_setup_catalog.py`, then creates the schemas and the volume. It runs the same `CREATE CATALOG ... IF NOT EXISTS`, so it is harmless to run after this.)
4. **Run it:**
   ```powershell
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run nhl_etl -t dev
   ```
5. Open **Jobs & Pipelines** in the workspace to watch the run. The pipeline page shows each table's row counts and the data-quality expectation results.

## Set up without the bundle (workspace UI)

1. Upload the code: `databricks workspace import-dir src /Workspace/Users/<you>/nhl_data/src` (or put the folder in a Git repo and add it as a **Git folder**).
2. Run the `00_setup_catalog` notebook, then `01_download_kaggle`.
3. **Jobs & Pipelines → Create → ETL pipeline.** Set catalog `nhl_data`, default schema `bronze`, serverless on. Set the root folder to `.../src/pipeline` and add the source code folder `transformations`.
4. Run the pipeline (this is the step the job's third task does).

## Changing things

- **Add or change a dataset:** edit `src/pipeline/utilities/config.py` (`DATASETS` for bronze, `SILVER_TABLES` for silver). The loops in `bronze.py` and `silver.py` create the tables.
- **Catalog name:** `CATALOG` in `config.py`, `catalog:` in `resources/nhl_pipeline.yml`, and `CATALOG` in `00_setup_catalog.py`.
- **Schedule:** add a `schedule:` to `resources/nhl_job.yml`. The Kaggle data is static, so it is run on demand.


## Troubleshooting

| Symptom | Likely cause and fix |
|---------|----------------------|
| `Catalog 'nhl_data' does not exist` when deploying or starting the pipeline | Run the setup notebook (or `CREATE CATALOG nhl_data`) first. |
| `Metastore storage root URL does not exist` when creating the catalog | No default storage. Provide a `managed_location` you have an external location for. |
| Download notebook: `Secret does not exist` | Create the `kaggle` secret scope and the `username` and `key` secrets (step 1). |
| Pipeline update fails with `expectation ... failed` | A data-quality rule was violated. The message names the rule (for example `HoA_valid`); inspect the matching bronze table. |
| `No module named utilities` | The pipeline root folder is not `src/pipeline`. Fix `root_path` / the root folder setting. |
| Pipeline runs but a bronze table is empty | The volume folder is empty. Run `01_download_kaggle` and check `/Volumes/nhl_data/landing/files/<dataset>/`. |
