# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Populator aggregates different (sometimes independent) data streams that take data (mostly from local files at the moment) and upload them to databases (mostly slqite and duckdb). In additon to this it also sometimes takes care of 1. sourcing those files (e.g. zero rate and par rate curves or spot FX rates by downloading them from AWS S3)  2. computing derived data before populating the databases with them (e.g. computing rolling Pearson/distance-correlation matrices between sources and writes them into a `window_corr` table.)


## Commands

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Run all commands from the repository root.

```bash
uv sync                                    # install/update dependencies

# 1. Download latest parquet data from S3 (most recent Friday, into LOCALDATA_PATH)
uv run python src/download_ycs.py

# 2. Load parquet -> DB, build correlation pickles, load pickles -> DB (pick one backend)
uv run python src/populate_ycs_sqlite.py --load-from-files --create-corr-files --populate-sqlite-corr-from-files
uv run python src/populate_ycs_duckdb.py --load-from-files --create-corr-files --populate-duckdb-corr-from-files

# Steps can be run independently via their own flags:
#   --load-from-files
#   --create-corr-files
#   --populate-<sqlite|duckdb>-corr-from-files

# Standalone correlation-file generation (reads rate tables from a DB, writes pickles only)
uv run python src/create_corr_files.py --backend duckdb --starting-year 2007
```

There is no test suite or linter configured in this repo (no `pytest`, `ruff`, etc. in `pyproject.toml` or `.venv`). Don't assume `uv run pytest` or similar will work. For formatter use Black formatter (installed as an extension in Cursor)

## Configuration

Settings come from `.env` (committed defaults) and `.secrets` (local, gitignored, S3-related). Both are loaded via `dotenv.load_dotenv`, with `.env` loaded first and `.secrets` typically taking precedence for S3 vars. Key variables:

| Variable | Used by | Description |
|---|---|---|
| `LOCALDATA_PATH` | download, populate | Local root for parquet files (subfolders: `zero_coupon`, `par`, `spot_fx_rates`) |
| `LOCALDATA_BUCKET_NAME` / `LOCALSOURCE_NAME` | download | S3 bucket / prefix root |
| `SQLITEDB_PATH` / `DUCKDB_PATH` | populate | Must end in `.db` / `.duckdb` respectively (validated at connect time) |
| `DERIVED_LOCALDATA_PATH` / `DERIVED_CORR_FOLDER` | populate | Where correlation pickle files are written/read |

AWS credentials must be configured locally (e.g. `~/.aws/credentials`) for S3 downloads. Since `.env`/`.secrets` are gitignored except `.env` (check `git status` before editing — `.env` currently has local modifications), never commit real paths or bucket names on the user's behalf without checking.

## Architecture

### Layering

```
download_ycs.py                  -- S3 -> local parquet files (standalone, no dependency on backends/ycs)
src/backends/                    -- storage-agnostic DB layer (SQLite, DuckDB)
src/ycs/                         -- pipeline logic shared by both backends
populate_ycs_sqlite.py / _duckdb.py  -- thin CLI entrypoints that pick a backend and call ycs.pipeline
create_corr_files.py             -- standalone entrypoint for the correlation-file-only step
```

### Backend abstraction (`src/backends/`)

`base.py` defines two protocols that everything above this layer depends on instead of a concrete DB:
- `DataBackend` — read-only surface (`list_tables`, `get_schema`, `run_query`).
- `DataSink` (ABC) — write surface (`create_table`, `insert`, `update`, `delete`, `execute`), plus concrete helper methods built on those abstract primitives: `create_table_from_polars`, `append_to_table` (schema-validates and dedupes against an existing table), and `remove_duplicates`.

`SQLiteSource` and `DuckDBSource` each implement `DataSink` (and therefore `DataBackend`). Both are context managers and take a `read_only` flag: `read_only=True` (default) physically opens the connection read-only and rejects write calls with `QueryError`; ingestion code always opens with `read_only=False`. Adding a new storage engine means implementing this one contract — no other code should need to change.

`backends/__init__.py` also exposes `create_backend()`, a factory for the read-only serving path (used by scripts that only query, not ingest).

### Pipeline (`src/ycs/`)

The populate pipeline is implemented as an [Apache Hamilton](https://github.com/DAGWorks-Inc/hamilton) DAG:
- `hamilton_nodes.py` — each top-level function is a DAG node; its parameter names are its upstream dependencies (Hamilton wires the graph by name, not by call). CLI flags and config (`source_class`, `backend`, `load_from_files`, `create_corr_files`, `populate_db_corr_from_files`, date range, etc.) are supplied as graph inputs/overrides at `dr.execute(...)` time, not read from globals inside nodes.
- `workflow.py` — the actual imperative logic the nodes call into (`load_rate_tables`, `build_window_corr_frames`, `save_window_corr`). Correlation pickle files are cached on disk (`_load_or_create_corr_matrices`) and reused across runs unless `overwrite_existing` is set.
- `pipeline.py` — builds the Hamilton `Driver` and exposes `run_populate_pipeline()` (full flow, terminal node `pipeline_summary`) and `run_create_corr_files()` (correlation-only flow, terminal node `corr_files_created`).
- `correlations.py` — the actual math: rolling Pearson correlation (pandas `.rolling().corr()`) and distance correlation (`dcor`, via `sliding_window_view`) per tenor, melted into long-form polars frames. Runs per-tenor jobs concurrently via `asyncio.to_thread` when `run_async=True`.
- `data_loading.py` — reads all `.parquet` files in a directory concurrently (`asyncio`) and concatenates them; tenor column names get normalized from raw numeric strings (e.g. `"1.0"`) into `Y001p0`-style labels via `DEFAULT_TENORS` in `config.py`.
- `cli.py` — shared `argparse` setup for the two backend-specific populate scripts (flag names are derived from the backend name, e.g. `--populate-sqlite-corr-from-files`).
- `coverage.py` — reporting helper (`get_coverage`, `get_coverage_plot` using `plotnine`) to inspect date-range coverage per source/table; not part of the populate CLI flow.

### Correlation table semantics

`window_corr` rows are keyed by `(date, observable, source1, source2, window_size, corr_type)` (`WINDOW_CORR_DEDUP_COLUMNS` / `WINDOW_CORR_TABLE` in `config.py`). `corr_type` is either `"pearson"` or `"dcorr"`. Appending always runs through `remove_duplicates(keep="last")` so re-running the populate step is idempotent — newer computations overwrite older ones for the same key.

### Tenor naming convention

Raw tenor values (years, e.g. `2.0`) are encoded as `Y{value:05.1f}` with the decimal point replaced by `p`, e.g. `2.0` -> `Y002p0`, `0.5` -> `Y000p5`. This happens in `data_loading.load_parquets_from_dir` and must match `DEFAULT_TENORS` in `src/ycs/config.py`.
