# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Populator loads local files (parquet, CSV, feather) into SQLite or DuckDB. It also supports:

1. **Sourcing** — downloading YCS parquet from AWS S3 ([`download_ycs.py`](src/download_ycs.py))
2. **Generic ingestion** — shared [`src/ingestion/`](src/ingestion/) module with optional Polars transforms and Hamilton orchestration
3. **YCS analytics** — rolling Pearson / distance-correlation matrices written to `window_corr` ([`src/ycs/`](src/ycs/))
4. **Domain entry scripts** — thin wrappers such as [`populate_ycs_duckdb.py`](src/populate_ycs_duckdb.py), [`populate_equity_db.py`](src/populate_equity_db.py)

## Commands

Run from the repository root:

```bash
uv sync
uv sync --extra dev          # pytest

# Tests
uv run pytest tests/ -v

# YCS: download then populate (pick one backend)
uv run python src/download_ycs.py
uv run python src/populate_ycs_sqlite.py --load-from-files --create-corr-files --populate-sqlite-corr-from-files
uv run python src/populate_ycs_duckdb.py --load-from-files --create-corr-files --populate-duckdb-corr-from-files

# YCS flags (independent steps)
#   --load-from-files
#   --create-corr-files
#   --populate-<sqlite|duckdb>-corr-from-files

# Correlation pickles only
uv run python src/create_corr_files.py --backend duckdb --starting-year 2007

# Equity EOD CSVs -> SQLite + DuckDB (paths in script)
uv run python src/populate_equity_db.py
```

Formatter: Black (Cursor extension). Tests: `pytest` with `pythonpath = ["src"]` in `pyproject.toml`.

## Configuration

Settings come from [`.env`](.env) (committed defaults) and [`.secrets`](.secrets) (local, gitignored, S3-related). Backend modules also call `load_dotenv` on import.

| Variable | Used by | Description |
|---|---|---|
| `LOCALDATA_PATH` | download, YCS `--load-from-files` | YCS parquet root (`zero_coupon`, `par`, `spot_fx_rates`). Usually in `.secrets`. |
| `LOCALDATA_BUCKET_NAME` / `LOCALSOURCE_NAME` | download | S3 bucket / prefix |
| `SQLITEDB_PATH` / `DUCKDB_PATH` | YCS populate | Must end in `.db` / `.duckdb` when resolved via env helpers |
| `DERIVED_LOCALDATA_PATH` / `DERIVED_CORR_FOLDER` | YCS correlations | Pickle output directory |

YCS populate entry scripts load **`.env` only** explicitly, then pass `SQLiteSource.get_full_db_path()` / `DuckDBSource.get_full_db_path()` into `run_populate_pipeline(..., db_path=...)`. `LOCALDATA_PATH` for parquet loading still arrives via backend `.secrets` load on import.

Equity populate uses **explicit paths** in [`populate_equity_db.py`](src/populate_equity_db.py), not env vars.

Never commit real bucket names or secrets without checking `git status`.

## Architecture

### Layering

```
download_ycs.py                     S3 -> local parquet (standalone)
src/backends/                       SQLite + DuckDB DataSink / DataBackend
src/ingestion/                      generic file read, transforms, DB load, Hamilton DAG
src/ycs/                            YCS correlation pipeline (Hamilton DAG composed with ingestion)
populate_ycs_sqlite.py / _duckdb.py thin YCS CLI -> ycs.pipeline.run_populate_pipeline
populate_equity_db.py               thin equity CLI -> ingestion.run_load_directories_into_tables
create_corr_files.py                standalone YCS corr pickle step
tests/                              pytest for ingestion + transforms
```

### Backend abstraction (`src/backends/`)

`base.py` defines:

- `DataBackend` — read-only (`list_tables`, `get_schema`, `run_query`)
- `DataSink` (ABC) — write (`create_table`, `insert`, …) plus helpers: `create_table_from_polars`, `append_to_table`, `remove_duplicates`

`SQLiteSource` and `DuckDBSource` implement both. Default is `read_only=True`; ingestion uses `read_only=False`. Paths can be passed explicitly or derived from `SQLITEDB_PATH` / `DUCKDB_PATH`.

`create_backend()` in `backends/__init__.py` is the read-only factory for serving/query paths.

### Ingestion (`src/ingestion/`)

Generic local-file loading used by YCS, equity, and future pipelines.

| Module | Role |
|---|---|
| `files.py` | Read parquet/CSV/feather; concurrent directory load via `asyncio` |
| `transforms.py` | Composable per-file Polars transforms (see below) |
| `load.py` | `load_directory_into_table`, `load_directories_into_tables` |
| `hamilton_nodes.py` | DAG nodes: `directories_loaded`, `load_summary` |
| `pipeline.py` | `build_driver`, `ingestion_overrides`, `run_load_directories_into_tables` |

**Hamilton note:** runtime values must be passed as **`overrides=`** to `dr.execute()`, not `inputs=`. Empty stub nodes return `None` if passed via `inputs`.

**Transforms** (optional, per file, in order):

- `MeltTransform` — long-format unpivot
- `PrefixedMeltTransform` — `{prefix}.{metric}` wide columns → one row per prefix
- `LitColumnTransform`, `FileSourceTransform`, `FilenamePartTransform`, `MapColumnTransform`, `CastDateColumnTransform`

Compose with `transforms=[...]` or legacy `file_transform=`. Use `compose_transforms()` internally.

### YCS pipeline (`src/ycs/`)

Hamilton DAG **composed with** `ingestion.hamilton_nodes` via `ingestion.pipeline.build_driver(hamilton_nodes)`.

| Module | Role |
|---|---|
| `hamilton_nodes.py` | Correlation DAG nodes; `rate_tables` depends on `directories_loaded` from ingestion |
| `workflow.py` | Imperative steps: `load_rate_tables`, `build_window_corr_frames`, `save_window_corr` |
| `pipeline.py` | `run_populate_pipeline`, `run_create_corr_files`; uses `ycs_ingestion_overrides()` |
| `data_loading.py` | YCS-only: `ycs_table_directories`, `ycs_parquet_transform`, `ycs_ingestion_overrides` |
| `correlations.py` | Rolling Pearson + distance correlation math |
| `cli.py` | Shared argparse for YCS populate scripts |
| `coverage.py` | Coverage plots (not part of populate CLI) |
| `config.py` | `DEFAULT_TENORS`, `WINDOW_CORR_*`, date defaults |

### Correlation table semantics

`window_corr` dedupe key: `(date, observable, source1, source2, window_size, corr_type)` — see `WINDOW_CORR_DEDUP_COLUMNS` in `config.py`. `corr_type` is `pearson` or `dcorr`. Appends use `remove_duplicates(keep="last")`.

### Tenor naming (YCS)

Numeric tenor columns in parquet are renamed to `Y{value:05.1f}` with `.` → `p` (e.g. `2.0` → `Y002p0`) in `ycs_parquet_transform`. Must align with `DEFAULT_TENORS` in `config.py`.

## Adding a new data pipeline

1. Define paths and transforms explicitly in a new `populate_*.py` entry script (see `populate_equity_db.py`).
2. Reuse `ingestion.run_load_directories_into_tables` or `load_directories_into_tables` for simple loads.
3. For multi-step DAGs, add Hamilton nodes and compose with `ingestion.pipeline.build_driver(your_nodes)`.
4. For YCS-like correlation workflows, extend `src/ycs/hamilton_nodes.py` rather than duplicating ingestion logic.

## Common pitfalls

- Hamilton `inputs=` does not override stub nodes — use `overrides=`.
- `rate_tables` (and downstream YCS nodes) must run **after** `directories_loaded` when using `--load-from-files`.
- SQLite `:memory:` is isolated per connection; multi-step in-memory tests need a single connection or a temp file.
- `PrefixedMeltTransform` requires meltable columns to contain the separator; put identifier columns in `exclude` / `ignore`.
