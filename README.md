# Populator

Tools for loading local files into SQLite or DuckDB, with optional sourcing (S3, Kaggle) and derived analytics for yield-curve studio (YCS) data.

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Run all commands from the repository root.

```bash
uv sync                  # install runtime dependencies
uv sync --extra dev      # include pytest for tests
```

## Project layout

| Path | Role |
|---|---|
| [`src/backends/`](src/backends/) | SQLite / DuckDB read-write backends (`DataSink`) |
| [`src/ingestion/`](src/ingestion/) | Generic local-file → database loading (parquet, CSV, feather) with optional Polars transforms and a Hamilton DAG |
| [`src/ycs/`](src/ycs/) | YCS-specific correlation pipeline (Hamilton DAG on top of `ingestion`) |
| [`src/download_ycs.py`](src/download_ycs.py) | Download YCS parquet from S3 |
| [`src/populate_ycs_db.py`](src/populate_ycs_db.py) | Populate YCS data into SQLite and DuckDB |
| [`src/populate_equity_db.py`](src/populate_equity_db.py) | Populate equity EOD CSVs into SQLite and DuckDB |
| [`src/create_corr_files.py`](src/create_corr_files.py) | Standalone YCS correlation pickle generation |
| [`tests/`](tests/) | `pytest` tests for ingestion and transforms |

---

## Configuration

Copy and edit [`.env`](.env). For S3 downloads, also create a local [`.secrets`](.secrets) (gitignored) with bucket and path overrides.

| Variable | Used by | Description |
|---|---|---|
| `LOCALDATA_PATH` | download, YCS populate | Local root for YCS parquet (`zero_coupon`, `par`, `spot_fx_rates` subfolders). Typically set in `.secrets`. |
| `LOCALDATA_BUCKET_NAME` | download | S3 bucket name (`.secrets`) |
| `LOCALSOURCE_NAME` | download | S3 prefix root, e.g. `augur` (`.secrets`) |
| `SQLITEDB_PATH` | YCS populate SQLite | SQLite database path (must end in `.db` when resolved from env) |
| `DUCKDB_PATH` | YCS populate DuckDB | DuckDB database path (must end in `.duckdb` when resolved from env) |
| `DERIVED_LOCALDATA_PATH` | YCS correlations | Root for derived outputs |
| `DERIVED_CORR_FOLDER` | YCS correlations | Subfolder for correlation pickle files |

YCS populate scripts load **only** [`.env`](.env) explicitly; `LOCALDATA_PATH` for `--load-from-files` is still picked up when backends import and load `.secrets`.

Equity populate ([`populate_equity_db.py`](src/populate_equity_db.py)) uses **hard-coded paths** at the top of the file (CSV directory and database files), not `.env`.

AWS credentials must be configured locally (for example `~/.aws/credentials`) for S3 downloads.

---

## Generic ingestion (`src/ingestion/`)

Shared module for reading local files and loading them into any `DataSink` backend.

### Supported file types

`.parquet`, `.csv`, `.feather`, `.ipc` — all files in a directory are read concurrently and concatenated. **Subdirectories are not scanned.**

### Main API

```python
from backends import DuckDBSource
from ingestion import (
    load_directories_into_tables,
    run_load_directories_into_tables,  # Hamilton wrapper
    PrefixedMeltTransform,
    FileSourceTransform,
)

run_load_directories_into_tables(
    DuckDBSource,
    {"my_table": r"D:\data\input"},
    db_path=r"D:\data\duckdb\my_data.duckdb",
    extensions=frozenset({".csv"}),
    transforms=[PrefixedMeltTransform(separator=".", group_column="Stock", exclude=["Index"])],
)
```

### Transforms (optional, applied per file in order)

| Transform | Purpose |
|---|---|
| `MeltTransform` | Unpivot to long format (`unpivot`) |
| `PrefixedMeltTransform` | Reshape `{prefix}.{metric}` columns into one row per prefix with wide metric columns |
| `LitColumnTransform` | Add a column with a fixed literal |
| `FileSourceTransform` | Add `file_source` with the file's absolute path |
| `FilenamePartTransform` | Add a column from a token of the filename stem (split on a separator) |
| `MapColumnTransform` | Map an existing column into a new column via a callable |
| `CastDateColumnTransform` | Cast a column to Polars `Date` |

Transforms are only applied when passed via `transforms=[...]` or the legacy `file_transform=` callable.

Hamilton drivers use **`overrides=`** (not `inputs=`) when calling `dr.execute()`.

### Tests

```bash
uv run pytest tests/ -v
```

---

## 1. Download YCS data from S3

[`src/download_ycs.py`](src/download_ycs.py) fetches transformed parquet files into `LOCALDATA_PATH`. By default it downloads the most recent Friday's data from:

```
s3://{LOCALDATA_BUCKET_NAME}/{LOCALSOURCE_NAME}/{date}/transformed/
```

To download from a different bucket or prefix, change the arguments to `download_s3_prefix` in the `__main__` block.

```bash
uv run python src/download_ycs.py
```

**Prerequisites:** `LOCALDATA_PATH`, `LOCALDATA_BUCKET_NAME`, and `LOCALSOURCE_NAME` in `.env` / `.secrets`, plus valid AWS credentials.

---

## 2. Populate YCS into SQLite and DuckDB

[`src/populate_ycs_db.py`](src/populate_ycs_db.py) resolves database paths from `SQLITEDB_PATH` and `DUCKDB_PATH` in `.env` and runs the shared YCS Hamilton pipeline for both backends.

```bash
# Load parquet only
uv run python src/populate_ycs_db.py --load-from-files

# Compute correlation pickles only (reads existing rate tables)
uv run python src/populate_ycs_db.py --create-corr-files

# Load pickles into window_corr table (one or both backends)
uv run python src/populate_ycs_db.py --populate-sqlite-corr-from-files
uv run python src/populate_ycs_db.py --populate-duckdb-corr-from-files

# Full pipeline
uv run python src/download_ycs.py
uv run python src/populate_ycs_db.py \
  --load-from-files \
  --create-corr-files \
  --populate-sqlite-corr-from-files \
  --populate-duckdb-corr-from-files
```

---

## 3. Standalone correlation files

[`src/create_corr_files.py`](src/create_corr_files.py) reads rate tables from a configured database and writes correlation pickles only (no `window_corr` load).

```bash
uv run python src/create_corr_files.py --backend duckdb --starting-year 2007
```

---

## 4. Populate equity EOD data

[`src/populate_equity_db.py`](src/populate_equity_db.py) loads top-level CSV files from a configured directory into a single `equity_eod` table on both DuckDB and SQLite. Paths and transforms are declared explicitly at the top of the script:

- **CSV directory:** `D:\data\equity\eod`
- **DuckDB:** `D:\data\duckdb\equity_eod_data.duckdb`
- **SQLite:** `D:\data\sqlite\equity_eod_data.sqlite`

Transforms applied (in order):

1. `PrefixedMeltTransform` — separator `.`, `Stock` column, `Index` excluded from melt
2. `FilenamePartTransform` — `EqIndex` from filename stem (split on `_`)
3. `FileSourceTransform` — absolute path in `file_source`
4. `CastDateColumnTransform` — `Index` cast to date

```bash
uv run python src/populate_equity_db.py
```

---

## Typical YCS workflow

```bash
uv run python src/download_ycs.py
uv run python src/populate_ycs_db.py \
  --load-from-files \
  --create-corr-files \
  --populate-sqlite-corr-from-files \
  --populate-duckdb-corr-from-files
```

YCS populate uses shared correlation logic in [`src/ycs/`](src/ycs/); file loading goes through [`src/ingestion/`](src/ingestion/). Database paths come from `SQLITEDB_PATH` and `DUCKDB_PATH` in `.env`.
