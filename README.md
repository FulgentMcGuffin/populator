# Populator

Tools for downloading yield-curve studio data from S3 and loading it into local SQLite or DuckDB databases.

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Run all commands from the repository root.

## Configuration

Copy and edit `.env` and `config.py` and set up your own `.secrets` (if required) before running the scripts.

| Variable | Used by | Description |
|---|---|---|
| `LOCALDATA_PATH` | download, populate | Local root directory for parquet files |
| `LOCALDATA_BUCKET_NAME` | download | S3 bucket name |
| `LOCALSOURCE_NAME` | download | S3 prefix root (e.g. `augur`) |
| `SQLITEDB_PATH` | populate SQLite | Path to the SQLite database file (`.db`) |
| `DUCKDB_PATH` | populate DuckDB | Path to the DuckDB database file (`.duckdb`) |
| `DERIVED_LOCALDATA_PATH` | populate | Root directory for derived outputs |
| `DERIVED_CORR_FOLDER` | populate | Subfolder under derived path for correlation pickles |

AWS credentials must be configured locally (for example via `~/.aws/credentials`) for S3 downloads.

---

## 1. Download data from S3

[`src/download_ycs.py`](src/download_ycs.py) fetches transformed parquet files from S3 into `LOCALDATA_PATH`. By default it downloads the most recent Friday's data under:

```
s3://{LOCALDATA_BUCKET_NAME}/{LOCALSOURCE_NAME}/{date}/transformed/
```

In order to download from an S3 bucket of your choice to a directory of your choice, change the arguments to `download_s3_prefix` under the `__main__` of [`src/download_ycs.py`](src/download_ycs.py) to your own preferences.

### Example

```bash
uv run python src/download_ycs.py
```

### Prerequisites

- `LOCALDATA_PATH`, `LOCALDATA_BUCKET_NAME`, and `LOCALSOURCE_NAME` set in `.env` / `.secrets`
- Valid AWS credentials with read access to the bucket

---

## 2. Populate a SQLite database

[`src/populate_ycs_sqlite.py`](src/populate_ycs_sqlite.py) loads local parquet files into SQLite, optionally computes rolling correlations, and writes results to the `window_corr` table.

The database path comes from `SQLITEDB_PATH`.

### Load parquet files only

Creates or overwrites `zero_rates`, `par_rates`, and `spotfx` tables from parquet directories under `LOCALDATA_PATH`.

```bash
uv run python src/populate_ycs_sqlite.py --load-from-files
```

### Compute correlation pickle files only

Reads rate tables from SQLite and writes melted correlation pickles to `{DERIVED_LOCALDATA_PATH}/{DERIVED_CORR_FOLDER}/`.

```bash
uv run python src/populate_ycs_sqlite.py --create-corr-files
```

### Load correlation pickles into SQLite

Reads existing pickle files and appends them to the `window_corr` table (deduplicated on date, observable, sources, window size, and correlation type).

```bash
uv run python src/populate_ycs_sqlite.py --populate-sqlite-corr-from-files
```

### Full pipeline

Download data first, then run all three steps together:

```bash
uv run python src/download_ycs.py

uv run python src/populate_ycs_sqlite.py \
  --load-from-files \
  --create-corr-files \
  --populate-sqlite-corr-from-files
```

---

## 3. Populate a DuckDB database

[`src/populate_ycs_duckdb.py`](src/populate_ycs_duckdb.py) mirrors the SQLite workflow using DuckDB. The database path comes from `DUCKDB_PATH`.

### Load parquet files only

```bash
uv run python src/populate_ycs_duckdb.py --load-from-files
```

### Compute correlation pickle files only

```bash
uv run python src/populate_ycs_duckdb.py --create-corr-files
```

### Load correlation pickles into DuckDB

```bash
uv run python src/populate_ycs_duckdb.py --populate-duckdb-corr-from-files
```

### Full pipeline

```bash
uv run python src/download_ycs.py

uv run python src/populate_ycs_duckdb.py \
  --load-from-files \
  --create-corr-files \
  --populate-duckdb-corr-from-files
```

---

## Typical workflow

```bash
# 1. Fetch latest transformed data from S3
uv run python src/download_ycs.py

# 2. Load into your chosen database and build correlations
uv run python src/populate_ycs_sqlite.py --load-from-files --create-corr-files --populate-sqlite-corr-from-files
# or
uv run python src/populate_ycs_duckdb.py --load-from-files --create-corr-files --populate-duckdb-corr-from-files
```

Both populate scripts share the same correlation logic in [`src/ycs/`](src/ycs/); only the database backend differs.
