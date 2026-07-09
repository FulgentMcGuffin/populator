# Populator

Tools for loading local files into SQLite or DuckDB, with optional sourcing (S3, Kaggle) and derived analytics for yield-curve studio (YCS) data.

The ingestion module can also be used from **Claude or Cursor** via an MCP server (`populator-mcp`): preview files, apply transforms, and populate databases through natural-language tool calls.

## Installation

Install from GitHub (includes the `populator-mcp` command when you add the `mcp` extra):

```bash
# Recommended: install globally with uv (puts populator-mcp on your PATH)
uv tool install "populator[mcp] @ git+https://github.com/FulgentMcGuffin/populator.git"

# Or into the active environment with pip
pip install "populator[mcp] @ git+https://github.com/FulgentMcGuffin/populator.git"

# Or run on demand without installing (uv downloads and caches the package)
uvx --from "populator[mcp] @ git+https://github.com/FulgentMcGuffin/populator.git" populator-mcp
```

Verify the MCP server entry point:

```bash
populator-mcp
```

The process waits on stdio for an MCP client (Claude Desktop, Cursor, etc.) — that is expected. Stop it with Ctrl+C when testing from a terminal.

### Development (clone the repo)

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Clone and install in editable mode from the repository root:

```bash
git clone https://github.com/FulgentMcGuffin/populator.git
cd populator
uv sync --extra dev --extra mcp
uv run populator-mcp
```

```bash
uv sync                  # runtime dependencies (editable install)
uv sync --extra dev      # include pytest
uv sync --extra mcp      # include FastMCP for populator-mcp
```

## Project layout

| Path | Role |
|---|---|
| [`src/backends/`](src/backends/) | SQLite / DuckDB read-write backends (`DataSink`) |
| [`src/ingestion/`](src/ingestion/) | Generic local-file → database loading (parquet, CSV, feather) with optional Polars transforms, a transform registry for MCP/JSON, and a Hamilton DAG |
| [`src/populator_mcp/`](src/populator_mcp/) | MCP server exposing ingestion, transforms, and database load to Claude / Cursor |
| [`src/ycs/`](src/ycs/) | YCS-specific correlation pipeline (Hamilton DAG on top of `ingestion`) |
| [`src/download_ycs.py`](src/download_ycs.py) | Download YCS parquet from S3 |
| [`src/populate_ycs_db.py`](src/populate_ycs_db.py) | Populate YCS data into SQLite and DuckDB |
| [`src/populate_equity_db.py`](src/populate_equity_db.py) | Populate equity EOD CSVs into SQLite and DuckDB |
| [`src/populate_worldbank_db.py`](src/populate_worldbank_db.py) | Populate World Bank CSVs into SQLite and DuckDB |
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
| `CastNumericStringColumnsTransform` | Parse string columns as numbers (e.g. CSV scientific notation `4e+05`) |
| `CastDateColumnTransform` | Cast a column to Polars `Date` |

Transforms are only applied when passed via `transforms=[...]` or the legacy `file_transform=` callable.

For MCP and other JSON-driven callers, the same transforms are described in [`transform_registry.py`](src/ingestion/transform_registry.py) as serializable step objects (see [MCP server](#mcp-server-claude--cursor) below).

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

## 5. Populate World Bank data

[`src/populate_worldbank_db.py`](src/populate_worldbank_db.py) loads two CSV files into SQLite and DuckDB:

- **DuckDB:** `D:\data\duckdb\world_bank.duckdb`
- **SQLite:** `D:\data\sqlite\world_bank.sqlite`
- **Tables:** `topic_mapping`, `indicators`

```bash
uv run python src/populate_worldbank_db.py
```

---

## MCP server (Claude / Cursor)

The `populator-mcp` command exposes [`src/ingestion/`](src/ingestion/) so Claude can load local CSV/parquet/feather files into SQLite or DuckDB, with optional transforms applied before write.

Requires the package with the **`mcp` extra** (see [Installation](#installation)).

### Connect the server

After `uv tool install ...` (or `pip install ...`), point your MCP client at the installed command:

**Claude Desktop** — `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "populator": {
      "command": "populator-mcp"
    }
  }
}
```

**Without a global install** — use `uvx` so the package is fetched from GitHub automatically:

```json
{
  "mcpServers": {
    "populator": {
      "command": "uvx",
      "args": [
        "--from",
        "populator[mcp] @ git+https://github.com/FulgentMcGuffin/populator.git",
        "populator-mcp"
      ]
    }
  }
}
```

**Cursor** — add the same entry under **Settings → MCP** (use `command` + `args` as above).

Restart the client after saving. The server uses stdio transport and stays running while the client is connected.

> **Note:** MCP tools read and write **your local files and database paths**. Paths in the examples below are illustrative — substitute your own CSV directories and `.duckdb` / `.sqlite` output locations.

### Tools

| Tool | Purpose |
|---|---|
| `list_transforms` | Transform types, JSON parameters, and presets (`none`, `equity`) |
| `preview_file` | Sample rows before/after applying transforms |
| `populate_from_files` | Load a file → table map (World Bank style) |
| `populate_from_directories` | Load a directory → table map (equity/YCS style) |
| `list_db_tables` | List tables in a database |
| `describe_db_table` | Column schema for one table |

Resource: `populator://transforms` — JSON catalog of transform types and presets.

`MapColumnTransform` is not exposed via MCP (it requires a Python callable).

### Example: load World Bank CSVs via Claude

After connecting the MCP server, you can ask Claude something like:

> Load my World Bank indicator CSVs into DuckDB. Preview the indicators file first, then create tables `topic_mapping` and `indicators`.

Claude would typically:

1. Call **`list_transforms`** (optional — no transforms needed for World Bank)
2. Call **`preview_file`** on your indicators CSV
3. Call **`populate_from_files`** (paths are whatever exists on your machine):

```json
{
  "backend": "duckdb",
  "db_path": "/path/to/world_bank.duckdb",
  "table_files": {
    "topic_mapping": "/path/to/indicator_topic_mapping.csv",
    "indicators": "/path/to/world_bank_indicators_long.csv"
  },
  "overwrite": true
}
```

4. Call **`list_db_tables`** to confirm `topic_mapping` and `indicators` exist

On Windows, use backslashes or doubled backslashes in JSON paths, e.g. `D:\\data\\duckdb\\world_bank.duckdb`.

### Example: load equity CSVs with transforms

Equity EOD data needs transforms (melt wide columns, parse dates, etc.). Use the built-in **`equity`** preset:

```json
{
  "backend": "duckdb",
  "db_path": "/path/to/equity_eod_data.duckdb",
  "table_directories": {
    "equity_eod": "/path/to/equity/csv/dir"
  },
  "extensions": [".csv"],
  "preset": "equity",
  "csv_infer_schema_length": 0,
  "overwrite": true
}
```

Or pass explicit transform steps (same pipeline as [`populate_equity_db.py`](src/populate_equity_db.py)):

```json
{
  "transforms": [
    {"type": "cast_numeric_string_columns", "exclude": ["Index"]},
    {"type": "prefixed_melt", "separator": ".", "group_column": "Stock", "exclude": ["Index"]},
    {"type": "filename_part", "column": "EqIndex", "separator": "_", "part_index": 0},
    {"type": "file_source"},
    {"type": "cast_date_column", "column": "Index", "format": "YYYY-mm-dd HH:MM:SS"}
  ]
}
```

Use **`preview_file`** with the same `preset` or `transforms` to inspect output before calling **`populate_from_directories`**.

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
