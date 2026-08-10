"""
add_equity_static.py

Builds/refreshes two reference tables in equity_eod_data.duckdb:

  equity_static          - one row per (Stock, EqIndex) ticker: company name,
                            sector/industry, index membership window, and
                            (for DAX30 only) deeper fields such as ISIN,
                            shares outstanding, employees, founding year.
  equity_static_history  - a change log: index add/remove events, name/ticker
                            changes, mergers, spin-offs, delistings, etc.

Data sources & methodology
---------------------------
DAX30 (30 tickers): hand-researched, one company at a time, from Wikipedia's
DAX constituent-change table plus each issuer's investor-relations pages
(shares outstanding, ISIN, employee counts). This is the deepest tier.
See data/dax30_static.csv / data/dax30_history.csv.

S&P 500 (505 tickers): bulk-sourced from Wikipedia's "List of S&P 500
companies" (GICS sector/sub-industry, headquarters, date added to the index,
founding year). ~370 of the 505 tickers in this dataset (which is a 2019
snapshot) still match a *current* S&P 500 constituent; the rest have since
been delisted, acquired, or renamed and are not individually re-researched
here (see notes column / match_status).

CAC40, FTSE100, MIB40, HSI50 (~209 tickers): Wikipedia's constituent tables
either weren't cleanly fetchable (FTSE100, MIB40, HSI50) or only covered the
*current* basket with several historical/legacy tickers missing (CAC40).
For these, company name + a reasonable sector/industry classification was
filled in from general knowledge of these well-known large-cap constituents,
NOT verified company-by-company against a primary source the way DAX30 was.
Treat sector/industry for these four indices as a best-effort classification
tier, not an audited one.

None of the non-DAX indices have shares outstanding, employee counts,
founding years, or a detailed membership-change history populated - that
depth of research does not scale to ~700 tickers within reasonable effort.
If deeper data is needed for a specific index/ticker, treat this script's
output as the scaffold and layer additional per-company research on top
(the DAX30 pipeline is the template for doing that).

Usage
-----
    python add_equity_static.py /path/to/equity_eod_data.duckdb

Idempotent: DROPs and recreates equity_static / equity_static_history each
run from the CSVs in data/.
"""
import sys
import csv
import os
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")


def read_csv(name):
    path = os.path.join(DATA_DIR, name)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def none_if_blank(v):
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else None


def build_static_rows(con):
    """Returns list of tuples matching STATIC_COLS, for every ticker across
    all 6 indices, joined against whatever reference data is available."""
    rows = []

    # ---- DAX30: already-researched rich rows, just pass through ----------
    dax_static = read_csv("dax30_static.csv")
    for r in dax_static:
        rows.append((
            r["stock"], r["company_name"], none_if_blank(r["isin"]), r["index_name"],
            r["exchange"], r["country"], none_if_blank(r["sector"]), none_if_blank(r["industry"]),
            int(r["founded_year"]) if none_if_blank(r["founded_year"]) else None,
            int(r["employees"]) if none_if_blank(r["employees"]) else None,
            none_if_blank(r["employees_as_of"]),
            int(r["shares_outstanding"]) if none_if_blank(r["shares_outstanding"]) else None,
            none_if_blank(r["shares_outstanding_as_of"]),
            none_if_blank(r["shares_outstanding_confidence"]),
            none_if_blank(r["index_member_since"]), none_if_blank(r["index_member_until"]),
            r["is_active_index_member"].lower() == "true",
            r["listing_status"], "sourced (DAX30 pilot: hand-researched per-company)", r["notes"],
        ))

    # ---- S&P500: bulk Wikipedia match -------------------------------------
    sp500_ref = {r["symbol"]: r for r in read_csv("sp500_constituents.csv")}
    db_tickers = con.execute(
        "SELECT DISTINCT Stock FROM equity_eod WHERE EqIndex='SP500'"
    ).fetchdf()["Stock"].tolist()
    for t in sorted(db_tickers):
        ref = sp500_ref.get(t)
        if ref:
            rows.append((
                t, ref["security"], None, "SP500", "NYSE/Nasdaq", "United States",
                ref["gics_sector"], ref["gics_subindustry"],
                _parse_founded(ref["founded"]), None, None, None, None, None,
                ref["date_added"], None, True, "Active",
                "bulk (Wikipedia: List of S&P 500 companies, current constituent match)",
                f"HQ: {ref['hq_location']}. CIK {ref['cik']}.",
            ))
        else:
            rows.append((
                t, None, None, "SP500", "NYSE/Nasdaq", "United States",
                None, None, None, None, None, None, None, None,
                None, None, False, "Unknown",
                "unmatched",
                "Ticker not found among current (2026) S&P 500 constituents on Wikipedia. "
                "This dataset is a ~2019 snapshot, so this is very likely a name that has "
                "since been delisted, acquired/merged, or re-ticked - not individually "
                "researched given the scale of the S&P 500 universe.",
            ))

    # ---- CAC40, FTSE100, MIB40, HSI50: lighter bulk/general-knowledge tier
    _add_simple_index(con, rows, "CAC40", "cac40_constituents.csv", "Euronext Paris", "France",
                       key_fn=lambda t: t)
    _add_simple_index(con, rows, "FTSE100", "ftse100_constituents.csv", "London Stock Exchange", "United Kingdom",
                       key_fn=lambda t: t)
    _add_simple_index(con, rows, "MIB40", "mib40_constituents.csv", "Borsa Italiana", "Italy",
                       key_fn=lambda t: t)
    _add_simple_index(con, rows, "HSI50", "hsi50_constituents.csv", "Hong Kong Stock Exchange", "Hong Kong",
                       key_fn=lambda t: t, ref_key="symbol")

    return rows


def _parse_founded(raw):
    if not raw:
        return None
    # e.g. "2013 (1888)" or "1904/1946/1959" -> take the first 4-digit year
    import re
    m = re.search(r"\d{4}", raw)
    return int(m.group()) if m else None


def _add_simple_index(con, rows, eq_index, ref_csv, exchange, country, key_fn, ref_key="symbol"):
    ref = {r[ref_key]: r for r in read_csv(ref_csv)}
    db_tickers = con.execute(
        f"SELECT DISTINCT Stock FROM equity_eod WHERE EqIndex='{eq_index}'"
    ).fetchdf()["Stock"].tolist()
    for t in sorted(db_tickers):
        r = ref.get(key_fn(t))
        if r:
            source_note = r.get("source", "general_knowledge")
            rows.append((
                t, r["security"], None, eq_index, exchange, country,
                none_if_blank(r.get("sector")), none_if_blank(r.get("industry")),
                None, None, None, None, None, None, None, None, True, "Active",
                f"bulk/light ({source_note})",
                "Sector/industry only; no shares outstanding, employee count, founding year, "
                "or membership-change history populated at this tier (see script docstring).",
            ))
        else:
            rows.append((
                t, None, None, eq_index, exchange, country,
                None, None, None, None, None, None, None, None, None, None, False, "Unknown",
                "unmatched",
                "Ticker not found in the reference constituent list used for this index - "
                "not individually researched.",
            ))


STATIC_COLS = [
    "stock", "company_name", "isin", "index_name", "exchange", "country",
    "sector", "industry", "founded_year", "employees", "employees_as_of",
    "shares_outstanding", "shares_outstanding_as_of", "shares_outstanding_confidence",
    "index_member_since", "index_member_until", "is_active_index_member",
    "listing_status", "data_tier", "notes",
]

HISTORY_COLS = [
    "stock", "change_date", "change_type", "field_changed", "old_value", "new_value",
    "description", "source",
]


def build_history_rows():
    """Only DAX30 has a hand-built change log at this stage."""
    rows = []
    for r in read_csv("dax30_history.csv"):
        rows.append((
            r["stock"], none_if_blank(r["change_date"]), r["change_type"],
            none_if_blank(r["field_changed"]), none_if_blank(r["old_value"]),
            none_if_blank(r["new_value"]), r["description"], r["source"],
        ))
    return rows


def main():
    if len(sys.argv) < 2:
        print("Usage: python add_equity_static.py /path/to/equity_eod_data.duckdb")
        sys.exit(1)
    db_path = sys.argv[1]

    con = duckdb.connect(db_path)

    static_rows = build_static_rows(con)
    history_rows = build_history_rows()

    con.execute("DROP TABLE IF EXISTS equity_static")
    con.execute(f"""
        CREATE TABLE equity_static (
            stock VARCHAR,
            company_name VARCHAR,
            isin VARCHAR,
            index_name VARCHAR,
            exchange VARCHAR,
            country VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            founded_year INTEGER,
            employees BIGINT,
            employees_as_of DATE,
            shares_outstanding BIGINT,
            shares_outstanding_as_of DATE,
            shares_outstanding_confidence VARCHAR,
            index_member_since DATE,
            index_member_until DATE,
            is_active_index_member BOOLEAN,
            listing_status VARCHAR,
            data_tier VARCHAR,
            notes VARCHAR,
            PRIMARY KEY (stock, index_name)
        )
    """)
    con.executemany(
        f"INSERT INTO equity_static ({','.join(STATIC_COLS)}) VALUES "
        f"({','.join(['?'] * len(STATIC_COLS))})",
        static_rows,
    )

    con.execute("DROP TABLE IF EXISTS equity_static_history")
    con.execute("""
        CREATE TABLE equity_static_history (
            history_id INTEGER,
            stock VARCHAR,
            change_date DATE,
            change_type VARCHAR,
            field_changed VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            description VARCHAR,
            source VARCHAR
        )
    """)
    history_with_id = [(i + 1, *row) for i, row in enumerate(history_rows)]
    con.executemany(
        f"INSERT INTO equity_static_history (history_id,{','.join(HISTORY_COLS)}) "
        f"VALUES ({','.join(['?'] * (len(HISTORY_COLS) + 1))})",
        history_with_id,
    )

    print(f"equity_static: {len(static_rows)} rows")
    print(f"equity_static_history: {len(history_rows)} rows")

    summary = con.execute("""
        SELECT index_name,
               COUNT(*) AS n_tickers,
               SUM(CASE WHEN data_tier = 'unmatched' THEN 1 ELSE 0 END) AS n_unmatched
        FROM equity_static GROUP BY index_name ORDER BY index_name
    """).fetchdf()
    print(summary.to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
