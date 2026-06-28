"""Default parameters for yield-curve population pipelines."""

DEFAULT_TENORS = [
    "Y000p5",
    "Y001p0",
    "Y002p0",
    "Y003p0",
    "Y004p0",
    "Y005p0",
    "Y007p0",
    "Y010p0",
    "Y012p0",
    "Y015p0",
    "Y020p0",
    "Y025p0",
    "Y030p0",
]

DEFAULT_CORRELATION_WINDOW_SIZES = [20, 40, 60, 90]

DEFAULT_START_YEAR = 2007
DEFAULT_START_MONTH = 1
DEFAULT_START_DAY = 1

WINDOW_CORR_TABLE = "window_corr"
WINDOW_CORR_DEDUP_COLUMNS = [
    "date",
    "observable",
    "source1",
    "source2",
    "window_size",
    "corr_type",
]

RATE_TABLES = ("zero_rates", "par_rates", "spotfx")
