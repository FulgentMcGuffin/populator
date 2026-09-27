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

SWAP_PAR_TABLE = "swap_par_rates"
REPO_RFR_TABLE = "repo_rfr_rates"
SWAP_PAR_STEM_SUFFIX = "_L"
REPO_RFR_STEM_SUFFIX = "_R"
UNSPECIFIED_INDEX = "UNSPECIFIED"

SWAP_TERM_TENORS = [
    "Y000p25",
    "Y000p5",
    "Y001p0",
    "Y002p0",
    "Y003p0",
    "Y004p0",
    "Y005p0",
    "Y006p0",
    "Y007p0",
    "Y008p0",
    "Y009p0",
    "Y010p0",
    "Y015p0",
    "Y020p0",
    "Y025p0",
    "Y030p0",
]

# (floating coupon_period, fixed coupon_period) per currency.
SWAP_COUPON_PERIODS: dict[str, tuple[str, str]] = {
    "USD": ("3M", "6M"),
    "EUR": ("6M", "1Y"),
    "GBP": ("6M", "6M"),
    "CHF": ("6M", "1Y"),
    "CAD": ("3M", "6M"),
    "AUD": ("6M", "6M"),
    "JPY": ("6M", "6M"),
    "SEK": ("6M", "6M"),
    "NZD": ("6M", "6M"),
    "NOK": ("1Y", "1Y"),
}
