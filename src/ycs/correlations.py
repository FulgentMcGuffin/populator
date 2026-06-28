"""Rolling correlation matrix computation."""

from __future__ import annotations

import asyncio

import dcor
import numpy as np
import pandas as pd
import polars as pl
from numpy.lib.stride_tricks import sliding_window_view
from tqdm import tqdm

__all__ = [
    "df_distance_correlation_np",
    "get_corr_matrix",
    "melt_distance_corr_results",
    "melt_rolling_corr",
    "rolling_corr_matrices",
    "to_picklable_corr_df",
]


def df_distance_correlation_np(arr, columns):
    """Compute a symmetric distance-correlation matrix for one window."""
    n_cols = arr.shape[1]
    out = np.empty((n_cols, n_cols), dtype=float)
    for i in range(n_cols):
        v_i = arr[:, i]
        out[i, i] = 1.0
        for j in range(i + 1, n_cols):
            v_j = arr[:, j]
            mask = ~(np.isnan(v_i) | np.isnan(v_j))
            if mask.sum() == 0:
                dcor_val = np.nan
            else:
                dcor_val = dcor.distance_correlation(v_i[mask], v_j[mask])
            out[i, j] = dcor_val
            out[j, i] = dcor_val
    return pd.DataFrame(out, index=columns, columns=columns)


def melt_rolling_corr(corr: pd.DataFrame) -> pd.DataFrame:
    """Long-form rolling correlations with one row per (date, issuer pair)."""
    melted = corr.stack().to_frame("Correlation").reset_index()
    melted.columns = ["date", "Var1", "Var2", "Correlation"]
    melted = melted[melted["Var1"] != melted["Var2"]]
    pairs = np.sort(melted[["Var1", "Var2"]].to_numpy(), axis=1)
    melted["Var1"] = pairs[:, 0]
    melted["Var2"] = pairs[:, 1]
    return (
        melted.drop_duplicates(subset=["date", "Var1", "Var2"])
        .loc[:, ["date", "Var1", "Var2", "Correlation"]]
        .astype({"Correlation": float})
    )


def to_picklable_corr_df(df: pl.DataFrame) -> pl.DataFrame:
    """Use native polars dtypes so correlation tables survive pickle round-trips."""
    if df.schema["date"] == pl.Object:
        df = df.with_columns(
            pl.col("date")
            .map_elements(
                lambda d: d.isoformat() if hasattr(d, "isoformat") else str(d),
                return_dtype=pl.Utf8,
            )
            .str.to_date()
        )
    else:
        df = df.with_columns(pl.col("date").cast(pl.Date))
    return df.with_columns(pl.col("Correlation").cast(pl.Float64))


def melt_distance_corr_results(
    results: np.ndarray,
    obs_dates: pd.Index,
    names: pd.Index | list[str],
) -> pl.DataFrame:
    """Long-form distance correlations with one row per (date, issuer pair)."""
    T, N, _ = results.shape
    names_arr = np.asarray(names)
    order = np.argsort(names_arr)
    names_sorted = names_arr[order]
    results = results[:, order, :][:, :, order]
    i, j = np.triu_indices(N, k=1)
    n_pairs = i.size
    date_vals = np.asarray(obs_dates, dtype="datetime64[D]")
    return to_picklable_corr_df(
        pl.DataFrame(
            {
                "date": np.repeat(date_vals, n_pairs),
                "Var1": np.tile(names_sorted[i], T),
                "Var2": np.tile(names_sorted[j], T),
                "Correlation": results[:, i, j].reshape(-1),
            }
        )
    )


def rolling_corr_matrices(
    df: pl.DataFrame,
    window: int = 252,
    tenors: list[str] | None = None,
    use_dcor: bool = False,
    run_async: bool = True,
) -> dict[str, pd.DataFrame | pl.DataFrame]:
    """Compute per-tenor rolling inter-issuer correlation matrices."""
    if tenors is None:
        tenors = ["Y1p0", "Y2p0", "Y5p0", "Y10p0", "Y30p0"]

    filtered = df.sort("date")

    def _process_tenor(tenor: str) -> tuple[str, pd.DataFrame | pl.DataFrame | None]:
        wide_pd = (
            filtered.select(["date", "source", tenor])
            .pivot(on="source", index="date", values=tenor)
            .sort("date")
            .to_pandas()
            .set_index("date")
        )
        if not use_dcor:
            corr = wide_pd.rolling(window=window, min_periods=int(window / 3)).corr()
            corr.index = corr.index.set_levels(corr.index.levels[0].date, level=0)
            return tenor, melt_rolling_corr(corr)

        arr = wide_pd.to_numpy()
        if window > arr.shape[0]:
            return tenor, None
        windows = sliding_window_view(
            arr, window_shape=(window, arr.shape[1])
        ).squeeze(1)
        results = np.array(
            [df_distance_correlation_np(w, wide_pd.columns) for w in tqdm(windows)]
        )
        obs_dates = wide_pd.index[window - 1 :]
        return tenor, melt_distance_corr_results(results, obs_dates, wide_pd.columns)

    if run_async:
        async def _process_all():
            tasks = [asyncio.to_thread(_process_tenor, tenor) for tenor in tenors]
            return await asyncio.gather(*tasks)

        results = asyncio.run(_process_all())
        return {
            tenor: result for tenor, result in results if result is not None
        }

    corr_by_tenor: dict[str, pd.DataFrame | pl.DataFrame] = {}
    for tenor in tenors:
        tenor_result, result = _process_tenor(tenor)
        if result is not None:
            corr_by_tenor[tenor_result] = result
    return corr_by_tenor


def get_corr_matrix(
    input_df: pl.DataFrame,
    window: int = 65,
    tenors: list[str] | None = None,
    use_dcor: bool = True,
    is_polars: bool = True,
    run_async: bool = False,
):
    if tenors is None:
        tenors = ["Y1p0", "Y2p0", "Y5p0", "Y10p0", "Y30p0"]

    corr_matrices = rolling_corr_matrices(
        input_df,
        window=window,
        tenors=tenors,
        use_dcor=use_dcor,
        run_async=run_async,
    )
    if not is_polars:
        return corr_matrices

    if isinstance(corr_matrices, pl.DataFrame):
        return corr_matrices
    if isinstance(corr_matrices, pd.DataFrame):
        return pl.DataFrame(corr_matrices)
    if isinstance(corr_matrices, dict):
        return {
            k: to_picklable_corr_df(
                pl.from_pandas(v) if isinstance(v, pd.DataFrame) else v
            )
            for k, v in corr_matrices.items()
        }
    raise ValueError(f"Invalid type: {type(corr_matrices)}")
