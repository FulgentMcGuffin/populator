"""Coverage reporting for populated rate tables."""

from __future__ import annotations

from typing import Any

import polars as pl
from plotnine import (
    aes,
    element_text,
    facet_wrap,
    geom_bar,
    geom_segment,
    ggplot,
    labs,
    scale_color_brewer,
    scale_fill_brewer,
    scale_x_date,
    theme,
    theme_538,
    theme_tufte,
)

__all__ = ["get_coverage", "get_coverage_plot"]


def get_coverage(source_class: type[Any], db_path: str | None = None) -> pl.DataFrame:
    """Return date-range coverage per source for each populated rate table."""
    with source_class(db_path, read_only=False) as db:
        coverage = (
            pl.concat(
                [
                    pl.DataFrame(
                        db.execute(
                            "SELECT MIN(date) AS start_date, MAX(date) AS end_date, "
                            "MIN(source) AS source FROM zero_rates GROUP BY source"
                        )
                    ).with_columns(pl.lit("zero_rates").alias("type")),
                    pl.DataFrame(
                        db.execute(
                            "SELECT MIN(date) AS start_date, MAX(date) AS end_date, "
                            "MIN(source) AS source FROM par_rates GROUP BY source"
                        )
                    ).with_columns(pl.lit("par_rates").alias("type")),
                    pl.DataFrame(
                        db.execute(
                            "SELECT MIN(date) AS start_date, MAX(date) AS end_date, "
                            "MIN(source) AS source FROM spotfx GROUP BY source"
                        )
                    ).with_columns(pl.lit("spotfx").alias("type")),
                ]
            )
            .with_columns(
                pl.col("start_date").str.to_date(),
                pl.col("end_date").str.to_date(),
            )
            .with_columns(
                (pl.col("end_date") - pl.col("start_date"))
                .dt.total_days()
                .alias("coverage_days")
            )
            .with_columns(pl.col("source").replace("spot_fx_rates", "fx"))
            .sort(["source", "type", "start_date", "end_date"])
        )
    return coverage


def get_coverage_plot(coverage: pl.DataFrame, is_date_coverage: bool = True):
    if is_date_coverage:
        return (
            ggplot(
                coverage,
                aes(
                    x="start_date",
                    xend="end_date",
                    y="source",
                    yend="source",
                    color="type",
                ),
            )
            + geom_segment(size=5, alpha=0.5)
            + scale_x_date(date_breaks="12 months", date_labels="%b %Y")
            + labs(
                title="Date Ranges by Source and Type",
                x="Date",
                y="Source",
                color="Rate Type",
            )
            + scale_color_brewer(type="qual", palette="Set1")
            + theme_538()
            + theme(
                figure_size=(16, 5),
                axis_text_x=element_text(angle=45, hjust=1, size=7),
            )
            + facet_wrap("type")
        )

    return (
        ggplot(coverage, aes(x="source", y="coverage_days", fill="type"))
        + geom_bar(stat="identity", position="dodge")
        + labs(
            title="Coverage by Source and Type",
            x="Source",
            y="Coverage (days)",
            fill="Rate Type",
        )
        + scale_fill_brewer(type="qual", palette="Set2")
        + theme_tufte()
        + theme(figure_size=(12, 3))
    )
