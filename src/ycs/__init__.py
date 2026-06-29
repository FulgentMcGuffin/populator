"""Shared yield-curve studio population logic."""

from .cli import parse_args
from .correlations import get_corr_matrix, rolling_corr_matrices
from .coverage import get_coverage, get_coverage_plot
from .data_loading import load_parquets_from_dir, populate_from_files
from .pipeline import run_create_corr_files, run_populate_pipeline

__all__ = [
    "get_corr_matrix",
    "get_coverage",
    "get_coverage_plot",
    "load_parquets_from_dir",
    "parse_args",
    "populate_from_files",
    "rolling_corr_matrices",
    "run_create_corr_files",
    "run_populate_pipeline",
]
