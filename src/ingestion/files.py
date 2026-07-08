"""Read parquet, CSV, and feather files from local directories."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from pathlib import Path

import polars as pl

SUPPORTED_EXTENSIONS = frozenset({".parquet", ".csv", ".feather", ".ipc"})

FileTransform = Callable[[str, pl.DataFrame], pl.DataFrame]

_READERS: dict[str, Callable[[str], pl.DataFrame]] = {
    ".parquet": pl.read_parquet,
    ".csv": pl.read_csv,
    ".feather": pl.read_ipc,
    ".ipc": pl.read_ipc,
}


def read_local_file(path: str | Path) -> pl.DataFrame:
    """Read a single parquet, CSV, or feather file into a Polars DataFrame."""
    suffix = Path(path).suffix.lower()
    reader = _READERS.get(suffix)
    if reader is None:
        supported = ", ".join(sorted(SUPPORTED_EXTENSIONS))
        raise ValueError(
            f"Unsupported file type {suffix!r} for {path}. Supported: {supported}"
        )
    return reader(str(path))


def _list_data_files(
    directory: str | Path,
    extensions: frozenset[str] | set[str] | None = None,
) -> list[str]:
    allowed = frozenset(extensions) if extensions is not None else SUPPORTED_EXTENSIONS
    return sorted(
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if Path(name).suffix.lower() in allowed
    )


async def _load_all_files(
    paths: list[str],
    transform: FileTransform | None,
) -> list[pl.DataFrame]:
    async def _read_one(path: str) -> pl.DataFrame:
        def _read() -> pl.DataFrame:
            df = read_local_file(path)
            if transform is not None:
                df = transform(path, df)
            return df

        return await asyncio.to_thread(_read)

    return list(await asyncio.gather(*[_read_one(path) for path in paths]))


def load_files_from_dir(
    directory: str | Path,
    *,
    extensions: frozenset[str] | set[str] | None = None,
    transform: FileTransform | None = None,
) -> pl.DataFrame:
    """Read all matching files in *directory* concurrently and concatenate them."""
    paths = _list_data_files(directory, extensions)
    if not paths:
        supported = ", ".join(sorted(extensions or SUPPORTED_EXTENSIONS))
        raise FileNotFoundError(
            f"No supported files ({supported}) found in {directory}"
        )
    frames = asyncio.run(_load_all_files(paths, transform))
    return pl.concat(frames)
