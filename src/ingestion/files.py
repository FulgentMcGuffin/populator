"""Read parquet, CSV, and feather files from local directories."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Sequence
from pathlib import Path

import polars as pl

from .transforms import FileTransform, IngestionTransform, compose_transforms

SUPPORTED_EXTENSIONS = frozenset({".parquet", ".csv", ".feather", ".ipc"})

_READERS: dict[str, Callable[[str], pl.DataFrame]] = {
    ".parquet": pl.read_parquet,
    ".csv": pl.read_csv,
    ".feather": pl.read_ipc,
    ".ipc": pl.read_ipc,
}


def read_local_file(
    path: str | Path,
    *,
    csv_infer_schema_length: int | None = None,
) -> pl.DataFrame:
    """Read a single parquet, CSV, or feather file into a Polars DataFrame."""
    suffix = Path(path).suffix.lower()
    if suffix == ".csv" and csv_infer_schema_length is not None:
        return pl.read_csv(str(path), infer_schema_length=csv_infer_schema_length)
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
    *,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
) -> list[pl.DataFrame]:
    async def _read_one(path: str) -> pl.DataFrame:
        def _read() -> pl.DataFrame:
            df = read_local_file(
                path,
                csv_infer_schema_length=csv_infer_schema_length,
            )
            if transform is not None:
                df = transform(path, df)
            return df

        return await asyncio.to_thread(_read)

    if not progress:
        return list(await asyncio.gather(*[_read_one(path) for path in paths]))

    from tqdm import tqdm

    frames: list[pl.DataFrame | None] = [None] * len(paths)
    bar = tqdm(total=len(paths), desc="Reading files", unit="file")

    async def _read_indexed(index: int, path: str) -> None:
        frames[index] = await _read_one(path)
        bar.set_postfix(file=Path(path).name, refresh=False)
        bar.update(1)

    try:
        await asyncio.gather(
            *[_read_indexed(index, path) for index, path in enumerate(paths)]
        )
    finally:
        bar.close()

    return [frame for frame in frames if frame is not None]


def load_files_from_dir(
    directory: str | Path,
    *,
    extensions: frozenset[str] | set[str] | None = None,
    transforms: Sequence[IngestionTransform] | None = None,
    transform: FileTransform | None = None,
    progress: bool = False,
    csv_infer_schema_length: int | None = None,
) -> pl.DataFrame:
    """Read all matching files in *directory* concurrently and concatenate them."""
    paths = _list_data_files(directory, extensions)
    if not paths:
        supported = ", ".join(sorted(extensions or SUPPORTED_EXTENSIONS))
        raise FileNotFoundError(
            f"No supported files ({supported}) found in {directory}"
        )
    file_transform = compose_transforms(transforms, file_transform=transform)
    if progress:
        from tqdm import tqdm

        tqdm.write(
            f"Found {len(paths)} file(s) in {directory}; applying transforms and concatenating"
        )
    frames = asyncio.run(
        _load_all_files(
            paths,
            file_transform,
            progress=progress,
            csv_infer_schema_length=csv_infer_schema_length,
        )
    )
    return pl.concat(frames)
