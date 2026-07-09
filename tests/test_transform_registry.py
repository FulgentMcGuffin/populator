"""Tests for serializable transform registry."""

from __future__ import annotations

import pytest

from ingestion import (
    TRANSFORM_PRESETS,
    build_transform,
    build_transforms,
    describe_transforms,
    serialize_transforms,
)
from ingestion.transforms import PrefixedMeltTransform


def test_build_equity_preset_matches_populate_script_steps() -> None:
    transforms = build_transforms(preset="equity")

    assert len(transforms) == 5
    assert isinstance(transforms[1], PrefixedMeltTransform)
    assert transforms[1].group_column == "Stock"


def test_build_transforms_from_explicit_steps() -> None:
    steps = [
        {"type": "lit_column", "column": "dataset", "value": "ycs"},
        {"type": "file_source"},
    ]

    transforms = build_transforms(steps)

    assert len(transforms) == 2


def test_describe_transforms_rejects_both_steps_and_preset() -> None:
    with pytest.raises(ValueError, match="either"):
        build_transforms([{"type": "file_source"}], preset="none")


def test_serialize_transforms_round_trip() -> None:
    original_steps = TRANSFORM_PRESETS["equity"]
    transforms = build_transforms(original_steps)
    serialized = serialize_transforms(transforms)

    assert serialized == original_steps


def test_build_transform_unknown_type_raises() -> None:
    with pytest.raises(ValueError, match="Unknown transform type"):
        build_transform({"type": "not_a_transform"})


def test_describe_transforms_preset() -> None:
    steps = describe_transforms(preset="none")
    assert steps == []
