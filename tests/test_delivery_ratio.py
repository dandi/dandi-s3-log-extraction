import math
import pathlib

import pandas
import pytest

from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    _compute_delivery_ratio_percentiles,
    _summarize_dandiset_delivery_ratio,
)

PERCENTILE_COLUMN_NAMES = (
    "delivery_ratio_p10",
    "delivery_ratio_p25",
    "delivery_ratio_p50",
    "delivery_ratio_p75",
    "delivery_ratio_p90",
)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("delivery_ratios", "expected_values"),
    [
        # Several assets interpolate linearly between data points
        ([1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0], (1.6, 2.5, 4.0, 7.5, 14.0)),
        # Two assets
        ([2.0, 3.0], (2.1, 2.25, 2.5, 2.75, 2.9)),
        # A single usable asset yields that asset's ratio for all five percentiles
        ([3.7], (3.7, 3.7, 3.7, 3.7, 3.7)),
    ],
)
def test_compute_delivery_ratio_percentiles(delivery_ratios: list[float], expected_values: tuple[float, ...]) -> None:
    percentiles = _compute_delivery_ratio_percentiles(delivery_ratios)

    assert tuple(percentiles.keys()) == PERCENTILE_COLUMN_NAMES
    for column_name, expected_value in zip(PERCENTILE_COLUMN_NAMES, expected_values):
        assert percentiles[column_name] == pytest.approx(expected_value)


@pytest.mark.ai_generated
def test_compute_delivery_ratio_percentiles_zero_assets() -> None:
    percentiles = _compute_delivery_ratio_percentiles([])

    assert tuple(percentiles.keys()) == PERCENTILE_COLUMN_NAMES
    for column_name in PERCENTILE_COLUMN_NAMES:
        assert math.isnan(percentiles[column_name])


def _write_blob_directory(*, parent: pathlib.Path, blob_id: str, bytes_sent: list[int]) -> pathlib.Path:
    blob_directory = parent / blob_id
    blob_directory.mkdir(parents=True)
    (blob_directory / "bytes_sent.txt").write_text("\n".join(str(value) for value in bytes_sent))
    return blob_directory


@pytest.mark.ai_generated
def test_summarize_dandiset_delivery_ratio_skips_unusable_assets(tmp_path: pathlib.Path) -> None:
    extraction_directory = tmp_path / "extraction"
    extraction_directory.mkdir()

    # Two usable assets (ratios 2.0 and 3.0), one with zero size, and one with a missing size; the latter two
    # must be excluded from the percentile computation
    blob_directories = [
        _write_blob_directory(parent=extraction_directory, blob_id="aaa", bytes_sent=[40, 60]),
        _write_blob_directory(parent=extraction_directory, blob_id="bbb", bytes_sent=[100, 200]),
        _write_blob_directory(parent=extraction_directory, blob_id="ccc", bytes_sent=[999]),
        _write_blob_directory(parent=extraction_directory, blob_id="ddd", bytes_sent=[50]),
    ]
    blob_id_to_size = {"aaa": 50, "bbb": 100, "ccc": 0}  # "ddd" is intentionally absent (missing size)

    summary_file_path = tmp_path / "summaries" / "000000" / "delivery_ratio.tsv"
    _summarize_dandiset_delivery_ratio(
        blob_directories=blob_directories,
        summary_file_path=summary_file_path,
        blob_id_to_size=blob_id_to_size,
    )

    summary_table = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert list(summary_table.columns) == [*PERCENTILE_COLUMN_NAMES, "delivery_ratio_weighted"]
    assert len(summary_table) == 1

    row = summary_table.iloc[0]
    expected_percentiles = (2.1, 2.25, 2.5, 2.75, 2.9)
    for column_name, expected_value in zip(PERCENTILE_COLUMN_NAMES, expected_percentiles):
        assert row[column_name] == pytest.approx(expected_value)
    # Volume-weighted ratio is the summed delivered bytes (100 + 300) over the summed usable sizes (50 + 100)
    assert row["delivery_ratio_weighted"] == pytest.approx(400 / 150)


@pytest.mark.ai_generated
def test_summarize_dandiset_delivery_ratio_single_asset(tmp_path: pathlib.Path) -> None:
    extraction_directory = tmp_path / "extraction"
    extraction_directory.mkdir()

    blob_directories = [_write_blob_directory(parent=extraction_directory, blob_id="aaa", bytes_sent=[150])]
    blob_id_to_size = {"aaa": 100}

    summary_file_path = tmp_path / "summaries" / "000000" / "delivery_ratio.tsv"
    _summarize_dandiset_delivery_ratio(
        blob_directories=blob_directories,
        summary_file_path=summary_file_path,
        blob_id_to_size=blob_id_to_size,
    )

    row = pandas.read_table(filepath_or_buffer=summary_file_path).iloc[0]
    for column_name in PERCENTILE_COLUMN_NAMES:
        assert row[column_name] == pytest.approx(1.5)
    assert row["delivery_ratio_weighted"] == pytest.approx(1.5)


@pytest.mark.ai_generated
def test_summarize_dandiset_delivery_ratio_zero_usable_assets(tmp_path: pathlib.Path) -> None:
    extraction_directory = tmp_path / "extraction"
    extraction_directory.mkdir()

    # The asset was accessed but its size cannot be resolved, so the row is all NaN but still has every column
    blob_directories = [_write_blob_directory(parent=extraction_directory, blob_id="aaa", bytes_sent=[150])]
    blob_id_to_size: dict[str, int | None] = {}

    summary_file_path = tmp_path / "summaries" / "000000" / "delivery_ratio.tsv"
    _summarize_dandiset_delivery_ratio(
        blob_directories=blob_directories,
        summary_file_path=summary_file_path,
        blob_id_to_size=blob_id_to_size,
    )

    summary_table = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert list(summary_table.columns) == [*PERCENTILE_COLUMN_NAMES, "delivery_ratio_weighted"]
    row = summary_table.iloc[0]
    for column_name in [*PERCENTILE_COLUMN_NAMES, "delivery_ratio_weighted"]:
        assert math.isnan(row[column_name])
