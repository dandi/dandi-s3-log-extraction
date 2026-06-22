import json
import math
import pathlib

import pandas
import pytest

import dandi_s3_log_extraction
from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    DELIVERY_RATIO_PERCENTILE_COLUMN,
    _compute_delivery_ratio_fields,
    _compute_delivery_ratio_percentiles,
    _format_delivery_ratio_percentiles,
    _pool_archive_delivery_ratios,
    _read_by_asset_delivery_ratios,
)

PERCENTILE_COLUMN_NAMES = (
    "delivery_ratio_p10",
    "delivery_ratio_p25",
    "delivery_ratio_p50",
    "delivery_ratio_p75",
    "delivery_ratio_p90",
)
DELIVERY_RATIO_FIELD_NAMES = (*PERCENTILE_COLUMN_NAMES, "delivery_ratio_weighted")


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


def _write_by_asset_tsv(*, directory: pathlib.Path, rows: list[tuple[str, int, float]]) -> pathlib.Path:
    directory.mkdir(parents=True, exist_ok=True)
    table = pandas.DataFrame(
        data={
            "asset_path": [row[0] for row in rows],
            "bytes_sent": [row[1] for row in rows],
            "number_of_requests": ["<50"] * len(rows),
            "number_of_downloads": ["<50"] * len(rows),
            "delivery_ratio": [row[2] for row in rows],
        }
    )
    by_asset_file_path = directory / "by_asset.tsv"
    table.to_csv(path_or_buf=by_asset_file_path, mode="w", sep="\t", header=True, index=False)
    return by_asset_file_path


@pytest.mark.ai_generated
def test_read_by_asset_delivery_ratios_excludes_undetermined_and_missing(tmp_path: pathlib.Path) -> None:
    by_asset_file_path = _write_by_asset_tsv(
        directory=tmp_path / "000001",
        rows=[
            ("sub-1/a.nwb", 100, 2.0),
            ("sub-1/b.nwb", 300, 3.0),
            ("sub-1/c.nwb", 7, float("nan")),  # size could not be resolved
            ("undetermined", 50, float("nan")),  # aggregated bucket, not a single asset
        ],
    )

    delivery_ratios, bytes_delivered = _read_by_asset_delivery_ratios(by_asset_file_path)

    assert delivery_ratios == [2.0, 3.0]
    assert bytes_delivered == [100, 300]


@pytest.mark.ai_generated
def test_read_by_asset_delivery_ratios_missing_file() -> None:
    delivery_ratios, bytes_delivered = _read_by_asset_delivery_ratios(pathlib.Path("does_not_exist.tsv"))

    assert delivery_ratios == []
    assert bytes_delivered == []


@pytest.mark.ai_generated
def test_compute_delivery_ratio_fields() -> None:
    fields = _compute_delivery_ratio_fields(delivery_ratios=[2.0, 3.0], bytes_delivered=[100, 300])

    assert tuple(fields.keys()) == DELIVERY_RATIO_FIELD_NAMES
    expected_percentiles = (2.1, 2.25, 2.5, 2.75, 2.9)
    for column_name, expected_value in zip(PERCENTILE_COLUMN_NAMES, expected_percentiles):
        assert fields[column_name] == pytest.approx(expected_value)
    # Volume-weighted ratio is summed delivered bytes (100 + 300) over summed sizes (100/2 + 300/3)
    assert fields["delivery_ratio_weighted"] == pytest.approx(400 / 150)


@pytest.mark.ai_generated
def test_compute_delivery_ratio_fields_single_asset() -> None:
    fields = _compute_delivery_ratio_fields(delivery_ratios=[1.5], bytes_delivered=[150])

    for column_name in PERCENTILE_COLUMN_NAMES:
        assert fields[column_name] == pytest.approx(1.5)
    assert fields["delivery_ratio_weighted"] == pytest.approx(1.5)


@pytest.mark.ai_generated
def test_compute_delivery_ratio_fields_zero_usable_assets() -> None:
    fields = _compute_delivery_ratio_fields(delivery_ratios=[], bytes_delivered=[])

    for column_name in DELIVERY_RATIO_FIELD_NAMES:
        assert math.isnan(fields[column_name])


@pytest.mark.ai_generated
def test_delivery_ratio_percentile_column_name() -> None:
    assert DELIVERY_RATIO_PERCENTILE_COLUMN == "delivery_ratio(p10,p25,p50,p75,p90)"


@pytest.mark.ai_generated
def test_format_delivery_ratio_percentiles() -> None:
    fields = _compute_delivery_ratio_fields(delivery_ratios=[2.0, 3.0], bytes_delivered=[100, 300])

    assert _format_delivery_ratio_percentiles(fields) == "2.1,2.25,2.5,2.75,2.9"


@pytest.mark.ai_generated
def test_format_delivery_ratio_percentiles_zero_usable_assets() -> None:
    fields = _compute_delivery_ratio_fields(delivery_ratios=[], bytes_delivered=[])

    # Every percentile is empty so the comma-separated tuple keeps its five slots
    assert _format_delivery_ratio_percentiles(fields) == ",,,,"


@pytest.mark.ai_generated
def test_pool_archive_delivery_ratios_skips_archive_directory(tmp_path: pathlib.Path) -> None:
    _write_by_asset_tsv(directory=tmp_path / "000001", rows=[("sub-1/a.nwb", 100, 2.0)])
    _write_by_asset_tsv(directory=tmp_path / "000002", rows=[("sub-2/b.nwb", 300, 3.0)])
    # An archive rollup of by_asset.tsv must never be pooled back into the archive computation
    _write_by_asset_tsv(directory=tmp_path / "archive", rows=[("sub-x/x.nwb", 999, 9.0)])

    delivery_ratios, bytes_delivered = _pool_archive_delivery_ratios(tmp_path)

    assert sorted(delivery_ratios) == [2.0, 3.0]
    assert sorted(bytes_delivered) == [100, 300]


def _write_minimal_dataset_summary(*, summary_directory: pathlib.Path, dataset_id: str, by_asset_rows) -> None:
    dataset_directory = summary_directory / dataset_id
    dataset_directory.mkdir(parents=True, exist_ok=True)

    pandas.DataFrame(
        data={"region": ["US/east"], "bytes_sent": [123], "number_of_requests": [3], "number_of_downloads": [1]}
    ).to_csv(path_or_buf=dataset_directory / "by_region.tsv", mode="w", sep="\t", header=True, index=False)

    pandas.DataFrame(
        data={"date": ["2024-01-01"], "bytes_sent": [123], "number_of_requests": [3], "number_of_downloads": [1]}
    ).to_csv(path_or_buf=dataset_directory / "by_day.tsv", mode="w", sep="\t", header=True, index=False)

    (dataset_directory / "requester_count.tsv").write_text("<50")

    _write_by_asset_tsv(directory=dataset_directory, rows=by_asset_rows)


@pytest.mark.ai_generated
def test_generate_dandiset_totals_adds_delivery_ratio_fields(tmp_path: pathlib.Path) -> None:
    summary_directory = tmp_path / "summaries"
    _write_minimal_dataset_summary(
        summary_directory=summary_directory,
        dataset_id="000001",
        by_asset_rows=[("sub-1/a.nwb", 100, 2.0), ("sub-1/b.nwb", 300, 3.0), ("undetermined", 50, float("nan"))],
    )
    # A Dandiset whose only accessed asset has an unresolved size yields no usable asset
    _write_minimal_dataset_summary(
        summary_directory=summary_directory,
        dataset_id="000002",
        by_asset_rows=[("sub-2/c.nwb", 10, float("nan"))],
    )

    dandi_s3_log_extraction.summarize.generate_dandiset_totals(cache_directory=tmp_path)

    all_dataset_totals = json.loads((summary_directory / "totals.json").read_text())

    expected_percentiles = (2.1, 2.25, 2.5, 2.75, 2.9)
    for column_name, expected_value in zip(PERCENTILE_COLUMN_NAMES, expected_percentiles):
        assert all_dataset_totals["000001"][column_name] == pytest.approx(expected_value)
    assert all_dataset_totals["000001"]["delivery_ratio_weighted"] == pytest.approx(400 / 150)

    # Zero usable assets means every delivery ratio field is null
    for field_name in DELIVERY_RATIO_FIELD_NAMES:
        assert all_dataset_totals["000002"][field_name] is None


@pytest.mark.ai_generated
def test_generate_archive_summaries_and_totals_pool_across_dandisets(tmp_path: pathlib.Path) -> None:
    summary_directory = tmp_path / "summaries"
    _write_minimal_dataset_summary(
        summary_directory=summary_directory,
        dataset_id="000001",
        by_asset_rows=[("sub-1/a.nwb", 100, 2.0), ("sub-1/b.nwb", 300, 3.0), ("undetermined", 50, float("nan"))],
    )
    _write_minimal_dataset_summary(
        summary_directory=summary_directory,
        dataset_id="000002",
        by_asset_rows=[("sub-2/c.nwb", 200, 4.0)],
    )

    dandi_s3_log_extraction.summarize.generate_archive_summaries(cache_directory=tmp_path)
    dandi_s3_log_extraction.summarize.generate_archive_totals(cache_directory=tmp_path)

    expected_percentiles = (2.2, 2.5, 3.0, 3.5, 3.8)

    archive_delivery_ratio = pandas.read_table(filepath_or_buffer=summary_directory / "archive" / "delivery_ratio.tsv")
    # The TSV reports the five percentiles as one comma-separated column plus the weighted scalar
    assert list(archive_delivery_ratio.columns) == [DELIVERY_RATIO_PERCENTILE_COLUMN, "delivery_ratio_weighted"]
    row = archive_delivery_ratio.iloc[0]
    assert str(row[DELIVERY_RATIO_PERCENTILE_COLUMN]) == ",".join(str(value) for value in expected_percentiles)
    assert row["delivery_ratio_weighted"] == pytest.approx(600 / 200)

    archive_totals = json.loads((summary_directory / "archive_totals.json").read_text())
    for column_name, expected_value in zip(PERCENTILE_COLUMN_NAMES, expected_percentiles):
        assert archive_totals[column_name] == pytest.approx(expected_value)
    assert archive_totals["delivery_ratio_weighted"] == pytest.approx(600 / 200)
