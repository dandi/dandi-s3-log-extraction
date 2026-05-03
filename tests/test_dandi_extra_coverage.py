"""Tests for dandi_s3_log_extraction covering remaining uncovered code paths."""

import gzip
import json
import os
import pathlib
import warnings
from unittest.mock import MagicMock, patch

import pandas
import pytest

import dandi_s3_log_extraction
import dandi_s3_log_extraction.summarize
from dandi_s3_log_extraction._parallel._utils import _handle_max_workers
from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    _summarize_archive_by_asset_type_per_week,
    _summarize_dandiset_by_asset,
    _summarize_dandiset_by_day,
    _summarize_dandiset_by_region,
)

# ─── _handle_max_workers ──────────────────────────────────────────────────────


@pytest.mark.ai_generated
def test_handle_max_workers_zero_warns() -> None:
    """workers=0 raises a warning and falls back to -2, returning cpu_count - 1."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = _handle_max_workers(workers=0)
    assert any("0" in str(w.message) for w in caught)
    cpu_count = os.cpu_count()
    assert result == cpu_count - 1


@pytest.mark.ai_generated
def test_handle_max_workers_negative() -> None:
    """Negative workers value produces cpu-relative result."""
    cpu_count = os.cpu_count()
    result = _handle_max_workers(workers=-2)
    assert result == cpu_count - 1


@pytest.mark.ai_generated
def test_handle_max_workers_exceeds_cpu_count() -> None:
    """workers > cpu_count is capped at cpu_count."""
    cpu_count = os.cpu_count()
    result = _handle_max_workers(workers=cpu_count + 100)
    assert result == cpu_count


# ─── DandiRemoteS3LogAccessExtractor ─────────────────────────────────────────


@pytest.mark.ai_generated
@pytest.mark.skipif(
    not os.environ.get("S3_LOG_EXTRACTION_PASSWORD"),
    reason="S3_LOG_EXTRACTION_PASSWORD not set",
)
def test_dandi_remote_extractor_init(tmp_path: pathlib.Path) -> None:
    """DandiRemoteS3LogAccessExtractor.__init__ sets expected attributes."""
    from dandi_s3_log_extraction.extractors import DandiRemoteS3LogAccessExtractor

    extractor = DandiRemoteS3LogAccessExtractor(cache_directory=tmp_path)
    assert extractor._relative_script_path.exists()
    assert "IPS_TO_SKIP_REGEX" in extractor._awk_env
    assert len(extractor._awk_env["IPS_TO_SKIP_REGEX"]) > 0


# ─── generate_dandiset_summaries error cases ─────────────────────────────────


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_pick_and_skip_raises(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries raises ValueError when both pick and skip are specified."""
    with pytest.raises(ValueError, match="Cannot specify"):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            pick=["000001"],
            skip=["000002"],
            workers=1,
        )


def _make_fake_gz_response(content_map: dict, status_code: int = 200) -> MagicMock:
    """Build a mock requests response with a gzip-compressed JSON body."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.content = gzip.compress(json.dumps(content_map).encode())
    mock_response.json.return_value = {"error": "request failed"}
    return mock_response


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_http_error_determinable(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries raises RuntimeError when content URL returns non-200 status."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.json.return_value = {"error": "not found"}

    with (
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries.requests.get",
            return_value=mock_response,
        ),
        pytest.raises(RuntimeError),
    ):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
        )


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_http_error_undetermined(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries raises RuntimeError for unassociated when URL returns non-200."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {"error": "server error"}

    with (
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries.requests.get",
            return_value=mock_response,
        ),
        pytest.raises(RuntimeError),
    ):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
            unassociated=True,
        )


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_pick_branch(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries uses pick list when pick is set (skip is None)."""
    with (
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries.requests.get",
            return_value=_make_fake_gz_response({}),
        ),
        patch("dandi.dandiapi.DandiAPIClient") as mock_client_cls,
        patch("dandi_s3_log_extraction.summarize._generate_dandiset_summaries._summarize_dandiset"),
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries._summarize_archive_by_asset_type_per_week"
        ),
    ):
        mock_client_cls.return_value = MagicMock()

        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
            pick=["000001"],
        )


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_skip_branch(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries uses skip list when skip is set (pick is None)."""
    mock_dandiset = MagicMock()
    mock_dandiset.identifier = "000002"  # Not in skip list, so included

    mock_client = MagicMock()
    mock_client.get_dandisets.return_value = [mock_dandiset]

    with (
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries.requests.get",
            return_value=_make_fake_gz_response({}),
        ),
        patch("dandi.dandiapi.DandiAPIClient", return_value=mock_client),
        patch("dandi_s3_log_extraction.summarize._generate_dandiset_summaries._summarize_dandiset"),
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries._summarize_archive_by_asset_type_per_week"
        ),
    ):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
            skip=["000001"],
        )


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_parallel_branch(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries uses ProcessPoolExecutor when workers > 1."""
    mock_client = MagicMock()
    mock_client.get_dandisets.return_value = []  # empty → no actual work submitted

    with (
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries.requests.get",
            return_value=_make_fake_gz_response({}),
        ),
        patch("dandi.dandiapi.DandiAPIClient", return_value=mock_client),
        patch(
            "dandi_s3_log_extraction.summarize._generate_dandiset_summaries._summarize_archive_by_asset_type_per_week"
        ),
    ):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=2,
        )


# ─── _summarize_archive_by_asset_type_per_week edge cases ────────────────────


@pytest.mark.ai_generated
def test_summarize_archive_empty_directory(tmp_path: pathlib.Path) -> None:
    """_summarize_archive_by_asset_type_per_week returns early when no TSV files exist."""
    summary_dir = tmp_path / "summaries"
    summary_dir.mkdir()

    _summarize_archive_by_asset_type_per_week(summary_directory=summary_dir)

    assert not (summary_dir / "archive" / "by_asset_type_per_week.tsv").exists()


@pytest.mark.ai_generated
def test_summarize_archive_only_week_start_column(tmp_path: pathlib.Path) -> None:
    """_summarize_archive_by_asset_type_per_week returns early when only week_start column present."""
    summary_dir = tmp_path / "summaries"
    dandiset_dir = summary_dir / "000001"
    dandiset_dir.mkdir(parents=True)

    tsv_data = pandas.DataFrame({"week_start": ["2020-01-01", "2020-01-08"]})
    tsv_data.to_csv(path_or_buf=dandiset_dir / "by_asset_type_per_week.tsv", sep="\t", index=False)

    _summarize_archive_by_asset_type_per_week(summary_directory=summary_dir)

    assert not (summary_dir / "archive" / "by_asset_type_per_week.tsv").exists()


# ─── generate_dandiset_totals edge cases ──────────────────────────────────────


@pytest.mark.ai_generated
def test_generate_dandiset_totals_empty_directory(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_totals returns early when no dandiset directories are present."""
    summary_dir = tmp_path / "summaries"
    summary_dir.mkdir()

    dandi_s3_log_extraction.summarize.generate_dandiset_totals(summary_directory=summary_dir)

    assert not (summary_dir / "totals.json").exists()


@pytest.mark.ai_generated
def test_generate_dandiset_totals_non_dir_item(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_totals skips non-directory items in the summary directory."""
    summary_dir = tmp_path / "summaries"
    summary_dir.mkdir()

    # A regular file (not a directory) – should be skipped via `continue`
    (summary_dir / "some_file.json").write_text("{}")

    # A valid dandiset directory with data
    dandiset_dir = summary_dir / "000001"
    dandiset_dir.mkdir()
    region_tsv = pandas.DataFrame({"region": ["US/California"], "bytes_sent": [100], "number_of_requests": [3]})
    region_tsv.to_csv(path_or_buf=dandiset_dir / "by_region.tsv", sep="\t", index=False)

    dandi_s3_log_extraction.summarize.generate_dandiset_totals(summary_directory=summary_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert "000001" in totals
    assert "some_file" not in totals
    assert totals["000001"]["total_number_of_requests"] == 3


@pytest.mark.ai_generated
def test_generate_dandiset_totals_various_regions(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_totals handles VPN, GitHub, unknown, normal, and AWS regions."""
    summary_dir = tmp_path / "summaries"
    dandiset_dir = summary_dir / "000001"
    dandiset_dir.mkdir(parents=True)

    # archive dir should be skipped (line 36: if dandiset_id == "archive": continue)
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir()

    region_tsv = pandas.DataFrame(
        {
            "region": ["VPN", "GitHub", "unknown", "US/California", "AWS/eu-west-1"],
            "bytes_sent": [100, 200, 300, 400, 500],
            "number_of_requests": [1, 2, 3, 4, 5],
        }
    )
    region_tsv.to_csv(path_or_buf=dandiset_dir / "by_region.tsv", sep="\t", index=False)

    dandi_s3_log_extraction.summarize.generate_dandiset_totals(summary_directory=summary_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert "000001" in totals
    assert totals["000001"]["total_bytes_sent"] == 1500
    # US/California → "US", AWS/eu-west-1 → "EU" (via AWS logic)
    assert totals["000001"]["number_of_unique_countries"] == 2
    assert totals["000001"]["total_number_of_requests"] == 15


# ─── number_of_requests column ───────────────────────────────────────────────


@pytest.mark.ai_generated
def test_summarize_dandiset_by_day_number_of_requests(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_day includes number_of_requests column with correct counts."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    (blob_dir / "timestamps.txt").write_text("200101050635\n200101224258\n200109050635\n")
    (blob_dir / "bytes_sent.txt").write_text("100\n200\n300\n")

    summary_file_path = tmp_path / "by_day.tsv"
    _summarize_dandiset_by_day(blob_directories=[blob_dir], summary_file_path=summary_file_path)

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert "number_of_requests" in result.columns
    row_2020_01_01 = result[result["date"] == "2020-01-01"].iloc[0]
    assert row_2020_01_01["bytes_sent"] == 300
    assert row_2020_01_01["number_of_requests"] == 2
    row_2020_01_09 = result[result["date"] == "2020-01-09"].iloc[0]
    assert row_2020_01_09["bytes_sent"] == 300
    assert row_2020_01_09["number_of_requests"] == 1


@pytest.mark.ai_generated
def test_summarize_dandiset_by_asset_number_of_requests(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_asset includes number_of_requests column with correct counts."""
    blob_dir = tmp_path / "blobid1"
    blob_dir.mkdir()
    (blob_dir / "bytes_sent.txt").write_text("512\n1024\n256\n")

    blob_id_to_asset_path = {"blobid1": "path/to/asset.nwb"}
    summary_file_path = tmp_path / "by_asset.tsv"
    _summarize_dandiset_by_asset(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        blob_id_to_asset_path=blob_id_to_asset_path,
    )

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert "number_of_requests" in result.columns
    assert result.iloc[0]["bytes_sent"] == 1792
    assert result.iloc[0]["number_of_requests"] == 3


@pytest.mark.ai_generated
def test_summarize_dandiset_by_region_number_of_requests(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_region includes number_of_requests column with correct counts."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    (blob_dir / "indexed_ips.txt").write_text("1\n2\n1\n")
    (blob_dir / "bytes_sent.txt").write_text("100\n200\n300\n")

    index_to_region = {1: "US/California", 2: "US/New York"}
    summary_file_path = tmp_path / "by_region.tsv"
    _summarize_dandiset_by_region(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        index_to_region=index_to_region,
    )

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert "number_of_requests" in result.columns
    ca_row = result[result["region"] == "US/California"].iloc[0]
    assert ca_row["bytes_sent"] == 400
    assert ca_row["number_of_requests"] == 2
    ny_row = result[result["region"] == "US/New York"].iloc[0]
    assert ny_row["bytes_sent"] == 200
    assert ny_row["number_of_requests"] == 1
