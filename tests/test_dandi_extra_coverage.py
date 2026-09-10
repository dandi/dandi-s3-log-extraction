"""Tests for dandi_s3_log_extraction covering remaining uncovered code paths."""

import json
import os
import pathlib
import warnings
from unittest.mock import MagicMock, patch

import pandas
import pytest
from s3_log_extraction.ip_utils import MappingRegionResolver

import dandi_s3_log_extraction
import dandi_s3_log_extraction.summarize
from dandi_s3_log_extraction._parallel._utils import _handle_max_workers
from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    _collect_unique_ips,
    _collect_views_by_blob_directory,
    _summarize_archive_by_asset_type_per_week,
    _summarize_archive_unique_requester_count,
    _summarize_dandiset_by_asset,
    _summarize_dandiset_by_day,
    _summarize_dandiset_by_region,
    _summarize_dandiset_unique_requester_count,
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
def test_dandi_remote_extractor_init(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """DandiRemoteS3LogAccessExtractor.__init__ sets expected attributes."""
    from dandi_s3_log_extraction.extractors import DandiRemoteS3LogAccessExtractor

    monkeypatch.setenv("IPS_TO_SKIP", "192.168.0.1|10.0.0.1")

    extractor = DandiRemoteS3LogAccessExtractor(cache_directory=tmp_path)
    assert extractor._relative_script_path.exists()
    assert "IPS_TO_SKIP_REGEX" in extractor._awk_env
    assert extractor._awk_env["IPS_TO_SKIP_REGEX"] == "192.168.0.1|10.0.0.1"
    assert extractor.use_encryption is False


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


def _make_fake_jsonl_response(content_map: dict, status_code: int = 200) -> MagicMock:
    """Build a mock requests response with a JSON Lines body."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.text = "\n".join(
        json.dumps({content_id: usage_dandiset_id_to_path})
        for content_id, usage_dandiset_id_to_path in content_map.items()
    )
    return mock_response


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_http_error_determinable(tmp_path: pathlib.Path) -> None:
    """generate_dandiset_summaries raises RuntimeError when content URL returns non-200 status."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "not found"

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
    mock_response.text = "server error"

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
            return_value=_make_fake_jsonl_response({}),
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
            return_value=_make_fake_jsonl_response({}),
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
            return_value=_make_fake_jsonl_response({}),
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


# ─── activity columns ────────────────────────────────────────────────────────


def _write_blob_directory(
    *, blob_directory: pathlib.Path, timestamps: list[str], ips: list[str], bytes_sent: list[int], downloads: list[int]
) -> None:
    """Write the line-aligned per-request files of a single blob."""
    blob_directory.mkdir(parents=True, exist_ok=True)
    (blob_directory / "timestamps.txt").write_text("\n".join(timestamps) + "\n")
    (blob_directory / "ips.txt").write_text("\n".join(ips) + "\n")
    (blob_directory / "bytes_sent.txt").write_text("\n".join(str(value) for value in bytes_sent) + "\n")
    (blob_directory / "download.txt").write_text("\n".join(str(value) for value in downloads) + "\n")


@pytest.mark.ai_generated
def test_summarize_dandiset_by_day_true_counts(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_day reports true request, download, and view counts."""
    blob_dir = tmp_path / "blob1"
    _write_blob_directory(
        blob_directory=blob_dir,
        timestamps=["200101050635", "200101224258", "200109050635"],
        ips=["192.0.2.1", "192.0.2.1", "192.0.2.1"],
        bytes_sent=[100, 200, 300],
        downloads=[1, 0, 1],
    )

    summary_file_path = tmp_path / "by_day.tsv"
    _summarize_dandiset_by_day(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        views_by_blob_directory=_collect_views_by_blob_directory([blob_dir]),
    )

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    row_2020_01_01 = result[result["date"] == "2020-01-01"].iloc[0]
    assert row_2020_01_01["bytes_sent"] == 300
    assert row_2020_01_01["number_of_requests"] == 2
    assert row_2020_01_01["number_of_downloads"] == 1
    assert row_2020_01_01["number_of_views"] == 1
    row_2020_01_09 = result[result["date"] == "2020-01-09"].iloc[0]
    assert row_2020_01_09["bytes_sent"] == 300
    assert row_2020_01_09["number_of_requests"] == 1
    assert row_2020_01_09["number_of_downloads"] == 1
    assert row_2020_01_09["number_of_views"] == 0


@pytest.mark.ai_generated
def test_summarize_dandiset_by_asset_true_counts(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_asset reports true request, download, and view counts."""
    blob_dir = tmp_path / "blobid1"
    _write_blob_directory(
        blob_directory=blob_dir,
        timestamps=["200101050635", "200101224258", "200109050635"],
        ips=["192.0.2.1", "192.0.2.1", "192.0.2.1"],
        bytes_sent=[512, 1024, 256],
        downloads=[1, 0, 1],
    )

    blob_id_to_asset_path = {"blobid1": "path/to/asset.nwb"}
    summary_file_path = tmp_path / "by_asset.tsv"
    _summarize_dandiset_by_asset(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        blob_id_to_asset_path=blob_id_to_asset_path,
        views_by_blob_directory=_collect_views_by_blob_directory([blob_dir]),
    )

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    assert result.iloc[0]["bytes_sent"] == 1792
    assert result.iloc[0]["number_of_requests"] == 3
    assert result.iloc[0]["number_of_downloads"] == 2
    assert result.iloc[0]["number_of_views"] == 1


@pytest.mark.ai_generated
def test_summarize_dandiset_by_region_true_counts(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_region reports true counts and attributes each view to its requester region."""
    blob_dir = tmp_path / "blob1"
    _write_blob_directory(
        blob_directory=blob_dir,
        timestamps=["200101050635", "200101224258", "200109050635"],
        ips=["192.0.2.1", "192.0.2.2", "192.0.2.1"],
        bytes_sent=[100, 200, 300],
        downloads=[1, 0, 1],
    )

    ip_to_region = {"192.0.2.1": "US/California", "192.0.2.2": "US/New York"}
    summary_file_path = tmp_path / "by_region.tsv"
    _summarize_dandiset_by_region(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        region_resolver=MappingRegionResolver(ip_to_region),
        views_by_blob_directory=_collect_views_by_blob_directory([blob_dir]),
        region_disclosure_threshold=0,
    )

    result = pandas.read_table(filepath_or_buffer=summary_file_path)
    california_row = result[result["region"] == "US/California"].iloc[0]
    assert california_row["bytes_sent"] == 400
    assert california_row["number_of_requests"] == 2
    assert california_row["number_of_downloads"] == 2
    assert california_row["number_of_views"] == 0
    new_york_row = result[result["region"] == "US/New York"].iloc[0]
    assert new_york_row["bytes_sent"] == 200
    assert new_york_row["number_of_requests"] == 1
    assert new_york_row["number_of_downloads"] == 0
    assert new_york_row["number_of_views"] == 1


@pytest.mark.ai_generated
def test_summaries_label_unplaced_requesters_as_missing(tmp_path: pathlib.Path) -> None:
    """A requester the resolver cannot place is summarized as ``missing``, and still counts as a requester."""
    blob_dir = tmp_path / "blob1"
    _write_blob_directory(
        blob_directory=blob_dir,
        timestamps=["200101050635", "200101224258", "200109050635"],
        ips=["192.0.2.1", "192.0.2.2", "192.0.2.3"],
        bytes_sent=[100, 200, 300],
        downloads=[1, 0, 0],
    )
    ip_to_region = {"192.0.2.1": None, "192.0.2.2": "bogon", "192.0.2.3": "USA/CA"}

    by_region_file_path = tmp_path / "by_region.tsv"
    _summarize_dandiset_by_region(
        blob_directories=[blob_dir],
        summary_file_path=by_region_file_path,
        region_resolver=MappingRegionResolver(ip_to_region),
        views_by_blob_directory=_collect_views_by_blob_directory([blob_dir]),
        region_disclosure_threshold=0,
    )
    by_region = pandas.read_table(filepath_or_buffer=by_region_file_path)
    assert sorted(by_region["region"]) == ["USA/CA", "bogon", "missing"]

    requester_count_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[blob_dir],
        summary_file_path=requester_count_file_path,
        region_resolver=MappingRegionResolver(ip_to_region),
    )
    assert requester_count_file_path.read_text() == "3"


@pytest.mark.ai_generated
def test_summarize_dandiset_by_region_withheld_below_threshold(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_by_region does not publish when too few resolved regions are updated."""
    blob_dir = tmp_path / "blob1"
    _write_blob_directory(
        blob_directory=blob_dir,
        timestamps=["200101050635", "200101224258"],
        ips=["192.0.2.1", "192.0.2.2"],
        bytes_sent=[100, 200],
        downloads=[1, 0],
    )

    ip_to_region = {"192.0.2.1": "US/California", "192.0.2.2": "US/New York"}
    summary_file_path = tmp_path / "by_region.tsv"
    _summarize_dandiset_by_region(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        region_resolver=MappingRegionResolver(ip_to_region),
        views_by_blob_directory=_collect_views_by_blob_directory([blob_dir]),
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_collect_views_by_blob_directory_sessionizes_streaming_requests(tmp_path: pathlib.Path) -> None:
    """A view is a run of streaming requests from one IP no more than eight hours apart."""
    blob_dir = tmp_path / "blob1"
    _write_blob_directory(
        blob_directory=blob_dir,
        # Two streaming requests an hour apart, then a third a full day later, then a download
        timestamps=["200101050635", "200101060635", "200102060635", "200102070000"],
        ips=["192.0.2.1", "192.0.2.1", "192.0.2.1", "192.0.2.1"],
        bytes_sent=[100, 200, 300, 400],
        downloads=[0, 0, 0, 1],
    )

    views_by_blob_directory = _collect_views_by_blob_directory([blob_dir])

    assert views_by_blob_directory[blob_dir] == [("2020-01-01", "192.0.2.1"), ("2020-01-02", "192.0.2.1")]


@pytest.mark.ai_generated
def test_collect_views_by_blob_directory_skips_missing_blob_dir(tmp_path: pathlib.Path) -> None:
    """Blob directories that were never accessed have no extracted files and so no views."""
    assert _collect_views_by_blob_directory([tmp_path / "nonexistent"]) == {}


@pytest.mark.ai_generated
def test_collect_views_by_blob_directory_raises_on_incomplete_extraction(tmp_path: pathlib.Path) -> None:
    """An extraction cache written before 'download.txt' existed is reported rather than counted as zero."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    (blob_dir / "timestamps.txt").write_text("200101050635\n")
    (blob_dir / "ips.txt").write_text("192.0.2.1\n")
    (blob_dir / "bytes_sent.txt").write_text("100\n")

    with pytest.raises(RuntimeError, match="download.txt"):
        _collect_views_by_blob_directory([blob_dir])


# ─── _collect_unique_ips ─────────────────────────────────────────────────────


@pytest.mark.ai_generated
def test_collect_unique_ips_basic(tmp_path: pathlib.Path) -> None:
    """_collect_unique_ips returns the union of IPs across all blob directories."""
    blob_dir1 = tmp_path / "blob1"
    blob_dir1.mkdir()
    (blob_dir1 / "ips.txt").write_text("192.0.2.10\n192.0.2.20\n192.0.2.10\n")

    blob_dir2 = tmp_path / "blob2"
    blob_dir2.mkdir()
    (blob_dir2 / "ips.txt").write_text("192.0.2.20\n192.0.2.30\n")

    result = _collect_unique_ips(blob_directories=[blob_dir1, blob_dir2])
    assert result == {"192.0.2.10", "192.0.2.20", "192.0.2.30"}


@pytest.mark.ai_generated
def test_collect_unique_ips_missing_file(tmp_path: pathlib.Path) -> None:
    """_collect_unique_ips skips directories without ips.txt."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    # No ips.txt

    result = _collect_unique_ips(blob_directories=[blob_dir])
    assert result == set()


@pytest.mark.ai_generated
def test_collect_unique_ips_missing_dir(tmp_path: pathlib.Path) -> None:
    """_collect_unique_ips skips non-existent directories."""
    missing_dir = tmp_path / "nonexistent"
    result = _collect_unique_ips(blob_directories=[missing_dir])
    assert result == set()


# ─── _summarize_dandiset_unique_requester_count ───────────────────────────────


@pytest.mark.ai_generated
def test_summarize_dandiset_unique_requester_count_writes_small_count(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_unique_requester_count writes the true count even when it is small."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    (blob_dir / "ips.txt").write_text("192.0.2.10\n192.0.2.20\n192.0.2.10\n")  # 2 unique IPs

    summary_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
    )

    assert summary_file_path.read_text() == "2"


@pytest.mark.ai_generated
def test_summarize_dandiset_unique_requester_count_writes_large_count(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_unique_requester_count writes the true count for many requesters."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    unique_ips = "\n".join(f"192.0.2.{i}" for i in range(55))
    (blob_dir / "ips.txt").write_text(unique_ips)

    summary_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
    )

    assert summary_file_path.read_text() == "55"


@pytest.mark.ai_generated
def test_summarize_dandiset_unique_requester_count_no_ips(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_unique_requester_count returns early when no ips.txt exists."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    # No ips.txt file

    summary_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_summarize_dandiset_unique_requester_count_missing_blob_dir(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_unique_requester_count skips non-existent blob directories."""
    missing_dir = tmp_path / "nonexistent"

    summary_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[missing_dir],
        summary_file_path=summary_file_path,
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_summarize_dandiset_unique_requester_count_excludes_cloud_service_ips(tmp_path: pathlib.Path) -> None:
    """_summarize_dandiset_unique_requester_count excludes IPs attributed to cloud/hosting/VPN services."""
    blob_dir = tmp_path / "blob1"
    blob_dir.mkdir()
    # 55 "real" unique IPs plus a handful of cloud service IPs that should be excluded
    real_ips = [f"192.0.2.{i}" for i in range(55)]
    cloud_ips = ["198.51.100.1", "198.51.100.2", "198.51.100.3", "198.51.100.4"]
    (blob_dir / "ips.txt").write_text("\n".join(real_ips + cloud_ips))

    ip_to_region = {
        "198.51.100.1": "AWS/us-east-2",
        "198.51.100.2": "GCP/us-central1",
        "198.51.100.3": "GitHub",
        "198.51.100.4": "VPN",
    }

    summary_file_path = tmp_path / "requester_count.tsv"
    _summarize_dandiset_unique_requester_count(
        blob_directories=[blob_dir],
        summary_file_path=summary_file_path,
        region_resolver=MappingRegionResolver(ip_to_region),
    )

    # 55 real unique IPs (cloud service IPs excluded from the count)
    assert summary_file_path.read_text() == "55"


# ─── _summarize_archive_unique_requester_count ────────────────────────────────


@pytest.mark.ai_generated
def test_summarize_archive_unique_requester_count_true_union(tmp_path: pathlib.Path) -> None:
    """_summarize_archive_unique_requester_count computes true union of IPs across all blobs."""
    blob_dir1 = tmp_path / "blob1"
    blob_dir1.mkdir()
    (blob_dir1 / "ips.txt").write_text("192.0.2.10\n192.0.2.20\n")

    blob_dir2 = tmp_path / "blob2"
    blob_dir2.mkdir()
    (blob_dir2 / "ips.txt").write_text("192.0.2.20\n192.0.2.30\n")  # 192.0.2.20 is shared

    archive_file = tmp_path / "archive" / "requester_count.tsv"
    _summarize_archive_unique_requester_count(
        blob_directories=[blob_dir1, blob_dir2],
        summary_file_path=archive_file,
    )

    # 3 unique IPs (192.0.2.10, 192.0.2.20, 192.0.2.30)
    assert archive_file.read_text() == "3"


@pytest.mark.ai_generated
def test_summarize_archive_unique_requester_count_empty(tmp_path: pathlib.Path) -> None:
    """_summarize_archive_unique_requester_count returns early when no IPs exist."""
    archive_file = tmp_path / "archive" / "requester_count.tsv"
    _summarize_archive_unique_requester_count(
        blob_directories=[],
        summary_file_path=archive_file,
    )

    assert not archive_file.exists()


@pytest.mark.ai_generated
def test_summarize_archive_unique_requester_count_excludes_cloud_service_ips(tmp_path: pathlib.Path) -> None:
    """_summarize_archive_unique_requester_count excludes IPs attributed to cloud/hosting/VPN services."""
    blob_dir1 = tmp_path / "blob1"
    blob_dir1.mkdir()
    (blob_dir1 / "ips.txt").write_text("\n".join(f"192.0.2.{i}" for i in range(30)))

    blob_dir2 = tmp_path / "blob2"
    blob_dir2.mkdir()
    (blob_dir2 / "ips.txt").write_text("\n".join(f"192.0.3.{i}" for i in range(25)) + "\n203.0.113.1\n203.0.113.2\n")

    ip_to_region = {"203.0.113.1": "AWS/us-east-1", "203.0.113.2": "GitHub"}

    archive_file = tmp_path / "archive" / "requester_count.tsv"
    _summarize_archive_unique_requester_count(
        blob_directories=[blob_dir1, blob_dir2],
        summary_file_path=archive_file,
        region_resolver=MappingRegionResolver(ip_to_region),
    )

    # 55 real unique IPs (cloud service IPs excluded from the count)
    assert archive_file.read_text() == "55"
