"""Tests for s3_log_extraction.summarize covering uncovered code paths."""

import json
import pathlib

import pandas
import pytest
import s3_log_extraction
from s3_log_extraction.ip_utils import MappingRegionResolver
from s3_log_extraction.summarize._generate_summaries import (
    _summarize_dataset_by_asset,
    _summarize_dataset_by_day,
    _summarize_dataset_by_region,
)


@pytest.mark.ai_generated
def test_generate_summaries_not_implemented(tmp_path: pathlib.Path) -> None:
    """generate_summaries with level != 0 raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        s3_log_extraction.summarize.generate_summaries(level=1, cache_directory=tmp_path)


@pytest.mark.ai_generated
def test_generate_summaries_with_data(tmp_path: pathlib.Path) -> None:
    """generate_summaries processes datasets with a mix of asset types."""
    extraction_dir = tmp_path / "extraction"

    # YYMMDDHHMMSS format: 200101050635 → 2020-01-01
    asset1 = extraction_dir / "dataset1" / "asset1"
    asset1.mkdir(parents=True)
    (asset1 / "timestamps.txt").write_text("200101050635\n200101224258\n")
    (asset1 / "bytes_sent.txt").write_text("512\n1526223\n")
    (asset1 / "ips.txt").write_text("192.0.2.1\n192.0.2.2\n")
    (asset1 / "download.txt").write_text("0\n1\n")

    asset2 = extraction_dir / "dataset2" / "asset2"
    asset2.mkdir(parents=True)
    (asset2 / "timestamps.txt").write_text("200101050635\n")
    (asset2 / "bytes_sent.txt").write_text("100\n")
    (asset2 / "ips.txt").write_text("192.0.2.1\n")
    (asset2 / "download.txt").write_text("1\n")

    region_resolver = MappingRegionResolver({"192.0.2.1": "US/California", "192.0.2.2": "unknown"})

    # A threshold of zero publishes the by-region summaries of these few regions
    s3_log_extraction.summarize.generate_summaries(
        cache_directory=tmp_path, use_encryption=False, region_disclosure_threshold=0, region_resolver=region_resolver
    )

    assert (tmp_path / "summaries" / "dataset1" / "by_day.tsv").exists()
    assert (tmp_path / "summaries" / "dataset1" / "by_asset.tsv").exists()
    assert (tmp_path / "summaries" / "dataset1" / "by_region.tsv").exists()

    by_asset = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "dataset1" / "by_asset.tsv")
    assert by_asset.iloc[0]["number_of_views"] == 1


@pytest.mark.ai_generated
def test_generate_summaries_incomplete_extraction_cache(tmp_path: pathlib.Path) -> None:
    """generate_summaries reports an asset extracted before 'download.txt' existed."""
    asset = tmp_path / "extraction" / "dataset1" / "asset1"
    asset.mkdir(parents=True)
    (asset / "timestamps.txt").write_text("200101050635\n")
    (asset / "bytes_sent.txt").write_text("512\n")
    (asset / "ips.txt").write_text("192.0.2.1\n")

    with pytest.raises(RuntimeError, match="download.txt"):
        s3_log_extraction.summarize.generate_summaries(
            cache_directory=tmp_path, use_encryption=False, region_resolver=MappingRegionResolver({})
        )


@pytest.mark.ai_generated
def test_summarize_dataset_by_asset_no_bytes_sent(tmp_path: pathlib.Path) -> None:
    """_summarize_dataset_by_asset skips asset directories that lack bytes_sent.txt."""
    # Asset directory with no bytes_sent.txt
    asset_dir = tmp_path / "extraction" / "dataset1" / "no_bytes_asset"
    asset_dir.mkdir(parents=True)
    # No bytes_sent.txt present

    summary_file_path = tmp_path / "summaries" / "dataset1" / "by_asset.tsv"

    _summarize_dataset_by_asset(
        asset_directories=[asset_dir],
        summary_file_path=summary_file_path,
        views_by_asset_directory={},
    )

    # No output because the only asset was skipped
    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_summarize_dataset_by_asset_empty_list(tmp_path: pathlib.Path) -> None:
    """_summarize_dataset_by_asset returns early when asset_directories is empty."""
    summary_file_path = tmp_path / "summaries" / "dataset1" / "by_asset.tsv"

    _summarize_dataset_by_asset(
        asset_directories=[],
        summary_file_path=summary_file_path,
        views_by_asset_directory={},
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_summarize_dataset_by_day_empty_result(tmp_path: pathlib.Path) -> None:
    """_summarize_dataset_by_day returns early when no timestamps are available."""
    asset_dir = tmp_path / "no_ts_asset"
    asset_dir.mkdir()
    (asset_dir / "bytes_sent.txt").write_text("100\n")
    # No timestamps.txt

    summary_file_path = tmp_path / "summaries" / "by_day.tsv"

    _summarize_dataset_by_day(
        asset_directories=[asset_dir],
        summary_file_path=summary_file_path,
        views_by_asset_directory={},
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_summarize_dataset_by_region_empty_result(tmp_path: pathlib.Path) -> None:
    """_summarize_dataset_by_region returns early when no indexed_ips are available."""
    asset_dir = tmp_path / "no_ips_asset"
    asset_dir.mkdir()
    # No indexed_ips.txt

    summary_file_path = tmp_path / "summaries" / "by_region.tsv"

    _summarize_dataset_by_region(
        asset_directories=[asset_dir],
        summary_file_path=summary_file_path,
        region_resolver=MappingRegionResolver({}),
        views_by_asset_directory={},
    )

    assert not summary_file_path.exists()


@pytest.mark.ai_generated
def test_generate_all_dataset_totals_all_region_types(tmp_path: pathlib.Path) -> None:
    """generate_all_dataset_totals handles VPN, GitHub, unknown, AWS, and normal regions."""
    summary_dir = tmp_path / "summaries"
    dataset_dir = summary_dir / "dataset1"
    dataset_dir.mkdir(parents=True)

    # File (not directory) - should be skipped via `continue`
    (summary_dir / "totals_old.json").write_text("{}")

    # Activity totals are read from the by-day summary, which is always published
    day_tsv = pandas.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02"],
            "bytes_sent": [500, 1000],
            "number_of_requests": [5, 10],
            "number_of_downloads": [2, 5],
            "number_of_views": [1, 3],
        }
    )
    day_tsv.to_csv(path_or_buf=dataset_dir / "by_day.tsv", sep="\t", index=False)

    # by_region.tsv with all region types
    region_tsv = pandas.DataFrame(
        {
            "region": ["VPN", "GitHub", "unknown", "US/California", "AWS/eu-west-1"],
            "bytes_sent": [100, 200, 300, 400, 500],
            "number_of_requests": [1, 2, 3, 4, 5],
            "number_of_downloads": [1, 1, 1, 2, 2],
            "number_of_views": [0, 1, 1, 1, 1],
        }
    )
    region_tsv.to_csv(path_or_buf=dataset_dir / "by_region.tsv", sep="\t", index=False)

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=tmp_path)

    output_path = summary_dir / "totals.json"
    assert output_path.exists()
    totals = json.loads(output_path.read_text())
    assert "dataset1" in totals
    assert totals["dataset1"]["total_bytes_sent"] == 1500
    assert totals["dataset1"]["total_number_of_requests"] == 15
    assert totals["dataset1"]["total_number_of_downloads"] == 7
    assert totals["dataset1"]["total_number_of_views"] == 4
    assert totals["dataset1"]["number_of_unique_regions"] == 5
    # US/California → country "US", AWS/eu-west-1 → country "EU"
    assert totals["dataset1"]["number_of_unique_countries"] == 2


@pytest.mark.ai_generated
def test_generate_all_dataset_totals_withheld_by_region(tmp_path: pathlib.Path) -> None:
    """A dataset whose by-region summary is still withheld reports zero regions and countries."""
    summary_dir = tmp_path / "summaries"
    dataset_dir = summary_dir / "dataset1"
    dataset_dir.mkdir(parents=True)

    day_tsv = pandas.DataFrame(
        {
            "date": ["2020-01-01"],
            "bytes_sent": [500],
            "number_of_requests": [5],
            "number_of_downloads": [2],
            "number_of_views": [1],
        }
    )
    day_tsv.to_csv(path_or_buf=dataset_dir / "by_day.tsv", sep="\t", index=False)

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=tmp_path)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert totals["dataset1"]["total_bytes_sent"] == 500
    assert totals["dataset1"]["number_of_unique_regions"] == 0
    assert totals["dataset1"]["number_of_unique_countries"] == 0


@pytest.mark.ai_generated
def test_generate_archive_totals_all_region_types(tmp_path: pathlib.Path) -> None:
    """generate_archive_totals handles VPN, GitHub, unknown, normal, and AWS regions."""
    summary_dir = tmp_path / "summaries"
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)

    day_tsv = pandas.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02"],
            "bytes_sent": [500, 1000],
            "number_of_requests": [5, 10],
            "number_of_downloads": [2, 5],
            "number_of_views": [1, 3],
        }
    )
    day_tsv.to_csv(path_or_buf=archive_dir / "by_day.tsv", sep="\t", index=False)

    region_tsv = pandas.DataFrame(
        {
            "region": ["VPN", "GitHub", "unknown", "US/California", "AWS/eu-west-1"],
            "bytes_sent": [100, 200, 300, 400, 500],
            "number_of_requests": [1, 2, 3, 4, 5],
            "number_of_downloads": [1, 1, 1, 2, 2],
            "number_of_views": [0, 1, 1, 1, 1],
        }
    )
    region_tsv.to_csv(path_or_buf=archive_dir / "by_region.tsv", sep="\t", index=False)
    (archive_dir / "requester_count.tsv").write_text("7\n")

    s3_log_extraction.summarize.generate_archive_totals(cache_directory=tmp_path)

    output_path = summary_dir / "archive_totals.json"
    assert output_path.exists()
    result = json.loads(output_path.read_text())
    assert result["total_bytes_sent"] == 1500
    assert result["total_number_of_requests"] == 15
    assert result["total_number_of_downloads"] == 7
    assert result["total_number_of_views"] == 4
    assert result["number_of_requesters"] == 7
    # US/California → "US", AWS/eu-west-1 → "EU" (from region_code.split("-")[0].upper())
    assert result["number_of_unique_countries"] == 2


@pytest.mark.ai_generated
def test_generate_archive_totals_without_by_day(tmp_path: pathlib.Path) -> None:
    """generate_archive_totals names the missing archive by-day summary."""
    (tmp_path / "summaries" / "archive").mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="by_day.tsv"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=tmp_path)
