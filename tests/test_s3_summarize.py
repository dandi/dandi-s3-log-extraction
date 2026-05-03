"""Tests for s3_log_extraction.summarize covering uncovered code paths."""

import json
import pathlib

import pandas
import pytest
import s3_log_extraction
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

    # dataset1: asset1 has all 3 files; asset2 has only bytes_sent (no timestamps, no indexed_ips)
    asset1 = extraction_dir / "dataset1" / "asset1"
    asset1.mkdir(parents=True)
    # YYMMDDHHMMSS format: 200101050635 → 2020-01-01
    (asset1 / "timestamps.txt").write_text("200101050635\n200101224258\n")
    (asset1 / "bytes_sent.txt").write_text("512\n1526223\n")
    (asset1 / "indexed_ips.txt").write_text("12345\n67890\n")

    asset2 = extraction_dir / "dataset1" / "asset2_no_ts"
    asset2.mkdir(parents=True)
    (asset2 / "bytes_sent.txt").write_text("100\n")
    # No timestamps.txt → exercises continue in _summarize_dataset_by_day
    # No indexed_ips.txt → exercises continue in _summarize_dataset_by_region

    # dataset2: all assets have only bytes_sent.txt → day/region summary early return
    asset3 = extraction_dir / "dataset2" / "asset3"
    asset3.mkdir(parents=True)
    (asset3 / "bytes_sent.txt").write_text("200\n")
    # No timestamps.txt → summarized_activity_by_day stays empty → early return at line 111-112
    # No indexed_ips.txt → summarized_activity_by_region stays empty → early return at line 182-183

    # Set up index_to_region cache
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "index_to_region.yaml").write_text("12345: 'US/California'\n67890: 'unknown'\n")

    s3_log_extraction.summarize.generate_summaries(cache_directory=tmp_path)

    assert (tmp_path / "summaries" / "dataset1" / "by_day.tsv").exists()
    assert (tmp_path / "summaries" / "dataset1" / "by_asset.tsv").exists()
    assert (tmp_path / "summaries" / "dataset1" / "by_region.tsv").exists()


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
        index_to_region={},
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

    # by_region.tsv with all region types
    region_tsv = pandas.DataFrame(
        {
            "region": ["VPN", "GitHub", "unknown", "US/California", "AWS/us-east-1"],
            "bytes_sent": [100, 200, 300, 400, 500],
        }
    )
    region_tsv.to_csv(path_or_buf=dataset_dir / "by_region.tsv", sep="\t", index=False)

    s3_log_extraction.summarize.generate_all_dataset_totals(summary_directory=summary_dir)

    output_path = summary_dir / "totals.json"
    assert output_path.exists()
    totals = json.loads(output_path.read_text())
    assert "dataset1" in totals
    assert totals["dataset1"]["total_bytes_sent"] == 1500
    # US/California → country "US", AWS/us-east-1 → country "US"
    assert totals["dataset1"]["number_of_unique_countries"] == 2


@pytest.mark.ai_generated
def test_generate_archive_totals_all_region_types(tmp_path: pathlib.Path) -> None:
    """generate_archive_totals handles VPN, GitHub, unknown, normal, and AWS regions."""
    summary_dir = tmp_path / "summaries"
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)

    region_tsv = pandas.DataFrame(
        {
            "region": ["VPN", "GitHub", "unknown", "US/California", "AWS/us-east-1"],
            "bytes_sent": [100, 200, 300, 400, 500],
        }
    )
    region_tsv.to_csv(path_or_buf=archive_dir / "by_region.tsv", sep="\t", index=False)

    s3_log_extraction.summarize.generate_archive_totals(summary_directory=summary_dir)

    output_path = summary_dir / "archive_totals.json"
    assert output_path.exists()
    result = json.loads(output_path.read_text())
    assert result["total_bytes_sent"] == 1500
    # US/California → "US", AWS/us-east-1 → "US" (from region_code.split("-")[0].upper())
    assert result["number_of_unique_countries"] == 2
