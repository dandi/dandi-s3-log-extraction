"""Tests for the JSON Lines content ID to usage Dandiset path cache used during summary generation."""

import json
import pathlib

import pandas
import pytest

import dandi_s3_log_extraction.summarize

_BOGON_IP = "192.0.2.0"


def _initialize_cache(cache_directory: pathlib.Path, /) -> pathlib.Path:
    """Create the IP cache required by summary generation and return the extraction directory."""
    ip_cache_directory = cache_directory / "ips"
    ip_cache_directory.mkdir(parents=True)
    (ip_cache_directory / "ip_to_region.yaml").write_text(f"{_BOGON_IP}: unknown\n")

    extraction_directory = cache_directory / "extraction"
    extraction_directory.mkdir()
    return extraction_directory


def _write_content_extraction(
    *,
    extraction_directory: pathlib.Path,
    content_id: str,
    is_zarr: bool = False,
) -> None:
    """Write a minimal single-request extraction fixture for one content ID."""
    content_directory = (
        extraction_directory / "zarr" / content_id
        if is_zarr
        else extraction_directory / "blobs" / content_id[:3] / content_id[3:6] / content_id
    )
    content_directory.mkdir(parents=True)
    (content_directory / "timestamps.txt").write_text("200101120000\n")
    (content_directory / "bytes_sent.txt").write_text("1000\n")
    (content_directory / "ips.txt").write_text(f"{_BOGON_IP}\n")
    (content_directory / "download.txt").write_text("1\n")


@pytest.mark.ai_generated
def test_summaries_from_local_jsonl_cache(tmp_path: pathlib.Path) -> None:
    """A local JSON Lines cache file maps blob and zarr content IDs to their asset paths."""
    extraction_directory = _initialize_cache(tmp_path)

    blob_id = "0a1b2c3d-1111-2222-3333-444455556666"
    zarr_id = "9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff"
    _write_content_extraction(extraction_directory=extraction_directory, content_id=blob_id)
    _write_content_extraction(extraction_directory=extraction_directory, content_id=zarr_id, is_zarr=True)

    mapping_file_path = tmp_path / "content_id_to_usage_dandiset_path.jsonl"
    mapping_lines = [
        json.dumps({blob_id: {"000001": "sub-A/sub-A.nwb"}}),
        json.dumps({zarr_id: {"000001": "sub-B/sub-B.ome.zarr"}}),
    ]
    mapping_file_path.write_text("\n".join(mapping_lines) + "\n")

    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=tmp_path,
        workers=1,
        pick=["000001"],
        content_id_to_usage_dandiset_path_url=str(mapping_file_path),
    )

    by_asset = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "000001" / "by_asset.tsv")
    assert sorted(by_asset["asset_path"]) == ["sub-A/sub-A.nwb", "sub-B/sub-B.ome.zarr"]
    assert by_asset["bytes_sent"].tolist() == [1000, 1000]


@pytest.mark.ai_generated
def test_content_id_with_multiple_usage_paths(tmp_path: pathlib.Path) -> None:
    """A content ID used by multiple Dandisets is summarized under each with its own asset path."""
    extraction_directory = _initialize_cache(tmp_path)

    shared_id = "5a6b7c8d-1234-5678-9abc-def012345678"
    _write_content_extraction(extraction_directory=extraction_directory, content_id=shared_id)

    mapping_file_path = tmp_path / "content_id_to_usage_dandiset_path.jsonl"
    mapping_file_path.write_text(
        json.dumps({shared_id: {"000001": "sub-A/shared.nwb", "000002": "sub-B/shared.nwb"}}) + "\n"
    )

    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=tmp_path,
        workers=1,
        pick=["000001", "000002"],
        content_id_to_usage_dandiset_path_url=str(mapping_file_path),
    )

    by_asset_first = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "000001" / "by_asset.tsv")
    assert by_asset_first["asset_path"].tolist() == ["sub-A/shared.nwb"]

    by_asset_second = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "000002" / "by_asset.tsv")
    assert by_asset_second["asset_path"].tolist() == ["sub-B/shared.nwb"]


@pytest.mark.ai_generated
def test_malformed_line_is_skipped_with_warning(tmp_path: pathlib.Path) -> None:
    """A malformed line in the JSON Lines cache raises a warning and is skipped."""
    extraction_directory = _initialize_cache(tmp_path)

    blob_id = "0a1b2c3d-1111-2222-3333-444455556666"
    _write_content_extraction(extraction_directory=extraction_directory, content_id=blob_id)

    mapping_file_path = tmp_path / "content_id_to_usage_dandiset_path.jsonl"
    mapping_lines = [
        "this is not JSON",
        json.dumps({blob_id: {"000001": "sub-A/sub-A.nwb"}}),
    ]
    mapping_file_path.write_text("\n".join(mapping_lines) + "\n")

    with pytest.warns(UserWarning, match="malformed JSON on line 1"):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
            pick=["000001"],
            content_id_to_usage_dandiset_path_url=str(mapping_file_path),
        )

    by_asset = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "000001" / "by_asset.tsv")
    assert by_asset["asset_path"].tolist() == ["sub-A/sub-A.nwb"]


@pytest.mark.ai_generated
def test_unassociated_blob_summarized_as_undetermined(tmp_path: pathlib.Path) -> None:
    """A content ID absent from the JSON Lines cache is summarized under the 'undetermined' key."""
    extraction_directory = _initialize_cache(tmp_path)

    mapped_id = "0a1b2c3d-1111-2222-3333-444455556666"
    unmapped_id = "9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff"
    _write_content_extraction(extraction_directory=extraction_directory, content_id=mapped_id)
    _write_content_extraction(extraction_directory=extraction_directory, content_id=unmapped_id)

    mapping_file_path = tmp_path / "content_id_to_usage_dandiset_path.jsonl"
    mapping_file_path.write_text(json.dumps({mapped_id: {"000001": "sub-A/sub-A.nwb"}}) + "\n")

    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=tmp_path,
        workers=1,
        unassociated=True,
        content_id_to_usage_dandiset_path_url=str(mapping_file_path),
    )

    by_asset = pandas.read_table(filepath_or_buffer=tmp_path / "summaries" / "undetermined" / "by_asset.tsv")
    assert by_asset["asset_path"].tolist() == ["undetermined"]
    assert by_asset["bytes_sent"].tolist() == [1000]


@pytest.mark.ai_generated
def test_missing_local_mapping_file_raises(tmp_path: pathlib.Path) -> None:
    """A nonexistent local cache file raises a FileNotFoundError."""
    _initialize_cache(tmp_path)

    with pytest.raises(FileNotFoundError, match="no such file"):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
            cache_directory=tmp_path,
            workers=1,
            pick=["000001"],
            content_id_to_usage_dandiset_path_url=str(tmp_path / "missing.jsonl"),
        )
