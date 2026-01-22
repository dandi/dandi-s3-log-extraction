"""Tests for the database submodule."""

import pathlib
import shutil

import polars
import py
import s3_log_extraction
import yaml

import dandi_s3_log_extraction


def test_bundle_database(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_sharing_dir = expected_output_dir / "sharing"

    test_extraction_dir = test_dir / "extraction"
    test_sharing_dir = test_dir / "sharing"
    test_database_dir = test_sharing_dir / "extracted_activity.parquet"

    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)
    s3_log_extraction.ip_utils.index_ips(cache_directory=test_dir, seed=0)

    dandi_s3_log_extraction.database.bundle_database(cache_directory=test_dir)
    assert test_sharing_dir.exists()
    assert test_database_dir.exists()

    expected_database_directory = expected_sharing_dir / "extracted_activity.parquet"
    test_blob_index_file = test_sharing_dir / "blob_index_to_id.yaml"
    expected_blob_index_file = expected_sharing_dir / "blob_index_to_id.yaml"
    assert test_blob_index_file.exists()

    with test_blob_index_file.open(mode="r") as f:
        output_blob_index_to_id = yaml.safe_load(f)
    with expected_blob_index_file.open(mode="r") as f:
        expected_blob_index_to_id = yaml.safe_load(f)

    assert len(output_blob_index_to_id) == len(expected_blob_index_to_id)
    assert set(output_blob_index_to_id.values()) == set(expected_blob_index_to_id.values())

    output_parquet_files = sorted(test_database_dir.rglob("*.parquet"))
    expected_parquet_files = sorted(expected_database_directory.rglob("*.parquet"))

    assert len(output_parquet_files) > 0, "No Parquet files were generated"

    test_parquet_paths = {f.relative_to(test_database_dir) for f in output_parquet_files}
    expected_parquet_paths = {f.relative_to(expected_database_directory) for f in expected_parquet_files}
    assert test_parquet_paths == expected_parquet_paths

    for output_file, expected_file in zip(output_parquet_files, expected_parquet_files):
        test_df = polars.read_parquet(output_file)
        expected_df = polars.read_parquet(expected_file)
        print(f"{test_df=}")
        print(f"{expected_df=}")

        assert test_df.shape == expected_df.shape, f"Shape mismatch in {output_file}"

        output_sorted = test_df.sort(by=["timestamp", "blob_index"])
        expected_sorted = expected_df.sort(by=["timestamp", "blob_index"])

        columns_to_compare = ["asset_type", "blob_head", "timestamp", "bytes_sent", "indexed_ip"]
        for col in columns_to_compare:
            assert (
                output_sorted[col].to_list() == expected_sorted[col].to_list()
            ), f"Column {col} mismatch in {output_file}"

        if test_df.shape[0] > 0:
            assert test_df["blob_index"].min() >= 0
            assert test_df["blob_index"].max() < len(output_blob_index_to_id)
