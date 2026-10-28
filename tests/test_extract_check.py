"""Tests for checking whether a remote DANDI extraction has anything to do, through `extract --check`."""

import csv
import gzip
import io
import json
import pathlib

import pytest
from click.testing import CliRunner

from dandi_s3_log_extraction._command_line_interface._cli import _dandis3logextraction_cli

_SOURCE_BUCKET = "dandiarchive-logs"
_KEYS = (
    "2024/01/01/2024-01-01-00-00-00-AAAA",
    "2024/01/01/2024-01-01-00-05-00-BBBB",
    "2024/01/02/2024-01-02-00-00-00-CCCC",
)
_PENDING_EXIT_CODE = 3


@pytest.fixture
def inventory_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A minimal local AWS S3 Inventory directory listing every key of `_KEYS`."""
    inventory_directory = tmp_path / "inventory"
    (inventory_directory / "data").mkdir(parents=True)
    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gzip_stream:
        writer = csv.writer(gzip_stream)
        for key in _KEYS:
            writer.writerow([_SOURCE_BUCKET, key])
    (inventory_directory / "data" / "test-uuid.csv.gz").write_bytes(csv_buffer.getvalue())

    manifest = {
        "sourceBucket": _SOURCE_BUCKET,
        "fileFormat": "CSV",
        "fileSchema": "Bucket, Key",
        "files": [{"key": f"inventory/{_SOURCE_BUCKET}/data/test-uuid.csv.gz", "size": len(csv_buffer.getvalue())}],
    }
    (inventory_directory / "2024-01-05T01-00Z").mkdir()
    (inventory_directory / "2024-01-05T01-00Z" / "manifest.json").write_text(json.dumps(manifest))

    partition_directory = inventory_directory / "hive" / "dt=2024-01-05-01-00"
    partition_directory.mkdir(parents=True)
    (partition_directory / "symlink.txt").write_text(
        f"s3://inventory-bucket/inventory/{_SOURCE_BUCKET}/data/test-uuid.csv.gz\n"
    )
    return inventory_directory


@pytest.fixture
def base_temporary_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Create the temporary directories of the extractor under a known base so that any directory shows."""
    base_temporary_directory = tmp_path / "tmp"
    base_temporary_directory.mkdir()
    monkeypatch.setattr(
        "s3_log_extraction.extractors._remote_s3_log_access_extractor.get_base_temporary_directory",
        lambda: base_temporary_directory,
    )
    return base_temporary_directory


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_processed_keys", "expected_output", "expected_exit_code"),
    [
        (0, "Log files are pending extraction.\n", _PENDING_EXIT_CODE),
        (2, "Log files are pending extraction.\n", _PENDING_EXIT_CODE),
        (3, "No log files are pending extraction.\n", 0),
    ],
)
def test_extract_check_reads_dandi_records_and_writes_nothing(
    tmp_path: pathlib.Path,
    inventory_directory: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    number_of_processed_keys: int,
    expected_output: str,
    expected_exit_code: int,
) -> None:
    """
    The check reads the records of the DANDI extractor, reports through its exit status whether anything is
    pending, and leaves neither a temporary directory nor any change to the records behind.
    """
    cache_directory = tmp_path / "cache"
    records_directory = cache_directory / "records"
    records_directory.mkdir(parents=True)
    processed_record = "".join(f"{key.split('/')[-1]}\n" for key in _KEYS[:number_of_processed_keys])
    for stage in ("start", "end"):
        (records_directory / f"DandiRemoteS3LogAccessExtractor_s3-url-processing-{stage}.txt").write_text(
            processed_record
        )
    records_before = {path.name: path.read_text() for path in records_directory.iterdir()}

    result = CliRunner().invoke(
        _dandis3logextraction_cli,
        [
            "extract",
            f"s3://{_SOURCE_BUCKET}",
            "--mode",
            "remote",
            "--inventory",
            str(inventory_directory),
            "--cache",
            str(cache_directory),
            "--check",
        ],
    )

    records_after = {path.name: path.read_text() for path in records_directory.iterdir()}
    assert result.output == expected_output
    assert result.exit_code == expected_exit_code
    assert list(base_temporary_directory.iterdir()) == []
    assert records_after == records_before
