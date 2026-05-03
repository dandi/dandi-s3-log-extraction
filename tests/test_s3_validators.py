"""Tests for s3_log_extraction.validate covering uncovered code paths."""

import os
import pathlib
from unittest.mock import patch

import pytest
from s3_log_extraction.validate import (
    BaseValidator,
    HttpEmptySplitPreValidator,
    HttpSplitCountPreValidator,
    TimestampsParsingPreValidator,
)

# A minimal valid S3 log line; uses REST.PUT.OBJECT so all pre-validators pass trivially
_VALID_LOG_LINE = (
    "8787a3c41bf7ce0d54359d9348ad5b08e16bd5bb8ae5aa4e1508b435773a066e"
    " dandiarchive [01/Jan/2020:05:06:35 +0000] 192.0.2.0 - J42N2W7ET0EC03CV"
    " REST.PUT.OBJECT blobs/100/4eb/1004eb73 \"PUT /blobs/100 HTTP/1.1\""
    " 200 - 512 512 53 52 \"-\" \"-\" - sig - AES256"
    " - bucket.s3.amazonaws.com TLSv1.2 - -"
)


@pytest.fixture()
def example_log_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Create a temporary .log file with a valid S3 log line."""
    log_file = tmp_path / "example.log"
    log_file.write_text(_VALID_LOG_LINE + "\n")
    return log_file


def _make_validator(validator_cls, records_dir: pathlib.Path):
    """Instantiate a validator with a custom records directory."""
    with patch("s3_log_extraction.validate._base_validator.get_records_directory", return_value=records_dir):
        return validator_cls()


@pytest.mark.ai_generated
def test_base_validator_abstract_run_validation_raises(tmp_path: pathlib.Path) -> None:
    """Calling BaseValidator._run_validation directly raises NotImplementedError."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(HttpEmptySplitPreValidator, records_dir)

    example_file = tmp_path / "test.log"
    example_file.write_text(_VALID_LOG_LINE + "\n")

    with pytest.raises(NotImplementedError):
        BaseValidator._run_validation(validator, file_path=example_file)



    """BaseValidator.__hash__ uses _run_validation bytecode checksum."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(HttpEmptySplitPreValidator, records_dir)
    # Call the base-class __hash__ directly (subclasses override it)
    base_hash = BaseValidator.__hash__(validator)
    assert isinstance(base_hash, int)


@pytest.mark.ai_generated
def test_http_empty_split_validator_full(tmp_path: pathlib.Path, example_log_file: pathlib.Path) -> None:
    """HttpEmptySplitPreValidator: validate_file new → cached → validate_directory."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(HttpEmptySplitPreValidator, records_dir)

    # First call: covers _run_validation, _record_success paths
    validator.validate_file(file_path=example_log_file)

    # Second call with same file: covers the early-return cached path
    validator.validate_file(file_path=example_log_file)

    # validate_directory with limit
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    another_log = log_dir / "another.log"
    another_log.write_text(_VALID_LOG_LINE + "\n")
    validator.validate_directory(directory=log_dir, limit=1)
    validator.validate_directory(directory=log_dir)


@pytest.mark.ai_generated
def test_http_split_count_validator_full(tmp_path: pathlib.Path, example_log_file: pathlib.Path) -> None:
    """HttpSplitCountPreValidator: validate_file new → cached → validate_directory."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(HttpSplitCountPreValidator, records_dir)

    validator.validate_file(file_path=example_log_file)
    validator.validate_file(file_path=example_log_file)

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "test.log").write_text(_VALID_LOG_LINE + "\n")
    validator.validate_directory(directory=log_dir, limit=1)


@pytest.mark.ai_generated
def test_timestamps_parsing_validator_full(tmp_path: pathlib.Path, example_log_file: pathlib.Path) -> None:
    """TimestampsParsingPreValidator: validate_file new → cached."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(TimestampsParsingPreValidator, records_dir)

    validator.validate_file(file_path=example_log_file)
    validator.validate_file(file_path=example_log_file)


@pytest.mark.ai_generated
def test_validator_load_existing_record(tmp_path: pathlib.Path, example_log_file: pathlib.Path) -> None:
    """BaseValidator.__init__ loads an existing record file when present."""
    records_dir = tmp_path / "records"
    records_dir.mkdir()

    # First instantiation – creates the record file path but it doesn't exist yet
    validator1 = _make_validator(HttpEmptySplitPreValidator, records_dir)
    record_file_path = validator1.record_file_path

    # Write an entry to the record file
    absolute_path = str(example_log_file.absolute())
    record_file_path.write_text(f"{absolute_path}\n")

    # Second instantiation – loads the existing record (covers lines 31-32 in _base_validator.py)
    validator2 = _make_validator(HttpEmptySplitPreValidator, records_dir)
    assert absolute_path in validator2.record


@pytest.mark.ai_generated
@pytest.mark.skipif(
    not os.environ.get("S3_LOG_EXTRACTION_PASSWORD"),
    reason="S3_LOG_EXTRACTION_PASSWORD not set",
)
def test_extraction_heuristic_validator(tmp_path: pathlib.Path, example_log_file: pathlib.Path) -> None:
    """ExtractionHeuristicPreValidator validates a log file when password is available."""
    from s3_log_extraction.validate import ExtractionHeuristicPreValidator

    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = _make_validator(ExtractionHeuristicPreValidator, records_dir)

    validator.validate_file(file_path=example_log_file)
    validator.validate_file(file_path=example_log_file)
