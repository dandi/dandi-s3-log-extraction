"""Tests for s3_log_extraction.testing covering assert_filetree_matches and generate_benchmark."""

import pathlib

import pytest
import s3_log_extraction.testing
from s3_log_extraction.testing._benchmarking import (
    _create_date_directories,
    _create_random_log_file,
    _create_random_log_files,
    _generate_object_key_levels,
    _generate_object_keys,
    _generate_random_lines,
)


@pytest.mark.ai_generated
def test_assert_filetree_matches_success(tmp_path: pathlib.Path) -> None:
    """assert_filetree_matches passes when both directories have identical file trees."""
    test_dir = tmp_path / "test"
    expected_dir = tmp_path / "expected"
    test_dir.mkdir()
    expected_dir.mkdir()

    (test_dir / "file1.txt").write_bytes(b"hello")
    (expected_dir / "file1.txt").write_bytes(b"hello")

    subdir_test = test_dir / "subdir"
    subdir_expected = expected_dir / "subdir"
    subdir_test.mkdir()
    subdir_expected.mkdir()
    (subdir_test / "file2.txt").write_bytes(b"world")
    (subdir_expected / "file2.txt").write_bytes(b"world")

    s3_log_extraction.testing.assert_filetree_matches(test_dir=test_dir, expected_dir=expected_dir)


@pytest.mark.ai_generated
def test_assert_filetree_matches_file_set_mismatch(tmp_path: pathlib.Path) -> None:
    """assert_filetree_matches raises AssertionError when file sets differ."""
    test_dir = tmp_path / "test"
    expected_dir = tmp_path / "expected"
    test_dir.mkdir()
    expected_dir.mkdir()

    (test_dir / "file1.txt").write_bytes(b"hello")
    (expected_dir / "file2.txt").write_bytes(b"hello")

    with pytest.raises(AssertionError, match="File trees do not match"):
        s3_log_extraction.testing.assert_filetree_matches(test_dir=test_dir, expected_dir=expected_dir)


@pytest.mark.ai_generated
def test_assert_filetree_matches_content_mismatch(tmp_path: pathlib.Path) -> None:
    """assert_filetree_matches raises AssertionError when file contents differ."""
    test_dir = tmp_path / "test"
    expected_dir = tmp_path / "expected"
    test_dir.mkdir()
    expected_dir.mkdir()

    (test_dir / "file1.txt").write_bytes(b"hello")
    (expected_dir / "file1.txt").write_bytes(b"world")

    with pytest.raises(AssertionError, match="Content mismatch"):
        s3_log_extraction.testing.assert_filetree_matches(test_dir=test_dir, expected_dir=expected_dir)


@pytest.mark.ai_generated
def test_generate_benchmark_runs(tmp_path: pathlib.Path) -> None:
    """generate_benchmark runs without error and creates the benchmark directory."""
    s3_log_extraction.testing.generate_benchmark(directory=tmp_path, seed=0)
    assert (tmp_path / "s3-log-extraction-benchmark").exists()


@pytest.mark.ai_generated
def test_generate_benchmark_existing_nonempty_warns(tmp_path: pathlib.Path) -> None:
    """generate_benchmark warns and removes an existing non-empty benchmark directory."""
    import warnings

    benchmark_dir = tmp_path / "s3-log-extraction-benchmark"
    benchmark_dir.mkdir()
    (benchmark_dir / "old_file.txt").write_text("old")

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        s3_log_extraction.testing.generate_benchmark(directory=tmp_path, seed=0)

    assert any("not empty" in str(w.message) for w in caught_warnings)
    # Old file was removed by shutil.rmtree
    assert not (benchmark_dir / "old_file.txt").exists()


@pytest.mark.ai_generated
def test_generate_object_key_levels_returns_expected_count() -> None:
    """_generate_object_key_levels yields the correct number of combined paths."""
    import random

    random.seed(0)
    levels = list(_generate_object_key_levels(number_of_object_key_levels=(2, 3)))
    # 2 outer × 3 inner = 6 combinations
    assert len(levels) == 6
    assert all("/" in level for level in levels)


@pytest.mark.ai_generated
def test_generate_object_keys_returns_expected_count() -> None:
    """_generate_object_keys yields the specified number of object keys."""
    import random

    random.seed(0)
    levels = ("abc/def", "ghi/jkl", "mno/pqr")
    keys = list(_generate_object_keys(number_of_object_keys=5, levels=levels))
    assert len(keys) == 5
    assert all("/" in key for key in keys)


@pytest.mark.ai_generated
def test_create_date_directories_minimal(tmp_path: pathlib.Path) -> None:
    """_create_date_directories creates year/month/day structure for the given range."""
    _create_date_directories(directory=tmp_path, start_year=2020, end_year=2020)

    year_dir = tmp_path / "2020"
    assert year_dir.exists()
    jan_dir = year_dir / "01"
    assert jan_dir.exists()
    day1 = jan_dir / "01"
    assert day1.exists()


@pytest.mark.ai_generated
def test_create_random_log_file(tmp_path: pathlib.Path) -> None:
    """_create_random_log_file creates a file with the expected content."""
    import random

    random.seed(0)
    object_keys = ["level1/level2/file1", "level1/level2/file2"]
    object_key_to_total_bytes = {k: 1_000_000 for k in object_keys}

    file_path = tmp_path / "2020-01-01-12-00-00-ABCDEF1234567890"
    _create_random_log_file(
        file_path=file_path,
        object_keys=object_keys,
        object_key_to_total_bytes=object_key_to_total_bytes,
    )
    assert file_path.exists()
    content = file_path.read_text()
    assert len(content) > 0


@pytest.mark.ai_generated
def test_create_random_log_files(tmp_path: pathlib.Path) -> None:
    """_create_random_log_files creates log files in the specified date directories."""
    import random

    random.seed(0)
    # Create minimal date structure for 1 day
    day_dir = tmp_path / "2020" / "01" / "01"
    day_dir.mkdir(parents=True)

    object_keys = ["abc/def/file1"]
    object_key_to_total_bytes = {k: 1_000_000 for k in object_keys}

    _create_random_log_files(
        directory=tmp_path,
        object_key_to_total_bytes=object_key_to_total_bytes,
        number_of_files_per_day_lower_bound=1,
        number_of_files_per_day_upper_bound=2,
    )

    # At least 1 file was created in the day directory
    created_files = list(tmp_path.rglob("*"))
    log_files = [f for f in created_files if f.is_file()]
    assert len(log_files) >= 1


@pytest.mark.ai_generated
def test_generate_random_lines() -> None:
    """_generate_random_lines yields the expected number of valid log lines."""
    import random

    random.seed(0)
    object_keys = ["abc/def/file1", "abc/def/file2"]
    object_key_to_total_bytes = {k: 1_000_000 for k in object_keys}

    lines = list(
        _generate_random_lines(
            number_of_lines=5,
            timestamp="2020-01-01-12-00-00",
            object_keys=object_keys,
            object_key_to_total_bytes=object_key_to_total_bytes,
        )
    )
    assert len(lines) == 5
    assert all(isinstance(line, str) and len(line) > 0 for line in lines)
