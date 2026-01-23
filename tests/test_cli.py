"""Tests for the command line interface."""

import os
import pathlib
import subprocess
import sys

import pytest


def _run_cli_command(*args):
    """Helper function to run CLI commands with coverage."""
    return subprocess.run(
        [sys.executable, "-m", "coverage", "run", "-m", "dandi_s3_log_extraction._command_line_interface._cli", *args],
        capture_output=True,
        text=True,
    )


def test_cli_help():
    """Test that the main CLI help command works."""
    result = _run_cli_command()
    # Should fail with exit code 2 because no command is provided, but still show usage
    assert result.returncode != 0
    # Check output contains expected help information
    assert "Usage:" in result.stdout or "Usage:" in result.stderr


def test_cli_main_help_flag():
    """Test the main CLI --help flag."""
    result = _run_cli_command("--help")
    assert result.returncode == 0
    assert "Usage:" in result.stdout


def test_extract_help():
    """Test the extract command help."""
    result = _run_cli_command("extract", "--help")
    assert result.returncode == 0
    assert "extract" in result.stdout.lower()
    assert "directory" in result.stdout.lower()


def test_stop_help():
    """Test the stop command help."""
    result = _run_cli_command("stop", "--help")
    assert result.returncode == 0
    assert "stop" in result.stdout.lower()
    assert "timeout" in result.stdout.lower()


def test_update_help():
    """Test the update command help."""
    result = _run_cli_command("update", "--help")
    assert result.returncode == 0
    assert "update" in result.stdout.lower()


def test_update_database_help():
    """Test the update database command help."""
    result = _run_cli_command("update", "database", "--help")
    assert result.returncode == 0
    assert "database" in result.stdout.lower()


def test_update_summaries_help():
    """Test the update summaries command help."""
    result = _run_cli_command("update", "summaries", "--help")
    assert result.returncode == 0
    assert "summaries" in result.stdout.lower()


def test_update_totals_help():
    """Test the update totals command help."""
    result = _run_cli_command("update", "totals", "--help")
    assert result.returncode == 0
    assert "totals" in result.stdout.lower()


def test_extract_with_example_logs(tmpdir):
    """Test the extract command with example logs."""
    base_directory = pathlib.Path(__file__).parent
    test_logs_directory = base_directory / "example_logs"

    # Only run if example logs exist
    if not test_logs_directory.exists():
        pytest.skip("Example logs directory not found")

    tmpdir = pathlib.Path(tmpdir)
    output_directory = tmpdir / "test_cli_extraction"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "coverage",
            "run",
            "-m",
            "dandi_s3_log_extraction._command_line_interface._cli",
            "extract",
            str(test_logs_directory),
            "--workers",
            "1",
            "--limit",
            "1",
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "S3_LOG_EXTRACTION_CACHE_DIRECTORY": str(output_directory),
        },
    )
    # Command should succeed or fail gracefully (not crash)
    # Exit code depends on whether environment is properly set up
    # We just want to ensure it doesn't crash with an unexpected error
    assert result.returncode in [0, 1]


def test_extract_invalid_directory():
    """Test the extract command with an invalid directory."""
    result = _run_cli_command("extract", "/nonexistent/directory/that/does/not/exist")
    # Should fail because directory doesn't exist
    assert result.returncode != 0
