"""Tests for the command-line interface."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from dandi_s3_log_extraction._command_line_interface._cli import _dandis3logextraction_cli


@pytest.mark.ai_generated
def test_extract_default_mode(tmp_path: pathlib.Path) -> None:
    """Test extract command with default mode calls DandiS3LogAccessExtractor.extract_directory."""
    runner = CliRunner()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(_dandis3logextraction_cli, ["extract", str(tmp_path)])

        assert result.exit_code == 0, result.output
        mock_extractor.extract_directory.assert_called_once_with(directory=str(tmp_path), limit=None, workers=-2)


@pytest.mark.ai_generated
def test_extract_remote_mode(tmp_path: pathlib.Path) -> None:
    """Test extract command with --mode remote calls DandiRemoteS3LogAccessExtractor.extract_s3_bucket."""
    runner = CliRunner()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiRemoteS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(_dandis3logextraction_cli, ["extract", str(tmp_path), "--mode", "remote"])

        assert result.exit_code == 0, result.output
        mock_extractor.extract_s3_bucket.assert_called_once_with(
            s3_root=str(tmp_path), limit=None, workers=-2, manifest_file_path=None, inventory_directory=None
        )


@pytest.mark.ai_generated
def test_extract_dandi_mode(tmp_path: pathlib.Path) -> None:
    """Test extract command with --mode dandi calls DandiS3LogAccessExtractor.extract_directory."""
    runner = CliRunner()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(_dandis3logextraction_cli, ["extract", str(tmp_path), "--mode", "dandi"])

        assert result.exit_code == 0, result.output
        mock_extractor.extract_directory.assert_called_once_with(directory=str(tmp_path), limit=None, workers=-2)


@pytest.mark.ai_generated
def test_extract_dandi_remote_mode(tmp_path: pathlib.Path) -> None:
    """Test extract command with --mode dandi-remote calls DandiS3LogAccessExtractor.extract_directory."""
    runner = CliRunner()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(_dandis3logextraction_cli, ["extract", str(tmp_path), "--mode", "dandi-remote"])

        assert result.exit_code == 0, result.output
        mock_extractor.extract_directory.assert_called_once_with(directory=str(tmp_path), limit=None, workers=-2)


@pytest.mark.ai_generated
def test_extract_with_limit_and_workers(tmp_path: pathlib.Path) -> None:
    """Test extract command with --limit and --workers options."""
    runner = CliRunner()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(_dandis3logextraction_cli, ["extract", str(tmp_path), "--limit", "5", "--workers", "2"])

        assert result.exit_code == 0, result.output
        mock_extractor.extract_directory.assert_called_once_with(directory=str(tmp_path), limit=5, workers=2)


@pytest.mark.ai_generated
def test_extract_remote_with_manifest(tmp_path: pathlib.Path) -> None:
    """Test extract command with --mode remote and --manifest passes manifest_file_path."""
    runner = CliRunner()
    manifest_path = str(tmp_path / "manifest.txt")
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiRemoteS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(
            _dandis3logextraction_cli,
            ["extract", str(tmp_path), "--mode", "remote", "--manifest", manifest_path],
        )

        assert result.exit_code == 0, result.output
        mock_extractor.extract_s3_bucket.assert_called_once_with(
            s3_root=str(tmp_path), limit=None, workers=-2, manifest_file_path=manifest_path, inventory_directory=None
        )


@pytest.mark.ai_generated
def test_extract_remote_with_inventory(tmp_path: pathlib.Path) -> None:
    """Test extract command with --mode remote and --inventory passes inventory_directory."""
    runner = CliRunner()
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir()
    with patch(
        "dandi_s3_log_extraction._command_line_interface._cli.DandiRemoteS3LogAccessExtractor"
    ) as mock_extractor_class:
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor

        result = runner.invoke(
            _dandis3logextraction_cli,
            ["extract", str(tmp_path), "--mode", "remote", "--inventory", str(inventory_dir)],
        )

        assert result.exit_code == 0, result.output
        mock_extractor.extract_s3_bucket.assert_called_once_with(
            s3_root=str(tmp_path),
            limit=None,
            workers=-2,
            manifest_file_path=None,
            inventory_directory=str(inventory_dir),
        )


@pytest.mark.ai_generated
def test_stop_default_timeout() -> None:
    """Test stop command calls stop_extraction with default timeout of 600 seconds."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.s3_log_extraction") as mock_s3:
        result = runner.invoke(_dandis3logextraction_cli, ["stop"])

        assert result.exit_code == 0, result.output
        mock_s3.extractors.stop_extraction.assert_called_once_with(max_timeout_in_seconds=600)


@pytest.mark.ai_generated
def test_stop_custom_timeout() -> None:
    """Test stop command calls stop_extraction with the given --timeout value."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.s3_log_extraction") as mock_s3:
        result = runner.invoke(_dandis3logextraction_cli, ["stop", "--timeout", "30"])

        assert result.exit_code == 0, result.output
        mock_s3.extractors.stop_extraction.assert_called_once_with(max_timeout_in_seconds=30)


@pytest.mark.ai_generated
def test_update_database() -> None:
    """Test update database command calls bundle_database."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.bundle_database") as mock_bundle:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "database"])

        assert result.exit_code == 0, result.output
        mock_bundle.assert_called_once_with()


@pytest.mark.ai_generated
def test_update_summaries_default_mode() -> None:
    """Test update summaries with default mode calls generate_dandiset_summaries."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_summaries") as mock_gen:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "summaries"])

        assert result.exit_code == 0, result.output
        mock_gen.assert_called_once_with(
            pick=None,
            skip=None,
            workers=-2,
            content_id_to_usage_dandiset_path_url=None,
            api_url=None,
            unassociated=False,
            cache_directory=None,
        )


@pytest.mark.ai_generated
def test_update_summaries_archive_mode() -> None:
    """Test update summaries with --mode archive calls generate_archive_summaries."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.s3_log_extraction") as mock_s3:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "summaries", "--mode", "archive"])

        assert result.exit_code == 0, result.output
        mock_s3.summarize.generate_archive_summaries.assert_called_once_with()


@pytest.mark.ai_generated
def test_update_summaries_with_pick_and_skip() -> None:
    """Test update summaries splits --pick and --skip comma-separated values into lists."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_summaries") as mock_gen:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "summaries", "--pick", "a,b", "--skip", "c,d"])

        assert result.exit_code == 0, result.output
        mock_gen.assert_called_once_with(
            pick=["a", "b"],
            skip=["c", "d"],
            workers=-2,
            content_id_to_usage_dandiset_path_url=None,
            api_url=None,
            unassociated=False,
            cache_directory=None,
        )


@pytest.mark.ai_generated
def test_update_summaries_with_all_options() -> None:
    """Test update summaries passes all optional flags and URL parameters through correctly."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_summaries") as mock_gen:
        result = runner.invoke(
            _dandis3logextraction_cli,
            [
                "update",
                "summaries",
                "--workers",
                "4",
                "--content-id-to-usage-dandiset-path-url",
                "https://example.com",
                "--api-url",
                "https://api.example.com",
                "--unassociated",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_gen.assert_called_once_with(
            pick=None,
            skip=None,
            workers=4,
            content_id_to_usage_dandiset_path_url="https://example.com",
            api_url="https://api.example.com",
            unassociated=True,
            cache_directory=None,
        )


@pytest.mark.ai_generated
def test_update_totals_default_mode() -> None:
    """Test update totals with default mode calls generate_dandiset_totals."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_totals") as mock_totals:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "totals"])

        assert result.exit_code == 0, result.output
        mock_totals.assert_called_once_with(summary_directory=None)


@pytest.mark.ai_generated
def test_update_totals_archive_mode() -> None:
    """Test update totals with --mode archive calls generate_archive_totals."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.s3_log_extraction") as mock_s3:
        result = runner.invoke(_dandis3logextraction_cli, ["update", "totals", "--mode", "archive"])

        assert result.exit_code == 0, result.output
        mock_s3.summarize.generate_archive_totals.assert_called_once_with()


@pytest.mark.ai_generated
def test_update_summaries_with_directory(tmp_path: pathlib.Path) -> None:
    """Test update summaries passes --directory to generate_dandiset_summaries as cache_directory."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_summaries") as mock_gen:
        result = runner.invoke(
            _dandis3logextraction_cli,
            ["update", "summaries", "--directory", str(tmp_path)],
        )

        assert result.exit_code == 0, result.output
        mock_gen.assert_called_once_with(
            pick=None,
            skip=None,
            workers=-2,
            content_id_to_usage_dandiset_path_url=None,
            api_url=None,
            unassociated=False,
            cache_directory=str(tmp_path),
        )


@pytest.mark.ai_generated
def test_update_totals_with_directory(tmp_path: pathlib.Path) -> None:
    """Test update totals passes --directory to generate_dandiset_totals as summary_directory."""
    runner = CliRunner()
    with patch("dandi_s3_log_extraction._command_line_interface._cli.generate_dandiset_totals") as mock_totals:
        result = runner.invoke(
            _dandis3logextraction_cli,
            ["update", "totals", "--directory", str(tmp_path)],
        )

        assert result.exit_code == 0, result.output
        mock_totals.assert_called_once_with(summary_directory=str(tmp_path))
