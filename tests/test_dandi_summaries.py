import json
import pathlib
import shutil

import pandas
import py
import s3_log_extraction

import dandi_s3_log_extraction
from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    _round_requester_count,
)


def test_dandiset_summaries(tmpdir: py.path.local):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"

    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)
    ip_cache_dir = test_dir / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "ip_to_region.yaml").write_text("192.0.2.0: unknown\n")

    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(cache_directory=test_dir, workers=1)
    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=test_dir, workers=1, unassociated=True
    )

    # Generate parent-level summaries/totals from dataset-level outputs
    s3_log_extraction.summarize.generate_parent_summaries(cache_directory=test_dir)

    test_file_paths = {
        path.relative_to(test_summary_dir): path
        for path in test_summary_dir.rglob(pattern="*.tsv")
        if path.name != "requester_count.tsv"
    }
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path
        for path in expected_summaries_dir.rglob(pattern="*.tsv")
        if path.name != "requester_count.tsv"
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)
        for column_name in ("number_of_requests", "number_of_downloads"):
            if column_name in expected_mapped_log.columns:
                expected_mapped_log[column_name] = expected_mapped_log[column_name].map(
                    lambda count: _round_requester_count(count=int(count), modulo=20, minimum=50)
                )

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log, check_dtype=False)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)

    expected_totals = json.loads((expected_summaries_dir / "totals.json").read_text())
    for dataset_totals in expected_totals.values():
        for column_name in ("total_number_of_requests", "total_number_of_downloads"):
            dataset_totals[column_name] = _round_requester_count(
                count=int(dataset_totals[column_name]), modulo=20, minimum=50
            )

    test_totals = json.loads((test_summary_dir / "totals.json").read_text())
    assert test_totals == expected_totals
    assert (test_summary_dir / "archive_totals.json").exists()

    # Verify requester_count.tsv files
    test_tsv_paths = {
        path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="requester_count.tsv")
    }
    expected_tsv_paths = {
        path.relative_to(expected_summaries_dir): path
        for path in expected_summaries_dir.rglob(pattern="requester_count.tsv")
    }
    assert set(test_tsv_paths.keys()) == set(expected_tsv_paths.keys())

    for relative_path, expected_tsv_path in expected_tsv_paths.items():
        test_tsv_path = test_summary_dir / relative_path
        assert test_tsv_path.read_text().strip() == expected_tsv_path.read_text().strip(), (
            f"\n\nMismatch in {relative_path}:\n"
            f"  test:     {test_tsv_path.read_text().strip()!r}\n"
            f"  expected: {expected_tsv_path.read_text().strip()!r}\n"
        )
