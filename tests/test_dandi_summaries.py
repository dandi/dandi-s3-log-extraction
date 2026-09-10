import json
import pathlib
import shutil

import pandas
import py
import s3_log_extraction.summarize
from s3_log_extraction.ip_utils import MappingRegionResolver

import dandi_s3_log_extraction
from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import (
    _summarize_archive_by_asset_type_per_week,
)

# A stand-in geolocation of the single requester of the example logs, so that the summaries have a resolved region
# to report. The requester is a documentation range address (RFC 5737), which a real resolution labels `bogon`
# rather than any place.
MOCKED_REGION_RESOLVER = MappingRegionResolver({"192.0.2.0": "US/California"})


def test_dandiset_summaries(tmpdir: py.path.local):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"

    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)

    # The example logs have a single resolved region, which is below the default disclosure threshold
    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=test_dir, workers=1, region_resolver=MOCKED_REGION_RESOLVER
    )
    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=test_dir, workers=1, unassociated=True, region_resolver=MOCKED_REGION_RESOLVER
    )
    assert list(test_summary_dir.rglob(pattern="by_region.tsv")) == []

    # Lowering the threshold to zero publishes the by-region summaries of that single region
    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=test_dir, workers=1, region_disclosure_threshold=0, region_resolver=MOCKED_REGION_RESOLVER
    )
    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(
        cache_directory=test_dir,
        workers=1,
        unassociated=True,
        region_disclosure_threshold=0,
        region_resolver=MOCKED_REGION_RESOLVER,
    )

    # The archive requester count is written by the Dandiset summaries above, since it is a union over the
    # extraction cache rather than something the archive summaries can aggregate from the per-Dandiset counts
    archive_requester_count_file_path = test_summary_dir / "archive" / "requester_count.tsv"
    assert archive_requester_count_file_path.exists()
    count_before_archive_summaries = archive_requester_count_file_path.read_text().strip()

    # Generate archive-level summaries with upstream + plugin-specific functions
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir, region_disclosure_threshold=0)
    _summarize_archive_by_asset_type_per_week(summary_directory=test_summary_dir)

    # The archive summaries aggregate the per-Dandiset summaries, so they must leave that union alone
    assert archive_requester_count_file_path.read_text().strip() == count_before_archive_summaries

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

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log, check_dtype=False)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)

    # Verify that upstream totals generation works on plugin-produced summaries
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    expected_totals = json.loads((expected_summaries_dir / "totals.json").read_text())
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
