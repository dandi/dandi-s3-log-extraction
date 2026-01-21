import pathlib
import shutil

import pandas
import py
import s3_log_extraction

import dandi_s3_log_extraction


def test_dandiset_summaries(tmpdir: py.path.local):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)
    s3_log_extraction.ip_utils.index_ips(cache_directory=test_dir)

    dandi_s3_log_extraction.summarize.generate_dandiset_summaries(cache_directory=test_dir, workers=1)
    assert test_summary_dir.is_dir()
    assert len(list(test_summary_dir.rglob("*.tsv"))) > 0

    # Cannot generate `by_region.tsv` here since it would need to expose a real IP
    # TODO: disconnect IP cache and allow it to be specific in tests with false values
    # shutil.copyfile(src=expected_summaries_dir / "000126" / "by_region.tsv", dst=test_summary_dir / "000126")

    dandi_s3_log_extraction.summarize.generate_dandiset_totals(summary_directory=test_summary_dir)
    assert (test_summary_dir / "totals.json").is_file()

    s3_log_extraction.summarize.generate_archive_summaries(summary_directory=test_summary_dir)

    test_file_paths = {path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="*.tsv")}
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path for path in expected_summaries_dir.rglob(pattern="*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)
