import pathlib

import py
import pytest

import dandi_s3_log_extraction


def test_extraction(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    output_directory = tmpdir / "test_extraction"
    output_directory.mkdir(exist_ok=True)

    with pytest.raises(NotImplementedError, match="DandiRemoteS3LogAccessExtractor"):
        dandi_s3_log_extraction.extractors.DandiS3LogAccessExtractor(cache_directory=output_directory)


def test_extraction_parallel(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    output_directory = tmpdir / "test_extraction"
    output_directory.mkdir(exist_ok=True)

    with pytest.raises(NotImplementedError, match="DandiRemoteS3LogAccessExtractor"):
        dandi_s3_log_extraction.extractors.DandiS3LogAccessExtractor(cache_directory=output_directory)


# TODO: CLI
