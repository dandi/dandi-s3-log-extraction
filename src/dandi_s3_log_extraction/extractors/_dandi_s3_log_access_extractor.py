import pathlib

import s3_log_extraction


class DandiS3LogAccessExtractor(s3_log_extraction.extractors.S3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in raw S3 logs.

    .. deprecated::
        This local extractor is not supported. Use ``DandiRemoteS3LogAccessExtractor`` instead.
    """

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        raise NotImplementedError(
            "The local DANDI S3 log extractor is not supported by this package. "
            "Please use 'DandiRemoteS3LogAccessExtractor' instead."
        )
