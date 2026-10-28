import collections.abc
import pathlib

import s3_log_extraction


class DandiRemoteS3LogAccessExtractor(s3_log_extraction.extractors.RemoteS3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in remotely stored raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - interruptible
          However, you must do so in one of two ways:
            - Invoke the command `s3logextraction stop` to end the processes after the current round of completion.
            - Manually create a file in the extraction cache called '.stop_extraction'.
      - updatable

    Parameters
    ----------
    cache_directory : pathlib.Path | None, optional
        The directory holding the extraction cache and its records.
        Defaults to the configured cache directory.
    excluded_ips : iterable of str, optional
        The IP addresses whose requests are dropped during extraction, so they never enter the cache.
        Defaults to `s3_log_extraction.config.get_excluded_ips()`, the addresses listed in
        `~/.s3-log-extraction/excluded_ips.txt`.
        An explicit value replaces that default, so pass an empty list to drop nothing.
    """

    def __init__(
        self,
        *,
        cache_directory: pathlib.Path | None = None,
        excluded_ips: collections.abc.Iterable[str] | None = None,
    ) -> None:
        super().__init__(cache_directory=cache_directory, use_encryption=False, excluded_ips=excluded_ips)

        self._relative_script_path = pathlib.Path(__file__).parent / "_dandi_extraction.awk"
