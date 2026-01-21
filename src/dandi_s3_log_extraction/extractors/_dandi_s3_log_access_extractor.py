import pathlib

import s3_log_extraction

from .._regex import DROGON_IP_REGEX_ENCRYPTED


class DandiS3LogAccessExtractor(s3_log_extraction.extractors.S3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in raw S3 logs.

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
    """

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        super().__init__(cache_directory=cache_directory)

        self._relative_script_path = pathlib.Path(__file__).parent / "_dandi_extraction.awk"

        ips_to_skip_regex = s3_log_extraction.encryption_utils.decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)
        self._awk_env["IPS_TO_SKIP_REGEX"] = ips_to_skip_regex.decode("utf-8")
