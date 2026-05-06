import pathlib

import pydantic
import s3_log_extraction


@pydantic.validate_call
def generate_dandiset_totals(summary_directory: str | pathlib.Path | None = None) -> None:
    """
    Generate top-level totals of summarized access activity for all Dandisets.

    Parameters
    ----------
    summary_directory : pathlib.Path
        Path to the folder containing all previously generated summaries of the S3 access logs.
        If `None`, the default summary directory from the configuration will be used.
    """
    summary_directory = (
        pathlib.Path(summary_directory)
        if summary_directory is not None
        else s3_log_extraction.config.get_summary_directory()
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(summary_directory=summary_directory)
