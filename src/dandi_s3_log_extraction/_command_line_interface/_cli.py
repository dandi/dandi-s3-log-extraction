"""Call the DANDI S3 log extraction tool from the command line."""

import os
import typing

import rich_click
import s3_log_extraction

from ..database import bundle_database
from ..extractors import DandiRemoteS3LogAccessExtractor, DandiS3LogAccessExtractor
from ..summarize import generate_dandiset_summaries, generate_dandiset_totals


# dandis3logextraction
@rich_click.group()
def _dandis3logextraction_cli():
    pass


# dandis3logextraction extract < directory >
@_dandis3logextraction_cli.command(name="extract")
@rich_click.argument("directory", type=rich_click.Path(writable=False))
@rich_click.option(
    "--limit",
    help="The maximum number of files to process. By default, all files will be processed.",
    required=False,
    type=rich_click.IntRange(min=1),
    default=None,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc. "
        "By default, "
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@rich_click.option(
    "--mode",
    help=(
        "Special parsing mode related to expected object key structure; "
        "for example, if 'dandi' then only extract 'blobs' and 'zarr' objects. "
        "By default, objects will be processed using the generic structure."
    ),
    required=False,
    type=rich_click.Choice(choices=["remote", "dandi", "dandi-remote"]),
    default=None,
)
@rich_click.option(
    "--manifest",
    "manifest_file_path",
    help=(
        "A custom manifest file specifying the paths of log files to process from the S3 bucket that would not be "
        "discovered by the natural nesting pattern. Typically used in cases where the storage pattern was swapped "
        "from flat to nested at a particular point in time."
    ),
    required=False,
    type=rich_click.Path(writable=False),
    default=None,
)
def _extract_cli(
    directory: str,
    limit: int | None = None,
    workers: int = -2,
    mode: typing.Literal["remote"] | None = None,
    manifest_file_path: str | None = None,
) -> None:
    """
    Extract S3 log access data from the specified directory.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.

    DIRECTORY : The path to the folder containing all raw S3 log files.
    """
    match mode:
        case "remote":
            extractor = DandiRemoteS3LogAccessExtractor()
            extractor.extract_s3_bucket(
                s3_root=directory,
                limit=limit,
                workers=workers,
                manifest_file_path=manifest_file_path,
            )
        case _:
            extractor = DandiS3LogAccessExtractor()
            extractor.extract_directory(directory=directory, limit=limit, workers=workers)


# dandis3logextraction stop
@_dandis3logextraction_cli.command(name="stop")
@rich_click.option(
    "--timeout",
    "max_timeout_in_seconds",
    help=(
        "The maximum time to wait (in seconds) for the extraction processes to stop before "
        "ceasing to track their status. This does not mean that the processes will not stop after this time."
        "Recall this command to start a new timeout."
    ),
    required=False,
    type=rich_click.IntRange(min=1),
    default=600,  # 10 minutes
)
def _stop_extraction_cli(max_timeout_in_seconds: int = 600) -> None:
    """
    Stop the extraction processes if any are currently running in other windows.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.
    """
    s3_log_extraction.extractors.stop_extraction(max_timeout_in_seconds=max_timeout_in_seconds)


# dandis3logextraction update
@_dandis3logextraction_cli.group(name="update")
def _update_cli() -> None:
    pass


# dandis3logextraction update database
@_update_cli.command(name="database")
def _bundle_database_cli() -> None:
    """Update (or create) a bundled Parquet database for easier sharing."""
    bundle_database()


# dandis3logextraction update summaries
@_update_cli.command(name="summaries")
@rich_click.option(
    "--mode",
    help=(
        "Generate condensed summaries of activity across the extracted data per object key. "
        "Mode 'dandi' will map asset hashes to Dandisets and their content filenames. "
        "Mode 'archive' aggregates over all dataset summaries."
    ),
    required=False,
    type=rich_click.Choice(choices=["dandi", "archive"]),
    default=None,
)
@rich_click.option(
    "--pick",
    help="A comma-separated list of directories to exclusively select when generating summaries.",
    required=False,
    type=rich_click.STRING,
    default=None,
)
@rich_click.option(
    "--skip",
    help="A comma-separated list of directories to exclude when generating summaries.",
    required=False,
    type=rich_click.STRING,
    default=None,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc. "
        "By default, "
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@rich_click.option(
    "--api-url",
    help=(
        "The DANDI API URL to use when generating Dandiset summaries. "
        "If not provided, the default DANDI API URL will be used."
    ),
    required=False,
    type=rich_click.STRING,
    default=None,
)
def _update_summaries_cli(
    mode: typing.Literal["dandi", "archive"] | None = None,
    pick: str | None = None,
    skip: str | None = None,
    workers: int = -2,
    api_url: str | None = None,
) -> None:
    """Generate condensed summaries of activity."""
    match mode:
        case "archive":
            s3_log_extraction.summarize.generate_archive_summaries()
        case _:
            pick_as_list = pick.split(",") if pick is not None else None
            skip_as_list = skip.split(",") if skip is not None else None
            generate_dandiset_summaries(pick=pick_as_list, skip=skip_as_list, workers=workers, api_url=api_url)


# dandis3logextraction update totals
@_update_cli.command(name="totals")
@rich_click.option(
    "--mode",
    help=(
        "Generate condensed summaries of activity across the extracted data per object key. "
        "Mode 'dandi' will map asset hashes to Dandisets and their content filenames. "
    ),
    required=False,
    type=rich_click.Choice(choices=["dandi", "archive"]),
    default=None,
)
def _update_totals_cli(mode: typing.Literal["dandi", "archive"] | None = None) -> None:
    """Generate grand totals of all extracted data."""
    match mode:
        case "archive":
            s3_log_extraction.summarize.generate_archive_totals()
        case _:
            generate_dandiset_totals()
