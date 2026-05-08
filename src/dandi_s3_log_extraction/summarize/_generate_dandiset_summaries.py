import collections
import concurrent.futures
import datetime
import gzip
import itertools
import json
import pathlib

import natsort
import pandas
import pydantic
import requests
import s3_log_extraction
import tqdm

from .._parallel._utils import _handle_max_workers

ASSET_TYPES_IN_ORDER = ("Neurophysiology", "Microscopy", "Video", "Miscellaneous")
NEUROPHYSIOLOGY_SUFFIXES = {".nwb"}
MICROSCOPY_SUFFIXES = {".nii", ".ome", ".tiff", ".tif", ".bvecs", ".bvals", ".trk"}
VIDEO_SUFFIXES = {".mp4", ".mov", ".wmv", ".avi", ".mkv"}


@pydantic.validate_call
def generate_dandiset_summaries(
    *,
    cache_directory: str | pathlib.Path | None = None,
    pick: list[str] | None = None,
    skip: list[str] | None = None,
    workers: int = -2,
    content_id_to_usage_dandiset_path_url: str | None = None,
    api_url: str | None = None,
    unassociated: bool = False,
) -> None:
    """
    Generate top-level summaries of access activity for all Dandisets.

    Parameters
    ----------
    cache_directory : pathlib.Path
        Path to the folder containing all previously extracted S3 access logs.
        If `None`, the default extraction directory from the configuration will be used.
    workers : int
        Number of workers to use for parallel processing.
        If -1, use all available cores. If -2, use all cores minus one.
        If 1, run in serial mode.
        Default is -2.
    pick : list of strings, optional
        A list of Dandiset IDs to exclusively select when generating summaries.
    skip : list of strings, optional
        A list of Dandiset IDs to exclude when generating summaries.
    content_id_to_usage_dandiset_path_url : str, optional
        URL to retrieve the mapping of content IDs to Dandiset paths.
        Defaults to the pre-generated mapping stored in the `dandi-cache` GitHub repository.
    api_url : str, optional
        Base API URL of the server to interact with.
        Defaults to using the main DANDI API server.
    unassociated : bool, optional
        Whether to generate summaries based on current undetermined status.
    """
    import dandi.dandiapi

    cache_directory = (
        pathlib.Path(cache_directory) if cache_directory is not None else s3_log_extraction.config.get_cache_directory()
    )

    summary_directory = cache_directory / "summaries"
    summary_directory.mkdir(exist_ok=True)

    if pick is not None and skip is not None:
        message = "Cannot specify both `pick` and `skip` parameters simultaneously."
        raise ValueError(message)
    max_workers = _handle_max_workers(workers=workers)

    content_id_to_usage_dandiset_path_url = content_id_to_usage_dandiset_path_url or (
        "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/"
        "refs/heads/min/derivatives/content_id_to_usage_dandiset_path.min.json.gz"
    )

    index_to_region = s3_log_extraction.ip_utils.load_ip_cache(
        cache_type="index_to_region", cache_directory=cache_directory
    )

    if unassociated:
        dandiset_id_to_local_content_directories, content_id_to_dandiset_path = _get_undetermined_dandi_asset_info(
            content_id_to_usage_dandiset_path_url=content_id_to_usage_dandiset_path_url,
            cache_directory=cache_directory,
        )

        # Special key for no current association
        dandiset_id = "undetermined"
        _summarize_dandiset(
            dandiset_id=dandiset_id,
            blob_directories=dandiset_id_to_local_content_directories.get(dandiset_id, []),
            summary_directory=summary_directory,
            index_to_region=index_to_region,
            blob_id_to_asset_path=content_id_to_dandiset_path,
        )
    else:
        dandiset_id_to_local_content_directories, content_id_to_dandiset_path = _get_determinable_dandi_asset_info(
            content_id_to_usage_dandiset_path_url=content_id_to_usage_dandiset_path_url,
            cache_directory=cache_directory,
        )

        client = dandi.dandiapi.DandiAPIClient(api_url=api_url)
        if pick is None and skip is not None:
            dandiset_ids_to_exclude = {dandiset_id: True for dandiset_id in skip}
            dandiset_ids_to_summarize = [
                dandiset.identifier
                for dandiset in client.get_dandisets()
                if dandiset_ids_to_exclude.get(dandiset.identifier, False) is False
            ]
        elif pick is not None and skip is None:
            dandiset_ids_to_summarize = pick
        else:
            dandiset_ids_to_summarize = [dandiset.identifier for dandiset in client.get_dandisets()]

        if max_workers == 1:
            for dandiset_id in tqdm.tqdm(
                iterable=dandiset_ids_to_summarize,
                total=len(dandiset_ids_to_summarize),
                desc="Summarizing Dandisets",
                position=0,
                leave=True,
                mininterval=5.0,
                smoothing=0,
                unit="dandisets",
            ):
                blob_directories = dandiset_id_to_local_content_directories.get(dandiset_id, [])

                _summarize_dandiset(
                    dandiset_id=dandiset_id,
                    blob_directories=blob_directories,
                    summary_directory=summary_directory,
                    index_to_region=index_to_region,
                    blob_id_to_asset_path=content_id_to_dandiset_path,
                )
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _summarize_dandiset,
                        dandiset_id=dandiset_id,
                        blob_directories=dandiset_id_to_local_content_directories.get(dandiset_id, []),
                        summary_directory=summary_directory,
                        index_to_region=index_to_region,
                        blob_id_to_asset_path=content_id_to_dandiset_path,
                    )
                    for dandiset_id in dandiset_ids_to_summarize
                ]
                collections.deque(
                    (
                        future.result()
                        for future in tqdm.tqdm(
                            iterable=concurrent.futures.as_completed(futures),
                            total=len(dandiset_ids_to_summarize),
                            desc="Summarizing Dandisets",
                            position=0,
                            leave=True,
                            mininterval=5.0,
                            smoothing=0,
                            unit="dandisets",
                        )
                    ),
                    maxlen=0,
                )

    _summarize_archive_by_asset_type_per_week(summary_directory=summary_directory)
    _summarize_archive_by_day(summary_directory=summary_directory)
    _summarize_archive_by_region(summary_directory=summary_directory)

    if unassociated:
        all_blob_directories_for_archive = dandiset_id_to_local_content_directories.get("undetermined", [])
    else:
        all_blob_directories_for_archive = []
        for dandiset_id in dandiset_ids_to_summarize:
            all_blob_directories_for_archive.extend(
                dandiset_id_to_local_content_directories.get(dandiset_id, [])
            )
    _summarize_archive_unique_requester_count(
        blob_directories=all_blob_directories_for_archive,
        summary_file_path=summary_directory / "archive" / "requester_count.tsv",
    )


def _get_determinable_dandi_asset_info(
    *,
    content_id_to_usage_dandiset_path_url: str,
    cache_directory: pathlib.Path,
) -> tuple[dict[str, list[pathlib.Path]], dict[str, str]]:
    extraction_directory = cache_directory / "extraction"

    response = requests.get(content_id_to_usage_dandiset_path_url)
    if response.status_code != 200:
        message = (
            f"Failed to retrieve content ID to usage path mapping from {content_id_to_usage_dandiset_path_url} - "
            f"status code {response.status_code}: {response.json()}"
        )
        raise RuntimeError(message)
    content_id_to_usage_dandiset_path = json.loads(gzip.decompress(data=response.content))

    content_id_to_dandiset_path: dict[str, str] = dict()
    dandiset_id_to_local_content_directories = collections.defaultdict(list)
    for content_id, unique_dandiset_id_and_path in tqdm.tqdm(
        iterable=content_id_to_usage_dandiset_path.items(),
        total=len(content_id_to_usage_dandiset_path),
        desc="Mapping unique blob IDs to local paths",
        unit="blobs",
        smoothing=0,
    ):
        dandiset_id, unique_path = next(iter(unique_dandiset_id_and_path.items()))

        local_content_directory = (
            extraction_directory / "zarr" / content_id
            if ".zarr" in unique_path
            else extraction_directory / "blobs" / content_id[:3] / content_id[3:6] / content_id
        )
        content_id_to_dandiset_path[content_id] = unique_path
        dandiset_id_to_local_content_directories[dandiset_id].append(local_content_directory)

    return dandiset_id_to_local_content_directories, content_id_to_dandiset_path


def _get_undetermined_dandi_asset_info(
    *,
    content_id_to_usage_dandiset_path_url: str,
    cache_directory: pathlib.Path,
) -> tuple[dict[str, list[pathlib.Path]], dict[str, str]]:
    extraction_directory = cache_directory / "extraction"

    response = requests.get(content_id_to_usage_dandiset_path_url)
    if response.status_code != 200:
        message = (
            f"Failed to retrieve content ID to usage path mapping from {content_id_to_usage_dandiset_path_url} - "
            f"status code {response.status_code}: {response.json()}"
        )
        raise RuntimeError(message)
    content_id_to_usage_dandiset_path = json.loads(gzip.decompress(data=response.content))

    content_id_to_dandiset_path: dict[str, str] = dict()
    dandiset_id_to_local_content_directories = collections.defaultdict(list)

    # The previous loop is 'bottom-up' from provided content ID mappings from the DANDI Cache
    # Next, do a 'top-down' search over the entire extraction cache to find any uncaught IDs
    batch_size = 1_000_000
    tqdm_iterable = tqdm.tqdm(
        iterable=itertools.batched(iterable=extraction_directory.rglob(pattern="full_ips.txt"), n=batch_size),
        total=0,
        desc="Mapping undetermined blob IDs to local paths",
        unit="batches",
        smoothing=0,
        position=0,
        leave=True,
    )
    for batch in tqdm_iterable:
        tqdm_iterable.total += 1

        for timestamps_file_path in tqdm.tqdm(
            iterable=batch,
            total=len(batch),
            desc="Processing batch",
            unit="files",
            smoothing=0,
            position=1,
            leave=False,
        ):
            local_content_directory = timestamps_file_path.parent
            content_id = local_content_directory.name

            if content_id in content_id_to_usage_dandiset_path:
                continue  # This content ID already has a Dandiset association in the usage cache

            dandiset_id_to_local_content_directories["undetermined"].append(local_content_directory)

    return dandiset_id_to_local_content_directories, content_id_to_dandiset_path


def _summarize_dandiset(
    *,
    dandiset_id: str,
    blob_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    index_to_region: dict[int, str],
    blob_id_to_asset_path: dict[str, str],
) -> None:
    _summarize_dandiset_by_day(
        blob_directories=blob_directories, summary_file_path=summary_directory / dandiset_id / "by_day.tsv"
    )
    _summarize_dandiset_by_asset(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_asset.tsv",
        blob_id_to_asset_path=blob_id_to_asset_path,
    )
    _summarize_dandiset_by_asset_per_week(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_asset_per_week.tsv",
        blob_id_to_asset_path=blob_id_to_asset_path,
    )
    _summarize_dandiset_by_asset_type_per_week(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_asset_type_per_week.tsv",
        blob_id_to_asset_path=blob_id_to_asset_path,
    )
    _summarize_dandiset_by_region(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_region.tsv",
        index_to_region=index_to_region,
    )
    _summarize_dandiset_unique_requester_count(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "requester_count.tsv",
    )


def _summarize_dandiset_by_day(*, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path) -> None:
    all_dates = []
    all_bytes_sent = []
    for blob_directory in blob_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        timestamps_file_path = blob_directory / "timestamps.txt"
        dates = [
            _timestamp_to_date_format(timestamp=timestamp)
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_day = collections.defaultdict(int)
    number_of_requests_by_day = collections.defaultdict(int)
    for date, bytes_sent in zip(all_dates, all_bytes_sent):
        summarized_activity_by_day[date] += bytes_sent
        number_of_requests_by_day[date] += 1

    if len(summarized_activity_by_day) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_dates_ordered = list(summarized_activity_by_day.keys())
    summary_table = pandas.DataFrame(
        data={
            "date": all_dates_ordered,
            "bytes_sent": list(summarized_activity_by_day.values()),
            "number_of_requests": [number_of_requests_by_day[date] for date in all_dates_ordered],
        }
    )
    summary_table.sort_values(by="date", inplace=True)
    summary_table.index = range(len(summary_table))
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _timestamp_to_date_format(*, timestamp: str) -> str:
    date = f"20{timestamp[:2]}-{timestamp[2:4]}-{timestamp[4:6]}"
    return date


def _timestamp_to_week_start_date(*, timestamp: str) -> str:
    date = datetime.date(year=int("20" + timestamp[:2]), month=int(timestamp[2:4]), day=int(timestamp[4:6]))
    week_start = date - datetime.timedelta(days=date.weekday())
    return week_start.strftime("%Y-%m-%d")


def _summarize_dandiset_by_asset_per_week(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, blob_id_to_asset_path: dict[str, str]
) -> None:
    summarized_activity_by_asset_per_week: dict[str, dict[str, int]] = collections.defaultdict(
        lambda: collections.defaultdict(int)
    )
    all_asset_paths: set[str] = set()
    all_week_starts: set[str] = set()

    for blob_directory in blob_directories:
        blob_id = blob_directory.name

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        asset_path = blob_id_to_asset_path.get(blob_id, "undetermined")

        timestamps_file_path = blob_directory / "timestamps.txt"
        week_starts = [
            _timestamp_to_week_start_date(timestamp=timestamp)
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        for week_start, bs in zip(week_starts, bytes_sent):
            summarized_activity_by_asset_per_week[week_start][asset_path] += bs
            all_week_starts.add(week_start)
            all_asset_paths.add(asset_path)

    if not summarized_activity_by_asset_per_week:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_week_starts = sorted(all_week_starts)
    sorted_asset_paths = sorted(all_asset_paths)

    data: dict[str, list] = {"week_start": sorted_week_starts}
    for asset_path in sorted_asset_paths:
        data[asset_path] = [
            summarized_activity_by_asset_per_week[week].get(asset_path, 0) for week in sorted_week_starts
        ]

    summary_table = pandas.DataFrame(data=data)
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _get_asset_type(*, asset_path: str) -> str:
    suffixes = tuple(pathlib.Path(asset_path).suffixes)
    suffix_set = {suffix.lower() for suffix in suffixes}

    if suffix_set.intersection(NEUROPHYSIOLOGY_SUFFIXES):
        return "Neurophysiology"

    if suffix_set.intersection(MICROSCOPY_SUFFIXES) or (".zarr" in suffix_set and len(suffixes) == 1):
        return "Microscopy"

    if suffix_set.intersection(VIDEO_SUFFIXES):
        return "Video"

    return "Miscellaneous"


def _sort_asset_type_columns(*, column_names: list[str]) -> list[str]:
    known_columns = [column_name for column_name in ASSET_TYPES_IN_ORDER if column_name in column_names]
    extra_columns = sorted(set(column_names).difference(ASSET_TYPES_IN_ORDER))
    return [*known_columns, *extra_columns]


def _summarize_dandiset_by_asset_type_per_week(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, blob_id_to_asset_path: dict[str, str]
) -> None:
    summarized_activity_by_asset_type_per_week: dict[str, dict[str, int]] = collections.defaultdict(
        lambda: collections.defaultdict(int)
    )
    all_asset_types: set[str] = set()
    all_week_starts: set[str] = set()

    for blob_directory in blob_directories:
        blob_id = blob_directory.name

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        asset_path = blob_id_to_asset_path.get(blob_id, "undetermined")
        asset_type = _get_asset_type(asset_path=asset_path)
        all_asset_types.add(asset_type)

        timestamps_file_path = blob_directory / "timestamps.txt"
        week_starts = [
            _timestamp_to_week_start_date(timestamp=timestamp)
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        for week_start, bytes_sent_value in zip(week_starts, bytes_sent):
            summarized_activity_by_asset_type_per_week[week_start][asset_type] += bytes_sent_value
            all_week_starts.add(week_start)

    if not summarized_activity_by_asset_type_per_week:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_week_starts = sorted(all_week_starts)
    sorted_asset_types = _sort_asset_type_columns(column_names=list(all_asset_types))

    data: dict[str, list] = {"week_start": sorted_week_starts}
    for asset_type in sorted_asset_types:
        data[asset_type] = [
            summarized_activity_by_asset_type_per_week[week].get(asset_type, 0) for week in sorted_week_starts
        ]

    summary_table = pandas.DataFrame(data=data)
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_archive_by_asset_type_per_week(*, summary_directory: pathlib.Path) -> None:
    all_summaries = [
        pandas.read_table(filepath_or_buffer=summary_file_path)
        for summary_file_path in summary_directory.rglob(pattern="by_asset_type_per_week.tsv")
        if summary_file_path.parent.name != "archive"
    ]
    if not all_summaries:
        return

    all_summary_data = pandas.concat(objs=all_summaries, ignore_index=True)
    all_summary_data.fillna(value=0, inplace=True)
    all_summary_data.sort_values(by="week_start", inplace=True)

    asset_type_columns = _sort_asset_type_columns(
        column_names=[column_name for column_name in all_summary_data.columns if column_name != "week_start"]
    )
    if not asset_type_columns:
        return

    archive_summary = (
        all_summary_data.groupby(by="week_start", as_index=False)[asset_type_columns]
        .sum()
        .reindex(columns=["week_start", *asset_type_columns])
    )
    archive_summary = archive_summary.astype(dtype={column_name: "int64" for column_name in asset_type_columns})
    archive_summary.sort_values(by="week_start", inplace=True)

    archive_summary_file_path = summary_directory / "archive" / "by_asset_type_per_week.tsv"
    archive_summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    archive_summary.to_csv(path_or_buf=archive_summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_archive_by_day(*, summary_directory: pathlib.Path) -> None:
    _summarize_archive_by_grouped_column(
        summary_directory=summary_directory,
        tsv_name="by_day.tsv",
        group_column="date",
    )


def _summarize_archive_by_region(*, summary_directory: pathlib.Path) -> None:
    _summarize_archive_by_grouped_column(
        summary_directory=summary_directory,
        tsv_name="by_region.tsv",
        group_column="region",
    )


def _summarize_archive_by_grouped_column(*, summary_directory: pathlib.Path, tsv_name: str, group_column: str) -> None:
    all_summaries = [
        pandas.read_table(filepath_or_buffer=summary_file_path)
        for summary_file_path in summary_directory.rglob(pattern=tsv_name)
        if summary_file_path.parent.name != "archive"
    ]
    if not all_summaries:
        return

    all_summary_data = pandas.concat(objs=all_summaries, ignore_index=True)

    archive_summary = (
        all_summary_data.groupby(by=group_column, as_index=False)[["bytes_sent", "number_of_requests"]]
        .sum()
        .reindex(columns=[group_column, "bytes_sent", "number_of_requests"])
    )
    archive_summary = archive_summary.astype(dtype={"bytes_sent": "int64", "number_of_requests": "int64"})
    archive_summary.sort_values(by=group_column, key=natsort.natsort_keygen(), inplace=True)

    archive_summary_file_path = summary_directory / "archive" / tsv_name
    archive_summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    archive_summary.to_csv(path_or_buf=archive_summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dandiset_by_asset(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, blob_id_to_asset_path: dict[str, str]
) -> None:
    summarized_activity_by_asset = collections.defaultdict(int)
    number_of_requests_by_asset = collections.defaultdict(int)
    for blob_directory in blob_directories:
        blob_id = blob_directory.name

        # No extracted logs found (possible asset was never accessed); skip to next asset
        if not blob_directory.exists():
            continue

        # It is possible that this blob cannot be uniquely associated with an asset path within the Dandiset
        # (the blob ID would not be in the asset path mapping in that case)
        asset_path = blob_id_to_asset_path.get(blob_id, "undetermined")

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        summarized_activity_by_asset[asset_path] += sum(bytes_sent)
        number_of_requests_by_asset[asset_path] += len(bytes_sent)

    if len(summarized_activity_by_asset) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_asset_paths = list(summarized_activity_by_asset.keys())
    summary_table = pandas.DataFrame(
        data={
            "asset_path": all_asset_paths,
            "bytes_sent": list(summarized_activity_by_asset.values()),
            "number_of_requests": [number_of_requests_by_asset[path] for path in all_asset_paths],
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dandiset_by_region(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, index_to_region: dict[int, str]
) -> None:
    all_regions = []
    all_bytes_sent = []
    for blob_directory in blob_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        indexed_ips_file_path = blob_directory / "indexed_ips.txt"
        indexed_ips = [int(ip_index.strip()) for ip_index in indexed_ips_file_path.read_text().splitlines()]
        regions = [index_to_region.get(ip_index, "unknown") for ip_index in indexed_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_region = collections.defaultdict(int)
    number_of_requests_by_region = collections.defaultdict(int)
    for region, bytes_sent in zip(all_regions, all_bytes_sent):
        summarized_activity_by_region[region] += bytes_sent
        number_of_requests_by_region[region] += 1

    if len(summarized_activity_by_region) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_regions_ordered = list(summarized_activity_by_region.keys())
    summary_table = pandas.DataFrame(
        data={
            "region": all_regions_ordered,
            "bytes_sent": list(summarized_activity_by_region.values()),
            "number_of_requests": [number_of_requests_by_region[region] for region in all_regions_ordered],
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _round_requester_count(count: int, modulo: int, minimum: int) -> str | int:
    """
    Round a unique requester count for privacy protection.

    If the count is less than ``minimum``, returns a sentinel string indicating
    the count is below the threshold (e.g., ``"<50"``). Otherwise, rounds to the
    nearest multiple of ``modulo``.

    Parameters
    ----------
    count : int
        The exact number of unique requesters to round.
    modulo : int
        The granularity used for rounding (e.g., ``20`` rounds to the nearest 20).
    minimum : int
        The minimum disclosure threshold. Counts below this value are reported
        as ``"<{minimum}"`` to protect privacy.

    Returns
    -------
    str or int
        A string of the form ``"<{minimum}"`` if ``count < minimum``, otherwise
        an integer rounded to the nearest multiple of ``modulo``.
    """
    if count < minimum:
        return f"<{minimum}"
    return round(count / modulo) * modulo


def _collect_unique_ip_indexes(blob_directories: list[pathlib.Path]) -> set[str]:
    """
    Collect all unique IP indexes across the given blob directories.

    Parameters
    ----------
    blob_directories : list of pathlib.Path
        Paths to per-blob extraction directories containing ``indexed_ips.txt`` files.

    Returns
    -------
    set of str
        The set of unique IP index strings found across all ``indexed_ips.txt`` files.
    """
    unique_ip_indexes: set[str] = set()
    for blob_directory in blob_directories:
        if not blob_directory.exists():
            continue

        indexed_ips_file_path = blob_directory / "indexed_ips.txt"
        if not indexed_ips_file_path.exists():
            continue

        unique_ip_indexes.update(ip.strip() for ip in indexed_ips_file_path.read_text().splitlines())
    return unique_ip_indexes


def _summarize_dandiset_unique_requester_count(
    *,
    blob_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    modulo: int = 20,
    minimum: int = 50,
) -> None:
    """
    Compute and save the privacy-rounded unique requester count for a Dandiset.

    Reads all ``indexed_ips.txt`` files from the given blob directories, counts the
    number of unique IP indexes across the entire Dandiset, rounds the result via
    :func:`_round_requester_count`, and writes the value to ``summary_file_path``.

    Parameters
    ----------
    blob_directories : list of pathlib.Path
        Paths to the per-blob extraction directories containing ``indexed_ips.txt`` files.
    summary_file_path : pathlib.Path
        Destination file where the rounded count (as a string) will be written.
    modulo : int, optional
        Granularity for rounding. Default is ``20``.
    minimum : int, optional
        Minimum disclosure threshold. Counts below this are reported as ``"<{minimum}"``.
        Default is ``50``.
    """
    unique_ip_indexes = _collect_unique_ip_indexes(blob_directories=blob_directories)

    if not unique_ip_indexes:
        return

    rounded_count = _round_requester_count(count=len(unique_ip_indexes), modulo=modulo, minimum=minimum)
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(rounded_count))


def _summarize_archive_unique_requester_count(
    *,
    blob_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    modulo: int = 20,
    minimum: int = 50,
) -> None:
    """
    Compute and save the privacy-rounded unique requester count for the archive.

    Collects unique IP indexes across all provided blob directories (a true union
    across all Dandisets), rounds the result, and writes the value to ``summary_file_path``.

    Parameters
    ----------
    blob_directories : list of pathlib.Path
        All per-blob extraction directories from all Dandisets.
    summary_file_path : pathlib.Path
        Destination file where the rounded count will be written.
    modulo : int, optional
        Granularity for rounding. Default is ``20``.
    minimum : int, optional
        Minimum disclosure threshold. Counts below this are reported as ``"<{minimum}"``.
        Default is ``50``.
    """
    unique_ip_indexes = _collect_unique_ip_indexes(blob_directories=blob_directories)

    if not unique_ip_indexes:
        return

    rounded_count = _round_requester_count(count=len(unique_ip_indexes), modulo=modulo, minimum=minimum)
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(rounded_count))
