import json
import pathlib

import pandas
import s3_log_extraction
from beartype import beartype


# TODO: can likely be replaced by the generic version
@beartype
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

    # TODO: can likely be replaced entirely by the generic one

    all_dandiset_totals = dict()
    for dandiset_id_folder_path in summary_directory.iterdir():
        if not dandiset_id_folder_path.is_dir():
            continue

        dandiset_id = dandiset_id_folder_path.name
        if dandiset_id == "archive":
            continue

        summary_file_path = summary_directory / dandiset_id / "by_region.tsv"
        summary = pandas.read_table(filepath_or_buffer=summary_file_path)

        unique_countries = dict()
        for region in summary["region"]:
            if region in ["VPN", "GitHub", "unknown"]:
                continue

            region_split = region.split("/")
            country_code = region_split[0]
            region_code = "-".join(region_split[1:])
            if "AWS" in country_code:
                country_code = region_code.split("-")[0].upper()

            unique_countries[country_code] = True

        number_of_unique_regions = len(summary["region"])
        number_of_unique_countries = len(unique_countries)

        requester_count_file_path = dandiset_id_folder_path / "requester_count.tsv"
        number_of_requesters: str | int = (
            requester_count_file_path.read_text().strip() if requester_count_file_path.exists() else 0
        )
        if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
            number_of_requesters = int(number_of_requesters)

        all_dandiset_totals[dandiset_id] = {
            "total_bytes_sent": int(summary["bytes_sent"].sum()),
            "number_of_unique_regions": number_of_unique_regions,
            "number_of_unique_countries": number_of_unique_countries,
            "total_number_of_requests": int(summary["number_of_requests"].sum()),
            "number_of_requesters": number_of_requesters,
        }

    if not all_dandiset_totals:
        return

    archive_requester_count_file_path = summary_directory / "archive" / "requester_count.tsv"
    archive_number_of_requesters: str | int = (
        archive_requester_count_file_path.read_text().strip() if archive_requester_count_file_path.exists() else 0
    )
    if isinstance(archive_number_of_requesters, str) and not archive_number_of_requesters.startswith("<"):
        archive_number_of_requesters = int(archive_number_of_requesters)

    archive_totals: dict[str, int | str] = {
        "total_bytes_sent": sum(entry["total_bytes_sent"] for entry in all_dandiset_totals.values()),
        "total_number_of_requests": sum(entry["total_number_of_requests"] for entry in all_dandiset_totals.values()),
        "number_of_requesters": archive_number_of_requesters,
    }

    top_level_summary_file_path = summary_directory / "totals.json"
    with top_level_summary_file_path.open(mode="w") as io:
        json.dump(obj={**all_dandiset_totals, "archive": archive_totals}, fp=io)
