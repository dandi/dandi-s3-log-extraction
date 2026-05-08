# CHANGELOG

# Upcoming

## Removals

Removed the database bundling tools (`bundle_database` function, `dandis3logextraction update database` CLI command, and `database` submodule). These are retired in favor of restic snapshots as the external sharing layer. The `sharing` optional dependency group (which provided `polars`) has also been removed.

Removed the `--manifest` CLI option and `manifest_file_path` parameter from `dandis3logextraction extract`. The manifest file feature remains available in the upstream `s3-log-extraction` package for users who need it, but is no longer exposed by this DANDI-specific plugin.

`DandiS3LogAccessExtractor` now raises `NotImplementedError` to indicate that this package only supports the remote extractor. Use `DandiRemoteS3LogAccessExtractor` instead. The CLI modes `dandi` and `dandi-remote` similarly raise `NotImplementedError`; use `--mode remote` for all extraction.

## Features

Added `download` field to the GAWK extraction step in `_dandi_extraction.awk`. The value is `1` when the HTTP status is exactly `200` (complete transfer) and `0` for all other 2xx codes (e.g. `206` partial). Values are stored in `download.txt` alongside `timestamps.txt`, `bytes_sent.txt`, and `full_ips.txt`.

Added `--inventory` CLI flag to `dandis3logextraction extract --mode remote`, exposing the upstream `inventory_directory` parameter on `DandiRemoteS3LogAccessExtractor.extract_s3_bucket`. When provided, unprocessed log files are discovered from a pre-downloaded local AWS S3 Inventory directory instead of performing live `s5cmd ls` calls against the bucket.

Added `--directory` CLI option to both `dandis3logextraction update summaries` and `dandis3logextraction update totals`, mapping to the `cache_directory` parameter. For `update summaries`, it is passed directly as `cache_directory`. For `update totals`, the CLI derives `summary_directory = cache_directory / "summaries"` internally.

Added `number_of_requesters` field to Dandiset and archive level summaries. The count of unique requesters
(unique IP indices) per Dandiset is computed during summarization and written to `requester_count.tsv` per
Dandiset. For privacy protection, counts below the minimum threshold (50) are reported as the sentinel
string `"<50"`, while counts at or above the threshold are rounded to the nearest multiple of 20.
The archive-level count is a true union of all unique IP indices across all Dandisets. The
`generate_dandiset_totals` function reads these values and emits `number_of_requesters` in `totals.json`
for each Dandiset and for the archive. The unique requester count is intentionally not coupled to region
information and is not reported at the per-asset level.

# v0.0.5

## Improvements

Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of S3 log lines (requests) per grouping (date, asset, or region).



# v0.01

## Improvements

Package was untangled from the parent `s3-log-extraction`.
