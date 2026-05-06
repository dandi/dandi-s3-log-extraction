# CHANGELOG

# Upcoming

## Removals

Removed the database bundling tools (`bundle_database` function, `dandis3logextraction update database` CLI command, and `database` submodule). These are retired in favor of restic snapshots as the external sharing layer. The `sharing` optional dependency group (which provided `polars`) has also been removed.

Removed the `--manifest` CLI option and `manifest_file_path` parameter from `dandis3logextraction extract`. The manifest file feature remains available in the upstream `s3-log-extraction` package for users who need it, but is no longer exposed by this DANDI-specific plugin.

`DandiS3LogAccessExtractor` now raises `NotImplementedError` to indicate that this package only supports the remote extractor. Use `DandiRemoteS3LogAccessExtractor` instead. The CLI modes `dandi` and `dandi-remote` similarly raise `NotImplementedError`; use `--mode remote` for all extraction.

Replaced the local `generate_dandiset_totals` implementation with a delegation to `s3_log_extraction.summarize.generate_all_dataset_totals`. The function signature is unchanged. Behavioral changes: the `archive` subdirectory is no longer explicitly excluded (if it has a `by_region.tsv` file it will be included in totals); an empty summary directory now always writes `totals.json` containing `{}`; JSON output now uses `indent=2`.

## Features

Added `download` field to the GAWK extraction step in `_dandi_extraction.awk`. The value is `1` when the HTTP status is exactly `200` (complete transfer) and `0` for all other 2xx codes (e.g. `206` partial). Values are stored in `download.txt` alongside `timestamps.txt`, `bytes_sent.txt`, and `full_ips.txt`.

Added `--inventory` CLI flag to `dandis3logextraction extract --mode remote`, exposing the upstream `inventory_directory` parameter on `DandiRemoteS3LogAccessExtractor.extract_s3_bucket`. When provided, unprocessed log files are discovered from a pre-downloaded local AWS S3 Inventory directory instead of performing live `s5cmd ls` calls against the bucket.

Added `--directory` CLI option to both `dandis3logextraction update summaries` and `dandis3logextraction update totals`, mapping to the `cache_directory` parameter. For `update summaries`, it is passed directly as `cache_directory`. For `update totals`, the CLI derives `summary_directory = cache_directory / "summaries"` internally.

# v0.0.5

## Improvements

Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of S3 log lines (requests) per grouping (date, asset, or region).



# v0.01

## Improvements

Package was untangled from the parent `s3-log-extraction`.
