# CHANGELOG

# Upcoming

## Removals

Removed the `--manifest` CLI option and `manifest_file_path` parameter from `dandis3logextraction extract`. The manifest file feature remains available in the upstream `s3-log-extraction` package for users who need it, but is no longer exposed by this DANDI-specific plugin.

## Features

Added `--inventory` CLI flag to `dandis3logextraction extract --mode remote`, exposing the upstream `inventory_directory` parameter on `DandiRemoteS3LogAccessExtractor.extract_s3_bucket`. When provided, unprocessed log files are discovered from a pre-downloaded local AWS S3 Inventory directory instead of performing live `s5cmd ls` calls against the bucket.

Added `--directory` CLI option to both `dandis3logextraction update summaries` and `dandis3logextraction update totals`, mapping to the `cache_directory` parameter. For `update summaries`, it is passed directly as `cache_directory`. For `update totals`, the CLI derives `summary_directory = cache_directory / "summaries"` internally.

# v0.0.5

## Improvements

Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of S3 log lines (requests) per grouping (date, asset, or region).



# v0.01

## Improvements

Package was untangled from the parent `s3-log-extraction`.
