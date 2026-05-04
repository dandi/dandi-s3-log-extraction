# CHANGELOG

# Upcoming

## Features

Added `--inventory` CLI flag to `dandis3logextraction extract --mode remote`, exposing the upstream `inventory_directory` parameter on `DandiRemoteS3LogAccessExtractor.extract_s3_bucket`. When provided, unprocessed log files are discovered from a pre-downloaded local AWS S3 Inventory directory instead of performing live `s5cmd ls` calls against the bucket.

Added `--summary-directory` CLI option to both `dandis3logextraction update summaries` and `dandis3logextraction update totals`, allowing users to specify a custom output directory for summaries instead of always using the default path from configuration.
Also exposed `summary_directory` as a direct parameter on the `generate_dandiset_summaries` Python API.

# v0.0.5

## Improvements

Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of S3 log lines (requests) per grouping (date, asset, or region).



# v0.01

## Improvements

Package was untangled from the parent `s3-log-extraction`.
