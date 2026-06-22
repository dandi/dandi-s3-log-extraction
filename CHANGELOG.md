# CHANGELOG

# Upcoming

### 🚀 Enhancement

- Added an experimental, DANDI-only delivery ratio feature. The delivery ratio is the total bytes delivered over the asset's true DANDI size, signaling streaming versus download intensity. A per-asset `delivery_ratio` column is added to `by_asset.tsv`. The asset-weighted percentiles `delivery_ratio_p10`, `delivery_ratio_p25`, `delivery_ratio_p50`, `delivery_ratio_p75`, `delivery_ratio_p90` plus the volume-weighted `delivery_ratio_weighted` are injected into per-Dandiset `totals.json`, into archive `archive_totals.json`, and into a new archive `delivery_ratio.tsv`, via DANDI wrappers around the upstream summary and totals methods. Asset sizes are fetched from the DANDI API and cached locally in `asset_sizes.json`. ([#86](https://github.com/dandi/dandi-s3-log-extraction/pull/86))
- Added the `dandis3logextraction update totals` command, with `--mode archive` for archive-wide totals, wrapping the upstream totals generation to also emit the delivery ratio fields. ([#86](https://github.com/dandi/dandi-s3-log-extraction/pull/86))
- Added `download` to `_dandi_extraction.awk` so extraction writes `download.txt` alongside the other per-request outputs. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--cache-directory` to `dandis3logextraction extract` so remote extraction can use a custom cache directory. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--inventory` to `dandis3logextraction extract --mode remote` so extraction can use a local S3 Inventory directory. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--directory` to `dandis3logextraction update summaries` and `dandis3logextraction update totals` for cache directory selection. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `number_of_requesters` to Dandiset and archive summaries. Counts below 50 are reported as `"<50"`, and higher counts are rounded to the nearest multiple of 20. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `number_of_downloads` to Dandiset and archive `by_day.tsv`, `by_asset.tsv`, `by_region.tsv`, and `totals.json` outputs for parity with upstream summary conventions. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Refactored `generate_dandiset_totals` to derive the summary directory from `cache_directory`. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Renamed the `--directory` CLI flag to `--cache` in the update commands. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))

### 📝 Documentation

- Documented the experimental delivery ratio metric in the `README.md`. ([#86](https://github.com/dandi/dandi-s3-log-extraction/pull/86))

### 🔩 Dependency Updates

- Updated compatibility for the latest `s3-log-extraction` release by pinning the lower bound to `>=1.9.2` and adapting extractor tests and summary columns. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))

### 🏠 Internal

- Swapped runtime argument type checking from `pydantic.validate_call` to `beartype` for DANDI summary generation functions. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Removed the database bundling tools. That includes `bundle_database`, `dandis3logextraction update database`, and the `database` submodule. The `sharing` optional dependency group was also removed. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Removed the `--manifest` CLI option and `manifest_file_path` from `dandis3logextraction extract`. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- `DandiS3LogAccessExtractor` and the `dandi` and `dandi-remote` CLI modes now raise `NotImplementedError`. Use `DandiRemoteS3LogAccessExtractor` and `--mode remote` instead. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))

# v0.0.5

### 🚀 Enhancement

- Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries. This counts the number of S3 log lines per grouping. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))



# v0.01

### 🏠 Internal

- Package was untangled from the parent `s3-log-extraction`. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
