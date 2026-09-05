# CHANGELOG

# Upcoming

### ⚠️ Breaking

- Reworked privacy protection to follow the upstream package. Individual values are no longer censored below a disclosure threshold or rounded to a modulo, so `by_day.tsv`, `by_asset.tsv`, `by_region.tsv`, and `requester_count.tsv` now report their true values. Protection instead gates the publication of `by_region.tsv`, which is the only summary that pairs activity with requester location. That file is written only when the update it carries moves more than `region_disclosure_threshold` (default `5`) resolved regions at once. A resolved region is any label naming a physical place, such as `US/California`. The consequence is that the totals of a `by_region.tsv` drift out of step with the other summaries between publications. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
- Summary generation now raises a `RuntimeError` when a blob's `timestamps.txt`, `download.txt`, or `ips.txt` is missing or when they are not line-aligned, instead of quietly counting that blob as having no downloads and no views. A missing or short `download.txt` means the blob was extracted before that file existed, so the extraction cache is incompatible and the blob must be re-extracted. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
- Switched the summary update step to the JSON Lines usage path cache from the `derivatives` branch of the `dandi-cache/content-id-to-usage-dandiset-path` repository. The previous gzipped JSON mapping on the `min` branch is no longer published upstream. ([#92](https://github.com/dandi/dandi-s3-log-extraction/pull/92))
- Replaced the encrypted `DROGON_IP_REGEX_ENCRYPTED` constant with an unencrypted `IPS_TO_SKIP` environment variable for the regular expression of Drogon IPs to skip during extraction. The `S3_LOG_EXTRACTION_PASSWORD` is no longer required by this package. ([#87](https://github.com/dandi/dandi-s3-log-extraction/pull/87))

### 🚀 Enhancement

- Added a `number_of_views` column to the `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries of each Dandiset and of the archive, along with `total_number_of_views` in `totals.json` and `archive_totals.json`. A view is a streaming session rather than a request, defined as a maximal run of streaming (HTTP 206) requests from one IP address to one asset in which no two consecutive requests are more than 8 hours apart. Full downloads (HTTP 200) are never views and continue to be reported by `number_of_downloads`. A session can straddle midnight, so it is counted on the day of its first request, and it is attributed to the region of the single requester that made it. Sessionization is delegated to the upstream package so that a view means the same thing in the DANDI summaries as it does in the generic ones. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
- Added a `--threshold` flag to `dandis3logextraction update summaries`, in both the default and the `archive` mode, which sets the `region_disclosure_threshold` the by-region summaries are published under. It defaults to `5`. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
- Excluded IPs attributed to known cloud/hosting/VPN services (AWS, GCP, GitHub, VPN) from the unique requester counts in Dandiset and archive summaries. Delegates to `s3_log_extraction.ip_utils.is_cloud_service_or_vpn_label` so that both packages agree on what counts as a requester. ([#93](https://github.com/dandi/dandi-s3-log-extraction/pull/93))
- `--content-id-to-usage-dandiset-path-url` (and the corresponding `content_id_to_usage_dandiset_path_url` keyword argument) now also accepts a local file path to the JSON Lines cache for offline use. Malformed cache lines are skipped with a warning. ([#92](https://github.com/dandi/dandi-s3-log-extraction/pull/92))
- Added `download` to `_dandi_extraction.awk` so extraction writes `download.txt` alongside the other per-request outputs. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--cache-directory` to `dandis3logextraction extract` so remote extraction can use a custom cache directory. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--inventory` to `dandis3logextraction extract --mode remote` so extraction can use a local S3 Inventory directory. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `--directory` to `dandis3logextraction update summaries` and `dandis3logextraction update totals` for cache directory selection. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `number_of_requesters` to Dandiset and archive summaries. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Added `number_of_downloads` to Dandiset and archive `by_day.tsv`, `by_asset.tsv`, `by_region.tsv`, and `totals.json` outputs for parity with upstream summary conventions. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Refactored `generate_dandiset_totals` to derive the summary directory from `cache_directory`. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))
- Renamed the `--directory` CLI flag to `--cache` in the update commands. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))

### 🐛 Bug Fix

- Fixed the archive requester count, which double-counted requesters. `generate_dandiset_summaries` writes each Dandiset's `requester_count.tsv`, but nothing wrote the archive-level one, so it was left to the upstream `generate_archive_summaries`, which summed the per-Dandiset counts and so counted a requester once per Dandiset it accessed. `generate_dandiset_summaries` now writes `archive/requester_count.tsv` itself as the union of unique requesters across the whole extraction cache, restoring the archive-level call dropped in [#73](https://github.com/dandi/dandi-s3-log-extraction/pull/73). Published `archive_totals.json` values of `number_of_requesters` were overstated by the amount of cross-Dandiset overlap and will drop when the summaries are next regenerated. ([#95](https://github.com/dandi/dandi-s3-log-extraction/pull/95))

### 🔩 Dependency Updates

- Raised the `s3_log_extraction` lower bound to `>=1.10.12`, which is where the archive `requester_count.tsv` stopped being overwritten with the sum of the per-dataset counts. ([#95](https://github.com/dandi/dandi-s3-log-extraction/pull/95))
- Raised the `s3_log_extraction` lower bound to `>=1.10.11` for the streaming session view counting and the reworked privacy protection. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
- Updated compatibility for the latest `s3-log-extraction` release by pinning the lower bound to `>=1.9.2` and adapting extractor tests and summary columns. ([#68](https://github.com/dandi/dandi-s3-log-extraction/pull/68))

### 🏠 Internal

- Pointed the temporary upstream installation step of the testing workflows at the default branch of `s3-log-extraction`, so that the suites run against the `1.10.12` lower bound before it is released to PyPI. ([#95](https://github.com/dandi/dandi-s3-log-extraction/pull/95))
- Installed `s3_log_extraction` from the `refs/pull/294/head` reference of the upstream repository in the testing workflows, so that the suites can run before the pinned `1.10.11` lower bound is released to PyPI. This step is temporary and should be removed once that release is published. ([#94](https://github.com/dandi/dandi-s3-log-extraction/pull/94))
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
