<div align="center">
  <picture>
    <img alt="dandi-s3-log-extraction logo" src="https://raw.githubusercontent.com/dandi/dandi-s3-log-extraction/main/docs/dandi-s3-log-extraction-logo.svg" width="200">
  </picture>

<h1>DANDI S3 Log Extraction</h1>
<p>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/s3-log-extraction.svg"></a>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="Ubuntu" src="https://img.shields.io/badge/Ubuntu-E95420?style=flat&logo=ubuntu&logoColor=white"></a>
    <a href="https://codecov.io/github/dandi/dandi-s3-log-extraction?branch=main"><img alt="codecov" src="https://codecov.io/github/dandi/dandi-s3-log-extraction/coverage.svg?branch=main"></a>
    <a href="https://github.com/dandi/dandi-s3-log-extraction/actions/workflows/daily_tests.yml/badge.svg"><img alt="Daily tests" src="https://github.com/dandi/dandi-s3-log-extraction/actions/workflows/daily_tests.yml/badge.svg"></a>
    <a href="https://github.com/dandi/dandi-s3-log-extraction/actions/workflows/daily_remote_tests.yml/badge.svg"><img alt="Daily remote tests" src="https://github.com/dandi/dandi-s3-log-extraction/actions/workflows/daily_remote_tests.yml/badge.svg"></a>

</p>
<p>
    <a href="https://pypi.org/project/s3-log-extraction/"><img alt="PyPI latest release version" src="https://badge.fury.io/py/s3-log-extraction.svg?id=py&kill_cache=1"></a>
    <a href="https://github.com/dandi/s3-log-extraction/blob/main/LICENSE.txt"><img alt="License: MIT" src="https://img.shields.io/pypi/l/s3-log-extraction.svg"></a>
</p>
<p>
    <a href="https://github.com/psf/black"><img alt="Python code style: Black" src="https://img.shields.io/badge/python_code_style-black-000000.svg"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Python code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
</p>
</div>

Fast extraction of access summary data from DANDI S3 logs.

Developed for the [DANDI Archive](https://dandiarchive.org/).



## Installation

```bash
pip install dandi-s3-log-extraction
```



## DANDI Usage

Usage on the DANDI archive logs requires a bit more customization than the generic package.

Begin by ensuring a special required environment variable is set:

**S3_LOG_EXTRACTION_PASSWORD**
  - Various sensitive information on Drogon is encrypted using this password, including:
    - the regular expression for all associated Drogon IPs.
    - the IP index and geolocation caches.

This allows us to store full IP information in a persistent way (in case we need to go back and do a lookup) while still being secure.

```bash
export S3_LOG_EXTRACTION_PASSWORD="ask_yarik_or_cody_for_password"
```

In fresh environments, the cache should be specified as:

```bash
dandis3logextraction config cache set /mnt/backup/dandi/s3-logs-extraction-cache
```

To run all the steps (such as for daily updates):

```bash
dandis3logextraction extract /mnt/backup/dandi/dandiarchive-logs
dandis3logextraction update ip indexes
dandis3logextraction update ip regions
dandis3logextraction update ip coordinates
dandis3logextraction update summaries
dandis3logextraction update totals
dandis3logextraction update summaries --mode archive
dandis3logextraction update totals --mode archive
```



## Delivery ratio (experimental)

The delivery ratio is an experimental, DANDI-only signal of streaming versus download intensity.

For each asset it is the total bytes delivered across all logged GET requests divided by the asset's true size in bytes. The true size comes from the DANDI API rather than the logs, and is fetched with a live lookup on each run. A ratio near 1 means access is download dominated. A ratio much greater than 1 means access is streaming dominated, since the asset's size worth of bytes has been delivered many times over across requests.

The metric appears in three places:

- A per-asset `delivery_ratio` column in each Dandiset's `by_asset.tsv`.
- Per-Dandiset percentiles in `totals.json`, and archive wide percentiles in `archive_totals.json` and `archive/delivery_ratio.tsv`. The reported fields are `delivery_ratio_p10`, `delivery_ratio_p25`, `delivery_ratio_p50`, `delivery_ratio_p75`, `delivery_ratio_p90`, and `delivery_ratio_weighted`.

Percentiles are reported at the Dandiset and archive level, rather than a single average, because per-asset delivery ratios are highly skewed. Within one Dandiset some assets are downloaded close to once while others are streamed many times over, so a mean would be dominated by a few heavily streamed assets and hide that spread. The five percentiles describe the shape of the distribution compactly, and the exact per-asset values are still available in `by_asset.tsv` for anyone who needs them.

The percentiles are asset weighted, where each asset contributes one ratio. The `delivery_ratio_weighted` field is volume weighted, computed as the total bytes delivered over the total asset size. The gap between the weighted value and the median is a deliberate heterogeneity signal, so both are reported. Assets with a missing or zero size are excluded from the computation. A Dandiset with no usable asset reports empty values for all six fields.
