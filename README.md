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

**IPS_TO_SKIP**
  - A regular expression matching all associated Drogon IPs, which are skipped during extraction.

```bash
export IPS_TO_SKIP="ask_yarik_or_cody_for_regex"
```

The `S3_LOG_EXTRACTION_PASSWORD` is no longer required by this package. It is only relevant if you opt in to the upstream `s3-log-extraction` encryption of the IP index and geolocation caches.

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
