"""
S3 log extraction
===================

Extraction of minimal information from consolidated raw S3 logs for public sharing and plotting.

Developed for the DANDI Archive.

A few summary facts as of 2024:

- A single line of a raw S3 log file can be between 400-1000+ bytes.
- Some of the busiest daily logs on the archive can have around 5,014,386 lines.
- There are more than 6 TB of log files collected in total.
- This parser reduces that total to around 20 GB of essential information.

The reduced information is then additionally mapped to currently available assets in persistent published Dandiset
versions and current drafts, which only comprise around 100 MB of the original data.
"""

__all__ = [
    # Public submodules
    "database",
    "extractors",
    "summarize",
]
