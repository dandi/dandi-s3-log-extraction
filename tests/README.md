# Contributing

If you would like to contribute some failing examples to this test suite, please contact the maintainer (cody.c.baker.phd@gamil.com) prior to opening a pull request.

There are multiple fields that may require anonymization prior to being shared openly.

The names of the following test files are patterned off of the S3 log filename convention and may not accurately respect the timestamps of the lines within.



# Test Suite Overview

## Extraction Tests

### 2020-01-01-05-06-35-0123456789ABCDEF (Easy lines)

The 'easy' collection contains the most typical lines which follow a nice, simple, and reliable structure.



### 2022-01-01-05-06-35-0123456789ABCDEF (Hard lines)

The 'hard' collection contains many of the most difficult lines to extract as they were found from error reports.



## CLI Tests (test_cli.py)

Tests for the command-line interface using subprocess execution. These tests:
- Verify all CLI commands and subcommands work correctly
- Test help text and error handling
- Use `coverage run` to capture subprocess coverage

To run CLI tests with coverage:
```bash
pytest tests/test_cli.py -vv --cov=dandi_s3_log_extraction --cov-append
```

The coverage configuration in `pyproject.toml` enables parallel and subprocess coverage collection.
