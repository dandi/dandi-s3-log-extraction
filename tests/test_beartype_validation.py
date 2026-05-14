import pytest
from beartype.roar import BeartypeCallHintParamViolation

import dandi_s3_log_extraction.summarize


@pytest.mark.ai_generated
def test_generate_dandiset_summaries_rejects_invalid_cache_directory_type() -> None:
    with pytest.raises(BeartypeCallHintParamViolation):
        dandi_s3_log_extraction.summarize.generate_dandiset_summaries(cache_directory=123, workers=1)


@pytest.mark.ai_generated
def test_generate_dandiset_totals_rejects_invalid_summary_directory_type() -> None:
    with pytest.raises(BeartypeCallHintParamViolation):
        dandi_s3_log_extraction.summarize.generate_dandiset_totals(summary_directory=123)
