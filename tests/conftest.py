import gzip
import json
import unittest.mock

import pytest
import requests

_CONTENT_ID_TO_USAGE_DANDISET_PATH = {
    "0801d996-200e-4173-ab49-d1784427e96a": {"000350": "sub-20161206-1/sub-20161206-1_ses-20161206T123501_ophys.nwb"},
    "3d780275-6a25-497f-9385-7424c642a247": {
        "000245": "sub-20220105001/sub-20220105001_ses-20220105T155227_slice-20220105001_cell-20220105001_icephys.nwb"
    },
    "a7b032b8-1e31-429f-975f-52a28cec6629": {
        "000108": (
            "sub-MITU01/ses-20211025h19m35s28/micr/"
            "sub-MITU01_ses-20211025h19m35s28_sample-101_stain-NN_run-1_chunk-2_SPIM.json"
        )
    },
    "cb65c877-882b-4554-8fa1-8f4e986e13a6": {
        "000108": (
            "sub-MITU01/ses-20220317h10m43s39/micr/"
            "sub-MITU01_ses-20220317h10m43s39_sample-21_stain-LEC_run-1_chunk-1_SPIM.ome.zarr"
        )
    },
    "11ec8933-1456-4942-922b-94e5878bb991": {"000126": "sub-1/sub-1.nwb"},
    "f35600ed-d1a2-4c00-88c8-ac9e2f7505a3": {
        "000296": "sub-14438671396432805383/sub-14438671396432805383_ses-20181201T162825_ophys.nwb"
    },
    # 9151aa34-ba0e-46e8-838d-bddebb3c76c7 is intentionally absent (goes to undetermined)
}


@pytest.fixture(autouse=True)
def mock_content_id_to_usage_dandiset_path(monkeypatch):
    """Mock HTTP requests for the content ID to usage dandiset path mapping URL to use local test fixture data."""
    original_get = requests.get

    def patched_get(url, *args, **kwargs):
        if "content_id_to_usage_dandiset_path" in str(url):
            mock_response = unittest.mock.Mock()
            mock_response.status_code = 200
            mock_response.content = gzip.compress(json.dumps(_CONTENT_ID_TO_USAGE_DANDISET_PATH).encode())
            return mock_response
        return original_get(url, *args, **kwargs)

    monkeypatch.setattr(requests, "get", patched_get)
