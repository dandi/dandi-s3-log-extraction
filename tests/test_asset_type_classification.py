import pytest

from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import _get_asset_type


@pytest.mark.parametrize(
    ("asset_path", "expected_asset_type"),
    [
        ("sub-001/sub-001_ephys.nwb", "Neurophysiology"),
        ("sub-001/sub-001_ephys.nwb.zarr", "Neurophysiology"),
        ("sub-001/sub-001_image.nii.gz", "Microscopy"),
        ("sub-001/sub-001_image.ome.zarr", "Microscopy"),
        ("sub-001/sub-001_tracks.trk", "Microscopy"),
        ("sub-001/sub-001_default.zarr", "Microscopy"),
        ("sub-001/sub-001_video.mp4", "Video"),
        ("sub-001/sub-001_notes.json", "Miscellaneous"),
        ("undetermined", "Miscellaneous"),
    ],
)
def test_get_asset_type(asset_path: str, expected_asset_type: str) -> None:
    assert _get_asset_type(asset_path=asset_path) == expected_asset_type
