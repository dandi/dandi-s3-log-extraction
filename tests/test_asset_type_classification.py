import pytest

from dandi_s3_log_extraction.summarize._generate_dandiset_summaries import _get_asset_type


@pytest.mark.parametrize(
    ("asset_path", "expected_asset_type"),
    [
        ("sub-001/sub-001_ephys.nwb", "Neurophysiology"),
        ("sub-001/sub-001_ephys.nwb.zarr", "Neurophysiology"),
        ("sub-001/sub-001_image.ome.zarr", "Microscopy"),
        ("sub-001/sub-001_default.zarr", "Microscopy"),
        ("sub-001/sub-001_tracks.trk", "Neuroimaging"),
        pytest.param("sub-001/dwi/sub-001_dwi.bvec", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/dwi/sub-001_dwi.bvecs", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/dwi/sub-001_dwi.bval", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/dwi/sub-001_dwi.bvals", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/anat/sub-001_T1w.nii", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/anat/sub-001_T1w.nii.gz", "Neuroimaging", marks=pytest.mark.ai_generated),
        pytest.param("sub-001/anat/sub-001_T1w.NII.GZ", "Neuroimaging", marks=pytest.mark.ai_generated),
        ("sub-001/sub-001_video.mp4", "Video"),
        ("sub-001/sub-001_notes.json", "Miscellaneous"),
        ("undetermined", "Miscellaneous"),
    ],
)
def test_get_asset_type(asset_path: str, expected_asset_type: str) -> None:
    assert _get_asset_type(asset_path=asset_path) == expected_asset_type
