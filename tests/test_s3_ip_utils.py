"""Tests for s3_log_extraction.ip_utils internal functions covering uncovered code paths."""

import os
import pathlib
from unittest.mock import MagicMock, patch

import pytest
import yaml
from s3_log_extraction.ip_utils._ip_utils import (
    _get_cidr_address_ranges_and_subregions,
    _request_cidr_range,
)
from s3_log_extraction.ip_utils._update_index_to_region_codes import (
    _get_region_code_from_ip_index,
    update_index_to_region_codes,
)
from s3_log_extraction.ip_utils._update_region_code_coordinates import (
    _get_coordinates_from_opencage,
    _get_coordinates_from_region_code,
    _get_service_coordinates_from_ipinfo,
    update_region_code_coordinates,
)


def _clear_lru_caches() -> None:
    """Clear LRU caches on ip_utils functions to ensure test isolation."""
    _request_cidr_range.cache_clear()
    _get_cidr_address_ranges_and_subregions.cache_clear()


# ─── _request_cidr_range ──────────────────────────────────────────────────────


@pytest.mark.ai_generated
def test_request_cidr_range_github() -> None:
    """_request_cidr_range fetches and returns the GitHub CIDR data."""
    _clear_lru_caches()
    fake_response = {"hooks": ["192.30.252.0/22"], "web": ["185.199.108.0/22"]}
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response

    with patch("s3_log_extraction.ip_utils._ip_utils.requests.get", return_value=mock_resp):
        result = _request_cidr_range("GitHub")

    assert result == fake_response
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_request_cidr_range_aws() -> None:
    """_request_cidr_range fetches and returns the AWS CIDR data."""
    _clear_lru_caches()
    fake_response = {"prefixes": [{"ip_prefix": "52.94.0.0/22", "region": "us-east-1"}]}
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response

    with patch("s3_log_extraction.ip_utils._ip_utils.requests.get", return_value=mock_resp):
        result = _request_cidr_range("AWS")

    assert result == fake_response
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_request_cidr_range_gcp() -> None:
    """_request_cidr_range fetches and returns the GCP CIDR data."""
    _clear_lru_caches()
    fake_response = {"prefixes": [{"ipv4Prefix": "34.64.0.0/10", "scope": "us-central1"}]}
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_response

    with patch("s3_log_extraction.ip_utils._ip_utils.requests.get", return_value=mock_resp):
        result = _request_cidr_range("GCP")

    assert result == fake_response
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_request_cidr_range_vpn() -> None:
    """_request_cidr_range fetches and returns the VPN CIDR list."""
    _clear_lru_caches()
    fake_content = b"1.0.0.0/24\n2.0.0.0/24\n"
    mock_resp = MagicMock()
    mock_resp.content = fake_content

    with patch("s3_log_extraction.ip_utils._ip_utils.requests.get", return_value=mock_resp):
        result = _request_cidr_range("VPN")

    assert result == ["1.0.0.0/24", "2.0.0.0/24"]
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_request_cidr_range_azure_raises() -> None:
    """_request_cidr_range raises NotImplementedError for Azure service."""
    _clear_lru_caches()
    with pytest.raises(NotImplementedError):
        _request_cidr_range("Azure")
    _clear_lru_caches()


# ─── _get_cidr_address_ranges_and_subregions ──────────────────────────────────


@pytest.mark.ai_generated
def test_get_cidr_address_ranges_github() -> None:
    """_get_cidr_address_ranges_and_subregions parses GitHub CIDR data."""
    _clear_lru_caches()
    fake_cidr = {"hooks": ["192.30.252.0/22"], "domains": ["example.com"]}

    with patch(
        "s3_log_extraction.ip_utils._ip_utils._request_cidr_range",
        return_value=fake_cidr,
    ):
        result = _get_cidr_address_ranges_and_subregions(service_name="GitHub")

    assert ("192.30.252.0/22", None) in result
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_cidr_address_ranges_aws() -> None:
    """_get_cidr_address_ranges_and_subregions parses AWS CIDR data."""
    _clear_lru_caches()
    fake_cidr = {"prefixes": [{"ip_prefix": "52.94.0.0/22", "region": "us-east-1"}]}

    with patch(
        "s3_log_extraction.ip_utils._ip_utils._request_cidr_range",
        return_value=fake_cidr,
    ):
        result = _get_cidr_address_ranges_and_subregions(service_name="AWS")

    assert ("52.94.0.0/22", "us-east-1") in result
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_cidr_address_ranges_gcp() -> None:
    """_get_cidr_address_ranges_and_subregions parses GCP CIDR data including IPv6-only filter."""
    _clear_lru_caches()
    fake_cidr = {
        "prefixes": [
            {"ipv4Prefix": "34.64.0.0/10", "scope": "us-central1"},
            {"ipv6Prefix": "2600::/32", "scope": "us-central1"},  # should be filtered out
        ]
    }

    with patch(
        "s3_log_extraction.ip_utils._ip_utils._request_cidr_range",
        return_value=fake_cidr,
    ):
        result = _get_cidr_address_ranges_and_subregions(service_name="GCP")

    assert ("34.64.0.0/10", "us-central1") in result
    assert all("2600" not in cidr for cidr, _ in result)
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_cidr_address_ranges_vpn() -> None:
    """_get_cidr_address_ranges_and_subregions parses VPN CIDR list."""
    _clear_lru_caches()
    fake_cidr = ["1.0.0.0/24", "2.0.0.0/24"]

    with patch(
        "s3_log_extraction.ip_utils._ip_utils._request_cidr_range",
        return_value=fake_cidr,
    ):
        result = _get_cidr_address_ranges_and_subregions(service_name="VPN")

    assert ("1.0.0.0/24", None) in result
    assert ("2.0.0.0/24", None) in result
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_cidr_address_ranges_azure_raises() -> None:
    """_get_cidr_address_ranges_and_subregions covers the Azure case when _request_cidr_range is mocked."""
    _clear_lru_caches()
    with patch(
        "s3_log_extraction.ip_utils._ip_utils._request_cidr_range",
        return_value={},
    ):
        with pytest.raises(NotImplementedError):
            _get_cidr_address_ranges_and_subregions(service_name="Azure")
    _clear_lru_caches()


# ─── update_index_to_region_codes ─────────────────────────────────────────────


@pytest.mark.ai_generated
def test_update_index_to_region_codes_no_api_key(tmp_path: pathlib.Path) -> None:
    """update_index_to_region_codes raises ValueError when IPINFO_API_KEY is not set."""
    env = {k: v for k, v in os.environ.items() if k != "IPINFO_API_KEY"}
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError, match="IPINFO_API_KEY"):
            update_index_to_region_codes(cache_directory=tmp_path)


@pytest.mark.ai_generated
def test_update_index_to_region_codes_with_mock(tmp_path: pathlib.Path) -> None:
    """update_index_to_region_codes processes IPs correctly with mocked ipinfo."""
    # Write unencrypted indexed_ips.yaml with 3 test entries (encrypt=False reads raw YAML)
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "indexed_ips.yaml").write_bytes(b"1: '1.2.3.4'\n2: '5.6.7.8'\n3: '9.10.11.12'\n")

    # Mock _get_region_code_from_ip_index to return three different scenarios
    call_results = [None, "unknown", "US/California"]

    def mock_get_region_code(ip_index, ip_address, ipinfo_handler, index_not_in_services):
        return call_results.pop(0)

    with (
        patch.dict(os.environ, {"IPINFO_API_KEY": "fake_key"}),
        patch("ipinfo.getHandler"),
        patch(
            "s3_log_extraction.ip_utils._update_index_to_region_codes._get_region_code_from_ip_index",
            side_effect=mock_get_region_code,
        ),
    ):
        update_index_to_region_codes(cache_directory=tmp_path, encrypt=False)

    # index_to_region.yaml should contain only the non-None, non-"unknown" entry
    result = yaml.safe_load((ip_cache_dir / "index_to_region.yaml").read_text())
    assert result is not None
    assert "US/California" in result.values()


@pytest.mark.ai_generated
def test_update_index_to_region_codes_with_batch_limit(tmp_path: pathlib.Path) -> None:
    """update_index_to_region_codes respects batch_limit parameter."""
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    # More IPs than batch_limit would process
    import yaml as _yaml

    ips = {i: f"1.2.3.{i}" for i in range(1, 6)}
    (ip_cache_dir / "indexed_ips.yaml").write_bytes(_yaml.dump(ips).encode())

    call_log = []

    def mock_get_region_code(ip_index, ip_address, ipinfo_handler, index_not_in_services):
        call_log.append(ip_index)
        return "US/TestRegion"

    with (
        patch.dict(os.environ, {"IPINFO_API_KEY": "fake_key"}),
        patch("ipinfo.getHandler"),
        patch(
            "s3_log_extraction.ip_utils._update_index_to_region_codes._get_region_code_from_ip_index",
            side_effect=mock_get_region_code,
        ),
    ):
        update_index_to_region_codes(cache_directory=tmp_path, encrypt=False, batch_limit=1, batch_size=2)

    # With batch_limit=1 and batch_size=2, at most 2 IPs are processed
    assert len(call_log) <= 2


# ─── _get_region_code_from_ip_index ──────────────────────────────────────────


@pytest.mark.ai_generated
def test_get_region_code_service_match_with_subregion() -> None:
    """_get_region_code_from_ip_index matches a CIDR range and includes subregion."""
    _clear_lru_caches()
    index_not_in_services: dict = {}
    mock_handler = MagicMock()

    with patch(
        "s3_log_extraction.ip_utils._update_index_to_region_codes._get_cidr_address_ranges_and_subregions"
    ) as mock_cidr:
        # First service ("GitHub") matches IP 1.2.3.4 in 1.2.3.0/24 with subregion "us-east-1"
        mock_cidr.return_value = [("1.2.3.0/24", "us-east-1")]

        result = _get_region_code_from_ip_index(
            ip_index=12345,
            ip_address="1.2.3.4",
            ipinfo_handler=mock_handler,
            index_not_in_services=index_not_in_services,
        )

    assert result == "GitHub/us-east-1"
    assert index_not_in_services[12345] is False
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_region_code_service_match_no_subregion() -> None:
    """_get_region_code_from_ip_index matches a CIDR range without a subregion."""
    _clear_lru_caches()
    index_not_in_services: dict = {}
    mock_handler = MagicMock()

    with patch(
        "s3_log_extraction.ip_utils._update_index_to_region_codes._get_cidr_address_ranges_and_subregions"
    ) as mock_cidr:
        # No subregion (None)
        mock_cidr.return_value = [("1.2.3.0/24", None)]

        result = _get_region_code_from_ip_index(
            ip_index=12345,
            ip_address="1.2.3.4",
            ipinfo_handler=mock_handler,
            index_not_in_services=index_not_in_services,
        )

    assert result == "GitHub"
    _clear_lru_caches()


@pytest.mark.ai_generated
def test_get_region_code_already_in_index_not_in_services() -> None:
    """_get_region_code_from_ip_index skips CIDR loop when ip_index is already known."""
    _clear_lru_caches()
    index_not_in_services: dict = {99999: True}  # already determined not in services
    mock_handler = MagicMock()

    with patch(
        "s3_log_extraction.ip_utils._update_index_to_region_codes._get_cidr_address_ranges_and_subregions"
    ) as mock_cidr:
        # Should NOT be called because ip_index is already in index_not_in_services
        _get_region_code_from_ip_index(
            ip_index=99999,
            ip_address="1.2.3.4",
            ipinfo_handler=mock_handler,
            index_not_in_services=index_not_in_services,
        )
        mock_cidr.assert_not_called()
    _clear_lru_caches()


# ─── update_region_code_coordinates ──────────────────────────────────────────


@pytest.mark.ai_generated
def test_update_region_code_coordinates_no_keys(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates raises ValueError when API keys are missing."""
    env = {k: v for k, v in os.environ.items() if k not in ("OPENCAGE_API_KEY", "IPINFO_API_KEY")}
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError, match="API_KEY"):
            update_region_code_coordinates(cache_directory=tmp_path)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_no_index_file(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates raises FileNotFoundError when index_to_region.yaml is absent."""
    with (
        patch.dict(os.environ, {"OPENCAGE_API_KEY": "fake", "IPINFO_API_KEY": "fake"}),
        patch("ipinfo.getHandler"),
        patch("opencage.geocoder.OpenCageGeocode"),
    ):
        with pytest.raises(FileNotFoundError):
            update_region_code_coordinates(cache_directory=tmp_path)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_full_mock(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates processes region codes with all mock dependencies."""
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)

    # Create index_to_region.yaml with several region types
    (ip_cache_dir / "index_to_region.yaml").write_text(
        "1: 'US/California'\n2: 'AWS/us-east-1'\n3: 'bogon'\n"
    )

    mock_ipinfo_client = MagicMock()
    mock_opencage_client = MagicMock()
    mock_opencage_client.geocode.return_value = [{"geometry": {"lat": 37.77, "lng": -122.41}}]

    with (
        patch.dict(os.environ, {"OPENCAGE_API_KEY": "fake", "IPINFO_API_KEY": "fake"}),
        patch("ipinfo.getHandler", return_value=mock_ipinfo_client),
        patch("opencage.geocoder.OpenCageGeocode", return_value=mock_opencage_client),
        patch(
            "s3_log_extraction.ip_utils._update_region_code_coordinates._get_cidr_address_ranges_and_subregions"
        ) as mock_cidr,
    ):
        # AWS/us-east-1 → service path via _get_service_coordinates_from_ipinfo
        mock_cidr.return_value = [("52.94.0.0/22", "us-east-1")]
        mock_ipinfo_details = MagicMock()
        mock_ipinfo_details.details = {"latitude": 39.0, "longitude": -77.0}
        mock_ipinfo_client.getDetails.return_value = mock_ipinfo_details

        update_region_code_coordinates(cache_directory=tmp_path)

    output_file = ip_cache_dir / "region_codes_to_coordinates.yaml"
    assert output_file.exists()


@pytest.mark.ai_generated
def test_update_region_code_coordinates_opencage_failure(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates prints message when OpenCage returns no results."""
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "index_to_region.yaml").write_text("1: 'XX/UnknownRegion'\n")

    mock_opencage_client = MagicMock()
    mock_opencage_client.geocode.return_value = []  # empty → failure

    with (
        patch.dict(os.environ, {"OPENCAGE_API_KEY": "fake", "IPINFO_API_KEY": "fake"}),
        patch("ipinfo.getHandler"),
        patch("opencage.geocoder.OpenCageGeocode", return_value=mock_opencage_client),
        patch("builtins.print") as mock_print,
    ):
        update_region_code_coordinates(cache_directory=tmp_path)

    mock_print.assert_called_once()
    assert "XX/UnknownRegion" in mock_print.call_args[0][0]


# ─── _get_coordinates_from_region_code ───────────────────────────────────────


@pytest.mark.ai_generated
def test_get_coordinates_from_region_code_service() -> None:
    """_get_coordinates_from_region_code dispatches to service path for known services."""
    mock_ipinfo_client = MagicMock()
    mock_ipinfo_details = MagicMock()
    mock_ipinfo_details.details = {"latitude": 39.0, "longitude": -77.0}
    mock_ipinfo_client.getDetails.return_value = mock_ipinfo_details
    service_coordinates: dict = {}

    with patch(
        "s3_log_extraction.ip_utils._update_region_code_coordinates._get_cidr_address_ranges_and_subregions"
    ) as mock_cidr:
        mock_cidr.return_value = [("52.94.0.1/32", "us-east-1")]

        result = _get_coordinates_from_region_code(
            country_and_region_code="AWS/us-east-1",
            ipinfo_client=mock_ipinfo_client,
            opencage_client=MagicMock(),
            service_coordinates=service_coordinates,
            opencage_failures=[],
        )

    assert result == {"latitude": 39.0, "longitude": -77.0}


@pytest.mark.ai_generated
def test_get_coordinates_from_region_code_regular() -> None:
    """_get_coordinates_from_region_code dispatches to OpenCage path for non-service regions."""
    mock_opencage_client = MagicMock()
    mock_opencage_client.geocode.return_value = [{"geometry": {"lat": 37.77, "lng": -122.41}}]

    result = _get_coordinates_from_region_code(
        country_and_region_code="US/California",
        ipinfo_client=MagicMock(),
        opencage_client=mock_opencage_client,
        service_coordinates={},
        opencage_failures=[],
    )

    assert result == {"latitude": 37.77, "longitude": -122.41}


@pytest.mark.ai_generated
def test_get_service_coordinates_cached() -> None:
    """_get_service_coordinates_from_ipinfo returns cached coordinates immediately."""
    cached_coords = {"latitude": 1.0, "longitude": 2.0}
    service_coordinates = {"AWS": cached_coords}  # Cached by service_name (not subregion key)

    result = _get_service_coordinates_from_ipinfo(
        country_and_region_code="AWS/us-east-1",
        ipinfo_client=MagicMock(),
        service_coordinates=service_coordinates,
    )

    assert result == cached_coords


@pytest.mark.ai_generated
def test_get_coordinates_from_opencage_no_results() -> None:
    """_get_coordinates_from_opencage returns None and records failure when no results."""
    mock_opencage_client = MagicMock()
    mock_opencage_client.geocode.return_value = []
    failures: list = []

    result = _get_coordinates_from_opencage(
        country_and_region_code="ZZ/Nowhere",
        opencage_client=mock_opencage_client,
        opencage_failures=failures,
    )

    assert result is None
    assert "ZZ/Nowhere" in failures
