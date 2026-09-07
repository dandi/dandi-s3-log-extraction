"""Tests for the upstream ``s3_log_extraction.ip_utils`` behaviors the DANDI pipeline relies on."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest
import s3_log_extraction
import yaml
from s3_log_extraction.ip_utils._ip_utils import (
    _get_cidr_address_ranges_and_subregions,
    _request_cidr_range,
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

    with patch("requests.get", return_value=mock_resp):
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

    with patch("requests.get", return_value=mock_resp):
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

    with patch("requests.get", return_value=mock_resp):
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

    with patch("requests.get", return_value=mock_resp):
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


# ─── Geolocation steps of the DANDI pipeline ─────────────────────────────────


@pytest.mark.ai_generated
def test_update_ip_to_region_codes_skips_database_when_cache_is_complete(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A daily run with no new IPs must not need MaxMind credentials or open the GeoLite2 database."""
    extraction_dir = tmp_path / "extraction"
    extraction_dir.mkdir(parents=True)
    (extraction_dir / "ips.txt").write_text("192.0.2.1\n")
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "ip_to_region.yaml").write_text("192.0.2.1: bogon\n")

    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=tmp_path, use_encryption=False)

    result = yaml.safe_load((ip_cache_dir / "ip_to_region.yaml").read_text())
    assert result == {"192.0.2.1": "bogon"}


@pytest.mark.ai_generated
def test_update_region_code_coordinates_no_index_file(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates raises FileNotFoundError when ip_to_region.yaml is absent."""
    with pytest.raises(FileNotFoundError):
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_runs_offline(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Geographic labels are located from the bundled ISO 3166 tables without credentials or network access."""
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "ip_to_region.yaml").write_text("192.0.2.1: USA/CA\n192.0.2.2: DEU/BE\n192.0.2.3: bogon\n")

    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    with patch("requests.get") as mock_get:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    mock_get.assert_not_called()

    coordinates = yaml.safe_load((ip_cache_dir / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["USA/CA"] == s3_log_extraction.ip_utils.get_region_coordinates("USA/CA")
    assert coordinates["DEU/BE"] == s3_log_extraction.ip_utils.get_region_coordinates("DEU/BE")
    assert coordinates["bogon"] == {"latitude": None, "longitude": None}


@pytest.mark.ai_generated
def test_update_region_code_coordinates_reports_unknown_labels(tmp_path: pathlib.Path) -> None:
    """A label the ISO 3166 tables do not know is reported rather than silently dropped."""
    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir(parents=True)
    (ip_cache_dir / "ip_to_region.yaml").write_text("192.0.2.1: XX/YY\n")

    with patch("builtins.print") as mock_print:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    mock_print.assert_called_once()
    assert "XX/YY" in mock_print.call_args[0][0]
