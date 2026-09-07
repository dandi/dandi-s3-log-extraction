"""Tests for the upstream ``s3_log_extraction.ip_utils`` behaviors the DANDI pipeline relies on."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest
import s3_log_extraction
import yaml
from s3_log_extraction.ip_utils import IpRegionResolver, MappingRegionResolver
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

_NO_SERVICE_NETWORKS = {"GitHub": [], "AWS": [], "GCP": [], "VPN": []}


def _write_by_region_summary(summary_file_path: pathlib.Path, regions: list[str]) -> None:
    """Write a minimal published by-region summary listing the given region labels."""
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(f"{region}\t1\t1\t0\t1" for region in regions)
    summary_file_path.write_text(
        f"region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n{rows}\n"
    )


@pytest.mark.ai_generated
def test_resolver_classifies_without_database_when_no_address_needs_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Service and non-routable addresses are labeled without MaxMind credentials or the GeoLite2 database."""
    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)
    service_networks = {"GitHub": [], "AWS": [("203.0.113.0/24", "us-east-1")], "GCP": [], "VPN": []}

    with patch("s3_log_extraction.ip_utils._resolver.open_geolite2_database") as mock_open:
        with IpRegionResolver(service_networks=service_networks) as resolver:
            assert resolver.resolve("192.0.2.1") == "bogon"
            assert resolver.resolve("203.0.113.7") == "AWS/us-east-1"
    mock_open.assert_not_called()


@pytest.mark.ai_generated
def test_resolver_places_public_address_with_geolite2() -> None:
    """A public address outside every service range is placed by the database, as an alpha-3 label."""
    response = MagicMock()
    response.country.iso_code = "US"
    response.subdivisions = [MagicMock(iso_code="CA")]
    reader = MagicMock()
    reader.city.return_value = response

    resolver = IpRegionResolver(service_networks=_NO_SERVICE_NETWORKS, geolite2_reader=reader)

    assert resolver.resolve("8.8.8.8") == "USA/CA"
    reader.city.assert_called_once_with("8.8.8.8")


@pytest.mark.ai_generated
def test_mapping_region_resolver_stands_in_for_a_live_one() -> None:
    """A fixed mapping resolves its addresses and labels every other address ``missing``."""
    resolver = MappingRegionResolver({"192.0.2.1": "USA/CA"})

    assert resolver.resolve("192.0.2.1") == "USA/CA"
    assert resolver.resolve("192.0.2.2") == "missing"


@pytest.mark.ai_generated
def test_update_region_code_coordinates_without_summaries(tmp_path: pathlib.Path) -> None:
    """update_region_code_coordinates writes only the default entries when no summary has been published."""
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["bogon"] == {"latitude": None, "longitude": None}
    assert not any("/" in label and not label.startswith(("AWS", "GCP")) for label in coordinates)


@pytest.mark.ai_generated
def test_update_region_code_coordinates_runs_offline(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Geographic labels of the published summaries are located from the bundled ISO 3166 tables, offline."""
    _write_by_region_summary(tmp_path / "summaries" / "000001" / "by_region.tsv", regions=["USA/CA", "DEU/BE", "bogon"])

    monkeypatch.delenv("MAXMIND_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("MAXMIND_LICENSE_KEY", raising=False)

    with patch("requests.get") as mock_get:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    mock_get.assert_not_called()

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text())
    assert coordinates["USA/CA"] == s3_log_extraction.ip_utils.get_region_coordinates("USA/CA")
    assert coordinates["DEU/BE"] == s3_log_extraction.ip_utils.get_region_coordinates("DEU/BE")
    assert coordinates["bogon"] == {"latitude": None, "longitude": None}


@pytest.mark.ai_generated
def test_update_region_code_coordinates_reports_unknown_labels(tmp_path: pathlib.Path) -> None:
    """A label the ISO 3166 tables do not know is reported rather than silently dropped."""
    _write_by_region_summary(tmp_path / "summaries" / "000001" / "by_region.tsv", regions=["XX/YY"])

    with patch("builtins.print") as mock_print:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)

    mock_print.assert_called_once()
    assert "XX/YY" in mock_print.call_args[0][0]
