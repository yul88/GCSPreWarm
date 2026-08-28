"""Unit tests for GCPAuthProvider."""

import asyncio
import datetime
from unittest.mock import MagicMock, patch
import pytest

from src.auth.gcp_auth import GCPAuthProvider, get_auth_provider


@pytest.mark.asyncio
async def test_auth_provider_mock_mode():
    """Verify mock mode auth header generation."""
    provider = GCPAuthProvider(mock_mode=True)
    headers = await provider.get_auth_headers(project_id="test-project")
    assert "Authorization" in headers
    assert "Bearer" in headers["Authorization"]


@pytest.mark.asyncio
async def test_auth_provider_non_mock_no_deadlock():
    """Verify that non-mock get_auth_headers acquires token and does not deadlock."""
    provider = GCPAuthProvider(mock_mode=False)

    # Mock _init_credentials and _refresh_sync to simulate standard ADC
    mock_creds = MagicMock()
    mock_creds.token = "real-simulated-token-12345"
    mock_creds.expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

    provider._credentials = mock_creds
    provider._request = MagicMock()

    # Call get_auth_headers with a timeout to catch any deadlock regression
    headers = await asyncio.wait_for(
        provider.get_auth_headers(project_id="my-gcp-project"),
        timeout=2.0,
    )

    assert headers["Authorization"] == "Bearer real-simulated-token-12345"
    assert headers["User-Agent"] == "GCSPreWarm/1.0"
    assert headers["x-goog-user-project"] == "my-gcp-project"

    # Fast path verification (cached headers returned)
    cached_headers = await asyncio.wait_for(
        provider.get_auth_headers(project_id="my-gcp-project"),
        timeout=0.1,
    )
    assert cached_headers is headers
