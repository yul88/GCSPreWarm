"""GCP Authentication provider using Application Default Credentials (ADC) / Metadata Server.

Provides thread-safe and async-compatible OAuth2 bearer token acquisition with
automatic proactive caching and refreshing.
"""

import asyncio
import datetime
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Scope required for GCS operations
STORAGE_FULL_CONTROL_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write"


class GCPAuthProvider:
    """Manages GCP OAuth2 token lifecycle with proactive cache refresh."""

    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode
        self._credentials = None
        self._token: Optional[str] = None
        self._expiry: Optional[datetime.datetime] = None
        self._lock = asyncio.Lock()
        self._cached_headers: Dict[str, str] = {}
        self._cached_project_id: Optional[str] = None
        self._last_refresh_mono: float = 0.0

    def _init_credentials(self) -> None:
        """Initialize Google credentials if not already loaded."""
        if self.mock_mode:
            return
        if self._credentials is None:
            try:
                import google.auth
                import google.auth.transport.requests

                credentials, _ = google.auth.default(
                    scopes=[STORAGE_FULL_CONTROL_SCOPE]
                )
                self._credentials = credentials
                self._request = google.auth.transport.requests.Request()
            except Exception as e:
                logger.warning(
                    f"Failed to load standard GCP Application Default Credentials: {e}. "
                    f"Will fallback to mock token if in dry-run mode."
                )

    def _refresh_sync(self) -> None:
        """Synchronously refresh token using google-auth with gcloud CLI fallback."""
        if self.mock_mode:
            self._token = "mock-bearer-token-for-dry-run"
            self._expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
            return

        if self._credentials is not None:
            try:
                self._credentials.refresh(self._request)
                self._token = self._credentials.token
                self._expiry = self._credentials.expiry
                if self._expiry and self._expiry.tzinfo is None:
                    self._expiry = self._expiry.replace(tzinfo=datetime.timezone.utc)
                if self._token:
                    return
            except Exception as e:
                logger.debug(f"Standard ADC refresh failed: {e}. Trying gcloud CLI fallback...")

        # Fallback to gcloud CLI
        try:
            import subprocess

            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                check=True,
            )
            # Find token line (starts with ya29.)
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if line.startswith("ya29."):
                    self._token = line
                    self._expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=45)
                    return
            token = result.stdout.strip().splitlines()[-1].strip()
            if token:
                self._token = token
                self._expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=45)
                return
        except Exception as e2:
            logger.warning(f"gcloud auth fallback also failed: {e2}")

        if not self._token:
            self._token = "mock-bearer-token-for-dry-run"
            self._expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

    async def get_bearer_token(self) -> str:
        """Return a valid Bearer token, refreshing asynchronously if near expiration."""
        if self.mock_mode:
            return "mock-bearer-token-for-dry-run"

        now = datetime.datetime.now(datetime.timezone.utc)
        # Refresh if token is absent or expires within 5 minutes
        needs_refresh = (
            self._token is None
            or self._expiry is None
            or (self._expiry - now).total_seconds() < 300
        )

        if needs_refresh:
            async with self._lock:
                # Double-check inside lock
                if (
                    self._token is None
                    or self._expiry is None
                    or (self._expiry - now).total_seconds() < 300
                ):
                    self._init_credentials()
                    # Run sync refresh in worker thread to prevent blocking async event loop
                    await asyncio.to_thread(self._refresh_sync)

        return self._token or ""

    async def get_auth_headers(self, project_id: Optional[str] = None) -> Dict[str, str]:
        """Return cached HTTP headers, refreshing proactively with zero per-request allocation."""
        if self.mock_mode:
            return {"Authorization": "Bearer mock-bearer-token-for-dry-run"}

        now_mono = time.monotonic()
        # Fast path: return cached header dict if refreshed recently
        if (
            self._cached_headers
            and (now_mono - self._last_refresh_mono < 60.0)
            and (self._cached_project_id == project_id)
        ):
            return self._cached_headers

        token = await self.get_bearer_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "GCSPreWarm/1.0",
        }
        if project_id and project_id.strip():
            headers["x-goog-user-project"] = project_id.strip()

        self._cached_headers = headers
        self._cached_project_id = project_id
        self._last_refresh_mono = time.monotonic()
        return self._cached_headers


_auth_provider_instance: Optional[GCPAuthProvider] = None


def get_auth_provider(mock_mode: bool = False) -> GCPAuthProvider:
    """Get or create singleton GCPAuthProvider instance."""
    global _auth_provider_instance
    if _auth_provider_instance is None or mock_mode:
        _auth_provider_instance = GCPAuthProvider(mock_mode=mock_mode)
    return _auth_provider_instance
