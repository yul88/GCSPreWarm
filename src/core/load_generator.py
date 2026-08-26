"""Asynchronous HTTP load generation engine for Google Cloud Storage.

Executes high-throughput Read and Write operations with connection pooling,
Application Default Credentials, and real-time telemetry recording.
"""

import asyncio
import logging
import random
import time
from typing import List, Optional, Set

import aiohttp

from src.auth.gcp_auth import GCPAuthProvider
from src.config.settings import Settings
from src.core.metrics import MetricsCollector
from src.core.partitioner import KeyPartitioner, ShardingPlan
from src.core.rate_limiter import AdaptiveRampController, TokenBucketRateLimiter

logger = logging.getLogger(__name__)


class GCSLoadEngine:
    """High-concurrency async load engine against GCS REST/XML API."""

    def __init__(
        self,
        settings: Settings,
        partitioner: KeyPartitioner,
        auth_provider: GCPAuthProvider,
        metrics: MetricsCollector,
        mock_mode: bool = False,
    ):
        self.settings = settings
        self.partitioner = partitioner
        self.plan: ShardingPlan = partitioner.plan
        self.auth_provider = auth_provider
        self.metrics = metrics
        self.mock_mode = mock_mode

        # Dummy payload buffer
        self._payload_bytes = b"X" * self.settings.object_size_bytes

        # Rate limiters
        self.read_limiter = TokenBucketRateLimiter(0.0)
        self.write_limiter = TokenBucketRateLimiter(0.0)

        # Tracked keys for cleanup
        self._created_keys: Set[str] = set()
        self._seed_keys: List[str] = []
        self._keys_lock = asyncio.Lock()

        # Session & Connection Pool
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self._concurrency_semaphore = asyncio.Semaphore(self.settings.http_max_connections)

        # Control flag
        self._is_running = False

    async def initialize(self) -> None:
        """Initialize HTTP connection pool and aiohttp ClientSession."""
        if self.mock_mode:
            return

        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        self._connector = aiohttp.TCPConnector(
            limit=self.settings.http_max_connections,
            limit_per_host=self.settings.http_max_connections,
            keepalive_timeout=self.settings.http_keep_alive_seconds,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
        )

    async def close(self) -> None:
        """Close connection pools and release resources."""
        self._is_running = False
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()

    def _get_object_url(self, key: str) -> str:
        """Construct full GCS REST/XML object endpoint URL."""
        return f"{self.settings.gcs_base_url}/{self.settings.gcs_bucket_name}/{key}"

    async def _execute_write(self, key: str) -> None:
        """Execute a single PUT request."""
        if self.mock_mode:
            t0 = time.perf_counter()
            await asyncio.sleep(random.uniform(0.005, 0.020))
            lat = (time.perf_counter() - t0) * 1000.0
            self.metrics.record_request("WRITE", 200, lat)
            async with self._keys_lock:
                self._created_keys.add(key)
            return

        url = self._get_object_url(key)
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)
        headers["Content-Type"] = "application/octet-stream"

        try:
            assert self._session is not None
            async with self._concurrency_semaphore:
                t0 = time.perf_counter()
                async with self._session.put(url, data=self._payload_bytes, headers=headers) as resp:
                    lat = (time.perf_counter() - t0) * 1000.0
                    status = resp.status
                    self.metrics.record_request("WRITE", status, lat)
                    if status < 300:
                        async with self._keys_lock:
                            self._created_keys.add(key)
        except Exception as e:
            self.metrics.record_request("WRITE", 0, 0.0, error=str(e))

    async def _execute_read(self, key: str) -> None:
        """Execute a single GET request."""
        if self.mock_mode:
            t0 = time.perf_counter()
            await asyncio.sleep(random.uniform(0.003, 0.015))
            lat = (time.perf_counter() - t0) * 1000.0
            self.metrics.record_request("READ", 200, lat)
            return

        url = self._get_object_url(key)
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)

        try:
            assert self._session is not None
            async with self._concurrency_semaphore:
                t0 = time.perf_counter()
                async with self._session.get(url, headers=headers) as resp:
                    # Read response body to complete transfer
                    await resp.read()
                    lat = (time.perf_counter() - t0) * 1000.0
                    self.metrics.record_request("READ", resp.status, lat)
        except Exception as e:
            self.metrics.record_request("READ", 0, 0.0, error=str(e))

    async def _execute_delete(self, key: str) -> bool:
        """Execute a single DELETE request."""
        if self.mock_mode:
            await asyncio.sleep(0.001)
            return True

        url = self._get_object_url(key)
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)

        try:
            assert self._session is not None
            async with self._session.delete(url, headers=headers) as resp:
                return resp.status in (200, 204, 404)
        except Exception as e:
            logger.debug(f"Failed to delete {key}: {e}")
            return False

    async def seed_objects(self, count_per_prefix: int) -> int:
        """Pre-populate seed objects across all prefix shards for read tests."""
        total_created = 0
        seed_tasks = []

        for prefix in self.plan.prefixes:
            for idx in range(count_per_prefix):
                seed_key = self.partitioner.generate_seed_key(prefix, idx)
                self._seed_keys.append(seed_key)
                seed_tasks.append(self._execute_write(seed_key))

        # Run seed writes in batches of 100
        batch_size = 100
        for i in range(0, len(seed_tasks), batch_size):
            batch = seed_tasks[i : i + batch_size]
            await asyncio.gather(*batch)
            total_created += len(batch)

        return total_created

    async def run_write_worker(self) -> None:
        """Continuous write worker loop governed by write_limiter."""
        prefixes = self.plan.prefixes
        num_prefixes = len(prefixes)
        idx = 0

        while self._is_running:
            await self.write_limiter.acquire()
            if not self._is_running:
                break
            prefix = prefixes[idx % num_prefixes]
            idx += 1
            key = self.partitioner.generate_write_key(prefix)
            asyncio.create_task(self._execute_write(key))

    async def run_read_worker(self) -> None:
        """Continuous read worker loop governed by read_limiter."""
        if not self._seed_keys:
            # Fallback: if no seed keys, generate dummy read keys
            for prefix in self.plan.prefixes:
                self._seed_keys.append(self.partitioner.generate_seed_key(prefix, 0))

        num_keys = len(self._seed_keys)
        while self._is_running:
            await self.read_limiter.acquire()
            if not self._is_running:
                break
            key = self._seed_keys[random.randint(0, num_keys - 1)]
            asyncio.create_task(self._execute_read(key))

    def update_rates(self, read_qps: float, write_qps: float) -> None:
        """Update active rate limiters with new QPS targets."""
        self.read_limiter.set_rate(read_qps)
        self.write_limiter.set_rate(write_qps)

    async def cleanup_all_objects(self, max_concurrency: int = 300) -> int:
        """Delete all created test objects and seed objects."""
        all_keys = list(self._created_keys) + list(self._seed_keys)
        if not all_keys:
            return 0

        semaphore = asyncio.Semaphore(max_concurrency)

        async def _bounded_delete(k: str) -> bool:
            async with semaphore:
                return await self._execute_delete(k)

        tasks = [_bounded_delete(k) for k in all_keys]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        deleted_count = sum(1 for r in results if r is True)
        return deleted_count
