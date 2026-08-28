"""Asynchronous HTTP load generation engine for Google Cloud Storage.

Executes high-throughput Read and Write operations with connection pooling,
Application Default Credentials, and real-time telemetry recording.
"""

import asyncio
import logging
import math
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

        # Session & Connection Pool
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None

        # Control flag & persistent worker coroutines
        self._is_running = False
        self._worker_coros: List[asyncio.Task] = []

    async def initialize(self) -> None:
        """Initialize HTTP connection pool and aiohttp ClientSession."""
        if self.mock_mode:
            return

        max_conn = self.settings.get_effective_http_connections()
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_seconds)
        self._connector = aiohttp.TCPConnector(
            limit=max_conn,
            limit_per_host=max_conn,
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
        if self._worker_coros:
            for t in list(self._worker_coros):
                if not t.done():
                    t.cancel()
            await asyncio.gather(*list(self._worker_coros), return_exceptions=True)
            self._worker_coros.clear()

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
            self._created_keys.add(key)
            return

        url = self._get_object_url(key)
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)

        try:
            assert self._session is not None
            t0 = time.perf_counter()
            async with self._session.put(url, data=self._payload_bytes, headers=headers) as resp:
                lat = (time.perf_counter() - t0) * 1000.0
                status = resp.status
                self.metrics.record_request("WRITE", status, lat)
                if status < 300:
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

    async def seed_objects(self, count_per_prefix: int, progress_callback: Optional[callable] = None) -> int:
        """Pre-populate seed objects across all prefix shards for read tests."""
        total_created = 0
        all_seed_keys = []

        for prefix in self.plan.prefixes:
            for idx in range(count_per_prefix):
                seed_key = self.partitioner.generate_seed_key(prefix, idx)
                self._seed_keys.append(seed_key)
                all_seed_keys.append(seed_key)

        # Bounded concurrency to safely seed objects without un-warmed bucket throttling
        semaphore = asyncio.Semaphore(50)

        async def _seed_one(k: str) -> None:
            nonlocal total_created
            async with semaphore:
                await self._execute_write(k)
                total_created += 1
                if progress_callback:
                    try:
                        progress_callback(total_created, len(all_seed_keys))
                    except Exception:
                        pass

        tasks = [_seed_one(k) for k in all_seed_keys]
        await asyncio.gather(*tasks, return_exceptions=True)
        return total_created

    def compute_pipeline_pool_size(self, target_qps: int) -> int:
        """Dynamically compute optimal coroutine pipeline depth based on target QPS."""
        if self.settings.worker_pool_size is not None and self.settings.worker_pool_size > 0:
            return self.settings.worker_pool_size

        if target_qps <= 0:
            return 10

        # Little's Law: (Target QPS / CPU Workers) * 50ms network RTT buffer
        workers_count = max(1, self.settings.num_workers)
        per_worker_qps = target_qps / workers_count
        computed = int(math.ceil(per_worker_qps * 0.05))
        # Clamp between 20 (minimum responsiveness) and 500 (safe max concurrency)
        return max(20, min(500, computed))

    async def run_write_pipeline(self, pool_size: Optional[int] = None) -> None:
        """Spawn a pool of persistent write worker coroutines that loop continuously."""
        if pool_size is None or pool_size <= 0:
            pool_size = self.compute_pipeline_pool_size(self.settings.target_write_qps)

        prefixes = self.plan.prefixes
        num_prefixes = len(prefixes)

        async def _write_worker_loop(worker_idx: int):
            idx = worker_idx
            while self._is_running:
                await self.write_limiter.acquire()
                if not self._is_running:
                    break
                prefix = prefixes[idx % num_prefixes]
                idx += 1
                key = self.partitioner.generate_write_key(prefix)
                await self._execute_write(key)

        workers = [asyncio.create_task(_write_worker_loop(i)) for i in range(pool_size)]
        self._worker_coros.extend(workers)
        await asyncio.gather(*workers, return_exceptions=True)

    async def run_read_pipeline(self, pool_size: Optional[int] = None) -> None:
        """Spawn a pool of persistent read worker coroutines that loop continuously."""
        if pool_size is None or pool_size <= 0:
            pool_size = self.compute_pipeline_pool_size(self.settings.target_read_qps)

        if not self._seed_keys:
            # Fallback: if no seed keys, generate dummy read keys
            for prefix in self.plan.prefixes:
                self._seed_keys.append(self.partitioner.generate_seed_key(prefix, 0))

        num_keys = len(self._seed_keys)

        async def _read_worker_loop():
            while self._is_running:
                await self.read_limiter.acquire()
                if not self._is_running:
                    break
                key = self._seed_keys[random.randint(0, num_keys - 1)]
                await self._execute_read(key)

        workers = [asyncio.create_task(_read_worker_loop()) for i in range(pool_size)]
        self._worker_coros.extend(workers)
        await asyncio.gather(*workers, return_exceptions=True)

    async def run_write_worker(self) -> None:
        """Backward-compatible alias for run_write_pipeline."""
        await self.run_write_pipeline()

    async def run_read_worker(self) -> None:
        """Backward-compatible alias for run_read_pipeline."""
        await self.run_read_pipeline()

    def update_rates(self, read_qps: float, write_qps: float) -> None:
        """Update active rate limiters with new QPS targets."""
        self.read_limiter.set_rate(read_qps)
        self.write_limiter.set_rate(write_qps)

    async def list_objects_by_prefix(self, prefix: str) -> List[str]:
        """List all object keys under a prefix via GCS JSON/REST API."""
        if self.mock_mode:
            return list(self._created_keys) + list(self._seed_keys)

        found_keys: List[str] = []
        page_token = None
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)

        try:
            assert self._session is not None
            while True:
                url = f"https://storage.googleapis.com/storage/v1/b/{self.settings.gcs_bucket_name}/o?prefix={prefix}&fields=items(name),nextPageToken&maxResults=1000"
                if page_token:
                    url += f"&pageToken={page_token}"

                async with self._session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    items = data.get("items", [])
                    for item in items:
                        if "name" in item:
                            found_keys.append(item["name"])

                    page_token = data.get("nextPageToken")
                    if not page_token:
                        break
        except Exception as e:
            logger.debug(f"Prefix listing failed: {e}")

        return found_keys

    async def cleanup_all_objects(
        self,
        max_concurrency: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Delete all created test objects, seed objects, and leftover objects under prefix."""
        if max_concurrency is None or max_concurrency <= 0:
            max_concurrency = self.settings.get_effective_cleanup_concurrency()

        all_keys = set(self._created_keys) | set(self._seed_keys)

        # In mock mode, instant cleanup
        if self.mock_mode:
            deleted_count = len(all_keys)
            self._created_keys.clear()
            self._seed_keys.clear()
            if progress_callback:
                try:
                    progress_callback(deleted_count, deleted_count)
                except Exception:
                    pass
            return deleted_count

        # Sweep objects under prefix if no known in-memory keys exist (e.g. standalone --clean-only mode)
        if not all_keys and self.settings.key_prefix_base:
            listed_keys = await self.list_objects_by_prefix(self.settings.key_prefix_base)
            all_keys.update(listed_keys)

        if not all_keys:
            return 0

        keys_list = list(all_keys)
        total_keys = len(keys_list)
        deleted_count = 0
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _bounded_delete(k: str) -> bool:
            nonlocal deleted_count
            async with semaphore:
                success = await self._execute_delete(k)
                if success:
                    deleted_count += 1
                if progress_callback:
                    try:
                        progress_callback(deleted_count, total_keys)
                    except Exception:
                        pass
                return success

        # Process in chunks of 1000 for efficient garbage collection
        chunk_size = 1000
        for i in range(0, total_keys, chunk_size):
            chunk = keys_list[i : i + chunk_size]
            tasks = [_bounded_delete(k) for k in chunk]
            await asyncio.gather(*tasks, return_exceptions=True)

        self._created_keys.clear()
        self._seed_keys.clear()
        return deleted_count
