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
        self._write_workers: List[asyncio.Task] = []
        self._read_workers: List[asyncio.Task] = []
        self._target_write_pool: int = 50
        self._target_read_pool: int = 50

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

    def compute_pipeline_pool_size(
        self,
        target_qps: int,
        observed_latency_ms: Optional[float] = None,
    ) -> int:
        """Dynamically compute optimal coroutine pipeline depth bounded by platform safety limits."""
        if self.settings.worker_pool_size is not None and self.settings.worker_pool_size > 0:
            return self.settings.worker_pool_size

        if target_qps <= 0:
            return 10

        min_clamp = self.settings.get_safe_min_concurrency_per_worker()
        max_clamp = self.settings.get_safe_max_concurrency_per_worker()

        # Dynamic Little's Law sizing with 50% safety headroom over real-time observed p95 latency:
        # Buffer = max(0.020s, min(1.0s, p95_latency * 1.5))
        if observed_latency_ms is not None and observed_latency_ms > 0:
            latency_sec = max(0.020, min(1.0, (observed_latency_ms / 1000.0) * 1.5))
        else:
            latency_sec = 0.12  # Baseline default before initial telemetry samples

        workers_count = max(1, self.settings.num_workers)
        per_worker_qps = target_qps / workers_count
        computed = int(math.ceil(per_worker_qps * latency_sec))

        # Clamp between platform-safe min and max limits
        return max(min_clamp, min(max_clamp, computed))

    def adjust_pipeline(
        self,
        target_read_qps: float,
        target_write_qps: float,
        observed_read_latency_ms: Optional[float] = None,
        observed_write_latency_ms: Optional[float] = None,
        observed_latency_ms: Optional[float] = None,
    ) -> None:
        """Dynamically adapt worker pool concurrency to match real-time observed Read & Write latencies independently."""
        read_lat = (
            observed_read_latency_ms
            if (observed_read_latency_ms is not None and observed_read_latency_ms > 0)
            else observed_latency_ms
        )
        write_lat = (
            observed_write_latency_ms
            if (observed_write_latency_ms is not None and observed_write_latency_ms > 0)
            else observed_latency_ms
        )

        if target_write_qps > 0:
            self._target_write_pool = self.compute_pipeline_pool_size(
                int(target_write_qps), observed_latency_ms=write_lat
            )
        if target_read_qps > 0:
            self._target_read_pool = self.compute_pipeline_pool_size(
                int(target_read_qps), observed_latency_ms=read_lat
            )

    async def run_write_pipeline(self, pool_size: Optional[int] = None) -> None:
        """Spawn a pool of persistent write worker coroutines that adapt dynamically."""
        if pool_size is None or pool_size <= 0:
            pool_size = self.compute_pipeline_pool_size(self.settings.target_write_qps)
        self._target_write_pool = pool_size

        prefixes = self.plan.prefixes
        num_prefixes = len(prefixes)
        min_safe = self.settings.get_safe_min_concurrency_per_worker()

        async def _write_worker_loop(worker_idx: int):
            idx = worker_idx
            while self._is_running:
                # If target pool scaled down, exit gracefully
                if len(self._write_workers) > self._target_write_pool and self._target_write_pool >= min_safe:
                    break
                await self.write_limiter.acquire()
                if not self._is_running:
                    break
                prefix = prefixes[idx % num_prefixes]
                slot = idx % self.settings.write_key_pool_size if self.settings.write_key_pool_size > 0 else None
                idx += 1
                key = self.partitioner.generate_write_key(prefix, slot_index=slot)
                await self._execute_write(key)

        for i in range(pool_size):
            t = asyncio.create_task(_write_worker_loop(i))
            self._write_workers.append(t)
            self._worker_coros.append(t)

        while self._is_running:
            self._write_workers = [t for t in self._write_workers if not t.done()]
            while len(self._write_workers) < self._target_write_pool and self._is_running:
                idx = len(self._write_workers)
                t = asyncio.create_task(_write_worker_loop(idx))
                self._write_workers.append(t)
                self._worker_coros.append(t)
            try:
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break

    async def run_read_pipeline(self, pool_size: Optional[int] = None) -> None:
        """Spawn a pool of persistent read worker coroutines that adapt dynamically."""
        if pool_size is None or pool_size <= 0:
            pool_size = self.compute_pipeline_pool_size(self.settings.target_read_qps)
        self._target_read_pool = pool_size

        if not self._seed_keys:
            # Fallback: if no seed keys, generate dummy read keys
            for prefix in self.plan.prefixes:
                self._seed_keys.append(self.partitioner.generate_seed_key(prefix, 0))

        num_keys = len(self._seed_keys)
        min_safe = self.settings.get_safe_min_concurrency_per_worker()

        async def _read_worker_loop():
            while self._is_running:
                if len(self._read_workers) > self._target_read_pool and self._target_read_pool >= min_safe:
                    break
                await self.read_limiter.acquire()
                if not self._is_running:
                    break
                key = self._seed_keys[random.randint(0, num_keys - 1)]
                await self._execute_read(key)

        for i in range(pool_size):
            t = asyncio.create_task(_read_worker_loop())
            self._read_workers.append(t)
            self._worker_coros.append(t)

        while self._is_running:
            self._read_workers = [t for t in self._read_workers if not t.done()]
            while len(self._read_workers) < self._target_read_pool and self._is_running:
                t = asyncio.create_task(_read_worker_loop())
                self._read_workers.append(t)
                self._worker_coros.append(t)
            try:
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break

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

    async def cleanup_prefix_shard(
        self,
        prefix: str,
        semaphore: asyncio.Semaphore,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Stream list and delete all objects within a specific prefix shard concurrently."""
        deleted_shard = 0
        page_token = None
        headers = await self.auth_provider.get_auth_headers(project_id=self.settings.gcp_project_id)

        async def _bounded_delete(k: str) -> bool:
            nonlocal deleted_shard
            async with semaphore:
                success = await self._execute_delete(k)
                if success:
                    deleted_shard += 1
                if progress_callback:
                    try:
                        progress_callback(1)
                    except Exception:
                        pass
                return success

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
                    keys = [item["name"] for item in items if "name" in item]

                    if keys:
                        tasks = [_bounded_delete(k) for k in keys]
                        await asyncio.gather(*tasks, return_exceptions=True)

                    page_token = data.get("nextPageToken")
                    if not page_token:
                        break
        except Exception as e:
            logger.debug(f"Shard cleanup for {prefix} failed: {e}")

        return deleted_shard

    async def cleanup_all_objects(
        self,
        max_concurrency: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Delete all created test objects, seed objects, and leftover objects under prefix shards."""
        if max_concurrency is None or max_concurrency <= 0:
            max_concurrency = self.settings.get_effective_cleanup_concurrency()

        # In mock mode, instant cleanup
        if self.mock_mode:
            deleted_count = len(self._seed_keys) or 100
            self._seed_keys.clear()
            if progress_callback:
                try:
                    progress_callback(deleted_count, deleted_count)
                except Exception:
                    pass
            return deleted_count

        total_deleted = 0
        semaphore = asyncio.Semaphore(max_concurrency)

        def _step_progress(inc: int):
            nonlocal total_deleted
            total_deleted += inc
            if progress_callback:
                try:
                    progress_callback(total_deleted, None)
                except Exception:
                    pass

        # Sweep all prefix shards concurrently in parallel
        tasks = [
            self.cleanup_prefix_shard(p, semaphore, _step_progress)
            for p in self.plan.prefixes
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        self._seed_keys.clear()
        return total_deleted
