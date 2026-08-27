"""Multi-process orchestration engine for parallel multi-core load generation.

Spawns independent worker processes (1 per CPU core) to bypass Python GIL,
allowing high-performance execution of tens of thousands of QPS.
"""

import asyncio
import logging
import multiprocessing
from multiprocessing import Event, Manager, Process, Queue, Value
import time
from typing import Dict, List, Optional

from src.auth.gcp_auth import get_auth_provider
from src.config.settings import Settings
from src.core.load_generator import GCSLoadEngine
from src.core.metrics import MetricSnapshot, MetricsCollector, aggregate_snapshots
from src.core.partitioner import KeyPartitioner

logger = logging.getLogger(__name__)


def _worker_process_entry(
    worker_id: int,
    settings: Settings,
    partitioner: KeyPartitioner,
    mock_mode: bool,
    shared_read_rate: Value,
    shared_write_rate: Value,
    stop_event: Event,
    metrics_queue: Queue,
    keys_queue: Queue,
) -> None:
    """Entry point for a single parallel worker process."""
    # Ensure fresh async event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _async_worker_loop():
        auth_provider = get_auth_provider(mock_mode=mock_mode)
        local_metrics = MetricsCollector(window_seconds=settings.report_interval_seconds)
        engine = GCSLoadEngine(
            settings=settings,
            partitioner=partitioner,
            auth_provider=auth_provider,
            metrics=local_metrics,
            mock_mode=mock_mode,
        )

        await engine.initialize()
        engine._is_running = True

        tasks = []
        if settings.target_write_qps > 0:
            tasks.append(asyncio.create_task(engine.run_write_worker()))
        if settings.target_read_qps > 0:
            tasks.append(asyncio.create_task(engine.run_read_worker()))

        while not stop_event.is_set():
            # Update local rate from master's shared memory slice
            r = shared_read_rate.value
            w = shared_write_rate.value
            engine.update_rates(r, w)

            # Emit local snapshot to metrics queue
            snapshot = local_metrics.get_snapshot()
            try:
                metrics_queue.put_nowait((worker_id, snapshot))
            except Exception:
                pass

            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break

        engine._is_running = False
        for t in tasks:
            t.cancel()

        # Collect created keys for cleanup (batch send)
        if engine._created_keys:
            try:
                keys_queue.put_nowait(list(engine._created_keys))
            except Exception:
                pass

        await engine.close()

    try:
        loop.run_until_complete(_async_worker_loop())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


class MultiProcessOrchestrator:
    """Orchestrates multiple independent worker processes across CPU cores."""

    def __init__(
        self,
        settings: Settings,
        partitioner: KeyPartitioner,
        mock_mode: bool = False,
    ):
        self.settings = settings
        self.partitioner = partitioner
        self.mock_mode = mock_mode
        self.num_workers = max(1, settings.num_workers)

        # Standard Multiprocessing IPC primitives (pipe-backed, ultra-fast)
        self._stop_event = multiprocessing.Event()
        self._metrics_queue = multiprocessing.Queue()
        self._keys_queue = multiprocessing.Queue()
        self._shared_read_rate = multiprocessing.Value("d", 0.0)
        self._shared_write_rate = multiprocessing.Value("d", 0.0)

        self._processes: List[Process] = []
        self._latest_worker_snapshots: Dict[int, MetricSnapshot] = {}
        self._collected_keys: Set[str] = set()

    def start(self) -> None:
        """Spawn all worker processes."""
        self._stop_event.clear()
        self._processes.clear()

        for worker_id in range(self.num_workers):
            p = Process(
                target=_worker_process_entry,
                args=(
                    worker_id,
                    self.settings,
                    self.partitioner,
                    self.mock_mode,
                    self._shared_read_rate,
                    self._shared_write_rate,
                    self._stop_event,
                    self._metrics_queue,
                    self._keys_queue,
                ),
                daemon=True,
            )
            p.start()
            self._processes.append(p)

    def set_target_rates(self, total_read_qps: float, total_write_qps: float) -> None:
        """Distribute total target QPS across all active worker processes."""
        per_worker_read = total_read_qps / self.num_workers
        per_worker_write = total_write_qps / self.num_workers
        self._shared_read_rate.value = per_worker_read
        self._shared_write_rate.value = per_worker_write

    def poll_metrics(self, elapsed_seconds: float) -> MetricSnapshot:
        """Drain incoming metrics from worker queue and return aggregated snapshot."""
        # Drain all available items from queue
        while True:
            try:
                worker_id, snapshot = self._metrics_queue.get_nowait()
                self._latest_worker_snapshots[worker_id] = snapshot
            except Exception:
                break

        snapshots = list(self._latest_worker_snapshots.values())
        return aggregate_snapshots(snapshots, elapsed_seconds)

    def stop(self) -> None:
        """Signal all worker processes to stop and join."""
        self._stop_event.set()
        for p in self._processes:
            p.join(timeout=1.0)
            if p.is_alive():
                p.terminate()

    def get_created_keys(self) -> List[str]:
        """Retrieve list of all keys created across all worker processes."""
        while True:
            try:
                batch = self._keys_queue.get_nowait()
                self._collected_keys.update(batch)
            except Exception:
                break
        return list(self._collected_keys)
