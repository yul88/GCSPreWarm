"""Real-time metrics collection, latency histograms, and status telemetry for GCSPreWarm.

Provides lock-free windowed QPS calculation, latency percentiles (p50, p95, p99),
and HTTP status code breakdowns.
"""

import collections
import dataclasses
import math
import threading
import time
from typing import Dict, List, Optional


@dataclasses.dataclass(frozen=True)
class MetricSnapshot:
    """Snapshot of real-time instantaneous and cumulative performance metrics."""

    timestamp: float
    elapsed_seconds: float

    # Instantaneous throughput (QPS over recent window)
    current_read_qps: float
    current_write_qps: float
    current_total_qps: float

    # Cumulative totals
    total_read_ops: int
    total_write_ops: int
    total_ops: int

    # HTTP Status code counts (Windowed)
    window_2xx: int
    window_429: int
    window_503: int
    window_5xx: int
    window_errors: int

    # HTTP Status code counts (Cumulative)
    cum_2xx: int
    cum_429: int
    cum_503: int
    cum_5xx: int
    cum_errors: int

    # Latency percentiles (milliseconds over recent window)
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float

    # Error & Throttling rate (Windowed ratio)
    throttling_rate: float
    error_rate: float


class MetricsCollector:
    """High-throughput in-memory metrics aggregator."""

    def __init__(self, window_seconds: float = 2.0):
        self.window_seconds = window_seconds
        self.start_time = time.perf_counter()

        # Cumulative counters
        self._cum_read_ops = 0
        self._cum_write_ops = 0
        self._cum_2xx = 0
        self._cum_429 = 0
        self._cum_503 = 0
        self._cum_5xx = 0
        self._cum_errors = 0

        # Ring buffers for windowed statistics: tuples of (timestamp, operation, status_code, latency_ms)
        self._samples: collections.deque = collections.deque()
        self._lock = threading.Lock()

    def record_request(
        self,
        operation: str,
        status_code: int,
        latency_ms: float,
        error: Optional[str] = None,
    ) -> None:
        """Record a single HTTP operation execution."""
        now = time.perf_counter()
        is_2xx = 200 <= status_code < 300
        is_429 = status_code == 429
        is_503 = status_code == 503
        is_5xx = 500 <= status_code < 600 and not is_503
        is_err = error is not None or status_code >= 400 or status_code == 0

        with self._lock:
            if operation.upper() == "READ":
                self._cum_read_ops += 1
            elif operation.upper() == "WRITE":
                self._cum_write_ops += 1

            if is_2xx:
                self._cum_2xx += 1
            if is_429:
                self._cum_429 += 1
            if is_503:
                self._cum_503 += 1
            if is_5xx:
                self._cum_5xx += 1
            if is_err:
                self._cum_errors += 1

            self._samples.append((now, operation.upper(), status_code, latency_ms, is_err))

    def _purge_old_samples(self, cutoff_time: float) -> None:
        """Remove samples outside the active metrics window."""
        while self._samples and self._samples[0][0] < cutoff_time:
            self._samples.popleft()

    def get_snapshot(self) -> MetricSnapshot:
        """Calculate and return a consistent MetricSnapshot over the recent window."""
        now = time.perf_counter()
        cutoff = now - self.window_seconds
        elapsed = now - self.start_time

        with self._lock:
            self._purge_old_samples(cutoff)
            samples = list(self._samples)

            cum_read = self._cum_read_ops
            cum_write = self._cum_write_ops
            cum_2xx = self._cum_2xx
            cum_429 = self._cum_429
            cum_503 = self._cum_503
            cum_5xx = self._cum_5xx
            cum_errors = self._cum_errors

        # Calculate windowed metrics
        window_read = 0
        window_write = 0
        w_2xx = 0
        w_429 = 0
        w_503 = 0
        w_5xx = 0
        w_err = 0
        latencies: List[float] = []

        for _, op, status, lat, is_err in samples:
            if op == "READ":
                window_read += 1
            elif op == "WRITE":
                window_write += 1

            if 200 <= status < 300:
                w_2xx += 1
            if status == 429:
                w_429 += 1
            if status == 503:
                w_503 += 1
            if 500 <= status < 600 and status != 503:
                w_5xx += 1
            if is_err:
                w_err += 1

            latencies.append(lat)

        effective_window = max(self.window_seconds, 0.001)
        cur_read_qps = window_read / effective_window
        cur_write_qps = window_write / effective_window
        cur_total_qps = (window_read + window_write) / effective_window

        total_window_ops = len(samples)
        throttling_rate = ((w_429 + w_503) / total_window_ops) if total_window_ops > 0 else 0.0
        error_rate = (w_err / total_window_ops) if total_window_ops > 0 else 0.0

        # Percentile calculations
        p50 = 0.0
        p95 = 0.0
        p99 = 0.0
        max_lat = 0.0

        if latencies:
            latencies.sort()
            n = len(latencies)
            p50 = latencies[int(n * 0.50)]
            p95 = latencies[min(int(n * 0.95), n - 1)]
            p99 = latencies[min(int(n * 0.99), n - 1)]
            max_lat = latencies[-1]

        return MetricSnapshot(
            timestamp=now,
            elapsed_seconds=elapsed,
            current_read_qps=cur_read_qps,
            current_write_qps=cur_write_qps,
            current_total_qps=cur_total_qps,
            total_read_ops=cum_read,
            total_write_ops=cum_write,
            total_ops=cum_read + cum_write,
            window_2xx=w_2xx,
            window_429=w_429,
            window_503=w_503,
            window_5xx=w_5xx,
            window_errors=w_err,
            cum_2xx=cum_2xx,
            cum_429=cum_429,
            cum_503=cum_503,
            cum_5xx=cum_5xx,
            cum_errors=cum_errors,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p99,
            max_latency_ms=max_lat,
            throttling_rate=throttling_rate,
            error_rate=error_rate,
        )
