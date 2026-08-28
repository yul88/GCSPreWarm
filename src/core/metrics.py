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

    # Overall latency percentiles (milliseconds over recent window)
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float

    # Read-specific latency percentiles
    read_p50_latency_ms: float = 0.0
    read_p95_latency_ms: float = 0.0
    read_p99_latency_ms: float = 0.0

    # Write-specific latency percentiles
    write_p50_latency_ms: float = 0.0
    write_p95_latency_ms: float = 0.0
    write_p99_latency_ms: float = 0.0

    # Error & Throttling rate (Windowed ratio)
    throttling_rate: float = 0.0
    error_rate: float = 0.0


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
        all_latencies: List[float] = []
        read_latencies: List[float] = []
        write_latencies: List[float] = []

        for _, op, status, lat, is_err in samples:
            if op == "READ":
                window_read += 1
                read_latencies.append(lat)
            elif op == "WRITE":
                window_write += 1
                write_latencies.append(lat)

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

            all_latencies.append(lat)

        effective_window = max(self.window_seconds, 0.001)
        cur_read_qps = window_read / effective_window
        cur_write_qps = window_write / effective_window
        cur_total_qps = (window_read + window_write) / effective_window

        total_window_ops = len(samples)
        throttling_rate = ((w_429 + w_503) / total_window_ops) if total_window_ops > 0 else 0.0
        error_rate = (w_err / total_window_ops) if total_window_ops > 0 else 0.0

        # Helper for calculating percentiles
        def _calc_p(lats: List[float]):
            if not lats:
                return 0.0, 0.0, 0.0, 0.0
            lats.sort()
            n = len(lats)
            return (
                lats[int(n * 0.50)],
                lats[min(int(n * 0.95), n - 1)],
                lats[min(int(n * 0.99), n - 1)],
                lats[-1],
            )

        p50, p95, p99, max_lat = _calc_p(all_latencies)
        r_p50, r_p95, r_p99, _ = _calc_p(read_latencies)
        w_p50, w_p95, w_p99, _ = _calc_p(write_latencies)

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
            read_p50_latency_ms=r_p50,
            read_p95_latency_ms=r_p95,
            read_p99_latency_ms=r_p99,
            write_p50_latency_ms=w_p50,
            write_p95_latency_ms=w_p95,
            write_p99_latency_ms=w_p99,
            throttling_rate=throttling_rate,
            error_rate=error_rate,
        )


def aggregate_snapshots(snapshots: List[MetricSnapshot], elapsed_seconds: float) -> MetricSnapshot:
    """Combine metric snapshots from multiple parallel worker processes."""
    if not snapshots:
        return MetricSnapshot(
            timestamp=time.perf_counter(),
            elapsed_seconds=elapsed_seconds,
            current_read_qps=0.0,
            current_write_qps=0.0,
            current_total_qps=0.0,
            total_read_ops=0,
            total_write_ops=0,
            total_ops=0,
            window_2xx=0,
            window_429=0,
            window_503=0,
            window_5xx=0,
            window_errors=0,
            cum_2xx=0,
            cum_429=0,
            cum_503=0,
            cum_5xx=0,
            cum_errors=0,
            p50_latency_ms=0.0,
            p95_latency_ms=0.0,
            p99_latency_ms=0.0,
            max_latency_ms=0.0,
            read_p50_latency_ms=0.0,
            read_p95_latency_ms=0.0,
            read_p99_latency_ms=0.0,
            write_p50_latency_ms=0.0,
            write_p95_latency_ms=0.0,
            write_p99_latency_ms=0.0,
            throttling_rate=0.0,
            error_rate=0.0,
        )

    cur_read_qps = sum(s.current_read_qps for s in snapshots)
    cur_write_qps = sum(s.current_write_qps for s in snapshots)
    cur_total_qps = sum(s.current_total_qps for s in snapshots)

    total_read_ops = sum(s.total_read_ops for s in snapshots)
    total_write_ops = sum(s.total_write_ops for s in snapshots)
    total_ops = sum(s.total_ops for s in snapshots)

    window_2xx = sum(s.window_2xx for s in snapshots)
    window_429 = sum(s.window_429 for s in snapshots)
    window_503 = sum(s.window_503 for s in snapshots)
    window_5xx = sum(s.window_5xx for s in snapshots)
    window_errors = sum(s.window_errors for s in snapshots)

    cum_2xx = sum(s.cum_2xx for s in snapshots)
    cum_429 = sum(s.cum_429 for s in snapshots)
    cum_503 = sum(s.cum_503 for s in snapshots)
    cum_5xx = sum(s.cum_5xx for s in snapshots)
    cum_errors = sum(s.cum_errors for s in snapshots)

    total_window_ops = sum(
        s.window_2xx + s.window_429 + s.window_503 + s.window_5xx + s.window_errors
        for s in snapshots
    )
    throttling_rate = (
        sum(s.window_429 + s.window_503 for s in snapshots) / total_window_ops
    ) if total_window_ops > 0 else 0.0
    error_rate = (window_errors / total_window_ops) if total_window_ops > 0 else 0.0

    def _avg_list(vals: List[float]) -> float:
        valid = [v for v in vals if v > 0]
        return sum(valid) / len(valid) if valid else 0.0

    valid_latencies_p50 = [s.p50_latency_ms for s in snapshots if s.p50_latency_ms > 0]
    valid_latencies_p95 = [s.p95_latency_ms for s in snapshots if s.p95_latency_ms > 0]
    valid_latencies_p99 = [s.p99_latency_ms for s in snapshots if s.p99_latency_ms > 0]
    valid_latencies_max = [s.max_latency_ms for s in snapshots if s.max_latency_ms > 0]

    p50 = sum(valid_latencies_p50) / len(valid_latencies_p50) if valid_latencies_p50 else 0.0
    p95 = sum(valid_latencies_p95) / len(valid_latencies_p95) if valid_latencies_p95 else 0.0
    p99 = sum(valid_latencies_p99) / len(valid_latencies_p99) if valid_latencies_p99 else 0.0
    max_lat = max(valid_latencies_max) if valid_latencies_max else 0.0

    read_p50 = _avg_list([s.read_p50_latency_ms for s in snapshots])
    read_p95 = _avg_list([s.read_p95_latency_ms for s in snapshots])
    read_p99 = _avg_list([s.read_p99_latency_ms for s in snapshots])

    write_p50 = _avg_list([s.write_p50_latency_ms for s in snapshots])
    write_p95 = _avg_list([s.write_p95_latency_ms for s in snapshots])
    write_p99 = _avg_list([s.write_p99_latency_ms for s in snapshots])

    return MetricSnapshot(
        timestamp=time.perf_counter(),
        elapsed_seconds=elapsed_seconds,
        current_read_qps=cur_read_qps,
        current_write_qps=cur_write_qps,
        current_total_qps=cur_total_qps,
        total_read_ops=total_read_ops,
        total_write_ops=total_write_ops,
        total_ops=total_ops,
        window_2xx=window_2xx,
        window_429=window_429,
        window_503=window_503,
        window_5xx=window_5xx,
        window_errors=window_errors,
        cum_2xx=cum_2xx,
        cum_429=cum_429,
        cum_503=cum_503,
        cum_5xx=cum_5xx,
        cum_errors=cum_errors,
        p50_latency_ms=p50,
        p95_latency_ms=p95,
        p99_latency_ms=p99,
        max_latency_ms=max_lat,
        read_p50_latency_ms=read_p50,
        read_p95_latency_ms=read_p95,
        read_p99_latency_ms=read_p99,
        write_p50_latency_ms=write_p50,
        write_p95_latency_ms=write_p95,
        write_p99_latency_ms=write_p99,
        throttling_rate=throttling_rate,
        error_rate=error_rate,
    )
