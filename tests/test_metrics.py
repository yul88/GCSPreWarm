"""Unit tests for metrics collector and latency percentiles."""

import pytest
from src.core.metrics import MetricsCollector


def test_metrics_collector_recording_and_snapshot():
    """Test recording requests and calculating percentiles."""
    collector = MetricsCollector(window_seconds=10.0)

    # Record 100 successful writes with latencies 1ms to 100ms
    for i in range(1, 101):
        collector.record_request("WRITE", 200, float(i))

    # Record 5 throttling 429 errors with 150ms latency
    for _ in range(5):
        collector.record_request("WRITE", 429, 150.0)

    # Record 50 successful reads with latencies 5ms to 54ms
    for i in range(5, 55):
        collector.record_request("READ", 200, float(i))

    snapshot = collector.get_snapshot()

    assert snapshot.total_write_ops == 105
    assert snapshot.total_read_ops == 50
    assert snapshot.cum_2xx == 150
    assert snapshot.cum_429 == 5
    assert snapshot.p50_latency_ms > 0
    assert snapshot.write_p50_latency_ms == pytest.approx(53.0, abs=5.0)
    assert snapshot.read_p50_latency_ms == pytest.approx(30.0, abs=5.0)
    assert snapshot.write_p95_latency_ms == pytest.approx(100.0, abs=5.0)
    assert snapshot.write_p99_latency_ms == 150.0
    assert snapshot.throttling_rate == pytest.approx(5 / 155, abs=0.01)


def test_check_platform_capacity():
    """Verify hardware capacity pre-check logic and warning threshold."""
    from src.config.settings import Settings
    from src.ui.console import ConsoleDashboard

    dashboard = ConsoleDashboard()

    # 1. Low target QPS on multi-core VM -> True (Sufficient)
    settings_ok = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=2000,
        target_write_qps=1000,
        num_workers=4,
    )
    assert dashboard.check_platform_capacity(settings_ok) is True

    # 2. Huge target QPS on single core -> False (Exceeds capacity)
    settings_excess = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=25000,
        target_write_qps=15000,
        num_workers=1,
    )
    assert dashboard.check_platform_capacity(settings_excess) is False


def test_metrics_collector_multithreaded_concurrency():
    """Verify thread safety under heavy multithreaded concurrent recording."""
    import concurrent.futures

    collector = MetricsCollector(window_seconds=5.0)

    def _worker(thread_idx: int):
        for i in range(100):
            collector.record_request("WRITE", 200, float((i % 50) + 1))
            collector.record_request("READ", 200, float((i % 30) + 1))

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_worker, i) for i in range(10)]
        concurrent.futures.wait(futures)

    snapshot = collector.get_snapshot()
    assert snapshot.total_write_ops == 1000
    assert snapshot.total_read_ops == 1000
    assert snapshot.total_ops == 2000
    assert snapshot.cum_2xx == 2000
    assert snapshot.read_p50_latency_ms > 0
    assert snapshot.write_p50_latency_ms > 0


def test_check_write_key_pool_capacity():
    """Verify write key pool size validation against GCS 1 write/s per object quota."""
    from src.config.settings import Settings
    from src.ui.console import ConsoleDashboard

    dashboard = ConsoleDashboard()

    # 1. Safe auto-calculated configuration: 5,000 Write QPS across 16 shards (Auto 4,096 slots -> ~0.08 writes/s)
    settings_safe = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=5000,
        target_read_qps=0,
    )
    assert dashboard.check_write_key_pool_capacity(settings_safe, total_shards=16) is True

    # 2. Dangerous configuration: 10,000 Write QPS across 16 shards with only 10 slots (62.5 writes/s per object!)
    settings_risky = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=10000,
        target_read_qps=0,
        write_key_pool_size=10,
    )
    assert dashboard.check_write_key_pool_capacity(settings_risky, total_shards=16) is False

    # 3. Read only (0 write QPS) -> Always True
    settings_read_only = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=0,
        target_read_qps=5000,
        write_key_pool_size=10,
    )
    assert dashboard.check_write_key_pool_capacity(settings_read_only, total_shards=16) is True

    # 4. Infinite unique mode (use_write_key_pool=False)
    settings_inf = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=5000,
        use_write_key_pool=False,
    )
    assert dashboard.check_write_key_pool_capacity(settings_inf, total_shards=16) is True

