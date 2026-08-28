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

    snapshot = collector.get_snapshot()

    assert snapshot.total_write_ops == 105
    assert snapshot.cum_2xx == 100
    assert snapshot.cum_429 == 5
    assert snapshot.p50_latency_ms == pytest.approx(53.0, abs=5.0)
    assert snapshot.p95_latency_ms == pytest.approx(100.0, abs=5.0)
    assert snapshot.p99_latency_ms == 150.0
    assert snapshot.throttling_rate == pytest.approx(5 / 105, abs=0.01)


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

