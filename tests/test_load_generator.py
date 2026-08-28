"""Unit tests for GCS load generator in mock mode."""

import asyncio
import pytest
from src.auth.gcp_auth import GCPAuthProvider
from src.config.settings import Settings
from src.core.load_generator import GCSLoadEngine
from src.core.metrics import MetricsCollector
from src.core.partitioner import KeyPartitioner


@pytest.mark.asyncio
async def test_mock_load_engine_lifecycle():
    """Test full load engine lifecycle in mock simulation mode."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=500,
        target_write_qps=500,
        seed_objects_per_prefix=2,
    )
    partitioner = KeyPartitioner(settings)
    auth = GCPAuthProvider(mock_mode=True)
    metrics = MetricsCollector(window_seconds=2.0)

    engine = GCSLoadEngine(
        settings=settings,
        partitioner=partitioner,
        auth_provider=auth,
        metrics=metrics,
        mock_mode=True,
    )

    await engine.initialize()

    # 1. Seed phase
    seeded = await engine.seed_objects(count_per_prefix=2)
    assert seeded == len(partitioner.plan.prefixes) * 2

    # 2. Run write & read operations
    engine._is_running = True
    engine.update_rates(read_qps=100.0, write_qps=100.0)

    write_task = asyncio.create_task(engine.run_write_worker())
    read_task = asyncio.create_task(engine.run_read_worker())

    await asyncio.sleep(0.2)
    engine._is_running = False
    write_task.cancel()
    read_task.cancel()

    # 3. Cleanup phase
    cleaned = await engine.cleanup_all_objects()
    assert cleaned > 0

    await engine.close()


@pytest.mark.asyncio
async def test_multiprocess_orchestrator():
    """Test MultiProcessOrchestrator across multiple worker processes."""
    from src.core.multi_worker import MultiProcessOrchestrator

    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=200,
        target_write_qps=200,
        num_workers=2,
    )
    partitioner = KeyPartitioner(settings)
    orchestrator = MultiProcessOrchestrator(
        settings=settings,
        partitioner=partitioner,
        mock_mode=True,
    )

    orchestrator.start()
    orchestrator.set_target_rates(200.0, 200.0)

    await asyncio.sleep(0.5)

    snapshot = orchestrator.poll_metrics(elapsed_seconds=0.5)
    assert snapshot is not None
    assert snapshot.total_ops > 0

    orchestrator.stop()


def test_dynamic_pipeline_pool_sizing():
    """Verify dynamic pool size calculation based on target QPS and CPU workers."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=10000,
        target_write_qps=5000,
        num_workers=8,
    )
    partitioner = KeyPartitioner(settings)
    auth = GCPAuthProvider(mock_mode=True)
    engine = GCSLoadEngine(
        settings=settings,
        partitioner=partitioner,
        auth_provider=auth,
        metrics=MetricsCollector(2.0),
        mock_mode=True,
    )

    # 1. Target Read QPS: 10,000 across 8 workers -> baseline 120ms buffer -> (10000 / 8) * 0.12 = 150 coroutines
    read_pool = engine.compute_pipeline_pool_size(10000)
    assert read_pool == 150

    # 2. Target Write QPS: 5,000 across 8 workers -> baseline 120ms buffer -> (5000 / 8) * 0.12 = 75 coroutines
    write_pool = engine.compute_pipeline_pool_size(5000)
    assert write_pool == 75

    # 3. Real-time latency feedback: p95 latency = 107.8ms -> (107.8ms * 1.5 = 161.7ms buffer)
    # per_worker = 625 -> ceil(625 * 0.1617) = 102 coroutines
    adaptive_pool = engine.compute_pipeline_pool_size(5000, observed_latency_ms=107.8)
    assert adaptive_pool == 102

    # 4. High latency feedback: p95 latency = 250ms -> (250ms * 1.5 = 375ms buffer)
    # per_worker = 625 -> ceil(625 * 0.375) = 235 coroutines
    high_lat_pool = engine.compute_pipeline_pool_size(5000, observed_latency_ms=250.0)
    assert high_lat_pool == 235

    # 5. Low latency feedback: p95 latency = 5ms -> clamps to min 50 coroutines
    low_lat_pool = engine.compute_pipeline_pool_size(5000, observed_latency_ms=5.0)
    assert low_lat_pool == 50

    # 6. adjust_pipeline updates internal target pools
    engine.adjust_pipeline(target_read_qps=10000, target_write_qps=5000, observed_latency_ms=107.8)
    assert engine._target_write_pool == 102
    assert engine._target_read_pool == 203  # ceil(1250 * 0.1617) = 203

    # 7. Manual override respected
    settings.worker_pool_size = 88
    assert engine.compute_pipeline_pool_size(10000) == 88

