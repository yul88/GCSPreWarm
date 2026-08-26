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
