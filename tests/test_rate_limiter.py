"""Unit tests for adaptive ramp controller and token bucket rate limiter."""

import asyncio
import pytest
from src.config.settings import Settings
from src.core.rate_limiter import AdaptiveRampController, ExecutionPhase, TokenBucketRateLimiter


def test_ramp_steps_calculation():
    """Test calculation of doubling steps and step durations."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=8000,  # Starts at 1000 -> 2000 -> 4000 -> 8000 (3 doubling steps)
        target_read_qps=0,
        ramp_profile="CUSTOM",
        ramp_duration_seconds=300,
    )
    controller = AdaptiveRampController(settings)
    assert controller.total_steps == 3
    assert controller.step_duration == 100.0


def test_ramp_profiles():
    """Test duration calculations across AUTO, FAST, STANDARD, CONSERVATIVE presets."""
    # Fast: 60s per step
    s_fast = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=8000, # 3 steps
        target_read_qps=0,
        ramp_profile="FAST",
    )
    c_fast = AdaptiveRampController(s_fast)
    assert c_fast.total_steps == 3
    assert c_fast.ramp_duration == 180.0
    assert c_fast.step_duration == 60.0

    # Standard: 100s per step
    s_std = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=8000, # 3 steps
        target_read_qps=0,
        ramp_profile="STANDARD",
    )
    c_std = AdaptiveRampController(s_std)
    assert c_std.ramp_duration == 300.0

    # Auto (8,000 QPS <= 10,000): 60s per step
    s_auto = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=8000,
        target_read_qps=0,
        ramp_profile="AUTO",
    )
    c_auto = AdaptiveRampController(s_auto)
    assert c_auto.ramp_duration == 180.0


def test_ramp_update_progression():
    """Test rate calculation as ramp progresses."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=4000,  # 1000 -> 2000 -> 4000 (2 steps)
        target_read_qps=10000, # 5000 -> 10000 (1 step) -> max steps = 2
        ramp_duration_seconds=100,
    )
    controller = AdaptiveRampController(settings)
    controller.start(ExecutionPhase.RAMPING)

    # Initial state (Step 1)
    state = controller.update()
    assert state.phase == ExecutionPhase.RAMPING
    assert state.current_step == 1
    assert state.current_write_target == 1000.0
    assert state.current_read_target == 5000.0

    # Advance effective elapsed time to Step 2
    controller.effective_ramp_elapsed = 60.0
    state2 = controller.update()
    assert state2.current_step == 2
    assert state2.current_write_target == 2000.0
    assert state2.current_read_target == 10000.0


def test_throttling_backoff_and_recovery():
    """Test backoff trigger on 429/503 error rate and stabilization cooldown."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=8000,
        target_read_qps=0,
        ramp_duration_seconds=300,
        throttling_error_threshold=0.01,
        stabilization_cooldown_seconds=10,
    )
    controller = AdaptiveRampController(settings)
    controller.start(ExecutionPhase.RAMPING)
    controller.effective_ramp_elapsed = 150.0  # Step 2

    # Trigger throttling error rate (> 1%)
    controller.report_metrics(throttling_error_rate=0.05)
    state = controller.update()

    assert state.phase == ExecutionPhase.THROTTLING_BACKOFF
    assert state.is_throttled is True
    assert state.current_write_target < 4000.0  # Dropped to backoff rate

    # Cooldown in progress with zero errors
    controller.throttling_start_time -= 15.0  # Simulate 15s elapsed
    controller.report_metrics(throttling_error_rate=0.0)
    recovered_state = controller.update()

    assert recovered_state.phase == ExecutionPhase.RAMPING
    assert recovered_state.is_throttled is False


@pytest.mark.asyncio
async def test_token_bucket_limiter():
    """Test token bucket pacing."""
    limiter = TokenBucketRateLimiter(initial_rate=100.0)
    # Acquiring initial tokens should succeed immediately
    for _ in range(5):
        await limiter.acquire()

    assert limiter.tokens < limiter.capacity


@pytest.mark.asyncio
async def test_token_bucket_high_concurrency_no_deadlock():
    """Stress test 100 concurrent coroutines acquiring tokens to prove zero deadlock."""
    limiter = TokenBucketRateLimiter(initial_rate=1000.0)

    async def _acquire_task():
        for _ in range(10):
            await limiter.acquire()

    # Run 100 coroutines concurrently with timeout
    tasks = [_acquire_task() for _ in range(100)]
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
