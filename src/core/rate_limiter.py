"""Rate limiting and adaptive ramp-up controller for GCSPreWarm.

Implements:
1. Stepped exponential doubling ramp schedule.
2. Adaptive throttling backoff & stabilization.
3. High-precision asynchronous token bucket rate pacing.
"""

import asyncio
import enum
import math
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from src.config.settings import Settings


class ExecutionPhase(str, enum.Enum):
    """Execution lifecycle phases."""

    IDLE = "IDLE"
    SEEDING = "SEEDING"
    RAMPING = "RAMPING"
    THROTTLING_BACKOFF = "THROTTLING_BACKOFF"
    SUSTAINING = "SUSTAINING"
    KEEP_WARM = "KEEP_WARM"
    CLEANUP = "CLEANUP"
    COMPLETED = "COMPLETED"


@dataclass
class RampState:
    """Current snapshot of ramp controller state."""

    phase: ExecutionPhase
    current_read_target: float
    current_write_target: float
    target_read_qps: int
    target_write_qps: int
    current_step: int
    total_steps: int
    elapsed_ramp_seconds: float
    remaining_ramp_seconds: float
    elapsed_sustain_seconds: float
    remaining_sustain_seconds: float
    is_throttled: bool
    backoff_seconds_remaining: float


class AdaptiveRampController:
    """Calculates stepped doubling curve and handles dynamic backoff."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.target_read_qps = settings.target_read_qps
        self.target_write_qps = settings.target_write_qps
        self.ramp_duration = settings.ramp_duration_seconds
        self.sustain_duration = settings.sustain_duration_seconds
        self.cooldown_duration = settings.stabilization_cooldown_seconds

        # Safe initial baselines
        self.initial_write_qps = min(self.target_write_qps, 1000) if self.target_write_qps > 0 else 0
        self.initial_read_qps = min(self.target_read_qps, 5000) if self.target_read_qps > 0 else 0

        # Calculate number of doubling steps
        write_steps = 1
        if self.target_write_qps > self.initial_write_qps and self.initial_write_qps > 0:
            write_steps = max(1, math.ceil(math.log2(self.target_write_qps / self.initial_write_qps)))

        read_steps = 1
        if self.target_read_qps > self.initial_read_qps and self.initial_read_qps > 0:
            read_steps = max(1, math.ceil(math.log2(self.target_read_qps / self.initial_read_qps)))

        self.total_steps = max(write_steps, read_steps, 1)
        self.step_duration = self.ramp_duration / self.total_steps

        # Runtime state
        self.phase = ExecutionPhase.IDLE
        self.start_time: Optional[float] = None
        self.effective_ramp_elapsed: float = 0.0
        self.last_update_time: Optional[float] = None

        self.sustain_start_time: Optional[float] = None

        # Backoff tracking
        self.is_throttled: bool = False
        self.throttling_start_time: Optional[float] = None
        self.last_stable_step: int = 0

    def start(self, initial_phase: ExecutionPhase = ExecutionPhase.RAMPING) -> None:
        """Start the ramp-up controller."""
        now = time.perf_counter()
        self.start_time = now
        self.last_update_time = now
        self.phase = initial_phase
        self.effective_ramp_elapsed = 0.0

    def start_sustain(self) -> None:
        """Transition into sustain phase."""
        self.phase = ExecutionPhase.SUSTAINING
        self.sustain_start_time = time.perf_counter()

    def report_metrics(self, throttling_error_rate: float) -> None:
        """Evaluate error rate and update throttling backoff state."""
        now = time.perf_counter()

        if throttling_error_rate >= self.settings.throttling_error_threshold:
            if not self.is_throttled and self.phase == ExecutionPhase.RAMPING:
                self.is_throttled = True
                self.throttling_start_time = now
                self.phase = ExecutionPhase.THROTTLING_BACKOFF
        elif self.is_throttled and self.throttling_start_time:
            # Check if cooldown period has elapsed and error rate is healthy
            cooldown_elapsed = now - self.throttling_start_time
            if cooldown_elapsed >= self.cooldown_duration and throttling_error_rate == 0.0:
                self.is_throttled = False
                self.throttling_start_time = None
                self.phase = ExecutionPhase.RAMPING

    def update(self) -> RampState:
        """Compute current target rates and phase state."""
        now = time.perf_counter()
        if self.start_time is None:
            self.start_time = now
        if self.last_update_time is None:
            self.last_update_time = now

        dt = now - self.last_update_time
        self.last_update_time = now

        backoff_remaining = 0.0
        if self.is_throttled and self.throttling_start_time:
            cooldown_elapsed = now - self.throttling_start_time
            backoff_remaining = max(0.0, self.cooldown_duration - cooldown_elapsed)
        elif self.phase == ExecutionPhase.RAMPING:
            self.effective_ramp_elapsed += dt

        # Determine current step
        current_step = min(
            self.total_steps - 1,
            int(self.effective_ramp_elapsed / self.step_duration),
        )

        if not self.is_throttled:
            self.last_stable_step = current_step

        # If we reached ramp completion and not sustaining yet
        if (
            self.effective_ramp_elapsed >= self.ramp_duration
            and self.phase == ExecutionPhase.RAMPING
        ):
            self.start_sustain()

        # Compute QPS for current step
        active_step = self.last_stable_step if self.is_throttled else current_step

        if self.phase in (ExecutionPhase.SUSTAINING, ExecutionPhase.COMPLETED):
            current_read = float(self.target_read_qps)
            current_write = float(self.target_write_qps)
        elif self.phase == ExecutionPhase.THROTTLING_BACKOFF:
            # Drop to previous step or safe 50%
            step_to_use = max(0, active_step - 1)
            current_read = min(float(self.initial_read_qps * (2**step_to_use)), float(self.target_read_qps))
            current_write = min(float(self.initial_write_qps * (2**step_to_use)), float(self.target_write_qps))
        else:
            current_read = min(float(self.initial_read_qps * (2**active_step)), float(self.target_read_qps))
            current_write = min(float(self.initial_write_qps * (2**active_step)), float(self.target_write_qps))

        # Sustain duration tracking
        elapsed_sustain = 0.0
        remaining_sustain = float(self.sustain_duration)
        if self.sustain_start_time:
            elapsed_sustain = now - self.sustain_start_time
            remaining_sustain = max(0.0, float(self.sustain_duration) - elapsed_sustain)
            if remaining_sustain <= 0.0 and self.phase == ExecutionPhase.SUSTAINING:
                self.phase = ExecutionPhase.COMPLETED

        remaining_ramp = max(0.0, float(self.ramp_duration) - self.effective_ramp_elapsed)

        return RampState(
            phase=self.phase,
            current_read_target=current_read,
            current_write_target=current_write,
            target_read_qps=self.target_read_qps,
            target_write_qps=self.target_write_qps,
            current_step=active_step + 1,
            total_steps=self.total_steps,
            elapsed_ramp_seconds=self.effective_ramp_elapsed,
            remaining_ramp_seconds=remaining_ramp,
            elapsed_sustain_seconds=elapsed_sustain,
            remaining_sustain_seconds=remaining_sustain,
            is_throttled=self.is_throttled,
            backoff_seconds_remaining=backoff_remaining,
        )


class TokenBucketRateLimiter:
    """High-precision asynchronous token bucket rate limiter."""

    def __init__(self, initial_rate: float):
        self.rate = max(0.0, initial_rate)
        self.capacity = max(10.0, self.rate)
        self.tokens = self.capacity
        self.last_refill = time.perf_counter()
        self._lock = asyncio.Lock()

    def set_rate(self, new_rate: float) -> None:
        """Dynamically update target rate and adjust capacity."""
        self.rate = max(0.0, new_rate)
        self.capacity = max(10.0, self.rate)

    async def acquire(self) -> None:
        """Asynchronously wait until a token is available."""
        if self.rate <= 0.0:
            # If rate is 0, sleep a bit to avoid busy loop
            await asyncio.sleep(0.1)
            return

        while True:
            async with self._lock:
                now = time.perf_counter()
                elapsed = now - self.last_refill
                self.last_refill = now

                # Add new tokens generated over elapsed time
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # Calculate sleep duration needed for 1 token
                missing = 1.0 - self.tokens
                sleep_duration = missing / self.rate

            # Sleep outside lock
            await asyncio.sleep(min(sleep_duration, 0.05))
