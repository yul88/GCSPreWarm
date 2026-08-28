"""Centralized configuration and settings for GCSPreWarm.

Strict separation of concerns:
- User runtime variables loaded from .env or environment variables.
- Technical engine defaults, connection limits, and tuning constants.
"""

import os
from typing import List, Literal, Optional
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralized configuration for GCSPreWarm."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # =========================================================================
    # User-Configurable Parameters (.env)
    # =========================================================================
    gcs_bucket_name: str = Field(
        default="",
        description="Target GCS bucket name to pre-warm/pre-split.",
    )
    gcp_project_id: Optional[str] = Field(
        default=None,
        description="Optional GCP Project ID.",
    )

    target_read_qps: int = Field(
        default=0,
        ge=0,
        description="Desired Read QPS to achieve (0 to disable read pre-warm).",
    )
    target_write_qps: int = Field(
        default=1000,
        ge=0,
        description="Desired Write QPS to achieve (0 to disable write pre-warm).",
    )

    ramp_profile: Literal["AUTO", "FAST", "STANDARD", "CONSERVATIVE", "CUSTOM"] = Field(
        default="AUTO",
        description="Ramp profile preset: AUTO (auto-scales by QPS tier), FAST (~60s/step), STANDARD (~100s/step), CONSERVATIVE (20m total), or CUSTOM.",
    )
    ramp_duration_seconds: int = Field(
        default=1200,
        gt=0,
        description="Duration (in seconds) of the gradual ramp-up phase (used when RAMP_PROFILE=CUSTOM or as custom override).",
    )
    sustain_duration_seconds: int = Field(
        default=600,
        ge=0,
        description="Duration (in seconds) to sustain target QPS after ramp-up completes.",
    )

    object_size_bytes: int = Field(
        default=4096,
        gt=0,
        description="Payload size in bytes for dummy test objects (default 4KB).",
    )

    key_strategy: Literal["HEX", "ALPHANUMERIC", "CUSTOM"] = Field(
        default="HEX",
        description="Strategy for prefix generation: HEX, ALPHANUMERIC, or CUSTOM.",
    )
    prefix_strategy: Literal["AUTO", "HEX_1", "HEX_2", "HEX_3"] = Field(
        default="AUTO",
        description="Prefix depth: AUTO (calculated from QPS), HEX_1, HEX_2, or HEX_3.",
    )

    custom_prefixes: str = Field(
        default="",
        description="Comma-separated list of customer prefixes when KEY_STRATEGY=CUSTOM.",
    )
    prefix_template: str = Field(
        default="",
        description="Sequence template when KEY_STRATEGY=CUSTOM (e.g., 'tenant_{001..050}/').",
    )
    key_prefix_base: str = Field(
        default="gcs_prewarm_test/",
        description="Base folder path inside the bucket (e.g. 'app_v1/' or '' for root).",
    )

    cleanup_on_finish: bool = Field(
        default=True,
        description="Automatically delete created test objects upon completion.",
    )
    keep_warm_mode: bool = Field(
        default=False,
        description="Maintain low-rate heartbeat traffic after test completes to keep shards warm.",
    )

    # =========================================================================
    # Engine & Network Tuning Parameters (Dynamic with Manual Overrides)
    # =========================================================================
    gcs_base_url: str = Field(
        default="https://storage.googleapis.com",
        description="Base GCS REST/XML API endpoint.",
    )
    http_max_connections: Optional[int] = Field(
        default=None,
        description="Optional manual override for TCP connection pool limit per worker (default: auto-sized from QPS).",
    )
    http_timeout_seconds: float = Field(
        default=10.0,
        gt=0.0,
        description="Per-request HTTP timeout in seconds.",
    )
    http_keep_alive_seconds: float = Field(
        default=60.0,
        gt=0.0,
        description="HTTP TCP Keep-Alive timeout in seconds.",
    )
    report_interval_seconds: float = Field(
        default=2.0,
        gt=0.0,
        description="Metrics reporting and UI refresh interval in seconds.",
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retries for transient errors (429/503/network).",
    )
    backoff_factor: float = Field(
        default=0.5,
        ge=0.0,
        description="Exponential backoff factor for retries.",
    )
    throttling_error_threshold: float = Field(
        default=0.01,
        ge=0.0,
        le=1.0,
        description="Error rate threshold (1%) to trigger adaptive ramp backoff.",
    )
    stabilization_cooldown_seconds: int = Field(
        default=60,
        ge=5,
        description="Cooldown duration (seconds) when throttling is detected before resuming ramp.",
    )

    num_workers: int = Field(
        default_factory=lambda: max(1, os.cpu_count() or 1),
        ge=1,
        description="Worker concurrency process/task multiplier (auto-detected CPU cores).",
    )
    worker_pool_size: Optional[int] = Field(
        default=None,
        description="Optional manual override for persistent worker coroutines per CPU process (default: auto-sized from target QPS).",
    )

    seed_objects_per_prefix: Optional[int] = Field(
        default=None,
        description="Optional manual override for seed objects per shard (default: auto-sized from Read QPS scale).",
    )

    def get_safe_min_concurrency_per_worker(self) -> int:
        """Compute minimum coroutine concurrency per worker ensuring low-overhead responsiveness."""
        max_target = max(self.target_read_qps, self.target_write_qps)
        if max_target <= 100 or self.num_workers == 1:
            return 20
        return 50

    def get_safe_max_concurrency_per_worker(self) -> int:
        """Compute maximum safe coroutine concurrency per worker bounded by OS file descriptors and CPU cores."""
        max_fd = optimize_system_resources()
        # Reserve 150 file descriptors for OS, logs, queues, pipes, and DNS sockets
        available_fd = max(100, max_fd - 150)
        # Each coroutine can hold 1 HTTP socket + 1 TCP connector socket buffer
        fd_based_max = max(50, available_fd // 2)
        # Core-based cap: 500 coroutines per CPU core to prevent thread/context switching thrashing
        return min(500, fd_based_max)

    def get_effective_http_connections(self) -> int:
        """Dynamically compute connection pool size per worker process bounded by platform limits."""
        if self.http_max_connections is not None and self.http_max_connections > 0:
            return self.http_max_connections
        max_safe = self.get_safe_max_concurrency_per_worker()
        min_safe = self.get_safe_min_concurrency_per_worker()
        workers_count = max(1, self.num_workers)
        max_target = max(self.target_read_qps, self.target_write_qps)
        per_worker_qps = max_target / workers_count
        pool_size = max(min_safe, min(max_safe, int(per_worker_qps * 0.12)))
        return max(500, min(2000, pool_size * 2))

    def get_effective_seed_count(self, total_shards: int) -> int:
        """Dynamically compute seed object count per prefix shard (default: 20 objects per shard)."""
        if self.seed_objects_per_prefix is not None and self.seed_objects_per_prefix > 0:
            return self.seed_objects_per_prefix
        if self.target_read_qps <= 0:
            return 20
        # 20 seed objects per shard is optimal for uniform partition read distribution
        return 20

    def get_effective_cleanup_concurrency(self) -> int:
        """Dynamically compute cleanup delete concurrency bounded by system file descriptors."""
        max_fd = optimize_system_resources()
        fd_safe_limit = max(50, (max_fd - 150) // 2)
        core_based = self.num_workers * 50
        return max(50, min(1000, min(core_based, fd_safe_limit)))

    @field_validator("key_prefix_base")
    @classmethod
    def normalize_key_prefix_base(cls, v: str) -> str:
        """Normalize key_prefix_base ensuring proper trailing slash if non-empty."""
        v = v.strip()
        if v and not v.endswith("/"):
            v = f"{v}/"
        return v

    @model_validator(mode="after")
    def validate_overall_config(self) -> "Settings":
        """Validate interdependent configuration constraints."""
        if self.target_read_qps == 0 and self.target_write_qps == 0:
            raise ValueError(
                "Both TARGET_READ_QPS and TARGET_WRITE_QPS cannot be 0. At least one must be > 0."
            )
        if self.key_strategy == "CUSTOM":
            if not self.custom_prefixes.strip() and not self.prefix_template.strip():
                raise ValueError(
                    "When KEY_STRATEGY='CUSTOM', either CUSTOM_PREFIXES or PREFIX_TEMPLATE must be provided."
                )
        return self

    def parsed_custom_prefixes(self) -> List[str]:
        """Return list of individual custom prefixes if defined."""
        if not self.custom_prefixes.strip():
            return []
        return [p.strip() for p in self.custom_prefixes.split(",") if p.strip()]


def optimize_system_resources() -> int:
    """Attempt to increase open file descriptor limit (ulimit -n) to 65,535 for high HTTP concurrency."""
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = min(65535, hard)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        return soft
    except Exception:
        return 1024


_global_settings: Optional[Settings] = None


def get_settings(env_file: Optional[str] = None) -> Settings:
    """Get or initialize global application settings."""
    global _global_settings
    if _global_settings is None or env_file is not None:
        if env_file:
            _global_settings = Settings(_env_file=env_file)
        else:
            _global_settings = Settings()
    return _global_settings
