"""Unit tests for centralized configuration and settings validation."""

import pytest
from src.config.settings import Settings


def test_default_settings():
    """Test default settings instantiation."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=5000,
        target_read_qps=0,
    )
    assert settings.gcs_bucket_name == "test-bucket"
    assert settings.target_write_qps == 5000
    assert settings.target_read_qps == 0
    assert settings.key_prefix_base == "gcs_prewarm_test/"
    assert settings.cleanup_on_finish is True


def test_zero_qps_validation_error():
    """Test validation fails when both read and write QPS are zero."""
    with pytest.raises(ValueError, match="Both TARGET_READ_QPS and TARGET_WRITE_QPS cannot be 0"):
        Settings(
            gcs_bucket_name="test-bucket",
            target_read_qps=0,
            target_write_qps=0,
        )


def test_custom_strategy_missing_prefixes():
    """Test validation fails when CUSTOM strategy is selected without prefixes."""
    with pytest.raises(ValueError, match="either CUSTOM_PREFIXES or PREFIX_TEMPLATE must be provided"):
        Settings(
            gcs_bucket_name="test-bucket",
            target_write_qps=1000,
            key_strategy="CUSTOM",
            custom_prefixes="",
            prefix_template="",
        )


def test_custom_prefixes_parsing():
    """Test parsing comma-separated custom prefixes."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=1000,
        key_strategy="CUSTOM",
        custom_prefixes="users/, orders/ , media/ , events",
    )
    parsed = settings.parsed_custom_prefixes()
    assert parsed == ["users/", "orders/", "media/", "events"]


def test_key_prefix_base_normalization():
    """Test key prefix base trailing slash normalization."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=1000,
        key_prefix_base="app_v1",
    )
    assert settings.key_prefix_base == "app_v1/"

    settings_empty = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=1000,
        key_prefix_base="",
    )
    assert settings_empty.key_prefix_base == ""


def test_dynamic_settings_resolution():
    """Test dynamic computation of HTTP connections, seed counts, and cleanup concurrency."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=10000,
        target_write_qps=5000,
        num_workers=8,
    )
    # 1. HTTP connections dynamic resolution
    # per_worker_qps = 10000 / 8 = 1250 -> pool_size = 150 -> connections = max(500, 150*2) = 500
    assert settings.get_effective_http_connections() == 500

    # 2. Seed count dynamic resolution
    # 20 seed objects per shard
    assert settings.get_effective_seed_count(total_shards=16) == 20

    # 3. Cleanup concurrency dynamic resolution
    # 8 workers * 50 = 400
    assert settings.get_effective_cleanup_concurrency() == 400

    # 4. Platform safe concurrency limits
    assert settings.get_safe_min_concurrency_per_worker() == 50
    assert 50 <= settings.get_safe_max_concurrency_per_worker() <= 500

    # Small workload min concurrency
    small_settings = Settings(
        gcs_bucket_name="test-bucket",
        target_read_qps=50,
        target_write_qps=50,
        num_workers=1,
    )
    assert small_settings.get_safe_min_concurrency_per_worker() == 20

    # 5. Manual overrides respected
    settings.http_max_connections = 1500
    assert settings.get_effective_http_connections() == 1500
    settings.seed_objects_per_prefix = 50
    assert settings.get_effective_seed_count(total_shards=16) == 50


def test_write_key_pool_configuration():
    """Test WRITE_KEY_POOL boolean flag, numeric sizing, and dynamic auto-calculation."""
    # 1. Default: use_write_key_pool=True, write_key_pool_size=None (Auto)
    s_default = Settings(gcs_bucket_name="test-bucket", target_write_qps=5000)
    assert s_default.use_write_key_pool is True
    assert s_default.write_key_pool_size is None
    # 5000 QPS across 16 shards -> 4096 keys/shard
    assert s_default.get_effective_write_key_pool_size(total_shards=16) == 4096

    # 2. Disabled via boolean: use_write_key_pool=False -> returns 0
    s_disabled = Settings(gcs_bucket_name="test-bucket", target_write_qps=5000, use_write_key_pool=False)
    assert s_disabled.use_write_key_pool is False
    assert s_disabled.get_effective_write_key_pool_size(total_shards=16) == 0

    # 3. Parsed from write_key_pool string "false" / "0" / "no"
    s_env_false = Settings(gcs_bucket_name="test-bucket", target_write_qps=5000, write_key_pool="false")
    assert s_env_false.use_write_key_pool is False
    assert s_env_false.get_effective_write_key_pool_size(total_shards=16) == 0

    s_env_zero = Settings(gcs_bucket_name="test-bucket", target_write_qps=5000, write_key_pool="0")
    assert s_env_zero.use_write_key_pool is False
    assert s_env_zero.get_effective_write_key_pool_size(total_shards=16) == 0

    # 4. Parsed from write_key_pool numeric override string "256"
    s_env_num = Settings(gcs_bucket_name="test-bucket", target_write_qps=5000, write_key_pool="256")
    assert s_env_num.use_write_key_pool is True
    assert s_env_num.write_key_pool_size == 256
    assert s_env_num.get_effective_write_key_pool_size(total_shards=16) == 256


