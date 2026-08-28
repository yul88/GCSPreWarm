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
    # per_worker_qps = 10000 / 8 = 1250 -> pool_size = 62 -> connections = max(200, 62*2) = 200
    assert settings.get_effective_http_connections() == 200

    # 2. Seed count dynamic resolution
    # 20 seed objects per shard
    assert settings.get_effective_seed_count(total_shards=16) == 20

    # 3. Cleanup concurrency dynamic resolution
    # 8 workers * 50 = 400
    assert settings.get_effective_cleanup_concurrency() == 400

    # 4. Manual overrides respected
    settings.http_max_connections = 1500
    assert settings.get_effective_http_connections() == 1500
    settings.seed_objects_per_prefix = 50
    assert settings.get_effective_seed_count(total_shards=16) == 50

