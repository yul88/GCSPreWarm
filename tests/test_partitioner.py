"""Unit tests for key partitioning and prefix generation logic."""

import pytest
from src.config.settings import Settings
from src.core.partitioner import KeyPartitioner


def test_hex_auto_sharding_small():
    """Test AUTO prefix strategy selects HEX_1 for small QPS."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=5000,  # 5 shards * 1.5 = 8 shards -> <=16 -> HEX_1
        target_read_qps=0,
        key_strategy="HEX",
        prefix_strategy="AUTO",
    )
    partitioner = KeyPartitioner(settings)
    plan = partitioner.plan

    assert plan.prefix_depth == 1
    assert plan.total_allocated_shards == 16
    assert plan.prefixes[0] == "gcs_prewarm_test/0/"
    assert plan.prefixes[-1] == "gcs_prewarm_test/f/"


def test_hex_auto_sharding_medium():
    """Test AUTO prefix strategy selects HEX_2 for medium QPS."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=50000,  # 50 shards * 1.5 = 75 shards -> <=256 -> HEX_2
        target_read_qps=0,
        key_strategy="HEX",
        prefix_strategy="AUTO",
    )
    partitioner = KeyPartitioner(settings)
    plan = partitioner.plan

    assert plan.prefix_depth == 2
    assert plan.total_allocated_shards == 256
    assert plan.prefixes[0] == "gcs_prewarm_test/00/"
    assert plan.prefixes[-1] == "gcs_prewarm_test/ff/"


def test_hex_auto_sharding_large():
    """Test AUTO prefix strategy selects HEX_3 for very high QPS."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=300000,  # 300 shards * 1.5 = 450 shards -> >256 -> HEX_3
        target_read_qps=0,
        key_strategy="HEX",
        prefix_strategy="AUTO",
    )
    partitioner = KeyPartitioner(settings)
    plan = partitioner.plan

    assert plan.prefix_depth == 3
    assert plan.total_allocated_shards == 4096
    assert plan.prefixes[0] == "gcs_prewarm_test/000/"
    assert plan.prefixes[-1] == "gcs_prewarm_test/fff/"


def test_alphanumeric_sharding():
    """Test ALPHANUMERIC key strategy generation."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=10000,
        target_read_qps=0,
        key_strategy="ALPHANUMERIC",
        key_prefix_base="",
    )
    partitioner = KeyPartitioner(settings)
    plan = partitioner.plan

    assert plan.total_allocated_shards == 64
    assert plan.prefixes[0] == "0/"
    assert plan.prefixes[-1] == "_/"


def test_custom_template_sharding():
    """Test CUSTOM key strategy with template expansion."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=5000,
        target_read_qps=0,
        key_strategy="CUSTOM",
        prefix_template="tenant_{001..020}/",
        key_prefix_base="apps/",
    )
    partitioner = KeyPartitioner(settings)
    plan = partitioner.plan

    assert plan.total_allocated_shards == 20
    assert plan.prefixes[0] == "apps/tenant_001/"
    assert plan.prefixes[-1] == "apps/tenant_020/"


def test_write_and_seed_key_generation():
    """Test unique write key and seed key formats."""
    settings = Settings(
        gcs_bucket_name="test-bucket",
        target_write_qps=1000,
        target_read_qps=1000,
    )
    partitioner = KeyPartitioner(settings)
    prefix = partitioner.plan.prefixes[0]

    write_key1 = partitioner.generate_write_key(prefix)
    write_key2 = partitioner.generate_write_key(prefix)
    assert write_key1.startswith(prefix)
    assert write_key1.endswith(".dat")
    assert write_key1 != write_key2

    seed_key = partitioner.generate_seed_key(prefix, 5)
    assert seed_key == f"{prefix}seed_object_0005.dat"
