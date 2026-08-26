"""Key Partitioning & Prefix Sharding Planner for GCSPreWarm.

Calculates required shards and generates uniform lexicographical prefix distributions
for HEX, ALPHANUMERIC, and CUSTOM customer key structures.
"""

import math
import re
import string
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

from src.config.settings import Settings

# Constants for GCS Baseline capacity
GCS_WRITE_QPS_PER_SHARD = 1000
GCS_READ_QPS_PER_SHARD = 5000
SAFETY_HEADROOM_FACTOR = 1.5

ALPHANUMERIC_CHARS = string.digits + string.ascii_lowercase + string.ascii_uppercase + "-_"


@dataclass(frozen=True)
class ShardingPlan:
    """Calculated sharding plan and prefix configuration."""

    target_read_qps: int
    target_write_qps: int
    min_required_shards: int
    total_allocated_shards: int
    key_strategy: str
    prefix_depth: int
    key_prefix_base: str
    prefixes: List[str]

    @property
    def estimated_write_capacity(self) -> int:
        """Estimated aggregate write QPS capacity supported by allocated shards."""
        return self.total_allocated_shards * GCS_WRITE_QPS_PER_SHARD

    @property
    def estimated_read_capacity(self) -> int:
        """Estimated aggregate read QPS capacity supported by allocated shards."""
        return self.total_allocated_shards * GCS_READ_QPS_PER_SHARD


class KeyPartitioner:
    """Manages partition key calculations and object path generation."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.plan = self._compute_sharding_plan()

    def _compute_min_required_shards(self) -> int:
        """Calculate minimum required shards using target QPS and safety headroom."""
        write_shards = 0
        if self.settings.target_write_qps > 0:
            write_shards = math.ceil(
                (self.settings.target_write_qps / GCS_WRITE_QPS_PER_SHARD)
                * SAFETY_HEADROOM_FACTOR
            )

        read_shards = 0
        if self.settings.target_read_qps > 0:
            read_shards = math.ceil(
                (self.settings.target_read_qps / GCS_READ_QPS_PER_SHARD)
                * SAFETY_HEADROOM_FACTOR
            )

        return max(write_shards, read_shards, 1)

    def _generate_hex_prefixes(self, min_shards: int) -> tuple[int, List[str]]:
        """Generate uniform hexadecimal prefixes."""
        strategy = self.settings.prefix_strategy

        if strategy == "HEX_1" or (strategy == "AUTO" and min_shards <= 16):
            depth = 1
            prefixes = [f"{i:x}/" for i in range(16)]
        elif strategy == "HEX_2" or (strategy == "AUTO" and min_shards <= 256):
            depth = 2
            prefixes = [f"{i:02x}/" for i in range(256)]
        else:
            # HEX_3 or AUTO > 256
            depth = 3
            prefixes = [f"{i:03x}/" for i in range(4096)]

        return depth, prefixes

    def _generate_alphanumeric_prefixes(self, min_shards: int) -> tuple[int, List[str]]:
        """Generate uniform alphanumeric prefixes."""
        if min_shards <= len(ALPHANUMERIC_CHARS):
            depth = 1
            prefixes = [f"{c}/" for c in ALPHANUMERIC_CHARS]
        else:
            depth = 2
            prefixes = [
                f"{c1}{c2}/"
                for c1 in ALPHANUMERIC_CHARS
                for c2 in ALPHANUMERIC_CHARS
            ]
        return depth, prefixes

    def _generate_custom_prefixes(self) -> tuple[int, List[str]]:
        """Parse custom prefix lists or template sequences."""
        custom_list = self.settings.parsed_custom_prefixes()
        if custom_list:
            # Ensure trailing slash
            normalized = [p if p.endswith("/") else f"{p}/" for p in custom_list]
            return 1, normalized

        template = self.settings.prefix_template.strip()
        if template:
            # Support template format: e.g. "tenant_{001..050}/" or "shard_{1..100}/"
            match = re.search(r"\{(\d+)\.\.(\d+)\}", template)
            if match:
                start_str, end_str = match.groups()
                start_num = int(start_str)
                end_num = int(end_num) if 'end_num' in locals() else int(end_str)
                width = len(start_str) if start_str.startswith("0") else 0

                prefixes = []
                for num in range(start_num, end_num + 1):
                    formatted_num = f"{num:0{width}d}" if width > 0 else str(num)
                    prefix_str = template[:match.start()] + formatted_num + template[match.end():]
                    if not prefix_str.endswith("/"):
                        prefix_str = f"{prefix_str}/"
                    prefixes.append(prefix_str)
                return 1, prefixes

        raise ValueError("Invalid custom prefix configuration: neither custom list nor template resolved.")

    def _compute_sharding_plan(self) -> ShardingPlan:
        """Construct the ShardingPlan based on configuration."""
        min_shards = self._compute_min_required_shards()
        strategy = self.settings.key_strategy

        if strategy == "HEX":
            depth, raw_prefixes = self._generate_hex_prefixes(min_shards)
        elif strategy == "ALPHANUMERIC":
            depth, raw_prefixes = self._generate_alphanumeric_prefixes(min_shards)
        elif strategy == "CUSTOM":
            depth, raw_prefixes = self._generate_custom_prefixes()
        else:
            raise ValueError(f"Unknown key strategy: {strategy}")

        base = self.settings.key_prefix_base
        full_prefixes = [f"{base}{p}" if base else p for p in raw_prefixes]

        return ShardingPlan(
            target_read_qps=self.settings.target_read_qps,
            target_write_qps=self.settings.target_write_qps,
            min_required_shards=min_shards,
            total_allocated_shards=len(full_prefixes),
            key_strategy=strategy,
            prefix_depth=depth,
            key_prefix_base=base,
            prefixes=full_prefixes,
        )

    def generate_write_key(self, prefix_shard: str) -> str:
        """Generate a distinct, unique object key within a given prefix shard."""
        unique_id = uuid.uuid4().hex[:12]
        timestamp_ns = time.time_ns()
        return f"{prefix_shard}obj_{timestamp_ns}_{unique_id}.dat"

    def generate_seed_key(self, prefix_shard: str, seed_index: int) -> str:
        """Generate a deterministic seed object key for read warming."""
        return f"{prefix_shard}seed_object_{seed_index:04d}.dat"
