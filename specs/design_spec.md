# Specification: GCS Bucket Pre-Warming & Pre-Splitting Tool (`GCSPreWarm`)

## 1. System Overview
`GCSPreWarm` is a Python-based utility engineered to pre-warm and pre-split Google Cloud Storage (GCS) buckets. GCS index partitions automatically scale up when traffic increases in a gradual, well-distributed manner across lexicographical key prefixes. This tool automates the ramp-up and partition splitting process so that applications can achieve a target Read and/or Write QPS without encountering `429 Too Many Requests` or `503 Service Unavailable` rate-limiting errors.

---

## 2. Configuration & Parameter Contract

### 2.1 User Environment Variables (`.env`)
| Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `GCS_BUCKET_NAME` | `str` | *Required* | Name of the target GCS bucket. |
| `GCP_PROJECT_ID` | `str` | `""` | Optional GCP Project ID. |
| `TARGET_READ_QPS` | `int` | `0` | Desired read requests per second to achieve. |
| `TARGET_WRITE_QPS` | `int` | `1000` | Desired write requests per second to achieve. |
| `RAMP_PROFILE` | `str` | `"AUTO"` | Preset profile: `AUTO`, `FAST` (60s/step), `STANDARD` (100s/step), `CONSERVATIVE` (20m), or `CUSTOM`. |
| `RAMP_DURATION_SECONDS`| `int` | `1200` | Duration (seconds) of ramp-up phase (used when `RAMP_PROFILE=CUSTOM`). |
| `SUSTAIN_DURATION_SECONDS`| `int` | `600` | Duration (seconds) to hold sustained target QPS. |
| `OBJECT_SIZE_BYTES` | `int` | `4096` | Payload size in bytes for dummy test objects (default 4KB). |
| `KEY_STRATEGY` | `str` | `"HEX"` | Key generation strategy: `HEX`, `ALPHANUMERIC`, `CUSTOM`. |
| `PREFIX_STRATEGY` | `str` | `"AUTO"` | Prefix depth: `AUTO`, `HEX_1`, `HEX_2`, `HEX_3`. |
| `CUSTOM_PREFIXES` | `str` | `""` | Comma-separated list of customer prefixes (e.g., `users/,orders/`). |
| `PREFIX_TEMPLATE` | `str` | `""` | Sequence template for prefixes (e.g., `tenant_{001..050}/`). |
| `KEY_PREFIX_BASE` | `str` | `"gcs_prewarm_test/"` | Base path/directory inside the bucket (e.g., `app_v1/` or `""` for root). |
| `CLEANUP_ON_FINISH` | `bool` | `true` | Asynchronously delete created test objects after completion. |
| `KEEP_WARM_MODE` | `bool` | `false` | Continue low-rate heartbeat traffic to maintain split shards. |

### 2.2 Centralized System Defaults (`src/config/settings.py`)
| Parameter | Type | Default | Dynamic Computation & Description |
| :--- | :--- | :--- | :--- |
| `GCS_BASE_URL` | `str` | `https://storage.googleapis.com` | Base GCS REST/XML API endpoint. |
| `NUM_WORKERS` | `int` | `CPU Cores` | Auto-detected CPU cores (`os.cpu_count()`). 1 worker process per core. |
| `WORKER_POOL_SIZE` | `int` | `Dynamic` | Auto-sized coroutines per process: $\text{clamp}(\lceil \frac{Q_{\text{target}}}{N_{\text{cpus}}} \times 0.05 \rceil, 20, 500)$. |
| `HTTP_MAX_CONNECTIONS`| `int` | `Dynamic` | Auto-sized TCP pool per worker: $\max(500, \min(2000, \text{PoolSize} \times 2))$. |
| `SEED_OBJECTS_PER_PREFIX`| `int`| `20` | Optimal seed count per shard (20 objects per shard, auto-created in Phase 1). |
| `CLEANUP_CONCURRENCY` | `int` | `Dynamic` | Auto-sized parallel deletion concurrency: $\max(100, \min(1000, N_{\text{cpus}} \times 50))$. |
| `HTTP_TIMEOUT_SECONDS`| `float`| `10.0` | Individual request timeout in seconds. |
| `HTTP_KEEP_ALIVE_SECONDS`| `float`| `60.0` | Keep-alive duration for TCP sockets. |
| `REPORT_INTERVAL_SECONDS`| `float`| `2.0` | Metrics reporting and console refresh interval. |
| `MAX_RETRIES` | `int` | `3` | Max retries for transient errors (429/503/network). |
| `BACKOFF_FACTOR` | `float`| `0.5` | Exponential backoff factor for retries. |
| `THROTTLING_ERROR_THRESHOLD`| `float`| `0.01` | Error rate (1%) triggering adaptive ramp backoff. |
| `STABILIZATION_COOLDOWN_SECONDS`| `int` | `60` | Cooldown period on throttling before resuming ramp. |

---

## 3. Core Algorithms & Math Specifications

### 3.1 Shard Requirement Calculation
GCS baseline capacities:
* Baseline Write QPS per partition: $C_w = 1,000$
* Baseline Read QPS per partition: $C_r = 5,000$
* Safety Headroom Factor: $S = 1.5$
* Configured Targets: $Q_{\text{write}}$ (Target Write QPS), $Q_{\text{read}}$ (Target Read QPS)

$$\text{Required Shards} = \max\left(\left\lceil \frac{Q_{\text{write}}}{C_w} \times S \right\rceil, \left\lceil \frac{Q_{\text{read}}}{C_r} \times S \right\rceil, 1\right)$$

### 3.2 Key Prefix Partition Allocation
* **`HEX` Mode**:
  * Shards $\le 16 \rightarrow$ 1-hex depth (`0/`, `1/`, ..., `f/`) = 16 shards
  * Shards $\le 256 \rightarrow$ 2-hex depth (`00/`, `01/`, ..., `ff/`) = 256 shards
  * Shards $> 256 \rightarrow$ 3-hex depth (`000/`, `001/`, ..., `fff/`) = 4,096 shards
* **`ALPHANUMERIC` Mode**:
  * Set: `0-9`, `a-z`, `A-Z`, `-`, `_` (64 characters)
  * Shards $\le 64 \rightarrow$ 1-char depth (64 shards)
  * Shards $> 64 \rightarrow$ 2-char depth (4,096 shards)
* **`CUSTOM` Mode**:
  * Parsed from `CUSTOM_PREFIXES` or generated from `PREFIX_TEMPLATE` (e.g. `tenant_{001..100}/`).

### 3.3 Stepped Exponential Ramp-Up Curve
* **Initial Safe Rate**:
  * $R_0 = \min(Q_{\text{read}}, 5000)$
  * $W_0 = \min(Q_{\text{write}}, 1000)$
* **Step Count**: Calculated based on doubling steps:
  $$N_{\text{steps}} = \max\left(\left\lceil \log_2 \frac{Q_{\text{write}}}{W_0} \right\rceil, \left\lceil \log_2 \frac{Q_{\text{read}}}{R_0} \right\rceil, 1\right)$$
* **Step Duration**: $\Delta t = \frac{T_{\text{ramp}}}{N_{\text{steps}}}$ (where $T_{\text{ramp}}$ is `RAMP_DURATION_SECONDS`)
* **Rate at Step $k \in [0, N_{\text{steps}}-1]$**:
  $$W_k = \min(W_0 \times 2^k, Q_{\text{write}})$$
  $$R_k = \min(R_0 \times 2^k, Q_{\text{read}})$$

### 3.4 Adaptive Throttling Backoff Protocol
1. **Detection**: Metric window checks ratio of `(429_count + 503_count) / total_requests`.
2. **Threshold Violation (> 1%)**:
   * State changes to `THROTTLING_BACKOFF`.
   * Freeze ramp timer.
   * Drop current target QPS by 50% or down to previous step $W_{k-1}, R_{k-1}$.
   * Hold for `STABILIZATION_COOLDOWN_SECONDS` to allow GCS backend partition splits to settle.
3. **Recovery**: When error rate returns to $0\%$, resume ramp timer and transition back to `RAMPING`.

---

## 4. Execution Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PRE_FLIGHT_CHECK: Load Config & Validate ADC
    PRE_FLIGHT_CHECK --> SEED_PHASE: Target Read QPS > 0
    PRE_FLIGHT_CHECK --> RAMP_PHASE: Target Read QPS == 0
    SEED_PHASE --> RAMP_PHASE: Pre-populate Seed Objects across Shards
    RAMP_PHASE --> SUSTAIN_PHASE: Target QPS Reached
    RAMP_PHASE --> THROTTLING_BACKOFF: 429/503 Error Rate > 1%
    THROTTLING_BACKOFF --> RAMP_PHASE: Errors Resolved & Stabilized
    SUSTAIN_PHASE --> CLEANUP_PHASE: Sustain Timer Complete & Cleanup=True
    SUSTAIN_PHASE --> KEEP_WARM_PHASE: Sustain Timer Complete & KeepWarm=True
    SUSTAIN_PHASE --> COMPLETED: Sustain Complete & Cleanup=False
    CLEANUP_PHASE --> COMPLETED: Test Objects Deleted
    KEEP_WARM_PHASE --> COMPLETED: User Interrupt (Ctrl+C)
    COMPLETED --> [*]
```

---

## 5. Security & Authentication Requirements
1. **Zero Secret Storage**: Never store service account keys in repository or configuration files.
2. **Standard GCP ADC**: Use `google.auth.default()` or direct GCP Instance Metadata Server queries.
3. **Token Management**: Thread-safe / async token caching with automatic proactive refresh before expiration.

---

## 6. Execution Environment & Machine Sizing Matrix

| Workload Range | Recommended Target | Specs | Architectural Rationale |
| :--- | :--- | :--- | :--- |
| **< 1.5k QPS** | Google Cloud Shell | Shared vCPU, ~1 Gbps | Free development environment for dry-run verification and low-rate pre-warming. |
| **1.5k – 10k QPS** | `e2-standard-4` / `n2-standard-4` | 4 vCPU, 16 GB, 10 Gbps | Standard VM sizing for moderate pre-warming. |
| **10k – 30k QPS** | `c2-standard-8` / `c3-standard-8` | 8 vCPU, 32 GB, 16–32 Gbps | Compute-optimized 3.8 GHz Turbo for high async HTTP socket concurrency with < 5ms RTT latency. |
| **30k – 100k+ QPS**| `c2-standard-16` / `c3-standard-22` | 16–22 vCPU, 64 GB, 50–100 Gbps Tier_1 | Massive enterprise pre-warming across 256–4096 shard keyspaces. |

* **Co-Location Requirement**: All VM instances MUST be deployed inside the same Google Cloud region as the target GCS bucket for direct intra-VPC storage network routing.

### 6.1 Platform Capacity Pre-Flight Validation
Prior to execution, the engine validates whether the current execution platform has sufficient vCPU and network bandwidth capacity to generate the configured target QPS:
* **Cloud Shell**: Capped at ~1,500 QPS (prompts user to deploy a dedicated GCE VM for $\ge 5,000$ QPS).
* **GCE / Linux VM**: Estimated at $\sim 2,500\text{ HTTPS QPS / vCPU}$.
* **Action on Over-subscription**: Warns the user with a prominent hardware sizing alert and provides specific machine type recommendations (e.g. `n4-highcpu-4`, `n4-highcpu-8`, `c3-highcpu-16`, or multi-client distribution).

---

## 7. High-Throughput Engine Optimizations

### 7.1 Independent Read & Write Latency-Adaptive Coroutine Pipelines
Worker coroutine pools are dynamically auto-sized per worker process based on Little's Law with separate real-time latency feedback for GET and PUT operations:

$$\text{Read Pool Size} = \text{clamp}\left(\left\lceil \frac{Q_{\text{read}}}{N_{\text{workers}}} \times \left(\frac{L_{r,\text{p95}}}{1000} \times 1.5\right) \right\rceil, \text{min}=20, \text{max}=500\right)$$

$$\text{Write Pool Size} = \text{clamp}\left(\left\lceil \frac{Q_{\text{write}}}{N_{\text{workers}}} \times \left(\frac{L_{w,\text{p95}}}{1000} \times 1.5\right) \right\rceil, \text{min}=20, \text{max}=500\right)$$

* $Q_{\text{read}}$, $Q_{\text{write}}$: Target Read and Write QPS.
* $N_{\text{workers}}$: Number of active worker processes (CPU cores).
* $L_{r,\text{p95}}$, $L_{w,\text{p95}}$: Observed real-time p95 latency (in milliseconds) for Read and Write requests.
* Continuously measures live latencies separately, automatically expanding the write pool to absorb distributed commit delays while keeping read pools lean and memory-efficient.

### 7.2 Streaming Parallel Shard Deletion Engine
* Queries all prefix partitions (`gcs_prewarm_test/0/` through `f/`) concurrently in parallel.
* Streams object keys from pagination pages directly into bounded deletion tasks without buffering millions of keys in RAM, rendering a live progress counter throughout Phase 5 cleanup.

### 7.3 Additional Performance Optimizations
1. **C-Based `uvloop` Event Loop**: Automatically attaches `uvloop` (libuv C-engine) on Linux/macOS for 2x–3x higher event loop throughput.
2. **Cached Authorization Headers**: Proactively caches OAuth2 header dictionaries in memory with non-blocking refresh, eliminating 15,000+ dictionary allocations per second.
3. **Lock-Free Concurrency**: Leverages `aiohttp.TCPConnector` native connection limits, eliminating redundant Python semaphores and lock contention.
4. **Direct Socket Tuning**: Enables `TCP_NODELAY` and HTTP Keep-Alive pooling for immediate packet dispatch without OS buffer delay.

