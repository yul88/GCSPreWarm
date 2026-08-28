# GCSPreWarm: Google Cloud Storage Bucket Pre-Warming & Pre-Splitting Tool

A lightweight, high-performance Python utility designed to help Google Cloud Platform (GCP) customers pre-warm and pre-split Google Cloud Storage (GCS) buckets to a desired Read/Write QPS without encountering `429 Too Many Requests` or `503 Service Unavailable` throttling during production traffic surges.

---

## 📌 Project Overview & Objectives

* **Core Goal**: Automate the process of scaling GCS internal index partitions by gradually ramping up distributed read or write load across uniform lexicographical key prefixes.
* **Target Platforms**: Zero-friction execution in **GCP Cloud Shell** and **Google Compute Engine (GCE) Linux VMs**.
* **Design Philosophy**:
  * **Customer-Friendly**: Clean, readable Python code that enterprise security and SRE teams can easily inspect, audit, and adapt.
  * **Zero Hardcoding**: All user parameters in `.env`, all engine/tuning constants in `src/config/settings.py`.
  * **High Concurrency**: Asynchronous I/O via `asyncio` and `aiohttp` with HTTP Keep-Alive connection pooling to drive tens of thousands of requests per second.
  * **Automatic Authentication**: Transparent GCP Application Default Credentials (ADC) and Metadata Server token resolution.

---

## 🛠️ Technology Stack & Architecture Decisions

| Component | Decision | Rationale |
| :--- | :--- | :--- |
| **Language** | **Python (3.10+)** | Pre-installed in Cloud Shell & GCE VMs; universal readability for customer audits. |
| **Multi-Core Scaling** | `multiprocessing` | 1 independent worker process per CPU core, bypassing Python GIL for linear multi-core throughput scaling. |
| **C-Event Loop Engine** | `uvloop` (libuv) | Drop-in Cython/C event loop yielding 2x–3x higher socket throughput on Linux/macOS. |
| **Async HTTP Engine** | `asyncio` + `aiohttp` | Connection-pooled asynchronous HTTP with `TCP_NODELAY` and Keep-Alive against GCS REST/XML endpoints. |
| **Authentication** | `google-auth` / GCP Metadata Server | Automatic resolution of Service Account / ADC with zero-allocation in-memory header caching. |
| **Configuration** | `.env` + `settings.py` (`pydantic-settings`) | Strict separation between user parameters (`.env`) and 100% dynamic engine auto-tuning (`settings.py`). |
| **Terminal UI & Observability** | `rich` | Real-time console dashboard displaying instantaneous QPS, latency percentiles (p50, p95, p99), and HTTP status code distribution (2xx, 429, 503, 5xx). |

---

## ⚙️ Configuration Architecture

### 1. User Parameters (`.env` / `.env.example`)
* `GCS_BUCKET_NAME`: Target GCS bucket to pre-warm.
* `GCP_PROJECT_ID`: (Optional) GCP Project ID.
* `TARGET_READ_QPS`: Desired Read QPS to achieve (set `0` if write-only).
* `TARGET_WRITE_QPS`: Desired Write QPS to achieve (set `0` if read-only).
* `RAMP_PROFILE`: Preset profile: `AUTO`, `FAST` (60s/step), `STANDARD` (100s/step), `CONSERVATIVE` (20m), or `CUSTOM`.
* `RAMP_DURATION_SECONDS`: Gradual ramp-up duration (used when `RAMP_PROFILE=CUSTOM`).
* `SUSTAIN_DURATION_SECONDS`: Time to sustain target QPS (e.g., `120s`).
* `OBJECT_SIZE_BYTES`: Size of dummy test payload (default `4096` bytes / 4 KB).
* `KEY_STRATEGY`: Strategy for prefix generation (`HEX`, `ALPHANUMERIC`, `CUSTOM`).
* `PREFIX_STRATEGY`: `AUTO` (computed from target QPS) or explicit (`HEX_1`, `HEX_2`, `HEX_3`).
* `CUSTOM_PREFIXES`: Comma-separated list of customer prefixes (e.g., `users/,orders/,media/,events/`).
* `PREFIX_TEMPLATE`: Sequence template for customer prefixes (e.g., `tenant_{001..050}/`).
* `KEY_PREFIX_BASE`: Base folder path inside the bucket (e.g., `gcs_prewarm_test/` or empty `""` for bucket root).
* `WRITE_KEY_POOL`: Enable bounded rotating write key pool (`true`/`false`, default: `true`). When enabled, key pool size per shard is dynamically co-designed with prefix shards ($\le 0.10\text{ writes/s per object}$, e.g. 4,096 keys/shard) to prevent GCS 1 write/s object immutability locks while capping objects to ~65k for <3s cleanup. Set `false` for infinite unique keys.
* `CLEANUP_ON_FINISH`: Automatically delete created test objects after run (`true`/`false`).
* `KEEP_WARM_MODE`: Keep sending heartbeat traffic after test finishes to sustain splits (`true`/`false`).

### 2. 100% Dynamic Engine Tuning (`src/config/settings.py`)
All internal engine tuning parameters are automatically calculated from `TARGET_READ_QPS`, `TARGET_WRITE_QPS`, and the VM's CPU core count:
* **Rotating Write Key Pool Size (`WRITE_KEY_POOL_SIZE`)**: Auto-calculated dynamically to guarantee $\le 0.10\text{ writes/s per object}$ ($K = \max(256, \lceil \frac{Q_w}{N_{\text{shards}}} \times 10 \rceil)$, e.g., 4,096 in HEX mode).
* **CPU Worker Processes (`NUM_WORKERS`)**: 1 worker per CPU core (`os.cpu_count()`).
* **Real-Time Latency-Adaptive Coroutine Pool (`WORKER_POOL_SIZE`)**: Auto-sized via Little's Law and continuously auto-tuned in real time from live p95 latency telemetry ($\text{Pool} \propto Q_{\text{target}} \times \text{Latency}_{\text{p95}}$), bounded by platform safety limits (`min 20/50`, `max 500`).
* **TCP Connection Pool (`HTTP_MAX_CONNECTIONS`)**: Auto-sized per worker process ($\max(500, \min(\text{MaxSafe} \times 2, 2000))$) to prevent OS socket exhaustion.
* **Seed Objects per Shard (`SEED_OBJECTS_PER_PREFIX`)**: Default 20 objects per shard (e.g., 320 objects across 16 shards, uploaded in < 1 second).
* **Cleanup Parallelism (`CLEANUP_CONCURRENCY`)**: Auto-scaled to CPU cores and file descriptor limits ($\min(1000, \min(N_{\text{cpus}} \times 50, \text{FD Limit}))$, sweeping all prefix partitions concurrently in parallel.
* **OS File Descriptor Tuning**: Auto-elevates `ulimit -n` to `65,535` via `resource.setrlimit`.

---

## 📐 GCS Partitioning & Key Pattern Mechanics

### 1. Why Key Pattern Alignment is Critical
Google Cloud Storage indexes bucket objects in **strict lexicographical order (UTF-8 byte order)**. Index partition splits happen only on the specific key ranges experiencing sustained traffic.
* **If keyspace matches**: E.g., Warming `00/`–`ff/` when customer writes hash-prefixed keys (`md5(id)/...`), requests land directly in the warm shards $\rightarrow$ **Success**.
* **If keyspace does NOT match**: E.g., Warming `00/`–`ff/` while customer writes to `user_uploads/...`, the `[u...]` key range was never split $\rightarrow$ **Customer still gets 429/503 errors**.
* **Target Directory Pre-warming**: If customer data lives inside a subfolder (e.g., `app_v1/`), pre-warming must target `KEY_PREFIX_BASE=app_v1/` to split the index inside that subfolder.

### 2. Supported Key Partitioning Strategies
1. **`HEX` (Default for Hashes & UUIDs)**:
   * Uniform hex characters (`0..f`, `00..ff`, `000..fff`).
   * Ideal for workloads using MD5, SHA-256, or UUID key prefixes.
2. **`ALPHANUMERIC` (Broad Character Set)**:
   * Spans `0-9`, `a-z`, `A-Z`, `-`, `_`.
   * Pre-splits across the broader ASCII range for varied root namespaces.
3. **`CUSTOM` (Application-Specific Prefixes)**:
   * Explicit folder list (e.g., `CUSTOM_PREFIXES=users/,orders/,media/,events/,logs/`) or template (e.g., `PREFIX_TEMPLATE=tenant_{001..050}/`).
   * Accurately pre-warms exact customer operational folders.

### 3. Shard Requirement Calculation
* **Baseline Partition Limits**: $\approx 1,000$ write QPS and $\approx 5,000$ read QPS per shard.
* **Calculation Formula** (where $Q_{\text{write}}$ is Target Write QPS and $Q_{\text{read}}$ is Target Read QPS):
  $$\text{Required Shards} = \max\left(\left\lceil \frac{Q_{\text{write}}}{1,000} \times 1.5 \right\rceil, \left\lceil \frac{Q_{\text{read}}}{5,000} \times 1.5 \right\rceil, 1\right)$$
* **Hex Prefix Allocation Levels**:
  * $\le 16$ shards: 1-hex character (`0/` – `f/`, 16 shards)
  * $\le 256$ shards: 2-hex characters (`00/` – `ff/`, 256 shards $\rightarrow$ up to 256k write QPS)
  * $> 256$ shards: 3-hex characters (`000/` – `fff/`, 4096 shards $\rightarrow$ up to 4M+ write QPS)

---

## 📈 Adaptive Ramp-Up & Rate Limiting Mechanics

### 1. Step-Wise Exponential Ramp Schedule
* **Starting Baseline**: Starts at safe initial baselines ($\min(Q_{\text{write}}, 1000)$ and $\min(Q_{\text{read}}, 5000)$).
* **Stepped Doubling Curve**: Gradually steps up throughput (doubling target rate per interval across `RAMP_DURATION_SECONDS`) until target QPS is reached.
* **Shard-Uniform Pacing**: Distributes target QPS across all active prefix shards using high-precision token-bucket rate limiters.

### 2. Adaptive Backoff on Throttling
* **Automatic Detection**: If HTTP `429 Too Many Requests` or `503 Service Unavailable` responses exceed a healthy threshold (e.g. > 1%), the ramp-up controller triggers adaptive backoff.
* **Hold & Stabilize**: Ramping is frozen immediately; the generator backs down to the last stable QPS level and holds load for a stabilization cooldown window (e.g., 60s) allowing GCS backend index splits to complete.
* **Gentle Recovery**: Once error rates drop back to 0%, the ramp automatically resumes toward the target QPS.

### 3. Real-Time Telemetry & Progress Reporting
* **Periodic Console Updates**: Emits status every few seconds (configurable via `REPORT_INTERVAL_SECONDS`, default `2s`):
  * **Target vs. Current QPS**: Read QPS and Write QPS target vs. measured instantaneous throughput.
  * **Latency Distribution**: Real-time p50, p95, p99, and max response times.
  * **Status Code Health**: Live counts of `2xx OK`, `429 Rate Limited`, `503 Unavailable`, `5xx Errors`.
  * **Execution Phase**: Clear status (`SEEDING`, `RAMPING [Step X/Y]`, `SUSTAINING`, `THROTTLING_BACKOFF`, `CLEANUP`).

---

## 🚀 Quick Start & Deployment Guide

### 1. Prerequisites & IAM Permissions
Ensure your GCP identity (Cloud Shell user or VM Service Account) has the following standard IAM roles:
* **For Write Pre-warm & Cleanup**: `roles/storage.objectUser` (or `roles/storage.objectAdmin`).
* **For Read-Only Pre-warm**: `roles/storage.objectViewer`.
* **Bucket Verification**: `storage.buckets.get` permission.

---

### 2. Option A: Running in Google Cloud Shell (Fastest - 1 Minute)

Cloud Shell comes with Python 3 and Google Cloud credentials pre-configured out of the box:

```bash
# 1. Clone the repository into Cloud Shell
git clone https://github.com/yul88/GCSPreWarm.git
cd GCSPreWarm

# 2. Create virtual environment & install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Create your configuration from the template
cp .env.example .env
nano .env  # Enter your GCS_BUCKET_NAME and TARGET_READ_QPS / TARGET_WRITE_QPS

# 4. Preview the sharding plan (Dry Run)
python3 src/main.py --dry-run

# 5. Run the pre-warming process
python3 src/main.py
```

> [!TIP]
> **Cloud Shell Keep-Alive**: Since Cloud Shell sessions may disconnect if idle for 20 minutes, for long ramp runs (e.g. 20-30 min), run the tool inside `tmux` or `screen`:
> ```bash
> tmux new -s prewarm
> python3 src/main.py
> # You can detach anytime with Ctrl+B then D, and re-attach with: tmux attach -t prewarm
> ```

---

### 3. Option B: Running on Google Compute Engine (GCE) VM

For mid-range to extreme throughput (Ks to 10Ks+ of QPS), running on a standard Compute Engine VM inside the **same GCP region as your bucket** is strongly recommended:

```bash
# 1. Create a VM with Cloud Storage read/write access in your bucket's region
gcloud compute instances create gcs-prewarm-runner \
    --zone=us-central1-a \
    --machine-type=c2-standard-8 \
    --scopes=https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/cloud-platform

# 2. SSH into the VM
gcloud compute ssh gcs-prewarm-runner --zone=us-central1-a

# 3. Clone and run
git clone https://github.com/yul88/GCSPreWarm.git
cd GCSPreWarm
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
python3 src/main.py
```

---

### 🖥️ Machine Sizing & VM Selection Matrix

Choose your execution environment based on your desired target QPS:

| Target Workload (QPS) | Recommended Machine Type | Specs (vCPUs / RAM / Egress) | Notes & Best Use Cases |
| :--- | :--- | :--- | :--- |
| **< 1,500 QPS** | **Google Cloud Shell** *(Free)* | Shared CPU, ~1 Gbps network | Quick testing, configuration dry-runs, and low-QPS pre-warming. |
| **1,500 – 10,000 QPS** *(Ks of QPS)* | **`e2-standard-4`** or **`n2-standard-4`** | 4 vCPUs, 16 GB RAM, 10 Gbps | Standard production pre-warm for moderate application traffic surges. |
| **10,000 – 30,000 QPS** *(10Ks of QPS)* | **`c2-standard-8`** or **`c3-standard-8`** | 8 vCPUs, 32 GB RAM, 16–32 Gbps | Compute-Optimized (3.8 GHz Turbo) for high async I/O and < 5ms latency. |
| **30,000 – 100,000+ QPS** *(Extreme Surge)* | **`c2-standard-16`** or **`c3-standard-22`** *(or multi-VM)* | 16–22 vCPUs, 64 GB RAM, Tier_1 Networking (50–100 Gbps) | Massive enterprise pre-warming for major data migrations or flash events. |

> [!IMPORTANT]
> **Co-Locate VM with Bucket Region**: Always deploy the GCE VM in the **same Google Cloud region** as your target GCS bucket (e.g. VM in `us-central1-a` for a `us-central1` bucket). Intra-region networking keeps round-trip latency at 2ms–5ms and incurs zero inter-region network egress charges.

---

### 4. CLI Command Flags & Quick Overrides

You can override any `.env` parameter directly from the command line:

| Flag | Description | Example |
| :--- | :--- | :--- |
| `--dry-run` / `--plan` | Inspect the sharding calculation and ramp schedule without sending any requests. | `python3 src/main.py --dry-run` |
| `--profile <name>` | Ramp preset profile (`AUTO`, `FAST`, `STANDARD`, `CONSERVATIVE`, `CUSTOM`). | `python3 src/main.py --profile FAST` |
| `--fast` | Fast turbo ramp shortcut (~60s per doubling step, equivalent to `--profile FAST`). | `python3 src/main.py --fast` |
| `--mock` | Run in local simulation mode (tests UI and workers without GCP network calls). | `python3 src/main.py --mock` |
| `--force`, `-f` | Force pre-warm execution even if target QPS is within initial GCS baseline limits. | `python3 src/main.py --force` |
| `--bucket <name>` | Override the target GCS bucket name. | `python3 src/main.py --bucket my-bucket` |
| `--target-write-qps <N>`| Override the target Write QPS. | `python3 src/main.py --target-write-qps 5000` |
| `--target-read-qps <N>` | Override the target Read QPS. | `python3 src/main.py --target-read-qps 10000` |
| `--ramp-duration <sec>` | Override ramp-up duration in seconds (sets profile to `CUSTOM`). | `python3 src/main.py --ramp-duration 300` |
| `--sustain-duration <sec>`| Override sustain duration in seconds. | `python3 src/main.py --sustain-duration 120` |
| `--workers <N>` | Override worker concurrency (defaults to auto-detected CPU cores). | `python3 src/main.py --workers 8` |
| `--write-key-pool` | Enable rotating write key pool (enabled by default). | `python3 src/main.py --write-key-pool` |
| `--no-write-key-pool` / `--unique-keys` | Disable rotating key pool; generate infinite unique timestamped objects. | `python3 src/main.py --no-write-key-pool` |
| `--write-key-pool-size <N>` | Manual override for write key pool size per shard (e.g. 4096). | `python3 src/main.py --write-key-pool-size 4096` |
| `--clean-only` | Perform standalone cleanup of all test objects under `KEY_PREFIX_BASE` without running load. | `python3 src/main.py --clean-only` |
| `--no-cleanup` | Keep created test objects after test completion. | `python3 src/main.py --no-cleanup` |
| `--keep-warm` | Maintain low-rate heartbeat traffic after sustain finishes until stopped. | `python3 src/main.py --keep-warm` |

---

## 🗂️ Project Directory Structure

```
GCSPreWarm/
├── .env.example              # Template environment file
├── .gitignore                # Git ignore rules for Python and env files
├── README.md                 # Project architecture, conclusions, and run guide
├── requirements.txt          # Minimal Python dependencies
├── specs/
│   └── design_spec.md        # Formal architectural specification & contract
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py       # Centralized settings loader with validation
│   ├── auth/
│   │   ├── __init__.py
│   │   └── gcp_auth.py       # Asynchronous ADC / Metadata token provider
│   ├── core/
│   │   ├── __init__.py
│   │   ├── partitioner.py    # Sharding & prefix generator (HEX, ALPHANUMERIC, CUSTOM)
│   │   ├── rate_limiter.py   # Adaptive token-bucket & ramp-up curve controller
│   │   ├── load_generator.py # Asynchronous HTTP engine for Read/Write QPS
│   │   ├── multi_worker.py   # Multi-process orchestrator across CPU cores
│   │   └── metrics.py        # Real-time metrics collector (QPS, p50/p95/p99, errors)
│   ├── ui/
│   │   ├── __init__.py
│   │   └── console.py        # Live console dashboard
│   └── main.py               # CLI entrypoint
└── tests/
    ├── __init__.py
    ├── test_auth.py
    ├── test_load_generator.py
    ├── test_metrics.py
    ├── test_partitioner.py
    ├── test_rate_limiter.py
    └── test_settings.py
```

---

## 📋 Discussion & Decisions Log

| Date | Topic | Decision / Conclusion |
| :--- | :--- | :--- |
| 2026-08-26 | **Language Selection** | **Python (3.10+)** chosen for universal customer auditability, pre-installation in GCP Cloud Shell / GCE VMs, and high async I/O performance via `asyncio` + `aiohttp`. |
| 2026-08-26 | **Configuration Pattern** | Strict two-layer configuration: User runtime variables in `.env` and technical defaults / connection settings in `settings.py`. |
| 2026-08-26 | **Documentation Cadence** | `README.md` acts as the single source of truth for discussion conclusions, updated after every milestone decision. |
| 2026-08-26 | **Ramp & Backoff Strategy** | Focus exclusively on the **most effective stepped exponential ramp** with **automatic adaptive backoff on 429/503 throttling** (freezing ramp, stabilizing shards, then resuming). |
| 2026-08-26 | **Live Telemetry Reporting** | Live progress reporting every few seconds displaying Target R/W QPS, Current R/W QPS, Latency percentiles (p50/p95/p99), and HTTP status breakdown. |
| 2026-08-26 | **Key Pattern Alignment** | GCS index splits are lexicographical; pre-warm keys must match customer key structure. Tool supports **HEX**, **ALPHANUMERIC**, **CUSTOM prefixes/templates**, and configurable **Base Path**. |
| 2026-08-26 | **Explicit QPS Parameters** | Replaced `WORKLOAD_TYPE` & `READ_RATIO` with direct `TARGET_READ_QPS` and `TARGET_WRITE_QPS` numbers for zero-friction customer configuration and automatic mode detection. |
| 2026-08-26 | **Baseline Capacity Check** | If target QPS is $\le 5,000$ Read and $\le 1,000$ Write, notify user that standard GCS natively supports the workload without pre-warming, avoiding unnecessary operations (bypassable with `--force`). |
| 2026-08-27 | **Multi-Process Architecture** | Upgraded from single-process event loop to **MultiProcessOrchestrator** (1 independent worker process per CPU core). Bypasses Python GIL, achieves 100% CPU core utilization, and scales linearly to 30,000–50,000+ QPS. |
| 2026-08-28 | **5x Engine Throughput Optimizations** | (1) **Dynamic Persistent Worker Pool** (auto-calculated from target QPS via Little's Law, zero Task allocations/sec), (2) **`uvloop` C-engine**, (3) **Cached Auth Header dicts**, (4) **Lock-free connection pooling**, (5) **`TCP_NODELAY` direct socket dispatch**. |
| 2026-08-28 | **100% Dynamic Parameter Auto-Tuning** | All engine parameters (CPU workers, worker pool size, TCP connection limits, seed object counts, cleanup deletion concurrency, sharding allocation, ramp durations) are now **dynamically calculated from user Target QPS and VM CPU core count**, with optional manual overrides. |
| 2026-08-28 | **Pre-Flight Hardware Capacity Check** | Automatically verifies whether the execution environment (Cloud Shell vs GCE VM cores) can sustain the requested target QPS ($\sim 2,500\text{ QPS/vCPU}$), alerting the user and providing specific machine sizing recommendations if under-provisioned. |
| 2026-08-28 | **Independent Read/Write Latency Auto-Tuning** | The engine independently monitors `read_p95_latency_ms` and `write_p95_latency_ms`, auto-scaling Read and Write coroutine worker pools separately to absorb write commit latency while keeping read pools lean. |
| 2026-08-28 | **Dynamic Write Key Pool & 429 Prevention** | Set `WRITE_KEY_POOL` as a boolean default `true` with dynamic auto-sizing ($\le 0.10\text{ writes/s/object}$, e.g. 4,096 keys/shard) and randomized uniform distribution across worker coroutines, completely eliminating GCS 1 write/s object immutability locks and multi-process harmonic collisions while capping total objects to ~65k for <3s cleanup. |
| 2026-08-28 | **Pre-Flight Write Key Pool & Rate Check** | Added automated pre-flight check validating `WRITE_KEY_POOL` against GCS's 1 write/sec per object limit, alerting users if pool size is undersized for configured QPS or in infinite object mode. |
| 2026-08-26 | **Full Implementation & Tests** | Complete async load engine, token-bucket rate limiter, metrics collector, CLI, and unit test suite verified. |
