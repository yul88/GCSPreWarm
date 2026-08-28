"""Rich terminal dashboard and progress reporting for GCSPreWarm."""

import math
import os
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.config.settings import Settings
from src.core.metrics import MetricSnapshot
from src.core.partitioner import ShardingPlan
from src.core.rate_limiter import ExecutionPhase, RampState


class ConsoleDashboard:
    """Renders formatted tables, progress metrics, and summary reports."""

    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()

    def print_header(self) -> None:
        """Print application banner."""
        title = Text("🚀 GCSPreWarm: GCS Bucket Pre-Warming & Pre-Splitting Engine", style="bold cyan")
        subtitle = Text("Automated Lexicographical Partition Scaling for High-QPS Workloads", style="dim")
        self.console.print(Panel(Text.assemble(title, "\n", subtitle), border_style="cyan"))

    def print_plan(self, plan: ShardingPlan, settings: Settings) -> None:
        """Display the calculated pre-warming and sharding plan."""
        table = Table(title="📋 Pre-Warming & Sharding Configuration Plan", border_style="blue", show_header=True)
        table.add_column("Parameter", style="bold white")
        table.add_column("Configured Value", style="green")
        table.add_column("Details / Capacity", style="cyan")

        table.add_row("Target Bucket", settings.gcs_bucket_name or "(None specified)", "GCS Target Container")
        table.add_row(
            "Target Read QPS",
            f"{plan.target_read_qps:,}" if plan.target_read_qps > 0 else "Disabled (0)",
            f"Estimated Shard Capacity: {plan.estimated_read_capacity:,} QPS",
        )
        table.add_row(
            "Target Write QPS",
            f"{plan.target_write_qps:,}" if plan.target_write_qps > 0 else "Disabled (0)",
            f"Estimated Shard Capacity: {plan.estimated_write_capacity:,} QPS",
        )
        table.add_row("Key Strategy", plan.key_strategy, f"Prefix Depth: {plan.prefix_depth}")
        table.add_row(
            "Allocated Shards",
            f"{plan.total_allocated_shards:,}",
            f"Min Required: {plan.min_required_shards:,} (with 1.5x safety headroom)",
        )
        table.add_row(
            "Key Prefix Base",
            f"'{plan.key_prefix_base}'" if plan.key_prefix_base else "(Root namespace)",
            "Base path inside bucket",
        )
        if plan.target_write_qps > 0:
            eff_pool = settings.get_effective_write_key_pool_size(plan.total_allocated_shards)
            if eff_pool > 0:
                total_slots = plan.total_allocated_shards * eff_pool
                writes_per_slot = plan.target_write_qps / total_slots
                pool_desc = f"{eff_pool:,} keys/shard" + (" (Auto)" if settings.write_key_pool_size is None else " (Manual)")
                table.add_row(
                    "Write Key Pool",
                    pool_desc,
                    f"{total_slots:,} total rotating slots (~{writes_per_slot:.2f} writes/s/key)",
                )
            else:
                table.add_row(
                    "Write Key Pool",
                    "0 (Infinite Unique Keys)",
                    "Generates a new timestamped object on every write",
                )
        profile_str = (getattr(settings, "ramp_profile", "AUTO") or "AUTO").upper()
        table.add_row(
            "Ramp Profile",
            profile_str,
            "Ramp duration preset (AUTO / FAST / STANDARD / CONSERVATIVE / CUSTOM)",
        )

        from src.core.rate_limiter import AdaptiveRampController
        controller = AdaptiveRampController(settings)
        effective_ramp_secs = int(controller.ramp_duration)
        table.add_row(
            "Ramp Duration",
            f"{effective_ramp_secs}s ({effective_ramp_secs // 60}m {effective_ramp_secs % 60}s)",
            f"Stepped doubling curve ({controller.total_steps} steps @ {int(controller.step_duration)}s/step)",
        )
        table.add_row(
            "Sustain Duration",
            f"{settings.sustain_duration_seconds}s ({settings.sustain_duration_seconds // 60}m)",
            "Hold steady at full target QPS",
        )
        table.add_row("Object Payload Size", f"{settings.object_size_bytes} bytes", "Dummy data payload per write")
        table.add_row("Worker Concurrency", f"{settings.num_workers} tasks", f"Auto-detected CPU cores ({os.cpu_count() or 1} CPUs)")
        table.add_row("Cleanup on Finish", str(settings.cleanup_on_finish), "Delete test objects after run")
        table.add_row("Keep-Warm Mode", str(settings.keep_warm_mode), "Maintain heartbeat after sustain")

        self.console.print(table)
        self.console.print()

    def check_platform_capacity(self, settings: Settings) -> bool:
        """Evaluate if current VM platform has sufficient vCPU capacity for target QPS.

        Returns True if capacity is sufficient, or False if target exceeds estimated limits.
        """
        total_target = settings.target_read_qps + settings.target_write_qps
        if total_target <= 0:
            return True

        is_cloud_shell = bool(
            os.environ.get("CLOUD_SHELL")
            or os.environ.get("DEVSHELL_CLIENT_PORT")
            or os.path.exists("/google/devshell")
        )
        cpus = max(1, os.cpu_count() or 1)

        if is_cloud_shell:
            max_capacity = 1500
            platform_desc = f"Google Cloud Shell (Shared vCPU, {cpus} cores detected)"
            suggestion = (
                "Google Cloud Shell has shared CPU/network bandwidth limits (~1,500 QPS max).\n"
                "• For 5,000 – 10,000 QPS: Deploy a dedicated [bold cyan]n4-highcpu-4[/bold cyan] (4 vCPUs) VM.\n"
                "• For 10,000 – 20,000+ QPS: Deploy a dedicated [bold cyan]n4-highcpu-8[/bold cyan] or [bold cyan]c3-highcpu-8[/bold cyan] (8 vCPUs) VM in the same GCP region as your bucket."
            )
        else:
            # GCE Linux / macOS VM: ~2,500 HTTPS QPS per dedicated physical/virtual core
            max_capacity = cpus * 2500
            platform_desc = f"Compute Engine / Linux VM ({cpus} vCPUs)"
            if total_target <= 10000:
                rec_vm = "n4-highcpu-4 or c3-highcpu-4 (4 vCPUs)"
            elif total_target <= 20000:
                rec_vm = "n4-highcpu-8 or c3-highcpu-8 (8 vCPUs)"
            elif total_target <= 40000:
                rec_vm = "n4-highcpu-16 or c3-highcpu-16 (16 vCPUs)"
            else:
                rec_vm = f"c3-highcpu-32 (32 vCPUs) or distribute across {max(2, total_target // 20000)}x 8-core VMs"

            suggestion = (
                f"Client-side HTTPS/TLS encryption and socket management typically scales to ~2,500 QPS per vCPU.\n"
                f"• Recommended VM Sizing: Upgrade to [bold cyan]{rec_vm}[/bold cyan] in the same GCP region as your bucket.\n"
                "• Alternatively, run multiple client instances in parallel across the same bucket."
            )

        if total_target > max_capacity:
            warning_text = (
                f"[bold yellow]⚠️ Platform Capacity Pre-Check Warning[/bold yellow]\n\n"
                f"• [bold white]Current Platform:[/bold white] {platform_desc}\n"
                f"• [bold white]Estimated Platform Max Capacity:[/bold white] [bold yellow]~{max_capacity:,} QPS[/bold yellow]\n"
                f"• [bold white]Requested Target Workload:[/bold white] [bold red]{total_target:,} QPS[/bold red] "
                f"({settings.target_read_qps:,} Read + {settings.target_write_qps:,} Write)\n\n"
                f"[bold cyan]💡 Sizing Recommendation:[/bold cyan]\n{suggestion}\n\n"
                "[dim](The engine will continue and attempt to drive maximum possible load from this machine)[/dim]"
            )
            self.console.print(Panel(warning_text, title="⚠️ Hardware Sizing Alert", border_style="yellow"))
            self.console.print()
            return False

        return True

    def check_write_key_pool_capacity(self, settings: Settings, total_shards: int) -> bool:
        """Validate WRITE_KEY_POOL_SIZE against target write QPS and total shard count, warning if misconfigured.

        Returns True if configured safely, or False if write rate per key exceeds GCS 1 write/s guidelines.
        """
        if settings.target_write_qps <= 0:
            return True

        if not settings.use_write_key_pool or settings.write_key_pool_size == 0:
            total_est_objects = settings.target_write_qps * max(60, settings.sustain_duration_seconds)
            notice_text = (
                f"[bold yellow]ℹ️ Infinite Unique Keys Mode Active (`WRITE_KEY_POOL=false`)[/bold yellow]\n\n"
                f"• Every write request generates a brand new timestamped object.\n"
                f"• Estimated Objects Created: [bold red]~{total_est_objects:,} objects[/bold red]\n"
                f"• [bold white]Cleanup Impact:[/bold white] Post-test cleanup requires 1 HTTP DELETE per object "
                f"and may take [bold yellow]several minutes to over an hour[/bold yellow].\n\n"
                f"[bold cyan]💡 Recommendation:[/bold cyan] Enable [bold green]WRITE_KEY_POOL=true[/bold green] "
                f"for auto-calculated pool size to achieve 100% write QPS and <3s cleanup."
            )
            self.console.print(Panel(notice_text, title="ℹ️ Key Generation Mode Notice", border_style="yellow"))
            self.console.print()
            return True

        total_shards = max(1, total_shards)
        pool_size = settings.get_effective_write_key_pool_size(total_shards)
        if pool_size <= 0:
            return True

        total_slots = total_shards * pool_size
        est_writes_per_slot = settings.target_write_qps / total_slots

        # GCS quota limit is 1 write per second to the same object name; warn if exceeded (>1.0/s)
        if est_writes_per_slot > 1.0:
            min_recommended_pool = max(256, math.ceil(settings.target_write_qps / total_shards * 10))
            suggested_pool = 4096 if (settings.key_strategy == "HEX" and min_recommended_pool <= 4096) else min_recommended_pool

            warning_text = (
                f"[bold yellow]⚠️ WRITE_KEY_POOL_SIZE Sizing Warning[/bold yellow]\n\n"
                f"• [bold white]Configured Write Workload:[/bold white] [bold red]{settings.target_write_qps:,} Write QPS[/bold red] across {total_shards} shards "
                f"({settings.target_write_qps / total_shards:,.1f} QPS/shard)\n"
                f"• [bold white]Current WRITE_KEY_POOL_SIZE:[/bold white] [bold yellow]{pool_size} keys/shard[/bold yellow] ({total_slots:,} total rotating slots)\n"
                f"• [bold white]Estimated Write Rate per Object:[/bold white] [bold red]~{est_writes_per_slot:.2f} writes/second[/bold red]\n"
                f"• [bold white]GCS Quota Limit:[/bold white] [bold cyan]1 write per second to the same object name[/bold cyan] (per GCS Quotas)\n\n"
                f"[bold cyan]💡 Sizing Recommendation:[/bold cyan]\n"
                f"Increase [bold green]WRITE_KEY_POOL_SIZE[/bold green] to at least [bold green]{suggested_pool}[/bold green] (via `--write-key-pool {suggested_pool}` or `.env`) "
                f"to prevent GCS object immutability lock contention (HTTP 429/503 errors).\n\n"
                f"[dim](The engine will continue and attempt to drive load with the current pool size)[/dim]"
            )
            self.console.print(Panel(warning_text, title="⚠️ Object Mutation Rate Warning", border_style="yellow"))
            self.console.print()
            return False

        return True

    def render_live_status(self, ramp: RampState, metrics: MetricSnapshot) -> Table:
        """Render live real-time status table for periodic reporting."""
        # Phase formatting
        phase_style = "bold green"
        phase_label = f"PHASE: {ramp.phase.value}"

        if ramp.phase == ExecutionPhase.RAMPING:
            phase_label += f" [Step {ramp.current_step}/{ramp.total_steps}]"
        elif ramp.phase == ExecutionPhase.THROTTLING_BACKOFF:
            phase_style = "bold red"
            phase_label += f" (⚠️ Backoff Active: {ramp.backoff_seconds_remaining:.0f}s left)"

        outer_table = Table(
            title=f"[{phase_style}]{phase_label}[/{phase_style}] | Elapsed: {int(metrics.elapsed_seconds)}s",
            border_style="magenta",
            show_header=True,
        )
        outer_table.add_column("Operation", style="bold white", width=12)
        outer_table.add_column("Target QPS", style="yellow", justify="right", width=14)
        outer_table.add_column("Current QPS", style="bold green", justify="right", width=14)
        outer_table.add_column("Progress", style="cyan", width=22)
        outer_table.add_column("Latency (p50 / p95 / p99)", style="white", width=26)

        # Helper for progress bar
        def _make_bar(cur: float, tgt: float) -> str:
            if tgt <= 0:
                return "Disabled"
            pct = min(100.0, (cur / tgt) * 100.0)
            filled = int(pct / 10)
            bar = "█" * filled + "░" * (10 - filled)
            return f"[{bar}] {pct:5.1f}%"

        # Read row
        if ramp.target_read_qps > 0:
            read_lat = f"{metrics.read_p50_latency_ms:.1f}ms / {metrics.read_p95_latency_ms:.1f}ms / {metrics.read_p99_latency_ms:.1f}ms"
            outer_table.add_row(
                "READ (GET)",
                f"{int(ramp.current_read_target):,} / {ramp.target_read_qps:,}",
                f"{metrics.current_read_qps:,.0f} QPS",
                _make_bar(metrics.current_read_qps, ramp.target_read_qps),
                read_lat,
            )

        # Write row
        if ramp.target_write_qps > 0:
            write_lat = f"{metrics.write_p50_latency_ms:.1f}ms / {metrics.write_p95_latency_ms:.1f}ms / {metrics.write_p99_latency_ms:.1f}ms"
            outer_table.add_row(
                "WRITE (PUT)",
                f"{int(ramp.current_write_target):,} / {ramp.target_write_qps:,}",
                f"{metrics.current_write_qps:,.0f} QPS",
                _make_bar(metrics.current_write_qps, ramp.target_write_qps),
                write_lat,
            )

        # HTTP Status summary line
        status_line = (
            f"[green]2xx OK: {metrics.window_2xx:,}[/green] | "
            f"[yellow]429 Throttled: {metrics.window_429:,}[/yellow] | "
            f"[yellow]503 Unavailable: {metrics.window_503:,}[/yellow] | "
            f"[red]5xx Errors: {metrics.window_5xx:,}[/red] | "
            f"Total Ops: {metrics.total_ops:,}"
        )
        outer_table.caption = status_line
        return outer_table

    def print_summary(self, metrics: MetricSnapshot, cleaned_up_objects: int) -> None:
        """Display final execution summary table."""
        self.console.print()
        table = Table(title="🏁 GCSPreWarm Execution Summary", border_style="green", show_header=True)
        table.add_column("Metric", style="bold white")
        table.add_column("Value", style="bold green")

        table.add_row("Total Run Duration", f"{metrics.elapsed_seconds:.1f} seconds")
        table.add_row("Total Read Operations", f"{metrics.total_read_ops:,}")
        table.add_row("Total Write Operations", f"{metrics.total_write_ops:,}")
        table.add_row("Total Executed Requests", f"{metrics.total_ops:,}")
        table.add_row("Successful Requests (2xx)", f"{metrics.cum_2xx:,}")
        table.add_row("Throttled Requests (429)", f"{metrics.cum_429:,}")
        table.add_row("Service Unavailable (503)", f"{metrics.cum_503:,}")
        table.add_row("Server Errors (5xx)", f"{metrics.cum_5xx:,}")
        if metrics.total_read_ops > 0:
            table.add_row("Read Latency p50 / p95 / p99", f"{metrics.read_p50_latency_ms:.1f}ms / {metrics.read_p95_latency_ms:.1f}ms / {metrics.read_p99_latency_ms:.1f}ms")
        if metrics.total_write_ops > 0:
            table.add_row("Write Latency p50 / p95 / p99", f"{metrics.write_p50_latency_ms:.1f}ms / {metrics.write_p95_latency_ms:.1f}ms / {metrics.write_p99_latency_ms:.1f}ms")
        table.add_row("Overall Latency p50 / p95 / p99", f"{metrics.p50_latency_ms:.1f}ms / {metrics.p95_latency_ms:.1f}ms / {metrics.p99_latency_ms:.1f}ms")
        table.add_row("Cleaned Up Test Objects", f"{cleaned_up_objects:,}")

        self.console.print(table)
        self.console.print(Panel("[bold green]✅ Bucket Pre-Warming & Pre-Splitting Complete![/bold green]\nYour GCS bucket partitions are warm and ready for high-throughput production traffic.", border_style="green"))
