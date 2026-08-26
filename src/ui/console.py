"""Rich terminal dashboard and progress reporting for GCSPreWarm."""

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
        table.add_row(
            "Ramp Duration",
            f"{settings.ramp_duration_seconds}s ({settings.ramp_duration_seconds // 60}m)",
            "Stepped exponential doubling curve",
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
            read_lat = f"{metrics.p50_latency_ms:.1f}ms / {metrics.p95_latency_ms:.1f}ms / {metrics.p99_latency_ms:.1f}ms"
            outer_table.add_row(
                "READ (GET)",
                f"{int(ramp.current_read_target):,} / {ramp.target_read_qps:,}",
                f"{metrics.current_read_qps:,.0f} QPS",
                _make_bar(metrics.current_read_qps, ramp.target_read_qps),
                read_lat,
            )

        # Write row
        if ramp.target_write_qps > 0:
            write_lat = f"{metrics.p50_latency_ms:.1f}ms / {metrics.p95_latency_ms:.1f}ms / {metrics.p99_latency_ms:.1f}ms"
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
        table.add_row("Overall Latency p50 / p95 / p99", f"{metrics.p50_latency_ms:.1f}ms / {metrics.p95_latency_ms:.1f}ms / {metrics.p99_latency_ms:.1f}ms")
        table.add_row("Cleaned Up Test Objects", f"{cleaned_up_objects:,}")

        self.console.print(table)
        self.console.print(Panel("[bold green]✅ Bucket Pre-Warming & Pre-Splitting Complete![/bold green]\nYour GCS bucket partitions are warm and ready for high-throughput production traffic.", border_style="green"))
