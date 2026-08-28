"""GCSPreWarm: Main CLI entrypoint for pre-warming and pre-splitting GCS buckets."""

import argparse
import asyncio
import os
import signal
import sys
import time
from typing import Optional

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.live import Live
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn

from src.auth.gcp_auth import GCPAuthProvider, get_auth_provider
from src.config.settings import Settings, get_settings
from src.core.load_generator import GCSLoadEngine
from src.core.metrics import MetricSnapshot, MetricsCollector
from src.core.multi_worker import MultiProcessOrchestrator
from src.core.partitioner import KeyPartitioner
from src.core.rate_limiter import AdaptiveRampController, ExecutionPhase
from src.ui.console import ConsoleDashboard


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="GCSPreWarm: Pre-warm and pre-split Google Cloud Storage buckets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Path to environment configuration file.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        help="Override GCS bucket name.",
    )
    parser.add_argument(
        "--target-read-qps",
        type=int,
        help="Override target Read QPS.",
    )
    parser.add_argument(
        "--target-write-qps",
        type=int,
        help="Override target Write QPS.",
    )
    parser.add_argument(
        "--profile",
        "--ramp-profile",
        dest="ramp_profile",
        type=str,
        choices=["AUTO", "FAST", "STANDARD", "CONSERVATIVE", "CUSTOM", "auto", "fast", "standard", "conservative", "custom"],
        help="Ramp duration preset profile: AUTO (auto by QPS tier), FAST (~60s/step), STANDARD (~100s/step), CONSERVATIVE (20m total), or CUSTOM.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Fast ramp shortcut: ~60s per doubling step (equivalent to --profile FAST).",
    )
    parser.add_argument(
        "--ramp-duration",
        type=int,
        help="Explicit override for ramp-up duration in seconds (sets profile to CUSTOM).",
    )
    parser.add_argument(
        "--sustain-duration",
        type=int,
        help="Override sustain duration in seconds.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="Override worker concurrency multiplier (defaults to auto-detected CPU cores).",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force execution even if target QPS is within initial GCS baseline limits (<= 5,000 Read, <= 1,000 Write).",
    )
    parser.add_argument(
        "--dry-run",
        "--plan",
        dest="dry_run",
        action="store_true",
        help="Print the sharding plan and calculations without issuing requests.",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Execute in local mock simulation mode (no GCP network calls or credentials required).",
    )
    parser.add_argument(
        "--clean-only",
        action="store_true",
        help="Perform standalone cleanup of all test objects under KEY_PREFIX_BASE without running pre-warm load.",
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="Skip cleaning up generated test objects upon completion.",
    )
    parser.add_argument(
        "--keep-warm",
        action="store_true",
        help="Enable keep-warm heartbeat loop after sustain completes.",
    )

    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    """Asynchronous entrypoint."""
    console = Console()
    dashboard = ConsoleDashboard(console)
    dashboard.print_header()

    # Load configuration
    env_file = args.env_file if os.path.exists(args.env_file) else None
    settings = get_settings(env_file=env_file)

    # Apply CLI overrides
    if args.bucket:
        settings.gcs_bucket_name = args.bucket
    if args.target_read_qps is not None:
        settings.target_read_qps = args.target_read_qps
    if args.target_write_qps is not None:
        settings.target_write_qps = args.target_write_qps
    if args.fast:
        settings.ramp_profile = "FAST"
    elif args.ramp_profile:
        settings.ramp_profile = args.ramp_profile.upper()
    if args.ramp_duration is not None:
        settings.ramp_duration_seconds = args.ramp_duration
        settings.ramp_profile = "CUSTOM"
    if args.sustain_duration is not None:
        settings.sustain_duration_seconds = args.sustain_duration
    if args.workers is not None:
        settings.num_workers = args.workers
    if args.cleanup is not None:
        settings.cleanup_on_finish = args.cleanup
    if args.keep_warm:
        settings.keep_warm_mode = True

    # Validate bucket name unless dry-run/mock
    if not settings.gcs_bucket_name and not (args.dry_run or args.mock):
        console.print("[bold red]❌ Error: GCS_BUCKET_NAME is required. Provide via .env or --bucket <name>[/bold red]")
        return 1

    # Compute sharding plan
    try:
        partitioner = KeyPartitioner(settings)
    except Exception as e:
        console.print(f"[bold red]❌ Configuration Error: {e}[/bold red]")
        return 1

    # Initialize auth and engine
    auth_provider = get_auth_provider(mock_mode=args.mock)
    seed_engine = GCSLoadEngine(
        settings=settings,
        partitioner=partitioner,
        auth_provider=auth_provider,
        metrics=MetricsCollector(window_seconds=settings.report_interval_seconds),
        mock_mode=args.mock,
    )
    await seed_engine.initialize()

    # Handle Standalone Clean-Only mode
    if args.clean_only:
        console.print(f"\n[bold cyan]🧹 Standalone Cleanup Mode: Sweeping gs://{settings.gcs_bucket_name}/{settings.key_prefix_base}...[/bold cyan]")
        cleaned_objects = await seed_engine.cleanup_all_objects()
        console.print(f"[bold green]✓ Standalone cleanup complete! Deleted {cleaned_objects:,} objects.[/bold green]\n")
        await seed_engine.close()
        return 0

    # Display plan
    dashboard.print_plan(partitioner.plan, settings)

    # Perform VM platform hardware capacity pre-check
    dashboard.check_platform_capacity(settings)

    if args.dry_run:
        console.print("[bold yellow]ℹ️ Dry-run mode completed. No traffic sent to GCS.[/bold yellow]")
        await seed_engine.close()
        return 0

    # Check if target QPS is within initial GCS baseline limits
    baseline_read = 5000
    baseline_write = 1000
    is_within_baseline = (
        settings.target_read_qps <= baseline_read
        and settings.target_write_qps <= baseline_write
    )

    if is_within_baseline and not (args.force or args.mock):
        console.print(
            "\n[bold yellow]ℹ️ Target QPS is within default GCS baseline capacity:[/bold yellow]\n"
            f"  • Configured Target: [bold green]{settings.target_read_qps:,} Read QPS[/bold green], [bold green]{settings.target_write_qps:,} Write QPS[/bold green]\n"
            f"  • Default GCS Baseline: [bold cyan]5,000 Read QPS[/bold cyan], [bold cyan]1,000 Write QPS[/bold cyan]\n\n"
            "Google Cloud Storage automatically supports this workload without pre-warming or index splitting.\n"
            "Pre-warming is only required when scaling beyond initial baseline limits.\n\n"
            "[bold white]To force pre-warming/load testing anyway, re-run with the [bold cyan]--force[/bold cyan] (or [bold cyan]-f[/bold cyan]) flag:[/bold white]\n"
            "  [dim]python3 src/main.py --force[/dim]\n"
        )
        await seed_engine.close()
        return 0

    ramp_controller = AdaptiveRampController(settings)

    # Multi-process orchestrator
    orchestrator = MultiProcessOrchestrator(
        settings=settings,
        partitioner=partitioner,
        mock_mode=args.mock,
    )

    # Cancellation & cleanup state
    interrupted = False
    cleaned_objects = 0
    sigint_count = 0
    final_snapshot: Optional[MetricSnapshot] = None

    # Handle Ctrl+C gracefully
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sigint_handler():
        nonlocal interrupted, sigint_count
        sigint_count += 1
        if sigint_count >= 2:
            console.print("\n[bold red]⚡ Force exit (Ctrl+C pressed again). Terminating process immediately...[/bold red]")
            orchestrator.stop()
            os._exit(130)
        interrupted = True
        console.print("\n[bold yellow]⚠️ Interrupt received (Ctrl+C). Initiating shutdown & cleanup... (Press Ctrl+C again to abort cleanup)[/bold yellow]")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sigint_handler)
        except (NotImplementedError, RuntimeError):
            # Windows or non-main thread fallback
            pass

    try:
        # =====================================================================
        # Phase 1: Seed Phase (If Target Read QPS > 0)
        # =====================================================================
        if settings.target_read_qps > 0 and not stop_event.is_set():
            ramp_controller.phase = ExecutionPhase.SEEDING
            effective_seed_count = settings.get_effective_seed_count(len(partitioner.plan.prefixes))
            total_expected_seeds = len(partitioner.plan.prefixes) * effective_seed_count

            with Progress(
                TextColumn("[cyan]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                seed_task_id = progress.add_task(
                    f"🌱 Phase 1: Pre-populating {total_expected_seeds:,} seed objects across {len(partitioner.plan.prefixes)} shards...",
                    total=total_expected_seeds,
                )

                def _on_seed_progress(completed: int, total: int):
                    progress.update(seed_task_id, completed=completed)

                seed_task = asyncio.create_task(
                    seed_engine.seed_objects(effective_seed_count, progress_callback=_on_seed_progress)
                )
                stop_task = asyncio.create_task(stop_event.wait())
                done, pending = await asyncio.wait(
                    [seed_task, stop_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

            if stop_event.is_set():
                seed_task.cancel()
                console.print("[yellow]⚠️ Seeding cancelled by user.[/yellow]")
            else:
                stop_task.cancel()
                seed_count = seed_task.result()
                console.print(f"[green]✓ Pre-populated {seed_count:,} seed objects across {len(partitioner.plan.prefixes)} shards.[/green]\n")

        # =====================================================================
        # Phase 2 & 3: Ramp-Up & Sustain Phases (Multi-Process Execution)
        # =====================================================================
        if not stop_event.is_set():
            ramp_controller.start(initial_phase=ExecutionPhase.RAMPING)
            orchestrator.start()

            start_t = time.perf_counter()

            # Status monitoring loop
            with Live(console=console, refresh_per_second=4) as live:
                while not stop_event.is_set():
                    elapsed = time.perf_counter() - start_t

                    # Poll multi-process metrics
                    snapshot = orchestrator.poll_metrics(elapsed_seconds=elapsed)
                    final_snapshot = snapshot

                    # Update ramp controller
                    ramp_controller.report_metrics(snapshot.throttling_rate)
                    ramp_state = ramp_controller.update()

                    # Distribute rates to all worker processes
                    orchestrator.set_target_rates(
                        ramp_state.current_read_target,
                        ramp_state.current_write_target,
                    )

                    # Render UI
                    table = dashboard.render_live_status(ramp_state, snapshot)
                    live.update(table)

                    if ramp_state.phase == ExecutionPhase.COMPLETED:
                        break

                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=settings.report_interval_seconds)
                    except asyncio.TimeoutError:
                        pass

            orchestrator.stop()

        # =====================================================================
        # Phase 4: Keep-Warm Heartbeat (Optional)
        # =====================================================================
        if settings.keep_warm_mode and not interrupted:
            console.print("\n[bold cyan]🔥 Entering Keep-Warm Heartbeat Mode (Ctrl+C to stop)...[/bold cyan]")
            ramp_controller.phase = ExecutionPhase.KEEP_WARM
            heartbeat_read = min(100.0, float(settings.target_read_qps)) if settings.target_read_qps > 0 else 0.0
            heartbeat_write = min(100.0, float(settings.target_write_qps)) if settings.target_write_qps > 0 else 0.0

            orchestrator.start()
            orchestrator.set_target_rates(heartbeat_read, heartbeat_write)
            await stop_event.wait()
            orchestrator.stop()

    finally:
        # =====================================================================
        # Phase 5: Cleanup Phase
        # =====================================================================
        orchestrator.stop()

        # Collect created keys from workers and seed engine
        all_created_keys = set(orchestrator.get_created_keys()) | seed_engine._created_keys | set(seed_engine._seed_keys)
        seed_engine._created_keys = all_created_keys

        if settings.cleanup_on_finish and all_created_keys:
            total_clean_keys = len(all_created_keys)
            with Progress(
                TextColumn("[cyan]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                clean_task_id = progress.add_task(
                    f"🧹 Cleaning up {total_clean_keys:,} test objects...",
                    total=total_clean_keys,
                )

                def _on_clean_progress(completed: int, total: int):
                    progress.update(clean_task_id, completed=completed)

                cleaned_objects = await seed_engine.cleanup_all_objects(progress_callback=_on_clean_progress)

            console.print(f"[green]✓ Cleaned up {cleaned_objects:,} objects.[/green]")

        await seed_engine.close()

    # Final summary report
    if final_snapshot is None:
        final_snapshot = MetricSnapshot(
            timestamp=time.perf_counter(),
            elapsed_seconds=0.0,
            current_read_qps=0.0,
            current_write_qps=0.0,
            current_total_qps=0.0,
            total_read_ops=0,
            total_write_ops=0,
            total_ops=0,
            window_2xx=0,
            window_429=0,
            window_503=0,
            window_5xx=0,
            window_errors=0,
            cum_2xx=0,
            cum_429=0,
            cum_503=0,
            cum_5xx=0,
            cum_errors=0,
            p50_latency_ms=0.0,
            p95_latency_ms=0.0,
            p99_latency_ms=0.0,
            max_latency_ms=0.0,
            throttling_rate=0.0,
            error_rate=0.0,
        )
    dashboard.print_summary(final_snapshot, cleaned_objects)
    return 0


def main() -> None:
    """CLI Synchronous entrypoint."""
    from src.config.settings import optimize_system_resources

    optimize_system_resources()

    try:
        import uvloop
        uvloop.install()
    except (ImportError, AttributeError):
        pass

    args = parse_args()
    try:
        sys.exit(asyncio.run(async_main(args)))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
