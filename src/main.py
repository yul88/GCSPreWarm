"""GCSPreWarm: Main CLI entrypoint for pre-warming and pre-splitting GCS buckets."""

import argparse
import asyncio
import os
import signal
import sys
from typing import Optional

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.live import Live

from src.auth.gcp_auth import GCPAuthProvider, get_auth_provider
from src.config.settings import Settings, get_settings
from src.core.load_generator import GCSLoadEngine
from src.core.metrics import MetricsCollector
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
        "--ramp-duration",
        type=int,
        help="Override ramp-up duration in seconds.",
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
    if args.ramp_duration is not None:
        settings.ramp_duration_seconds = args.ramp_duration
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

    # Display plan
    dashboard.print_plan(partitioner.plan, settings)

    if args.dry_run:
        console.print("[bold yellow]ℹ️ Dry-run mode completed. No traffic sent to GCS.[/bold yellow]")
        return 0

    # Initialize components
    auth_provider = get_auth_provider(mock_mode=args.mock)
    metrics = MetricsCollector(window_seconds=settings.report_interval_seconds)
    ramp_controller = AdaptiveRampController(settings)
    engine = GCSLoadEngine(
        settings=settings,
        partitioner=partitioner,
        auth_provider=auth_provider,
        metrics=metrics,
        mock_mode=args.mock,
    )

    await engine.initialize()

    # Cancellation & cleanup state
    interrupted = False
    cleaned_objects = 0

    # Handle Ctrl+C gracefully
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sigint_handler():
        nonlocal interrupted
        interrupted = True
        console.print("\n[bold yellow]⚠️ Interrupt received (Ctrl+C). Initiating graceful shutdown...[/bold yellow]")
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
        if settings.target_read_qps > 0:
            console.print("[cyan]🌱 Phase 1: Pre-populating seed objects for read pre-warm...[/cyan]")
            ramp_controller.phase = ExecutionPhase.SEEDING
            seed_count = await engine.seed_objects(settings.seed_objects_per_prefix)
            console.print(f"[green]✓ Pre-populated {seed_count:,} seed objects across {len(partitioner.plan.prefixes)} shards.[/green]\n")

        # =====================================================================
        # Phase 2 & 3: Ramp-Up & Sustain Phases
        # =====================================================================
        engine._is_running = True
        ramp_controller.start(initial_phase=ExecutionPhase.RAMPING)

        # Launch workers (scaled across available CPU cores)
        worker_tasks = []
        for _ in range(settings.num_workers):
            if settings.target_write_qps > 0:
                worker_tasks.append(asyncio.create_task(engine.run_write_worker()))
            if settings.target_read_qps > 0:
                worker_tasks.append(asyncio.create_task(engine.run_read_worker()))

        # Status monitoring loop
        with Live(console=console, refresh_per_second=4) as live:
            while not stop_event.is_set():
                # Get metrics
                snapshot = metrics.get_snapshot()

                # Update ramp controller
                ramp_controller.report_metrics(snapshot.throttling_rate)
                ramp_state = ramp_controller.update()

                # Update engine rate limits
                engine.update_rates(ramp_state.current_read_target, ramp_state.current_write_target)

                # Render UI
                table = dashboard.render_live_status(ramp_state, snapshot)
                live.update(table)

                if ramp_state.phase == ExecutionPhase.COMPLETED:
                    break

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=settings.report_interval_seconds)
                except asyncio.TimeoutError:
                    pass

        # Stop workers
        engine._is_running = False
        for t in worker_tasks:
            t.cancel()

        # =====================================================================
        # Phase 4: Keep-Warm Heartbeat (Optional)
        # =====================================================================
        if settings.keep_warm_mode and not interrupted:
            console.print("\n[bold cyan]🔥 Entering Keep-Warm Heartbeat Mode (Ctrl+C to stop)...[/bold cyan]")
            ramp_controller.phase = ExecutionPhase.KEEP_WARM
            engine._is_running = True
            # Maintain 5 QPS per shard or target
            heartbeat_read = min(100.0, float(settings.target_read_qps)) if settings.target_read_qps > 0 else 0.0
            heartbeat_write = min(100.0, float(settings.target_write_qps)) if settings.target_write_qps > 0 else 0.0
            engine.update_rates(heartbeat_read, heartbeat_write)

            kw_tasks = []
            if heartbeat_write > 0:
                kw_tasks.append(asyncio.create_task(engine.run_write_worker()))
            if heartbeat_read > 0:
                kw_tasks.append(asyncio.create_task(engine.run_read_worker()))

            await stop_event.wait()
            engine._is_running = False
            for t in kw_tasks:
                t.cancel()

    finally:
        # =====================================================================
        # Phase 5: Cleanup Phase
        # =====================================================================
        if settings.cleanup_on_finish:
            console.print("\n[bold cyan]🧹 Cleaning up created test objects...[/bold cyan]")
            cleaned_objects = await engine.cleanup_all_objects()
            console.print(f"[green]✓ Cleaned up {cleaned_objects:,} objects.[/green]")

        await engine.close()

    # Final summary report
    final_snapshot = metrics.get_snapshot()
    dashboard.print_summary(final_snapshot, cleaned_objects)
    return 0


def main() -> None:
    """CLI Synchronous entrypoint."""
    args = parse_args()
    try:
        sys.exit(asyncio.run(async_main(args)))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
