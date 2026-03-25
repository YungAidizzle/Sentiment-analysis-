from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from backend.collectors.bluesky_worker import (
    normalize_authors_for_authors_table,
    normalize_posts_for_raw_table,
    run_firehose_window,
)
from backend.config import WorkerConfig
from backend.db import PostgresStore
from backend.logging_setup import log_event, setup_logging


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_cursor(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def _state_summary(state: dict[str, Any] | None) -> dict[str, Any]:
    state = dict(state or {})
    return {
        "status": state.get("status"),
        "healthStatus": state.get("healthStatus"),
        "connectionStatus": state.get("connectionStatus"),
        "lastError": state.get("lastError"),
        "lastEventAt": state.get("lastEventAt"),
        "lastSyncEvents": state.get("lastSyncEvents"),
        "eventsPerMinute": state.get("eventsPerMinute"),
        "reconnectCount": state.get("reconnectCount"),
        "rawPersistSuccessRate": state.get("rawPersistSuccessRate"),
        "normalizationSuccessRate": state.get("normalizationSuccessRate"),
    }


def _build_run_notes(
    *,
    cycle: int,
    cursor_us: int | None,
    rows_inserted: int,
    shutdown_reason: str | None,
    state: dict[str, Any] | None,
    stats: dict[str, Any] | None,
    timings: dict[str, Any] | None,
    last_error: str | None,
) -> dict[str, Any]:
    return {
        "worker": "bluesky_firehose_worker",
        "cycle": cycle,
        "last_cursor_us": cursor_us or 0,
        "rows_inserted": rows_inserted,
        "state": _state_summary(state),
        "stats": dict(stats or {}),
        "timings": dict(timings or {}),
        "last_error": last_error,
        "shutdown_reason": shutdown_reason,
        "updated_at": _utc_now().isoformat(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persistent Bluesky ingestion worker")
    parser.add_argument("--once", action="store_true", help="Run one firehose window and exit.")
    parser.add_argument("--sleep-seconds", type=float, default=None)
    parser.add_argument("--retry-seconds", type=float, default=None)
    parser.add_argument("--source", type=str, default="")
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Stop after N cycles. 0 means run forever.",
    )
    parser.add_argument(
        "--verify-db-only",
        action="store_true",
        help="Verify DB connection/schema and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = WorkerConfig.from_env(
            sleep_seconds=args.sleep_seconds,
            retry_seconds=args.retry_seconds,
        )
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    setup_logging(config.log_level)
    logger = logging.getLogger("backend.worker")
    source = args.source.strip() or config.source

    store = PostgresStore(
        database_url=config.database_url,
        batch_size=config.batch_size,
        logger=logger,
    )

    try:
        store.verify_connection()
        log_event(logger, logging.INFO, "db_connected", source=source)
    except Exception as error:
        log_event(logger, logging.ERROR, "db_connect_failed", source=source, error=str(error))
        store.close()
        return 1

    if args.verify_db_only:
        log_event(logger, logging.INFO, "db_verify_succeeded", source=source)
        store.close()
        return 0

    started_at = _utc_now()
    rows_inserted_total = 0
    cycle = 0
    cursor_us = store.fetch_resume_cursor(source=source)
    shutdown_reason = "running"
    last_error: str | None = None
    last_state: dict[str, Any] = {}
    last_stats: dict[str, Any] = {}
    last_timings: dict[str, Any] = {}
    run_status = "running"

    stop_requested = False

    def handle_signal(signum: int, _frame: Any) -> None:
        nonlocal stop_requested, shutdown_reason
        stop_requested = True
        shutdown_reason = f"signal_{signum}"
        log_event(logger, logging.INFO, "shutdown_signal", signal=signum)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    initial_notes = _build_run_notes(
        cycle=cycle,
        cursor_us=cursor_us,
        rows_inserted=rows_inserted_total,
        shutdown_reason=None,
        state=None,
        stats=None,
        timings=None,
        last_error=None,
    )

    try:
        store.create_ingestion_run(
            source=source,
            started_at=started_at,
            notes=initial_notes,
        )
    except Exception as error:
        log_event(logger, logging.ERROR, "create_ingestion_run_failed", source=source, error=str(error))
        store.close()
        return 1

    log_event(
        logger,
        logging.INFO,
        "worker_start",
        source=source,
        started_at=started_at.isoformat(),
        resumed_cursor=cursor_us or 0,
        once=args.once,
        max_cycles=args.max_cycles,
    )

    try:
        while not stop_requested:
            if args.max_cycles > 0 and cycle >= args.max_cycles:
                run_status = "completed"
                shutdown_reason = "max_cycles_reached"
                break

            cycle += 1
            cycle_started = time.monotonic()
            progress_written_at = time.monotonic()
            progress_state: dict[str, Any] = {}

            def write_progress(progress: dict[str, Any]) -> None:
                nonlocal progress_written_at, cursor_us, progress_state
                progress_state = dict(progress or {})
                candidate_cursor = _parse_cursor(progress_state.get("cursor"))
                if candidate_cursor is not None:
                    cursor_us = candidate_cursor

                elapsed = time.monotonic() - progress_written_at
                if elapsed < config.progress_update_seconds:
                    return

                notes = _build_run_notes(
                    cycle=cycle,
                    cursor_us=cursor_us,
                    rows_inserted=rows_inserted_total,
                    shutdown_reason=None,
                    state=progress_state,
                    stats=last_stats,
                    timings=last_timings,
                    last_error=last_error,
                )
                try:
                    store.update_ingestion_run(
                        source=source,
                        started_at=started_at,
                        status="running",
                        rows_inserted=rows_inserted_total,
                        notes=notes,
                    )
                    progress_written_at = time.monotonic()
                except Exception as update_error:
                    log_event(
                        logger,
                        logging.ERROR,
                        "progress_update_failed",
                        cycle=cycle,
                        error=str(update_error),
                    )

            log_event(
                logger,
                logging.INFO,
                "cycle_start",
                cycle=cycle,
                cursor_us=cursor_us or 0,
            )

            try:
                result = run_firehose_window(cursor_us=cursor_us, progress_callback=write_progress)
                ingested_at = _utc_now()
                posts = list(result.get("posts") or [])
                profiles = list(result.get("profiles") or [])
                state = dict(result.get("state") or {})
                stats = dict(result.get("stats") or {})
                timings = dict(result.get("timings") or {})

                raw_post_rows = normalize_posts_for_raw_table(posts, ingested_at=ingested_at)
                author_rows = normalize_authors_for_authors_table(
                    profiles=profiles,
                    posts=posts,
                    observed_at=ingested_at,
                )

                inserted_posts = store.upsert_raw_posts(raw_post_rows)
                upserted_authors = store.upsert_authors(author_rows)
                rows_inserted_total += inserted_posts

                candidate_cursor = _parse_cursor(state.get("cursor"))
                if candidate_cursor is not None:
                    cursor_us = candidate_cursor

                last_state = state
                last_stats = stats
                last_timings = timings
                last_error = None

                notes = _build_run_notes(
                    cycle=cycle,
                    cursor_us=cursor_us,
                    rows_inserted=rows_inserted_total,
                    shutdown_reason=None,
                    state=state,
                    stats=stats,
                    timings=timings,
                    last_error=None,
                )
                store.update_ingestion_run(
                    source=source,
                    started_at=started_at,
                    status="running",
                    rows_inserted=rows_inserted_total,
                    notes=notes,
                )

                cycle_duration_ms = round((time.monotonic() - cycle_started) * 1000, 1)
                log_event(
                    logger,
                    logging.INFO,
                    "cycle_complete",
                    cycle=cycle,
                    events_processed=int(state.get("lastSyncEvents") or 0),
                    posts_normalized=len(raw_post_rows),
                    posts_inserted=inserted_posts,
                    authors_upserted=upserted_authors,
                    rows_inserted_total=rows_inserted_total,
                    cursor_us=cursor_us or 0,
                    health_status=state.get("healthStatus"),
                    connection_status=state.get("connectionStatus"),
                    duration_ms=cycle_duration_ms,
                )
            except KeyboardInterrupt:
                stop_requested = True
                run_status = "stopped"
                shutdown_reason = "keyboard_interrupt"
                break
            except Exception as error:
                last_error = str(error)
                notes = _build_run_notes(
                    cycle=cycle,
                    cursor_us=cursor_us,
                    rows_inserted=rows_inserted_total,
                    shutdown_reason=None,
                    state=progress_state or last_state,
                    stats=last_stats,
                    timings=last_timings,
                    last_error=last_error,
                )
                try:
                    store.update_ingestion_run(
                        source=source,
                        started_at=started_at,
                        status="running",
                        rows_inserted=rows_inserted_total,
                        notes=notes,
                    )
                except Exception as update_error:
                    log_event(
                        logger,
                        logging.ERROR,
                        "cycle_error_run_update_failed",
                        cycle=cycle,
                        error=str(update_error),
                    )

                log_event(
                    logger,
                    logging.ERROR,
                    "cycle_error",
                    cycle=cycle,
                    error=last_error,
                )

                if args.once:
                    run_status = "failed"
                    shutdown_reason = "once_cycle_error"
                    break
                time.sleep(config.retry_seconds)
                continue

            if args.once:
                run_status = "completed"
                shutdown_reason = "once"
                break

            if config.loop_sleep_seconds > 0 and not stop_requested:
                time.sleep(config.loop_sleep_seconds)

        if run_status == "running":
            run_status = "stopped"
            shutdown_reason = shutdown_reason if shutdown_reason != "running" else "loop_exit"
    finally:
        final_notes = _build_run_notes(
            cycle=cycle,
            cursor_us=cursor_us,
            rows_inserted=rows_inserted_total,
            shutdown_reason=shutdown_reason,
            state=last_state,
            stats=last_stats,
            timings=last_timings,
            last_error=last_error,
        )
        try:
            store.update_ingestion_run(
                source=source,
                started_at=started_at,
                status=run_status,
                rows_inserted=rows_inserted_total,
                notes=final_notes,
                ended_at=_utc_now(),
            )
        except Exception as error:
            log_event(
                logger,
                logging.ERROR,
                "finalize_ingestion_run_failed",
                source=source,
                error=str(error),
            )
        store.close()

    log_event(
        logger,
        logging.INFO,
        "worker_shutdown",
        source=source,
        status=run_status,
        rows_inserted_total=rows_inserted_total,
        shutdown_reason=shutdown_reason,
        last_error=last_error,
    )

    return 0 if run_status in {"completed", "stopped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
