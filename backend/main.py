from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from backend.collectors.bluesky_worker import (
    analyzeSentiment as analyzePostSentiment,
    extractTopicEntities as extractPostTopicEntities,
    normalizeIncomingEvent,
    normalize_authors_for_authors_table,
    processRawPost as buildProcessedPostFromRaw,
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


def ingestRawPost(*, store: PostgresStore, normalized_event: dict[str, Any]) -> dict[str, Any] | None:
    return store.ingest_raw_post(normalized_event)


def extractTopicEntities(*, raw_post: dict[str, Any]) -> list[dict[str, str]]:
    return extractPostTopicEntities(raw_post)


def analyzeSentiment(*, raw_post: dict[str, Any]) -> dict[str, Any]:
    clean_text = str(raw_post.get("text_content") or raw_post.get("raw_text") or "").strip()
    language = str(raw_post.get("language") or "").strip() or None
    return analyzePostSentiment(clean_text=clean_text, language=language)


def processRawPost(
    *,
    store: PostgresStore,
    raw_row: dict[str, Any],
    processed_at: datetime,
    topic_entities: list[dict[str, str]] | None = None,
    sentiment: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    extracted_topics = list(topic_entities or extractTopicEntities(raw_post=raw_row))
    sentiment_payload = dict(sentiment or analyzeSentiment(raw_post=raw_row))
    processed_row = buildProcessedPostFromRaw(raw_row, processed_at=processed_at)
    if extracted_topics:
        processed_row["topic_records"] = extracted_topics
        processed_row["topic_entities"] = [
            str(row.get("normalized_topic") or "").strip()
            for row in extracted_topics
            if str(row.get("normalized_topic") or "").strip()
        ]
        if not processed_row.get("topic_key_candidate"):
            processed_row["topic_key_candidate"] = str(extracted_topics[0].get("normalized_topic") or "general")
            processed_row["topic"] = processed_row["topic_key_candidate"]
    processed_row.update(sentiment_payload)
    return store.upsert_processed_post_record(processed_row)


def persistPostTopics(
    *,
    store: PostgresStore,
    raw_row: dict[str, Any],
    processed_row: dict[str, Any],
    topic_entities: list[dict[str, str]],
    created_at: datetime,
) -> int:
    if not topic_entities:
        return 0

    source_created_at = raw_row.get("created_at") or created_at
    bucket_minute = raw_row.get("created_at") or created_at
    try:
        if isinstance(bucket_minute, datetime):
            bucket_minute = bucket_minute.replace(second=0, microsecond=0)
    except Exception:
        bucket_minute = created_at.replace(second=0, microsecond=0)

    rows: list[dict[str, Any]] = []
    for topic in topic_entities:
        normalized_topic = str(topic.get("normalized_topic") or "").strip()
        if not normalized_topic:
            continue
        topic_text = str(topic.get("topic_text") or normalized_topic).strip() or normalized_topic
        rows.append(
            {
                "raw_post_id": raw_row.get("id"),
                "processed_post_id": processed_row.get("id"),
                "platform": str(raw_row.get("platform") or "bluesky"),
                "source_post_id": str(raw_row.get("source_post_id") or raw_row.get("post_id") or "").strip(),
                "topic_text": topic_text,
                "normalized_topic": normalized_topic,
                "topic_type": str(topic.get("topic_type") or "entity"),
                "language": str(raw_row.get("language") or "").strip() or None,
                "source_created_at": source_created_at,
                "bucket_minute": bucket_minute,
                "created_at": created_at,
            }
        )

    return store.persist_post_topics(rows)


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
    parser.add_argument(
        "--aggregate-1m",
        action="store_true",
        help="Aggregate raw_posts into metric_buckets_1m and exit.",
    )
    parser.add_argument(
        "--aggregate-1h",
        action="store_true",
        help="Aggregate raw_posts into metric_buckets_1h and exit.",
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
        store.ensure_processed_topic_tables()
        log_event(logger, logging.INFO, "db_connected", source=source)
    except Exception as error:
        log_event(logger, logging.ERROR, "db_connect_failed", source=source, error=str(error))
        store.close()
        return 1

    if args.verify_db_only:
        log_event(logger, logging.INFO, "db_verify_succeeded", source=source)
        store.close()
        return 0

    if args.aggregate_1m or args.aggregate_1h:
        try:
            store.ensure_metric_bucket_tables()
            store.ensure_processed_topic_tables()
            rows_1m = 0
            rows_1h = 0
            processed_rows = 0
            topic_rows_1m = 0
            if args.aggregate_1m:
                processed_rows = store.refresh_processed_posts_from_raw_posts()
                topic_rows_1m = store.aggregate_topic_buckets_1m_from_processed_posts()
                rows_1m = store.aggregate_metric_buckets_1m()
                log_event(
                    logger,
                    logging.INFO,
                    "aggregation_complete_1m",
                    source=source,
                    rows_affected=rows_1m,
                    processed_rows_affected=processed_rows,
                    topic_rows_affected=topic_rows_1m,
                )
            if args.aggregate_1h:
                rows_1h = store.aggregate_metric_buckets_1h()
                log_event(
                    logger,
                    logging.INFO,
                    "aggregation_complete_1h",
                    source=source,
                    rows_affected=rows_1h,
                )
            log_event(
                logger,
                logging.INFO,
                "aggregation_finished",
                source=source,
                aggregate_1m=args.aggregate_1m,
                aggregate_1h=args.aggregate_1h,
                rows_1m=rows_1m,
                rows_1h=rows_1h,
                processed_rows=processed_rows,
                topic_rows_1m=topic_rows_1m,
            )
            store.close()
            return 0
        except Exception as error:
            log_event(
                logger,
                logging.ERROR,
                "aggregation_failed",
                source=source,
                error=str(error),
            )
            store.close()
            return 1

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
    last_raw_cleanup_at_monotonic = 0.0

    stop_requested = False

    def handle_signal(signum: int, _frame: Any) -> None:
        nonlocal stop_requested, shutdown_reason
        stop_requested = True
        shutdown_reason = f"signal_{signum}"
        log_event(logger, logging.INFO, "shutdown_signal", signal=signum)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    def run_raw_cleanup_if_due(*, force: bool = False) -> None:
        nonlocal last_raw_cleanup_at_monotonic
        if config.raw_retention_hours <= 0:
            return

        now_monotonic = time.monotonic()
        due = force or (
            last_raw_cleanup_at_monotonic <= 0
            or (now_monotonic - last_raw_cleanup_at_monotonic) >= config.raw_cleanup_interval_seconds
        )
        if not due:
            return

        try:
            deleted_count = store.prune_raw_posts_older_than(hours=config.raw_retention_hours)
            if deleted_count > 0:
                log_event(
                    logger,
                    logging.INFO,
                    "raw_cleanup_complete",
                    deleted_raw_posts=deleted_count,
                    retention_hours=config.raw_retention_hours,
                )
        except Exception as cleanup_error:
            log_event(
                logger,
                logging.ERROR,
                "raw_cleanup_failed",
                error=str(cleanup_error),
                retention_hours=config.raw_retention_hours,
            )
        finally:
            last_raw_cleanup_at_monotonic = now_monotonic

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
        raw_retention_hours=config.raw_retention_hours,
        raw_cleanup_interval_seconds=config.raw_cleanup_interval_seconds,
    )

    try:
        run_raw_cleanup_if_due(force=True)
        while not stop_requested:
            if args.max_cycles > 0 and cycle >= args.max_cycles:
                run_status = "completed"
                shutdown_reason = "max_cycles_reached"
                break

            run_raw_cleanup_if_due()

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

                normalized_by_source_id: dict[str, dict[str, Any]] = {}
                for event in posts:
                    normalized_event = normalizeIncomingEvent(event, ingested_at=ingested_at)
                    if not normalized_event:
                        continue
                    source_id = str(
                        normalized_event.get("source_post_id")
                        or normalized_event.get("post_id")
                        or ""
                    ).strip()
                    if not source_id:
                        continue
                    normalized_by_source_id[source_id] = normalized_event

                normalized_events = list(normalized_by_source_id.values())
                author_rows = normalize_authors_for_authors_table(
                    profiles=profiles,
                    posts=posts,
                    observed_at=ingested_at,
                )

                inserted_posts = 0
                processed_posts = 0
                post_topics_persisted = 0
                processing_errors = 0
                for normalized_event in normalized_events:
                    ingested_raw = ingestRawPost(
                        store=store,
                        normalized_event=normalized_event,
                    )
                    if not ingested_raw:
                        continue
                    inserted_posts += 1

                    try:
                        topic_entities = extractTopicEntities(raw_post=ingested_raw)
                        sentiment = analyzeSentiment(raw_post=ingested_raw)
                        processed_result = processRawPost(
                            store=store,
                            raw_row=ingested_raw,
                            processed_at=ingested_at,
                            topic_entities=topic_entities,
                            sentiment=sentiment,
                        )
                        if processed_result:
                            processed_posts += 1
                            post_topics_persisted += persistPostTopics(
                                store=store,
                                raw_row=ingested_raw,
                                processed_row=processed_result,
                                topic_entities=topic_entities,
                                created_at=ingested_at,
                            )
                    except Exception as processing_error:
                        processing_errors += 1
                        log_event(
                            logger,
                            logging.ERROR,
                            "process_raw_post_failed",
                            cycle=cycle,
                            raw_post_id=ingested_raw.get("id"),
                            source_post_id=ingested_raw.get("source_post_id"),
                            error=str(processing_error),
                        )

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
                    posts_normalized=len(normalized_events),
                    posts_inserted=inserted_posts,
                    posts_processed=processed_posts,
                    post_topics_persisted=post_topics_persisted,
                    processing_errors=processing_errors,
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
