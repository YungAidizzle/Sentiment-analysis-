from __future__ import annotations

import os
from dataclasses import dataclass

from backend.env import load_repo_env

load_repo_env()


def _parse_int_env(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(minimum, value)


def _parse_float_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError:
        value = default
    return max(minimum, value)


@dataclass(frozen=True)
class WorkerConfig:
    database_url: str
    source: str
    batch_size: int
    loop_sleep_seconds: float
    retry_seconds: float
    topic_aggregate_interval_seconds: float
    progress_update_seconds: float
    raw_retention_hours: float
    raw_cleanup_interval_seconds: float
    log_level: str

    @classmethod
    def from_env(
        cls,
        *,
        sleep_seconds: float | None = None,
        retry_seconds: float | None = None,
        topic_aggregate_interval_seconds: float | None = None,
    ) -> "WorkerConfig":
        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise ValueError("DATABASE_URL is required")

        source = os.getenv("BLUESKY_WORKER_SOURCE", "bluesky_firehose_worker").strip()
        if not source:
            source = "bluesky_firehose_worker"

        batch_size = _parse_int_env("BLUESKY_DB_BATCH_SIZE", 200, minimum=1)
        configured_sleep_seconds = _parse_float_env(
            "BLUESKY_WORKER_LOOP_SLEEP_SECONDS",
            2.0,
            minimum=0.0,
        )
        configured_retry_seconds = _parse_float_env(
            "BLUESKY_WORKER_RETRY_SECONDS",
            5.0,
            minimum=0.5,
        )
        configured_topic_aggregate_interval_seconds = _parse_float_env(
            "BLUESKY_TOPIC_AGGREGATE_INTERVAL_SECONDS",
            20.0,
            minimum=0.0,
        )
        progress_update_seconds = _parse_float_env(
            "BLUESKY_WORKER_PROGRESS_UPDATE_SECONDS",
            15.0,
            minimum=1.0,
        )
        raw_retention_hours = _parse_float_env(
            "BLUESKY_RAW_RETENTION_HOURS",
            1.0,
            minimum=0.0,
        )
        raw_cleanup_interval_seconds = _parse_float_env(
            "BLUESKY_RAW_CLEANUP_INTERVAL_SECONDS",
            60.0,
            minimum=5.0,
        )
        log_level = os.getenv("BLUESKY_WORKER_LOG_LEVEL", "INFO").strip().upper() or "INFO"

        return cls(
            database_url=database_url,
            source=source,
            batch_size=batch_size,
            loop_sleep_seconds=max(0.0, sleep_seconds if sleep_seconds is not None else configured_sleep_seconds),
            retry_seconds=max(0.5, retry_seconds if retry_seconds is not None else configured_retry_seconds),
            topic_aggregate_interval_seconds=max(
                0.0,
                topic_aggregate_interval_seconds
                if topic_aggregate_interval_seconds is not None
                else configured_topic_aggregate_interval_seconds,
            ),
            progress_update_seconds=progress_update_seconds,
            raw_retention_hours=raw_retention_hours,
            raw_cleanup_interval_seconds=raw_cleanup_interval_seconds,
            log_level=log_level,
        )
