from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.bluesky_config import BLUESKY_RAW_EVENT_STORE_PATH

RAW_EVENT_STORE_PATH = Path(BLUESKY_RAW_EVENT_STORE_PATH)


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS jetstream_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            received_date TEXT NOT NULL,
            event_time_us INTEGER,
            cursor_us INTEGER,
            kind TEXT,
            collection TEXT,
            did TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jetstream_raw_events_received_date
        ON jetstream_raw_events (received_date, id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jetstream_raw_events_event_time
        ON jetstream_raw_events (event_time_us)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jetstream_raw_events_kind_collection
        ON jetstream_raw_events (kind, collection)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jetstream_raw_events_did
        ON jetstream_raw_events (did)
        """
    )


def append_raw_events(
    rows: Iterable[dict[str, Any]],
    *,
    store_path: Path | None = None,
) -> dict[str, Any]:
    payload = list(rows)
    if not payload:
        return {"stored": 0, "path": str((store_path or RAW_EVENT_STORE_PATH).resolve())}

    path = (store_path or RAW_EVENT_STORE_PATH).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path) as connection:
        _ensure_schema(connection)
        connection.executemany(
            """
            INSERT INTO jetstream_raw_events (
                received_at,
                received_date,
                event_time_us,
                cursor_us,
                kind,
                collection,
                did,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(row.get("receivedAt") or ""),
                    str(row.get("receivedDate") or ""),
                    row.get("eventTimeUs"),
                    row.get("cursorUs"),
                    row.get("kind"),
                    row.get("collection"),
                    row.get("did"),
                    json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                )
                for row in payload
            ],
        )

    return {"stored": len(payload), "path": str(path)}


def raw_event_store_path() -> Path:
    return RAW_EVENT_STORE_PATH.resolve()


def _is_reply_post_record(event: dict[str, Any]) -> bool:
    commit = event.get("commit")
    if not isinstance(commit, dict):
        return False
    record = commit.get("record")
    return isinstance(record, dict) and isinstance(record.get("reply"), dict)


def _is_quote_post_record(event: dict[str, Any]) -> bool:
    commit = event.get("commit")
    if not isinstance(commit, dict):
        return False
    record = commit.get("record")
    if not isinstance(record, dict):
        return False
    embed = record.get("embed")
    if not isinstance(embed, dict):
        return False
    nested_record = embed.get("record")
    return isinstance(nested_record, dict) and bool(nested_record.get("uri"))


def summarize_recent_raw_events(
    *,
    window_hours: int = 24,
    bucket_minutes: int = 5,
    rate_window_minutes: int = 5,
) -> dict[str, Any]:
    path = RAW_EVENT_STORE_PATH.resolve()
    if not path.exists():
        return {
            "rawReplay": [],
            "eventsPerMinute": 0.0,
            "postsPerMinute": 0.0,
            "likesPerMinute": 0.0,
            "repostsPerMinute": 0.0,
            "repliesPerMinute": 0.0,
            "quotesPerMinute": 0.0,
            "lastReceivedAt": None,
        }

    now = datetime.now(timezone.utc)
    replay_window_hours = max(1, int(window_hours))
    bucket_size_minutes = max(1, int(bucket_minutes))
    rate_window = max(1, int(rate_window_minutes))
    replay_start = now - timedelta(hours=replay_window_hours)
    rate_start = now - timedelta(minutes=rate_window)
    bucket_count = max(1, int((replay_window_hours * 60) / bucket_size_minutes))
    bucket_size_seconds = bucket_size_minutes * 60
    replay = []
    for index in range(bucket_count):
        bucket_timestamp = replay_start + timedelta(minutes=bucket_size_minutes * (index + 1))
        if bucket_timestamp > now:
            bucket_timestamp = now
        replay.append(
            {
                "timestamp": bucket_timestamp.isoformat(),
                "value": 0,
            }
        )

    events_in_rate_window = 0
    posts_in_rate_window = 0
    likes_in_rate_window = 0
    reposts_in_rate_window = 0
    replies_in_rate_window = 0
    quotes_in_rate_window = 0
    last_received_at: str | None = None

    with sqlite3.connect(path) as connection:
        _ensure_schema(connection)
        rows = connection.execute(
            """
            SELECT received_at, event_time_us, collection, payload_json
            FROM jetstream_raw_events
            WHERE received_at >= ?
            ORDER BY received_at ASC
            """,
            (replay_start.isoformat(),),
        ).fetchall()

    for received_at_raw, event_time_us_raw, collection_raw, payload_json in rows:
        try:
            received_at = datetime.fromisoformat(str(received_at_raw))
        except Exception:
            continue

        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)

        if received_at > now:
            continue

        last_received_at = received_at.isoformat()

        event_at: datetime | None = None
        try:
            event_time_us = int(event_time_us_raw or 0)
        except Exception:
            event_time_us = 0
        if event_time_us > 0:
            try:
                event_at = datetime.fromtimestamp(event_time_us / 1_000_000, tz=timezone.utc)
            except Exception:
                event_at = None

        # Plot replay activity against the event timestamp when available so delayed
        # backlog catch-up does not visually masquerade as "current" activity.
        bucket_time = event_at if event_at and event_at <= now else received_at
        if bucket_time < replay_start:
            continue

        elapsed_seconds = max(0.0, (bucket_time - replay_start).total_seconds())
        bucket_index = min(bucket_count - 1, int(elapsed_seconds // bucket_size_seconds))
        replay[bucket_index]["value"] += 1

        if received_at < rate_start:
            continue

        events_in_rate_window += 1
        collection = str(collection_raw or "")
        if collection == "app.bsky.feed.like":
            likes_in_rate_window += 1
            continue
        if collection == "app.bsky.feed.repost":
            reposts_in_rate_window += 1
            continue
        if collection != "app.bsky.feed.post":
            continue

        posts_in_rate_window += 1
        try:
            event = json.loads(str(payload_json or "{}"))
        except Exception:
            event = {}

        if _is_reply_post_record(event):
            replies_in_rate_window += 1
        elif _is_quote_post_record(event):
            quotes_in_rate_window += 1

    return {
        "rawReplay": replay,
        "eventsPerMinute": round(events_in_rate_window / rate_window, 2),
        "postsPerMinute": round(posts_in_rate_window / rate_window, 2),
        "likesPerMinute": round(likes_in_rate_window / rate_window, 2),
        "repostsPerMinute": round(reposts_in_rate_window / rate_window, 2),
        "repliesPerMinute": round(replies_in_rate_window / rate_window, 2),
        "quotesPerMinute": round(quotes_in_rate_window / rate_window, 2),
        "lastReceivedAt": last_received_at,
    }
