from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Tuple

from backend.reddit_dev_only_config import ROLLING_WINDOW_HOURS


def _created_utc(row: dict[str, Any]) -> float:
    value = row.get("createdUtc")
    if value is None:
        value = row.get("created_utc", 0)
    return float(value or 0)


def _post_id(row: dict[str, Any]) -> str:
    value = row.get("postId")
    if value is None:
        value = row.get("post_id", "")
    return str(value or "")


def rolling_window_bounds(
    *,
    reference_time: datetime | None = None,
    hours: int = ROLLING_WINDOW_HOURS,
) -> Tuple[datetime, datetime]:
    end_time = reference_time or datetime.now(timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    start_time = end_time - timedelta(hours=hours)
    return start_time, end_time


def rolling_window_cutoff_utc(
    *,
    reference_time: datetime | None = None,
    hours: int = ROLLING_WINDOW_HOURS,
) -> int:
    start_time, _ = rolling_window_bounds(reference_time=reference_time, hours=hours)
    return int(start_time.timestamp())


def within_rolling_window(
    created_utc: int | float,
    *,
    reference_time: datetime | None = None,
    hours: int = ROLLING_WINDOW_HOURS,
) -> bool:
    start_time, _ = rolling_window_bounds(reference_time=reference_time, hours=hours)
    return datetime.fromtimestamp(float(created_utc), tz=timezone.utc) >= start_time


def prune_records_to_window(
    posts: Iterable[dict[str, Any]],
    comments: Iterable[dict[str, Any]],
    *,
    reference_time: datetime | None = None,
    hours: int = ROLLING_WINDOW_HOURS,
) -> tuple[List[dict[str, Any]], List[dict[str, Any]]]:
    start_time, _ = rolling_window_bounds(reference_time=reference_time, hours=hours)
    kept_posts = [
        row
        for row in posts
        if datetime.fromtimestamp(_created_utc(row), tz=timezone.utc) >= start_time
    ]
    kept_post_ids = {str(row.get("id", "")) for row in kept_posts if str(row.get("id", ""))}
    kept_comments = [
        row
        for row in comments
        if datetime.fromtimestamp(_created_utc(row), tz=timezone.utc) >= start_time
        and _post_id(row) in kept_post_ids
    ]

    kept_posts.sort(key=lambda row: (_created_utc(row), str(row.get("id", ""))), reverse=True)
    kept_comments.sort(
        key=lambda row: (_created_utc(row), str(row.get("id", ""))),
        reverse=True,
    )
    return kept_posts, kept_comments
