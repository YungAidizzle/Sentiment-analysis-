from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from backend.bluesky_firehose import sync_bluesky_firehose

PLATFORM = "bluesky"
URL_PATTERN = re.compile(r"https?://[^\s<>\"]+")
HASHTAG_PATTERN = re.compile(r"(?:^|\s)#([A-Za-z0-9_]{1,100})")


def _to_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

    candidate = str(value or "").strip()
    if not candidate:
        return None
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _extract_urls(text: str) -> List[str]:
    if not text:
        return []
    seen: set[str] = set()
    urls: List[str] = []
    for match in URL_PATTERN.findall(text):
        url = match.strip().rstrip(".,)")
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_hashtags(text: str) -> List[str]:
    if not text:
        return []
    seen: set[str] = set()
    tags: List[str] = []
    for match in HASHTAG_PATTERN.findall(text):
        hashtag = str(match or "").strip().lower()
        if not hashtag:
            continue
        if hashtag in seen:
            continue
        seen.add(hashtag)
        tags.append(hashtag)
    return tags


def _parse_language(post: Dict[str, Any]) -> str | None:
    langs = post.get("langs")
    if isinstance(langs, list):
        for value in langs:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
    language = str(post.get("language") or "").strip()
    return language or None


def _parse_followers_count(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def run_firehose_window(
    *,
    cursor_us: int | None,
    progress_callback: Any | None = None,
) -> Dict[str, Any]:
    existing_state: Dict[str, Any] = {}
    if isinstance(cursor_us, int) and cursor_us > 0:
        existing_state["cursor"] = cursor_us
    return sync_bluesky_firehose(
        existing_posts=[],
        existing_snapshots=[],
        existing_profiles=[],
        existing_interactions=[],
        existing_state=existing_state,
        progress_callback=progress_callback,
    )


def normalize_posts_for_raw_table(
    posts: Iterable[Dict[str, Any]],
    *,
    ingested_at: datetime,
) -> List[Dict[str, Any]]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    for post in posts:
        post_id = str(post.get("id") or post.get("uri") or "").strip()
        if not post_id:
            continue

        text_content = str(post.get("summary") or post.get("title") or "").strip()
        created_at = _to_utc_datetime(post.get("createdUtc"))
        if created_at is None:
            created_at = _to_utc_datetime(post.get("indexedAt"))
        if created_at is None:
            created_at = ingested_at

        urls = _extract_urls(text_content)
        hashtags = _extract_hashtags(text_content)

        metrics_json = {
            "score": int(post.get("score", 0) or 0),
            "likeCount": int(post.get("likeCount", 0) or 0),
            "replyCount": int(post.get("replyCount", 0) or 0),
            "repostCount": int(post.get("repostCount", 0) or 0),
            "quoteCount": int(post.get("quoteCount", 0) or 0),
            "bookmarkCount": int(post.get("bookmarkCount", 0) or 0),
            "interactionCounts": dict(post.get("interactionCounts") or {}),
            "priorityScore": float(post.get("priorityScore", 0.0) or 0.0),
        }

        rows_by_id[post_id] = {
            "platform": PLATFORM,
            "post_id": post_id,
            "author_id": str(post.get("authorDid") or post.get("authorHandle") or "").strip() or None,
            "author_handle": str(post.get("authorHandle") or "").strip() or None,
            "root_post_id": str(post.get("rootUri") or "").strip() or None,
            "reply_parent_id": str(post.get("parentUri") or "").strip() or None,
            "created_at": created_at,
            "ingested_at": ingested_at,
            "text_content": text_content or None,
            "language": _parse_language(post),
            "urls": urls,
            "hashtags": hashtags,
            "metrics_json": metrics_json,
            "raw_json": dict(post),
        }

    return list(rows_by_id.values())


def _merge_author_rows(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    first_seen_existing = _to_utc_datetime(existing.get("first_seen_at"))
    first_seen_incoming = _to_utc_datetime(incoming.get("first_seen_at"))
    last_seen_existing = _to_utc_datetime(existing.get("last_seen_at"))
    last_seen_incoming = _to_utc_datetime(incoming.get("last_seen_at"))

    first_seen = first_seen_incoming or first_seen_existing
    if first_seen_existing and first_seen_incoming:
        first_seen = min(first_seen_existing, first_seen_incoming)

    last_seen = last_seen_incoming or last_seen_existing
    if last_seen_existing and last_seen_incoming:
        last_seen = max(last_seen_existing, last_seen_incoming)

    metadata = dict(existing.get("metadata_json") or {})
    metadata.update(dict(incoming.get("metadata_json") or {}))

    return {
        **existing,
        "author_handle": incoming.get("author_handle") or existing.get("author_handle"),
        "display_name": incoming.get("display_name") or existing.get("display_name"),
        "followers_count": (
            incoming.get("followers_count")
            if incoming.get("followers_count") is not None
            else existing.get("followers_count")
        ),
        "metadata_json": metadata,
        "first_seen_at": first_seen,
        "last_seen_at": last_seen,
    }


def normalize_authors_for_authors_table(
    *,
    profiles: Iterable[Dict[str, Any]],
    posts: Iterable[Dict[str, Any]],
    observed_at: datetime,
) -> List[Dict[str, Any]]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}

    for profile in profiles:
        author_id = str(profile.get("did") or profile.get("handle") or "").strip()
        if not author_id:
            continue
        first_seen = _to_utc_datetime(profile.get("firstObservedAt")) or _to_utc_datetime(
            profile.get("fetchedAt")
        )
        last_seen = _to_utc_datetime(profile.get("lastObservedAt")) or _to_utc_datetime(
            profile.get("fetchedAt")
        )
        candidate = {
            "platform": PLATFORM,
            "author_id": author_id,
            "author_handle": str(profile.get("handle") or "").strip() or None,
            "display_name": str(profile.get("displayName") or "").strip() or None,
            "followers_count": _parse_followers_count(profile.get("followersCount")),
            "metadata_json": dict(profile),
            "first_seen_at": first_seen or observed_at,
            "last_seen_at": last_seen or observed_at,
        }
        existing = rows_by_id.get(author_id)
        rows_by_id[author_id] = _merge_author_rows(existing, candidate) if existing else candidate

    for post in posts:
        author_id = str(post.get("authorDid") or post.get("authorHandle") or "").strip()
        if not author_id:
            continue
        first_seen = _to_utc_datetime(post.get("firstSeenAt")) or _to_utc_datetime(
            post.get("firstObservedAt")
        )
        last_seen = _to_utc_datetime(post.get("lastObservedAt")) or _to_utc_datetime(
            post.get("fetchedAt")
        )
        candidate = {
            "platform": PLATFORM,
            "author_id": author_id,
            "author_handle": str(post.get("authorHandle") or "").strip() or None,
            "display_name": str(post.get("authorDisplayName") or "").strip() or None,
            "followers_count": _parse_followers_count(post.get("authorFollowersCount")),
            "metadata_json": {
                "did": post.get("authorDid"),
                "handle": post.get("authorHandle"),
                "displayName": post.get("authorDisplayName"),
                "avatar": post.get("authorAvatar"),
                "description": post.get("authorDescription"),
                "lastObservedPostUri": post.get("id"),
            },
            "first_seen_at": first_seen or observed_at,
            "last_seen_at": last_seen or observed_at,
        }
        existing = rows_by_id.get(author_id)
        rows_by_id[author_id] = _merge_author_rows(existing, candidate) if existing else candidate

    return list(rows_by_id.values())
