from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from time import monotonic, perf_counter, sleep
from typing import Any, Callable, Dict, Iterable, Iterator, List
from urllib.parse import urlencode

from backend.bluesky_config import (
    BLUESKY_ENABLED,
    BLUESKY_FIREHOSE_BOOTSTRAP_MODE,
    BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES,
    BLUESKY_FIREHOSE_COLLECTIONS,
    BLUESKY_FIREHOSE_CURSOR_REWIND_SECONDS,
    BLUESKY_FIREHOSE_ENABLED,
    BLUESKY_FIREHOSE_ENDPOINT,
    BLUESKY_FIREHOSE_IDLE_TIMEOUT_SECONDS,
    BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS,
    BLUESKY_FIREHOSE_RECONNECT_MAX_ATTEMPTS,
    BLUESKY_FIREHOSE_RECONNECT_MAX_DELAY_SECONDS,
    BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN,
    BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN,
    BLUESKY_RAW_PERSIST_BATCH_SIZE,
    BLUESKY_RAW_PERSIST_ENABLED,
    BLUESKY_FIREHOSE_RETENTION_HOURS,
    BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES,
    BLUESKY_MAX_SNAPSHOTS_PER_POST,
)
from backend.bluesky_raw_store import append_raw_events, raw_event_store_path
from backend.bluesky_refresh import (
    _build_post_url,
    _clean_text,
    _extract_query_phrases,
    _extract_quoted_uri,
    _extract_reply_parent_uri,
    _extract_reply_root_uri,
    _extract_text,
    _interaction_counts,
    _merge_unique_strings,
    _parse_iso_datetime,
    _safe_slug,
    _to_utc_timestamp,
)

FirehoseEventIteratorFactory = Callable[..., Iterable[Dict[str, Any]]]
BlueskyFirehoseProgressCallback = Callable[[Dict[str, Any]], None]

JETSTREAM_COLLECTION_POST = "app.bsky.feed.post"
JETSTREAM_COLLECTION_LIKE = "app.bsky.feed.like"
JETSTREAM_COLLECTION_REPOST = "app.bsky.feed.repost"
JETSTREAM_COLLECTION_PROFILE = "app.bsky.actor.profile"
JETSTREAM_COLLECTION_FOLLOW = "app.bsky.graph.follow"

JETSTREAM_RELEVANT_COLLECTIONS = {
    JETSTREAM_COLLECTION_POST,
    JETSTREAM_COLLECTION_LIKE,
    JETSTREAM_COLLECTION_REPOST,
    JETSTREAM_COLLECTION_PROFILE,
    JETSTREAM_COLLECTION_FOLLOW,
}

JETSTREAM_AMPLIFICATION_WEIGHTS = {
    "like": 1.0,
    "repost": 3.0,
    "reply": 4.0,
    "quote": 4.5,
}


def _utc_now(reference_time: datetime | None = None) -> datetime:
    return reference_time or datetime.now(timezone.utc)


def _iso_from_time_us(value: int | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()


def _parse_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _truncate_text(value: str, max_length: int) -> str:
    if not value or len(value) <= max_length:
        return value
    return value[: max_length - 1].rstrip() + "..."


def _event_time_us(event: Dict[str, Any]) -> int:
    return _parse_int(event.get("time_us"))


def _event_created_iso(event: Dict[str, Any], record: Dict[str, Any] | None = None) -> str:
    candidate = str((record or {}).get("createdAt") or "")
    if candidate:
        return candidate
    fallback = _iso_from_time_us(_event_time_us(event))
    if fallback:
        return fallback
    return datetime.now(timezone.utc).isoformat()


def _event_created_utc(event: Dict[str, Any], record: Dict[str, Any] | None = None) -> int:
    created_utc = _to_utc_timestamp(_event_created_iso(event, record))
    if created_utc is not None:
        return created_utc
    return int(_utc_now().timestamp())


def _commit_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    commit = event.get("commit")
    return commit if isinstance(commit, dict) else {}


def _commit_record(event: Dict[str, Any]) -> Dict[str, Any]:
    record = _commit_payload(event).get("record")
    return record if isinstance(record, dict) else {}


def _commit_collection(event: Dict[str, Any]) -> str:
    return str(_commit_payload(event).get("collection") or "")


def _commit_operation(event: Dict[str, Any]) -> str:
    return str(_commit_payload(event).get("operation") or "")


def _commit_rkey(event: Dict[str, Any]) -> str:
    return str(_commit_payload(event).get("rkey") or "")


def _commit_cid(event: Dict[str, Any]) -> str | None:
    cid = str(_commit_payload(event).get("cid") or "")
    return cid or None


def _event_did(event: Dict[str, Any]) -> str:
    return str(event.get("did") or "")


def _profile_key(did: str | None, handle: str | None) -> str:
    return str(did or handle or "")


def _build_at_uri(did: str, collection: str, rkey: str) -> str:
    return f"at://{did}/{collection}/{rkey}"


def _post_type(record: Dict[str, Any]) -> str:
    if _extract_reply_parent_uri(record):
        return "reply"
    if _extract_quoted_uri(record):
        return "quote"
    return "root"


def _load_websocket_client():
    try:
        import websocket  # type: ignore
    except Exception as error:  # pragma: no cover
        raise RuntimeError(
            "websocket-client is required for Bluesky firehose sync. Install requirements.txt."
        ) from error
    return websocket


def _build_subscribe_url(endpoint: str, *, cursor: int | None) -> str:
    params: list[tuple[str, str | int]] = []
    if cursor:
        params.append(("cursor", cursor))
    # Empty wantedCollections means "all collections" in Jetstream.
    # This is still the simplified JSON Jetstream stream, not the raw subscribeRepos firehose.
    for collection in BLUESKY_FIREHOSE_COLLECTIONS:
        params.append(("wantedCollections", collection))
    query = urlencode(params, doseq=True)
    separator = "&" if "?" in endpoint else "?"
    return f"{endpoint}{separator}{query}" if query else endpoint


def _jetstream_mode() -> str:
    return "full" if not BLUESKY_FIREHOSE_COLLECTIONS else "filtered"


def _jetstream_mode_description() -> str:
    if not BLUESKY_FIREHOSE_COLLECTIONS:
        return "all collections via Jetstream JSON stream"
    return f"{len(BLUESKY_FIREHOSE_COLLECTIONS)} collection filters"


def _raw_event_row(event: Dict[str, Any], *, received_at: str) -> Dict[str, Any]:
    event_time_us = _event_time_us(event) or None
    return {
        "receivedAt": received_at,
        "receivedDate": received_at[:10],
        "eventTimeUs": event_time_us,
        "cursorUs": event_time_us,
        "kind": str(event.get("kind") or "") or None,
        "collection": _commit_collection(event) or None,
        "did": _event_did(event) or None,
        "payload": event,
    }


def _is_recoverable_firehose_error(error: Exception, websocket_module: Any | None = None) -> bool:
    if isinstance(error, (ConnectionError, TimeoutError, OSError)):
        return True

    websocket_errors: list[type[BaseException]] = []
    for name in (
        "WebSocketConnectionClosedException",
        "WebSocketTimeoutException",
        "WebSocketBadStatusException",
    ):
        candidate = getattr(websocket_module, name, None) if websocket_module else None
        if isinstance(candidate, type):
            websocket_errors.append(candidate)

    if websocket_errors and isinstance(error, tuple(websocket_errors)):
        return True

    message = str(error).strip().lower()
    return any(
        token in message
        for token in (
            "connection to remote host was lost",
            "connection reset",
            "connection aborted",
            "connection closed",
            "broken pipe",
            "timed out",
            "1006",
            "eof",
        )
    )


def _firehose_backoff_seconds(attempt: int) -> float:
    return min(
        BLUESKY_FIREHOSE_RECONNECT_MAX_DELAY_SECONDS,
        BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1)),
    )


def _connection_status(
    *,
    last_iterator_error: Exception | None,
    events_processed: int,
    raw_persist_error: Exception | None,
) -> str:
    if raw_persist_error is not None:
        return "degraded"
    if last_iterator_error is None:
        return "connected" if events_processed > 0 else "idle"
    if events_processed > 0:
        return "reconnecting"
    return "disconnected"


def _iter_jetstream_events(
    *,
    cursor: int | None = None,
    endpoint: str | None = None,
    max_seconds: int | None = None,
    max_events: int | None = None,
) -> Iterator[Dict[str, Any]]:
    websocket = _load_websocket_client()
    endpoint = endpoint or BLUESKY_FIREHOSE_ENDPOINT
    url = _build_subscribe_url(endpoint, cursor=cursor)
    deadline = monotonic() + max(1, max_seconds or BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN)
    idle_timeout = max(1, BLUESKY_FIREHOSE_IDLE_TIMEOUT_SECONDS)
    processed = 0
    ws = None
    print(
        f"[bluesky-firehose] connect cursor={cursor or 0} endpoint={endpoint} max_seconds={int(max(1, max_seconds or BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN))} max_events={int(max_events or BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN)}"
    )
    try:
        ws = websocket.create_connection(url, timeout=idle_timeout)
        ws.settimeout(idle_timeout)
        print("[bluesky-firehose] connected")
        while monotonic() < deadline and processed < (max_events or BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN):
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                print("[bluesky-firehose] idle timeout reached; ending current stream window")
                break
            if not raw:
                raise ConnectionError("Jetstream connection closed without payload")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            event = json.loads(raw)
            if isinstance(event, dict):
                processed += 1
                yield event
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def _copy_profile(existing: Dict[str, Any] | None, *, fetched_at: str) -> Dict[str, Any]:
    current = dict(existing or {})
    if fetched_at:
        current["fetchedAt"] = fetched_at
        current["lastObservedAt"] = fetched_at
        current["lastSeenAt"] = fetched_at
    return current


def _upsert_profile(
    profile_map: Dict[str, Dict[str, Any]],
    *,
    did: str | None,
    handle: str | None = None,
    display_name: str | None = None,
    description: str | None = None,
    avatar: str | None = None,
    banner: str | None = None,
    created_at: str | None = None,
    indexed_at: str | None = None,
    labels: Iterable[str] | None = None,
    fetched_at: str,
    source_post_uri: str | None = None,
    last_amplified_uri: str | None = None,
) -> Dict[str, Any]:
    key = _profile_key(did, handle)
    if not key:
        key = _safe_slug(f"{did}:{handle}:{fetched_at}")
    current = _copy_profile(profile_map.get(key), fetched_at=fetched_at)
    profile = {
        **current,
        "did": did or current.get("did"),
        "handle": handle or current.get("handle"),
        "displayName": display_name or current.get("displayName"),
        "description": description if description is not None else current.get("description"),
        "avatar": avatar if avatar is not None else current.get("avatar"),
        "banner": banner if banner is not None else current.get("banner"),
        "createdAt": created_at or current.get("createdAt"),
        "indexedAt": indexed_at or current.get("indexedAt"),
        "labels": _merge_unique_strings(current.get("labels"), labels),
        "fetchedAt": fetched_at,
        "firstObservedAt": current.get("firstObservedAt") or fetched_at,
        "lastObservedAt": fetched_at,
        "lastSeenAt": fetched_at,
        "discoveryLanes": _merge_unique_strings(current.get("discoveryLanes"), ["firehose"]),
        "sourcePostUris": _merge_unique_strings(
            current.get("sourcePostUris"),
            [source_post_uri] if source_post_uri else None,
        ),
        "lastObservedPostUri": source_post_uri or current.get("lastObservedPostUri"),
        "lastAmplifiedUri": last_amplified_uri or current.get("lastAmplifiedUri"),
    }
    profile_map[key] = profile
    return profile


def _find_profile_for_did(
    profile_map: Dict[str, Dict[str, Any]],
    did: str,
) -> Dict[str, Any]:
    if not did:
        return {}
    for row in profile_map.values():
        if str(row.get("did") or "") == did:
            return row
    return {}


def _normalize_post_from_firehose_event(
    event: Dict[str, Any],
    *,
    profile_map: Dict[str, Dict[str, Any]],
    previous: Dict[str, Any] | None,
    fetched_at: str,
) -> Dict[str, Any]:
    record = _commit_record(event)
    did = _event_did(event)
    uri = _build_at_uri(did, _commit_collection(event), _commit_rkey(event))
    profile = _find_profile_for_did(profile_map, did)
    text = _extract_text(record)
    created_utc = _event_created_utc(event, record)
    root_uri = _extract_reply_root_uri(record)
    parent_uri = _extract_reply_parent_uri(record)
    quoted_uri = _extract_quoted_uri(record)
    previous = previous or {}
    summary = text or "Bluesky post"
    if len(summary) > 500:
        summary = summary[:499].rstrip() + "..."
    title = _truncate_text(summary.split("\n", 1)[0] or "Bluesky post", 220)
    handle = str(profile.get("handle") or previous.get("authorHandle") or did)
    display_name = str(profile.get("displayName") or previous.get("authorDisplayName") or handle)

    return {
        **previous,
        "id": uri,
        "uri": uri,
        "cid": _commit_cid(event) or previous.get("cid"),
        "source": "bluesky",
        "sourceType": "bluesky",
        "postType": previous.get("postType") or _post_type(record),
        "rootUri": previous.get("rootUri") or root_uri,
        "parentUri": previous.get("parentUri") or parent_uri,
        "quotedUri": previous.get("quotedUri") or quoted_uri,
        "authorDid": did,
        "authorHandle": handle,
        "authorDisplayName": display_name,
        "authorAvatar": profile.get("avatar") or previous.get("authorAvatar"),
        "authorDescription": profile.get("description") or previous.get("authorDescription"),
        "title": title,
        "summary": summary,
        "url": _build_post_url(handle or did, uri),
        "createdUtc": created_utc,
        "indexedAt": _iso_from_time_us(_event_time_us(event)),
        "score": max(1, int(previous.get("score", 1) or 1)),
        "likeCount": int(previous.get("likeCount", 0) or 0),
        "replyCount": int(previous.get("replyCount", 0) or 0),
        "repostCount": int(previous.get("repostCount", 0) or 0),
        "quoteCount": int(previous.get("quoteCount", 0) or 0),
        "bookmarkCount": int(previous.get("bookmarkCount", 0) or 0),
        "interactionCounts": previous.get("interactionCounts") or _interaction_counts(posts=1),
        "discoveryLanes": _merge_unique_strings(previous.get("discoveryLanes"), ["firehose"]),
        "discoveredQueries": _merge_unique_strings(previous.get("discoveredQueries")),
        "topicHints": _merge_unique_strings(
            previous.get("topicHints"),
            _extract_query_phrases(summary),
            [handle] if handle else None,
        )[:10],
        "labels": _merge_unique_strings(previous.get("labels")),
        "langs": _merge_unique_strings(
            previous.get("langs"),
            record.get("langs") if isinstance(record.get("langs"), list) else None,
        ),
        "lastObservedAt": fetched_at,
        "fetchedAt": fetched_at,
        "firstSeenAt": previous.get("firstSeenAt") or fetched_at,
        "lastSnapshotAt": previous.get("lastSnapshotAt"),
        "snapshotCount": int(previous.get("snapshotCount", 0) or 0),
        "initialLikeCount": int(previous.get("initialLikeCount", 0) or 0),
        "initialRepostCount": int(previous.get("initialRepostCount", 0) or 0),
        "initialReplyCount": int(previous.get("initialReplyCount", 0) or 0),
        "initialQuoteCount": int(previous.get("initialQuoteCount", 0) or 0),
        "deltaLikeCount": int(previous.get("deltaLikeCount", 0) or 0),
        "deltaRepostCount": int(previous.get("deltaRepostCount", 0) or 0),
        "deltaCommentCount": int(previous.get("deltaCommentCount", 0) or 0),
        "deltaQuoteCount": int(previous.get("deltaQuoteCount", 0) or 0),
        "priorityScore": float(previous.get("priorityScore", 1.0) or 1.0),
    }


def _normalize_post_interaction(
    post: Dict[str, Any],
    *,
    fetched_at: str,
) -> Dict[str, Any] | None:
    interaction_type = str(post.get("postType") or "")
    if interaction_type not in {"reply", "quote"}:
        return None
    target_uri = str(
        post.get("rootUri")
        if interaction_type == "reply"
        else post.get("quotedUri") or post.get("rootUri") or post.get("id")
    )
    if not target_uri:
        return None
    return {
        "id": f"{interaction_type}:{post['id']}",
        "source": "bluesky",
        "sourceType": "bluesky_interaction",
        "interactionType": interaction_type,
        "postUri": target_uri,
        "eventUri": post["id"],
        "rootUri": post.get("rootUri") or target_uri,
        "parentUri": post.get("parentUri"),
        "actorDid": post.get("authorDid"),
        "actorHandle": post.get("authorHandle"),
        "actorDisplayName": post.get("authorDisplayName"),
        "actorFollowersCount": None,
        "text": post.get("summary"),
        "createdUtc": int(post.get("createdUtc", 0) or 0),
        "likeCount": 0,
        "repostCount": 0,
        "replyCount": 0,
        "quoteCount": 0,
        "url": post.get("url"),
        "sourceLane": "firehose",
        "fetchedAt": fetched_at,
        "firstSeenAt": post.get("firstSeenAt") or fetched_at,
        "lastFetchedAt": fetched_at,
    }


def _normalize_subject_interaction(
    event: Dict[str, Any],
    *,
    interaction_type: str,
    profile_map: Dict[str, Dict[str, Any]],
    fetched_at: str,
) -> Dict[str, Any] | None:
    record = _commit_record(event)
    subject = record.get("subject")
    if not isinstance(subject, dict):
        return None
    post_uri = str(subject.get("uri") or "")
    if not post_uri:
        return None
    did = _event_did(event)
    rkey = _commit_rkey(event)
    profile = _find_profile_for_did(profile_map, did)
    created_utc = _event_created_utc(event, record)
    return {
        "id": f"{interaction_type}:{did}:{rkey}",
        "source": "bluesky",
        "sourceType": "bluesky_interaction",
        "interactionType": interaction_type,
        "postUri": post_uri,
        "eventUri": _build_at_uri(did, _commit_collection(event), rkey),
        "rootUri": post_uri,
        "parentUri": None,
        "actorDid": did,
        "actorHandle": profile.get("handle"),
        "actorDisplayName": profile.get("displayName") or profile.get("handle") or did,
        "actorFollowersCount": profile.get("followersCount"),
        "text": None,
        "createdUtc": created_utc,
        "likeCount": 0,
        "repostCount": 0,
        "replyCount": 0,
        "quoteCount": 0,
        "url": None,
        "sourceLane": "firehose",
        "fetchedAt": fetched_at,
        "firstSeenAt": fetched_at,
        "lastFetchedAt": fetched_at,
    }


def _merge_interactions(
    existing_rows: Iterable[Dict[str, Any]],
    incoming_rows: Iterable[Dict[str, Any]],
    *,
    cutoff_utc: int,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in list(existing_rows) + list(incoming_rows):
        interaction_id = str(row.get("id") or "")
        if not interaction_id:
            continue
        created_utc = int(row.get("createdUtc", 0) or 0)
        if created_utc < cutoff_utc:
            continue
        current = merged.get(interaction_id)
        if current:
            merged[interaction_id] = {
                **current,
                **row,
                "firstSeenAt": current.get("firstSeenAt") or row.get("firstSeenAt") or row.get("fetchedAt"),
                "lastFetchedAt": row.get("fetchedAt") or current.get("lastFetchedAt"),
            }
            continue
        merged[interaction_id] = {
            **row,
            "firstSeenAt": row.get("firstSeenAt") or row.get("fetchedAt"),
            "lastFetchedAt": row.get("fetchedAt"),
        }
    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("id") or ""),
        ),
        reverse=True,
    )


def _observed_post_counts(interactions: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = {}
    for row in interactions:
        post_uri = str(row.get("postUri") or "")
        if not post_uri:
            continue
        current = counts.setdefault(
            post_uri,
            {"likes": 0, "reposts": 0, "replies": 0, "quotes": 0},
        )
        interaction_type = str(row.get("interactionType") or "")
        if interaction_type == "like":
            current["likes"] += 1
        elif interaction_type == "repost":
            current["reposts"] += 1
        elif interaction_type == "reply":
            current["replies"] += 1
        elif interaction_type == "quote":
            current["quotes"] += 1
    return counts


def _hydrate_posts_from_profiles(
    posts: List[Dict[str, Any]],
    *,
    profile_map: Dict[str, Dict[str, Any]],
    counts_by_post: Dict[str, Dict[str, int]],
) -> List[Dict[str, Any]]:
    profile_by_did: Dict[str, Dict[str, Any]] = {}
    for profile in profile_map.values():
        did = str(profile.get("did") or "")
        if did and did not in profile_by_did:
            profile_by_did[did] = profile

    hydrated: List[Dict[str, Any]] = []
    for row in posts:
        did = str(row.get("authorDid") or "")
        profile = profile_by_did.get(did, {})
        counts = counts_by_post.get(str(row.get("id") or ""), {})
        like_count = int(counts.get("likes", 0))
        repost_count = int(counts.get("reposts", 0))
        reply_count = int(counts.get("replies", 0))
        quote_count = int(counts.get("quotes", 0))
        interaction_counts = _interaction_counts(
            posts=1,
            reposts=repost_count + quote_count,
            comments=reply_count,
            likes=like_count,
        )
        priority_score = round(
            5
            + min(16, (repost_count + quote_count) * 1.8)
            + min(14, reply_count * 1.4)
            + min(10, like_count * 0.18),
            1,
        )
        hydrated.append(
            {
                **row,
                "authorHandle": profile.get("handle") or row.get("authorHandle") or did,
                "authorDisplayName": profile.get("displayName") or row.get("authorDisplayName") or row.get("authorHandle") or did,
                "authorAvatar": profile.get("avatar") or row.get("authorAvatar"),
                "authorDescription": profile.get("description") or row.get("authorDescription"),
                "authorFollowersCount": profile.get("followersCount") or row.get("authorFollowersCount"),
                "url": _build_post_url(
                    str(profile.get("handle") or row.get("authorHandle") or did),
                    str(row.get("id") or ""),
                ),
                "likeCount": like_count,
                "repostCount": repost_count,
                "replyCount": reply_count,
                "quoteCount": quote_count,
                "interactionCounts": interaction_counts,
                "score": max(
                    1,
                    like_count + repost_count * 3 + quote_count * 4 + reply_count * 4,
                ),
                "priorityScore": priority_score,
                "initialLikeCount": int(row.get("initialLikeCount", like_count) or like_count),
                "initialRepostCount": int(row.get("initialRepostCount", repost_count) or repost_count),
                "initialReplyCount": int(row.get("initialReplyCount", reply_count) or reply_count),
                "initialQuoteCount": int(row.get("initialQuoteCount", quote_count) or quote_count),
            }
        )
    return hydrated


def _refresh_profile_activity(
    profiles: List[Dict[str, Any]],
    *,
    posts: List[Dict[str, Any]],
    interactions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    post_count_by_did: Dict[str, int] = {}
    amplification_by_did: Dict[str, float] = {}
    last_activity_by_did: Dict[str, int] = {}
    last_post_uri_by_did: Dict[str, str] = {}

    for post in posts:
        did = str(post.get("authorDid") or "")
        if not did:
            continue
        post_count_by_did[did] = post_count_by_did.get(did, 0) + 1
        last_activity_by_did[did] = max(last_activity_by_did.get(did, 0), int(post.get("createdUtc", 0) or 0))
        last_post_uri_by_did[did] = str(post.get("id") or "")

    for interaction in interactions:
        did = str(interaction.get("actorDid") or "")
        if not did:
            continue
        weight = JETSTREAM_AMPLIFICATION_WEIGHTS.get(str(interaction.get("interactionType") or ""), 0.5)
        amplification_by_did[did] = amplification_by_did.get(did, 0.0) + weight
        last_activity_by_did[did] = max(last_activity_by_did.get(did, 0), int(interaction.get("createdUtc", 0) or 0))
        target_uri = str(interaction.get("postUri") or "")
        if target_uri:
            last_post_uri_by_did[did] = target_uri

    refreshed: List[Dict[str, Any]] = []
    for profile in profiles:
        did = str(profile.get("did") or "")
        last_activity_utc = last_activity_by_did.get(did, 0)
        refreshed.append(
            {
                **profile,
                "observedPostCount": post_count_by_did.get(did, int(profile.get("observedPostCount", 0) or 0)),
                "amplificationScore": round(amplification_by_did.get(did, float(profile.get("amplificationScore", 0.0) or 0.0)), 1),
                "lastObservedPostUri": last_post_uri_by_did.get(did, profile.get("lastObservedPostUri")),
                "lastActivityAt": datetime.fromtimestamp(last_activity_utc, tz=timezone.utc).isoformat()
                if last_activity_utc > 0
                else profile.get("lastActivityAt"),
            }
        )
    return refreshed


def _build_snapshot_for_post(
    post: Dict[str, Any],
    *,
    fetched_at: str,
    previous: Dict[str, Any] | None,
) -> Dict[str, Any]:
    previous = previous or {}
    previous_fetched = _parse_iso_datetime(str(previous.get("fetchedAt") or ""))
    current_fetched = _parse_iso_datetime(fetched_at)
    delta_window_minutes = 0.0
    if previous_fetched and current_fetched and current_fetched > previous_fetched:
        delta_window_minutes = round((current_fetched - previous_fetched).total_seconds() / 60.0, 1)
    snapshot_key = f"{post['id']}|{fetched_at}"
    return {
        "id": f"bluesky-snapshot:{hashlib.sha256(snapshot_key.encode('utf-8')).hexdigest()[:16]}",
        "postUri": post["id"],
        "fetchedAt": fetched_at,
        "createdUtc": int(post.get("createdUtc", 0) or 0),
        "likeCount": int(post.get("likeCount", 0) or 0),
        "replyCount": int(post.get("replyCount", 0) or 0),
        "repostCount": int(post.get("repostCount", 0) or 0),
        "quoteCount": int(post.get("quoteCount", 0) or 0),
        "deltaLikeCount": max(0, int(post.get("likeCount", 0) or 0) - int(previous.get("likeCount", 0) or 0)),
        "deltaCommentCount": max(0, int(post.get("replyCount", 0) or 0) - int(previous.get("replyCount", 0) or 0)),
        "deltaRepostCount": max(0, int(post.get("repostCount", 0) or 0) - int(previous.get("repostCount", 0) or 0)),
        "deltaQuoteCount": max(0, int(post.get("quoteCount", 0) or 0) - int(previous.get("quoteCount", 0) or 0)),
        "deltaWindowMinutes": delta_window_minutes,
    }


def _merge_snapshots(
    existing_rows: Iterable[Dict[str, Any]],
    incoming_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(existing_rows) + list(incoming_rows):
        post_uri = str(row.get("postUri") or "")
        if not post_uri:
            continue
        grouped.setdefault(post_uri, []).append(dict(row))
    merged: List[Dict[str, Any]] = []
    for rows in grouped.values():
        rows.sort(key=lambda row: str(row.get("fetchedAt") or ""))
        merged.extend(rows[-BLUESKY_MAX_SNAPSHOTS_PER_POST :])
    return sorted(
        merged,
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("fetchedAt") or ""),
        ),
        reverse=True,
    )


def _prune_posts(
    posts: Iterable[Dict[str, Any]],
    *,
    cutoff_utc: int,
    active_post_ids: set[str],
) -> List[Dict[str, Any]]:
    retained: List[Dict[str, Any]] = []
    for row in posts:
        post_id = str(row.get("id") or "")
        created_utc = int(row.get("createdUtc", 0) or 0)
        if not post_id:
            continue
        if created_utc >= cutoff_utc or post_id in active_post_ids:
            retained.append(dict(row))
    return sorted(
        retained,
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("id") or ""),
        ),
        reverse=True,
    )


def _prune_profiles(
    profiles: Iterable[Dict[str, Any]],
    *,
    active_dids: set[str],
    cutoff: datetime,
) -> List[Dict[str, Any]]:
    retained: List[Dict[str, Any]] = []
    for row in profiles:
        did = str(row.get("did") or "")
        last_observed_at = _parse_iso_datetime(str(row.get("lastObservedAt") or row.get("fetchedAt") or ""))
        if did in active_dids or (last_observed_at and last_observed_at >= cutoff):
            retained.append(dict(row))
    return sorted(
        retained,
        key=lambda row: (
            str(row.get("lastActivityAt") or row.get("lastObservedAt") or ""),
            str(row.get("did") or row.get("handle") or ""),
        ),
        reverse=True,
    )


def sync_bluesky_firehose(
    *,
    existing_posts: Iterable[Dict[str, Any]] | None = None,
    existing_snapshots: Iterable[Dict[str, Any]] | None = None,
    existing_profiles: Iterable[Dict[str, Any]] | None = None,
    existing_interactions: Iterable[Dict[str, Any]] | None = None,
    existing_state: Dict[str, Any] | None = None,
    reference_time: datetime | None = None,
    firehose_enabled: bool | None = None,
    event_iter_factory: FirehoseEventIteratorFactory | None = None,
    max_seconds: int | None = None,
    max_events: int | None = None,
    progress_callback: BlueskyFirehoseProgressCallback | None = None,
) -> Dict[str, Any]:
    started_at = perf_counter()
    now = _utc_now(reference_time)
    fetched_at = now.isoformat()
    effective_max_seconds = max(
        1,
        int(max_seconds if max_seconds is not None else BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN),
    )
    effective_max_events = max(
        1,
        int(max_events if max_events is not None else BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN),
    )
    cutoff_utc = int((now - timedelta(hours=BLUESKY_FIREHOSE_RETENTION_HOURS)).timestamp())
    cutoff_dt = now - timedelta(hours=BLUESKY_FIREHOSE_RETENTION_HOURS)
    firehose_enabled = (
        BLUESKY_ENABLED and BLUESKY_FIREHOSE_ENABLED
        if firehose_enabled is None
        else firehose_enabled
    )

    posts = [dict(row) for row in existing_posts or []]
    snapshots = [dict(row) for row in existing_snapshots or []]
    profiles = [dict(row) for row in existing_profiles or []]
    interactions = [dict(row) for row in existing_interactions or []]
    state = dict(existing_state or {})

    if not firehose_enabled:
        return {
            "posts": posts,
            "snapshots": snapshots,
            "profiles": profiles,
            "interactions": interactions,
            "state": {
                **state,
                "enabled": False,
                "status": "disabled",
                "lastSyncCompletedAt": fetched_at,
            },
            "sourceUpdates": {},
            "fetchedCount": 0,
            "snapshotCount": 0,
            "profileCount": len(profiles),
            "interactionCount": len(interactions),
            "timings": {"totalMs": 0.0, "perSourceMs": {}},
            "stats": {},
        }

    post_map = {str(row.get("id") or ""): dict(row) for row in posts if str(row.get("id") or "")}
    profile_map = {
        _profile_key(str(row.get("did") or ""), str(row.get("handle") or "")): dict(row)
        for row in profiles
        if _profile_key(str(row.get("did") or ""), str(row.get("handle") or ""))
    }
    latest_snapshot_map: Dict[str, Dict[str, Any]] = {}
    for snapshot in snapshots:
        post_uri = str(snapshot.get("postUri") or "")
        if not post_uri:
            continue
        current = latest_snapshot_map.get(post_uri)
        if current is None or str(snapshot.get("fetchedAt") or "") > str(current.get("fetchedAt") or ""):
            latest_snapshot_map[post_uri] = dict(snapshot)

    previous_cursor = _parse_int(state.get("cursor"))
    rewind_us = max(0, BLUESKY_FIREHOSE_CURSOR_REWIND_SECONDS) * 1_000_000
    bootstrap_cursor = int((now - timedelta(minutes=BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES)).timestamp() * 1_000_000)
    previous_cursor_age_minutes = None
    if previous_cursor > 0:
        previous_cursor_time = datetime.fromtimestamp(previous_cursor / 1_000_000, tz=timezone.utc)
        previous_cursor_age_minutes = max(
            0,
            round((now - previous_cursor_time).total_seconds() / 60.0, 1),
        )
    stale_cursor_detected = (
        previous_cursor > 0
        and BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES > 0
        and previous_cursor_age_minutes is not None
        and previous_cursor_age_minutes > BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES
    )
    if stale_cursor_detected:
        print(
            "[bluesky-firehose] stale cursor detected "
            f"age_minutes={previous_cursor_age_minutes} "
            f"max_age_minutes={BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES} "
            f"bootstrap_mode={BLUESKY_FIREHOSE_BOOTSTRAP_MODE}"
        )
    has_usable_previous_cursor = previous_cursor > 0 and not stale_cursor_detected
    if has_usable_previous_cursor:
        cursor = previous_cursor - rewind_us
    elif BLUESKY_FIREHOSE_BOOTSTRAP_MODE == "head":
        cursor = 0
    else:
        cursor = bootstrap_cursor
    cursor = max(0, cursor)

    source_started = perf_counter()
    jetstream_mode = _jetstream_mode()
    print(
        f"[bluesky-firehose] Jetstream mode: {jetstream_mode} "
        f"({ _jetstream_mode_description() }) endpoint={BLUESKY_FIREHOSE_ENDPOINT}"
    )
    stats = {
        "eventsProcessed": 0,
        "postsObserved": 0,
        "likesObserved": 0,
        "repostsObserved": 0,
        "repliesObserved": 0,
        "quotesObserved": 0,
        "followsObserved": 0,
        "profilesObserved": 0,
        "identitiesObserved": 0,
        "accountsObserved": 0,
        "rawPersistedEvents": 0,
        "rawPersistFailures": 0,
        "normalizationAttempts": 0,
        "normalizationSuccesses": 0,
        "normalizationFailures": 0,
    }
    touched_post_ids: set[str] = set()
    incoming_posts: List[Dict[str, Any]] = []
    incoming_interactions: List[Dict[str, Any]] = []
    incoming_profiles: Dict[str, Dict[str, Any]] = {}
    last_event_time_us = previous_cursor if has_usable_previous_cursor else 0
    normalization_errors: List[str] = []
    raw_persist_error: Exception | None = None
    raw_buffer: List[Dict[str, Any]] = []

    def current_lag_minutes() -> float | None:
        if last_event_time_us <= 0:
            return None
        return max(
            0,
            round(
                (
                    _utc_now()
                    - datetime.fromtimestamp(last_event_time_us / 1_000_000, tz=timezone.utc)
                ).total_seconds()
                / 60.0,
                1,
            ),
        )

    def current_health_status() -> str:
        if raw_persist_error is not None:
            return "persistence_failed"
        if stats["normalizationFailures"] > 0 and stats["normalizationSuccesses"] == 0:
            return "normalization_failed"
        if stats["normalizationFailures"] > 0:
            return "normalization_degraded"
        if stats["eventsProcessed"] > 0:
            return "healthy_live"
        return "connecting"

    def emit_progress() -> None:
        if progress_callback is None:
            return

        source_duration_ms = round((perf_counter() - source_started) * 1000, 1)
        duration_minutes = max(source_duration_ms / 60_000, 1 / 60)
        progress_state = {
            **state,
            "enabled": True,
            "sourceStatus": "active",
            "provider": "jetstream",
            "endpoint": BLUESKY_FIREHOSE_ENDPOINT,
            "streamMode": jetstream_mode,
            "streamModeLabel": "Bluesky full firehose" if not BLUESKY_FIREHOSE_COLLECTIONS else "Bluesky firehose",
            "collectionBehavior": (
                "all_collections"
                if not BLUESKY_FIREHOSE_COLLECTIONS
                else f"filtered:{','.join(BLUESKY_FIREHOSE_COLLECTIONS)}"
            ),
            "wantedCollections": list(BLUESKY_FIREHOSE_COLLECTIONS),
            "cursor": last_event_time_us or cursor,
            "lastEventTimeUs": last_event_time_us or None,
            "lastEventAt": _iso_from_time_us(last_event_time_us),
            "lastSyncStartedAt": fetched_at,
            "lastSyncDurationMs": round((perf_counter() - started_at) * 1000, 1),
            "lastSyncEvents": stats["eventsProcessed"],
            "backlogLagMinutes": current_lag_minutes(),
            "eventsPerMinute": round(stats["eventsProcessed"] / duration_minutes, 2),
            "rawPersistedEvents": stats["rawPersistedEvents"],
            "rawPersistFailures": stats["rawPersistFailures"],
            "rawPersistSuccessRate": round(
                (
                    stats["rawPersistedEvents"]
                    / max(1, stats["rawPersistedEvents"] + stats["rawPersistFailures"])
                )
                * 100,
                1,
            ),
            "normalizationAttempts": stats["normalizationAttempts"],
            "normalizationFailures": stats["normalizationFailures"],
            "normalizationSuccessRate": round(
                (stats["normalizationSuccesses"] / max(1, stats["normalizationAttempts"])) * 100,
                1,
            ),
            "normalizationErrorSamples": normalization_errors,
            "snapshotFreshnessMinutes": 0.0,
            "bootstrapCursorTimeUs": bootstrap_cursor,
            "bootstrapMode": "resume" if has_usable_previous_cursor else "bootstrap",
            "status": "running",
            "healthStatus": current_health_status(),
            "connectionStatus": "connecting",
            "lastError": str(raw_persist_error) if raw_persist_error else None,
            "workerPid": os.getpid(),
            "workerHeartbeatAt": _utc_now().isoformat(),
            "workerAlive": True,
            "previousCursorAgeMinutes": previous_cursor_age_minutes,
            "cursorResetReason": "stale_cursor" if stale_cursor_detected else None,
            "reconnectCount": reconnect_attempt,
            "lastPersistenceAt": _utc_now().isoformat(),
            "lastAggregateRefreshAt": _utc_now().isoformat(),
            "wallClockLagMinutes": current_lag_minutes(),
            "rawEventStorePath": str(raw_event_store_path()),
            "stats": dict(stats),
        }
        try:
            progress_callback(progress_state)
        except Exception as error:
            print(f"[bluesky-firehose] progress callback error={error}")

    def normalize_event(event: Dict[str, Any]) -> None:
        nonlocal last_event_time_us, profile_map
        stats["normalizationAttempts"] += 1
        event_time_us = _event_time_us(event)
        if event_time_us > 0:
            last_event_time_us = max(last_event_time_us, event_time_us)

        kind = str(event.get("kind") or "")
        if kind == "identity":
            identity = event.get("identity")
            if isinstance(identity, dict):
                incoming_profiles[_profile_key(str(identity.get("did") or ""), str(identity.get("handle") or ""))] = _upsert_profile(
                    {**profile_map, **incoming_profiles},
                    did=str(identity.get("did") or "") or None,
                    handle=str(identity.get("handle") or "") or None,
                    indexed_at=str(identity.get("time") or "") or fetched_at,
                    fetched_at=fetched_at,
                )
            stats["identitiesObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return
        if kind == "account":
            account = event.get("account")
            if isinstance(account, dict):
                active_label = "active" if bool(account.get("active")) else "inactive"
                incoming_profiles[_profile_key(str(account.get("did") or ""), None)] = _upsert_profile(
                    {**profile_map, **incoming_profiles},
                    did=str(account.get("did") or "") or None,
                    description=f"Account status: {active_label}",
                    indexed_at=str(account.get("time") or "") or fetched_at,
                    fetched_at=fetched_at,
                )
            stats["accountsObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return
        if kind != "commit":
            stats["normalizationSuccesses"] += 1
            return

        collection = _commit_collection(event)
        operation = _commit_operation(event)
        if collection not in JETSTREAM_RELEVANT_COLLECTIONS or operation == "delete":
            if collection == JETSTREAM_COLLECTION_FOLLOW:
                stats["followsObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return

        did = _event_did(event)

        if collection == JETSTREAM_COLLECTION_PROFILE:
            record = _commit_record(event)
            incoming_profiles[_profile_key(did, None)] = _upsert_profile(
                {**profile_map, **incoming_profiles},
                did=did or None,
                display_name=_clean_text(str(record.get("displayName") or "")) or None,
                description=_clean_text(str(record.get("description") or "")) or None,
                avatar=str(record.get("avatar") or "") or None,
                banner=str(record.get("banner") or "") or None,
                created_at=_event_created_iso(event, record),
                indexed_at=_iso_from_time_us(event_time_us),
                labels=[
                    label.get("val")
                    for label in record.get("labels", [])
                    if isinstance(label, dict) and label.get("val")
                ],
                fetched_at=fetched_at,
            )
            stats["profilesObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return

        if collection == JETSTREAM_COLLECTION_POST:
            profile_map = {**profile_map, **incoming_profiles}
            post = _normalize_post_from_firehose_event(
                event,
                profile_map=profile_map,
                previous=post_map.get(_build_at_uri(did, collection, _commit_rkey(event))),
                fetched_at=fetched_at,
            )
            incoming_posts.append(post)
            post_map[post["id"]] = post
            touched_post_ids.add(post["id"])
            incoming_profiles[_profile_key(did, str(post.get("authorHandle") or ""))] = _upsert_profile(
                {**profile_map, **incoming_profiles},
                did=did or None,
                handle=str(post.get("authorHandle") or "") or None,
                display_name=str(post.get("authorDisplayName") or "") or None,
                avatar=str(post.get("authorAvatar") or "") or None,
                description=str(post.get("authorDescription") or "") or None,
                fetched_at=fetched_at,
                source_post_uri=str(post.get("id") or "") or None,
            )
            interaction = _normalize_post_interaction(post, fetched_at=fetched_at)
            if interaction:
                incoming_interactions.append(interaction)
                touched_post_ids.add(str(interaction.get("postUri") or ""))
                if interaction["interactionType"] == "reply":
                    stats["repliesObserved"] += 1
                elif interaction["interactionType"] == "quote":
                    stats["quotesObserved"] += 1
            stats["postsObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return

        if collection == JETSTREAM_COLLECTION_LIKE:
            interaction = _normalize_subject_interaction(
                event,
                interaction_type="like",
                profile_map={**profile_map, **incoming_profiles},
                fetched_at=fetched_at,
            )
            if interaction:
                incoming_interactions.append(interaction)
                touched_post_ids.add(str(interaction.get("postUri") or ""))
                incoming_profiles[_profile_key(str(interaction.get("actorDid") or ""), str(interaction.get("actorHandle") or ""))] = _upsert_profile(
                    {**profile_map, **incoming_profiles},
                    did=str(interaction.get("actorDid") or "") or None,
                    handle=str(interaction.get("actorHandle") or "") or None,
                    display_name=str(interaction.get("actorDisplayName") or "") or None,
                    fetched_at=fetched_at,
                    last_amplified_uri=str(interaction.get("postUri") or "") or None,
                )
                stats["likesObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return

        if collection == JETSTREAM_COLLECTION_REPOST:
            interaction = _normalize_subject_interaction(
                event,
                interaction_type="repost",
                profile_map={**profile_map, **incoming_profiles},
                fetched_at=fetched_at,
            )
            if interaction:
                incoming_interactions.append(interaction)
                touched_post_ids.add(str(interaction.get("postUri") or ""))
                incoming_profiles[_profile_key(str(interaction.get("actorDid") or ""), str(interaction.get("actorHandle") or ""))] = _upsert_profile(
                    {**profile_map, **incoming_profiles},
                    did=str(interaction.get("actorDid") or "") or None,
                    handle=str(interaction.get("actorHandle") or "") or None,
                    display_name=str(interaction.get("actorDisplayName") or "") or None,
                    fetched_at=fetched_at,
                    last_amplified_uri=str(interaction.get("postUri") or "") or None,
                )
                stats["repostsObserved"] += 1
            stats["normalizationSuccesses"] += 1
            return

        if collection == JETSTREAM_COLLECTION_FOLLOW:
            stats["followsObserved"] += 1
        stats["normalizationSuccesses"] += 1

    def flush_raw_buffer() -> bool:
        nonlocal raw_persist_error
        if not raw_buffer:
            return True

        batch = list(raw_buffer)
        raw_buffer.clear()

        if BLUESKY_RAW_PERSIST_ENABLED:
            try:
                stored = append_raw_events(batch)
                stats["rawPersistedEvents"] += int(stored.get("stored", 0) or 0)
            except Exception as error:
                stats["rawPersistFailures"] += len(batch)
                raw_persist_error = error
                print(f"[bluesky-firehose] raw persistence error={error}")
                return False
        else:
            stats["rawPersistedEvents"] += len(batch)

        for row in batch:
            try:
                normalize_event(dict(row.get("payload") or {}))
            except Exception as error:
                stats["normalizationFailures"] += 1
                if len(normalization_errors) < 6:
                    normalization_errors.append(str(error))
                print(f"[bluesky-firehose] normalization error={error}")
        emit_progress()
        return True

    last_iterator_error: Exception | None = None
    reconnect_attempt = 0
    stream_deadline = monotonic() + effective_max_seconds

    while (
        monotonic() < stream_deadline
        and stats["eventsProcessed"] < effective_max_events
    ):
        remaining_seconds = max(1, int(stream_deadline - monotonic()))
        remaining_events = max(1, effective_max_events - stats["eventsProcessed"])
        resume_cursor = last_event_time_us or cursor
        if resume_cursor > 0:
            resume_cursor = max(0, resume_cursor - rewind_us)

        print(
            f"[bluesky-firehose] connect attempt={reconnect_attempt + 1} resume_cursor={resume_cursor} remaining_seconds={remaining_seconds} remaining_events={remaining_events}"
        )

        iterator = (
            event_iter_factory(
                cursor=resume_cursor,
                endpoint=BLUESKY_FIREHOSE_ENDPOINT,
                max_seconds=remaining_seconds,
                max_events=remaining_events,
            )
            if event_iter_factory
            else _iter_jetstream_events(
                cursor=resume_cursor,
                endpoint=BLUESKY_FIREHOSE_ENDPOINT,
                max_seconds=remaining_seconds,
                max_events=remaining_events,
            )
        )

        try:
            saw_any_event = False
            persistence_failed = False
            for event in iterator:
                saw_any_event = True
                stats["eventsProcessed"] += 1
                raw_buffer.append(
                    _raw_event_row(
                        event,
                        received_at=datetime.now(timezone.utc).isoformat(),
                    )
                )
                if len(raw_buffer) >= BLUESKY_RAW_PERSIST_BATCH_SIZE:
                    if not flush_raw_buffer():
                        persistence_failed = True
                        last_iterator_error = RuntimeError("raw Jetstream persistence failed")
                        break
            if not persistence_failed and not flush_raw_buffer():
                persistence_failed = True
                last_iterator_error = RuntimeError("raw Jetstream persistence failed")
            if persistence_failed:
                break
            if saw_any_event:
                last_iterator_error = None
            break
        except Exception as error:
            if raw_buffer and not flush_raw_buffer():
                last_iterator_error = RuntimeError("raw Jetstream persistence failed")
                break
            last_iterator_error = error
            if not _is_recoverable_firehose_error(error):
                raise

            reconnect_attempt += 1
            if reconnect_attempt > BLUESKY_FIREHOSE_RECONNECT_MAX_ATTEMPTS:
                print(
                    f"[bluesky-firehose] reconnect exhausted after {reconnect_attempt - 1} retries; last_error={error}"
                )
                if stats["eventsProcessed"] == 0:
                    raise
                break

            backoff_seconds = _firehose_backoff_seconds(reconnect_attempt)
            print(
                f"[bluesky-firehose] disconnect detected error={error}; retrying in {round(backoff_seconds, 2)}s"
            )
            sleep_for = min(backoff_seconds, max(0.0, stream_deadline - monotonic()))
            if sleep_for > 0:
                sleep(sleep_for)

    source_duration_ms = round((perf_counter() - source_started) * 1000, 1)
    duration_minutes = max(source_duration_ms / 60_000, 1 / 60)

    merged_interactions = _merge_interactions(
        interactions,
        incoming_interactions,
        cutoff_utc=cutoff_utc,
    )
    counts_by_post = _observed_post_counts(merged_interactions)

    merged_posts = {
        **post_map,
        **{str(row.get("id") or ""): row for row in incoming_posts if str(row.get("id") or "")},
    }
    active_post_ids = {
        *touched_post_ids,
        *{str(row.get("postUri") or "") for row in merged_interactions if str(row.get("postUri") or "")},
        *{str(row.get("rootUri") or "") for row in merged_interactions if str(row.get("rootUri") or "")},
    }
    hydrated_posts = _hydrate_posts_from_profiles(
        list(merged_posts.values()),
        profile_map={**profile_map, **incoming_profiles},
        counts_by_post=counts_by_post,
    )
    pruned_posts = _prune_posts(
        hydrated_posts,
        cutoff_utc=cutoff_utc,
        active_post_ids=active_post_ids,
    )
    post_by_id = {str(row.get("id") or ""): row for row in pruned_posts}

    incoming_snapshots: List[Dict[str, Any]] = []
    for post_id in touched_post_ids:
        post = post_by_id.get(post_id)
        if not post:
            continue
        snapshot = _build_snapshot_for_post(
            post,
            fetched_at=fetched_at,
            previous=latest_snapshot_map.get(post_id),
        )
        incoming_snapshots.append(snapshot)
        post["lastSnapshotAt"] = fetched_at
        post["snapshotCount"] = int(post.get("snapshotCount", 0) or 0) + 1
        post["deltaLikeCount"] = int(snapshot.get("deltaLikeCount", 0) or 0)
        post["deltaRepostCount"] = int(snapshot.get("deltaRepostCount", 0) or 0)
        post["deltaCommentCount"] = int(snapshot.get("deltaCommentCount", 0) or 0)
        post["deltaQuoteCount"] = int(snapshot.get("deltaQuoteCount", 0) or 0)

    merged_snapshots = _merge_snapshots(snapshots, incoming_snapshots)

    merged_profiles = _refresh_profile_activity(
        _prune_profiles(
            list({**profile_map, **incoming_profiles}.values()),
            active_dids={
                *{str(row.get("authorDid") or "") for row in pruned_posts if str(row.get("authorDid") or "")},
                *{str(row.get("actorDid") or "") for row in merged_interactions if str(row.get("actorDid") or "")},
            },
            cutoff=cutoff_dt,
        ),
        posts=pruned_posts,
        interactions=merged_interactions,
    )

    lag_minutes = None
    if last_event_time_us > 0:
        lag_minutes = max(
            0,
            round((now - datetime.fromtimestamp(last_event_time_us / 1_000_000, tz=timezone.utc)).total_seconds() / 60.0, 1),
        )

    completed_at = _utc_now().isoformat()
    firehose_label = "Bluesky full firehose" if not BLUESKY_FIREHOSE_COLLECTIONS else "Bluesky firehose"
    raw_persist_success_rate = round(
        (
            stats["rawPersistedEvents"]
            / max(1, stats["rawPersistedEvents"] + stats["rawPersistFailures"])
        )
        * 100,
        1,
    )
    normalization_success_rate = round(
        (stats["normalizationSuccesses"] / max(1, stats["normalizationAttempts"])) * 100,
        1,
    )
    events_per_minute = round(stats["eventsProcessed"] / duration_minutes, 2)
    health_status = "healthy_live"
    if raw_persist_error is not None:
        health_status = "persistence_failed"
    elif stats["normalizationFailures"] > 0 and stats["normalizationSuccesses"] == 0:
        health_status = "normalization_failed"
    elif stats["normalizationFailures"] > 0:
        health_status = "normalization_degraded"
    elif last_iterator_error is not None and stats["eventsProcessed"] == 0:
        health_status = "disconnected"
    elif events_per_minute < 1 and source_duration_ms >= 30_000:
        health_status = "low_throughput"

    state = {
        **state,
        "enabled": True,
        "sourceStatus": "active",
        "provider": "jetstream",
        "endpoint": BLUESKY_FIREHOSE_ENDPOINT,
        "streamMode": jetstream_mode,
        "streamModeLabel": firehose_label,
        "collectionBehavior": (
            "all_collections"
            if not BLUESKY_FIREHOSE_COLLECTIONS
            else f"filtered:{','.join(BLUESKY_FIREHOSE_COLLECTIONS)}"
        ),
        "wantedCollections": list(BLUESKY_FIREHOSE_COLLECTIONS),
        "cursor": last_event_time_us or cursor,
        "lastEventTimeUs": last_event_time_us or None,
        "lastEventAt": _iso_from_time_us(last_event_time_us),
        "lastSyncStartedAt": fetched_at,
        "lastSyncCompletedAt": completed_at,
        "lastSyncDurationMs": round((perf_counter() - started_at) * 1000, 1),
        "lastSyncEvents": stats["eventsProcessed"],
        "backlogLagMinutes": lag_minutes,
        "eventsPerMinute": events_per_minute,
        "rawPersistedEvents": stats["rawPersistedEvents"],
        "rawPersistFailures": stats["rawPersistFailures"],
        "rawPersistSuccessRate": raw_persist_success_rate,
        "normalizationAttempts": stats["normalizationAttempts"],
        "normalizationFailures": stats["normalizationFailures"],
        "normalizationSuccessRate": normalization_success_rate,
        "normalizationErrorSamples": normalization_errors,
        "snapshotFreshnessMinutes": 0.0,
        "bootstrapCursorTimeUs": bootstrap_cursor,
        "bootstrapMode": "resume" if has_usable_previous_cursor else "bootstrap",
        "status": "failed" if last_iterator_error else "succeeded",
        "healthStatus": health_status,
        "connectionStatus": _connection_status(
            last_iterator_error=last_iterator_error,
            events_processed=stats["eventsProcessed"],
            raw_persist_error=raw_persist_error,
        ),
        "lastError": str(last_iterator_error) if last_iterator_error else None,
        "workerPid": os.getpid(),
        "workerHeartbeatAt": completed_at,
        "workerAlive": last_iterator_error is None,
        "reconnectCount": reconnect_attempt,
        "lastPersistenceAt": completed_at,
        "lastAggregateRefreshAt": completed_at,
        "wallClockLagMinutes": lag_minutes,
        "previousCursorAgeMinutes": previous_cursor_age_minutes,
        "cursorResetReason": "stale_cursor" if stale_cursor_detected else None,
        "rawEventStorePath": str(raw_event_store_path()),
        "stats": stats,
    }

    return {
        "posts": pruned_posts,
        "snapshots": merged_snapshots,
        "profiles": merged_profiles,
        "interactions": merged_interactions,
        "state": state,
        "sourceUpdates": {
            "bluesky:firehose": {
                "sourceId": "bluesky:firehose",
                "label": firehose_label,
                "kind": "bluesky_firehose",
                "success": last_iterator_error is None,
                "itemsFetched": stats["eventsProcessed"],
                "error": str(last_iterator_error) if last_iterator_error else None,
                "durationMs": source_duration_ms,
                "cursor": state["cursor"],
                "lagMinutes": lag_minutes,
            }
        },
        "fetchedCount": len(touched_post_ids),
        "snapshotCount": len(merged_snapshots),
        "profileCount": len(merged_profiles),
        "interactionCount": len(merged_interactions),
        "timings": {
            "totalMs": round((perf_counter() - started_at) * 1000, 1),
            "perSourceMs": {"bluesky:firehose": source_duration_ms},
        },
        "stats": stats,
    }
