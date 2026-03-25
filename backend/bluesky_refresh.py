from __future__ import annotations

import hashlib
import html
import json
import math
import re
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Callable, Dict, Iterable, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from backend.bluesky_config import (
    BLUESKY_API_BASE_URL,
    BLUESKY_DISCOVERY_MAX_AGE_HOURS,
    BLUESKY_DISCOVERY_QUERIES,
    BLUESKY_ENABLED,
    BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN,
    BLUESKY_MAX_AUTHOR_FEED_POSTS,
    BLUESKY_MAX_BREAKOUT_QUERIES_PER_RUN,
    BLUESKY_MAX_QUOTES_PER_POST,
    BLUESKY_MAX_REPOSTED_BY_PER_POST,
    BLUESKY_MAX_SEARCH_QUERIES_PER_RUN,
    BLUESKY_MAX_SNAPSHOTS_PER_POST,
    BLUESKY_MAX_THREAD_POSTS_PER_RUN,
    BLUESKY_MAX_TRACKED_POSTS_PER_RUN,
    BLUESKY_QUOTA_BUDGET_PER_RUN,
    BLUESKY_SEARCH_RESULTS_PER_QUERY,
)
from backend.reddit_dev_only_config import REDDIT_USER_AGENT

RequestJson = Callable[[str], Any]
BuildUrl = Callable[[str, Dict[str, Any]], str]

BLUESKY_QUERY_STOP_WORDS = {
    "about",
    "after",
    "again",
    "analysis",
    "breaking",
    "from",
    "full",
    "interview",
    "latest",
    "live",
    "news",
    "official",
    "reaction",
    "reactions",
    "story",
    "stories",
    "today",
    "update",
    "updates",
    "video",
    "videos",
    "watch",
}


def _request_json(url: str) -> Any:
    request = Request(url, headers={"User-Agent": REDDIT_USER_AGENT})
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _build_url(resource: str, params: Dict[str, Any]) -> str:
    payload = {key: value for key, value in params.items() if value not in (None, "")}
    return f"{BLUESKY_API_BASE_URL}/{resource}?{urlencode(payload)}"


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _truncate_text(value: str, max_length: int) -> str:
    if not value or len(value) <= max_length:
        return value
    return value[: max_length - 1].rstrip() + "..."


def _parse_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_utc_timestamp(value: str | None) -> int | None:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def _safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _merge_unique_strings(*collections: Iterable[str] | None) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for collection in collections:
        for value in collection or []:
            normalized = str(value or "").strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(normalized)
    return ordered


def _extract_query_phrases(value: str, *, max_phrases: int = 8) -> List[str]:
    tokens = [re.sub(r"[^a-z0-9]+", "", part.lower()) for part in re.split(r"[^A-Za-z0-9]+", value or "")]
    informative = [
        token
        for token in tokens
        if len(token) >= 3 and token not in BLUESKY_QUERY_STOP_WORDS and not token.isdigit()
    ]
    phrases: List[str] = []
    seen: set[str] = set()
    for size in (3, 2):
        for index in range(0, max(0, len(informative) - size + 1)):
            phrase = " ".join(informative[index : index + size]).strip()
            if not phrase or phrase in seen:
                continue
            seen.add(phrase)
            phrases.append(phrase)
            if len(phrases) >= max_phrases:
                return phrases
    return phrases


def _breakout_queries(
    *,
    reddit_posts: Iterable[Dict[str, Any]],
    public_items: Iterable[Dict[str, Any]],
    existing_posts: Iterable[Dict[str, Any]],
    limit: int,
) -> List[str]:
    weighted: Dict[str, float] = {}
    support: Dict[str, set[str]] = {}
    for row in list(reddit_posts) + list(public_items) + list(existing_posts):
        title = _clean_text(str(row.get("title", "") or ""))
        body = _clean_text(str(row.get("selftext") or row.get("summary") or row.get("body") or ""))
        source_key = str(row.get("subreddit") or row.get("sourceName") or row.get("author") or row.get("id") or "").lower()
        row_weight = 1.0
        row_weight += math.log10(int(row.get("score", 0) or 0) + 1) * 5
        row_weight += math.log10(int(row.get("numComments", 0) or 0) + 1) * 4
        row_weight += math.log10(int(row.get("replyCount", 0) or 0) + 1) * 4
        for text in (title, body):
            for phrase in _extract_query_phrases(text):
                weighted[phrase] = weighted.get(phrase, 0.0) + row_weight
                support.setdefault(phrase, set()).add(source_key)
    ranked = sorted(
        weighted.items(),
        key=lambda entry: (len(support.get(entry[0], set())), entry[1], len(entry[0])),
        reverse=True,
    )
    return [phrase for phrase, _ in ranked[:limit]]


def _interaction_counts(*, posts: int = 0, reposts: int = 0, comments: int = 0, likes: int = 0) -> Dict[str, int]:
    return {
        "posts": max(0, int(posts)),
        "reposts": max(0, int(reposts)),
        "comments": max(0, int(comments)),
        "likes": max(0, int(likes)),
    }


def _extract_uri(item: Dict[str, Any]) -> str:
    return str(item.get("uri") or item.get("postUri") or item.get("id") or "")


def _extract_rkey(uri: str) -> str:
    return uri.split("/")[-1] if uri else ""


def _build_post_url(author_handle: str | None, uri: str) -> str:
    rkey = _extract_rkey(uri)
    if author_handle:
        return f"https://bsky.app/profile/{author_handle}/post/{rkey}"
    return f"https://bsky.app/profile/{_safe_slug(uri)}/post/{rkey}"


def _get_author(item: Dict[str, Any]) -> Dict[str, Any]:
    author = item.get("author")
    if not isinstance(author, dict):
        return {}
    return {
        "did": str(author.get("did") or ""),
        "handle": str(author.get("handle") or ""),
        "displayName": str(author.get("displayName") or ""),
        "avatar": author.get("avatar"),
        "description": str(author.get("description") or ""),
        "indexedAt": author.get("indexedAt"),
        "createdAt": author.get("createdAt"),
    }


def _get_record(item: Dict[str, Any]) -> Dict[str, Any]:
    record = item.get("record")
    return record if isinstance(record, dict) else {}


def _extract_text(record: Dict[str, Any]) -> str:
    text = _clean_text(str(record.get("text", "") or ""))
    if text:
        return text
    embed = record.get("embed")
    if isinstance(embed, dict):
        external = embed.get("external")
        if isinstance(external, dict):
            return _clean_text(" ".join(
                part for part in [
                    str(external.get("title", "") or ""),
                    str(external.get("description", "") or ""),
                ]
                if part
            ))
    return ""


def _extract_reply_parent_uri(record: Dict[str, Any]) -> str | None:
    reply = record.get("reply")
    if not isinstance(reply, dict):
        return None
    parent = reply.get("parent")
    if isinstance(parent, dict):
        return str(parent.get("uri") or "")
    return None


def _extract_reply_root_uri(record: Dict[str, Any]) -> str | None:
    reply = record.get("reply")
    if not isinstance(reply, dict):
        return None
    root = reply.get("root")
    if isinstance(root, dict):
        return str(root.get("uri") or "")
    return None


def _extract_quoted_uri(record: Dict[str, Any]) -> str | None:
    embed = record.get("embed")
    if not isinstance(embed, dict):
        return None
    inner = embed.get("record")
    if isinstance(inner, dict):
        return str(inner.get("uri") or "")
    return None


def _get_created_at(item: Dict[str, Any]) -> str | None:
    record = _get_record(item)
    return str(record.get("createdAt") or item.get("indexedAt") or "")


def _post_type(record: Dict[str, Any]) -> str:
    if _extract_reply_parent_uri(record):
        return "reply"
    if _extract_quoted_uri(record):
        return "quote"
    return "root"


def _score_post(item: Dict[str, Any]) -> int:
    record = _get_record(item)
    text = _extract_text(record)
    like_count = _parse_int(item.get("likeCount"))
    repost_count = _parse_int(item.get("repostCount"))
    reply_count = _parse_int(item.get("replyCount"))
    quote_count = _parse_int(item.get("quoteCount"))
    weighted = (
        6
        + math.log10(like_count + 1) * 7
        + math.log10(repost_count + 1) * 10
        + math.log10(reply_count + 1) * 12
        + math.log10(quote_count + 1) * 8
        + min(10, len(text.split()) // 10)
    )
    return max(1, min(100, round(weighted)))


def _normalize_post(
    item: Dict[str, Any],
    *,
    fetched_at: str,
    discovery_lanes: Iterable[str] | None = None,
    discovered_queries: Iterable[str] | None = None,
    previous: Dict[str, Any] | None = None,
    lane: str,
) -> Dict[str, Any]:
    uri = _extract_uri(item)
    record = _get_record(item)
    author = _get_author(item)
    text = _extract_text(record)
    created_at = _get_created_at(item) or fetched_at
    created_utc = _to_utc_timestamp(created_at) or _to_utc_timestamp(fetched_at) or int(datetime.now(timezone.utc).timestamp())
    root_uri = _extract_reply_root_uri(record)
    parent_uri = _extract_reply_parent_uri(record)
    quoted_uri = _extract_quoted_uri(record)
    previous = previous or {}
    like_count = _parse_int(item.get("likeCount"))
    repost_count = _parse_int(item.get("repostCount"))
    reply_count = _parse_int(item.get("replyCount"))
    quote_count = _parse_int(item.get("quoteCount"))
    prev_like = _parse_int(previous.get("likeCount"))
    prev_repost = _parse_int(previous.get("repostCount"))
    prev_reply = _parse_int(previous.get("replyCount"))
    prev_quote = _parse_int(previous.get("quoteCount"))
    previous_snapshot_at = str(previous.get("lastSnapshotAt") or previous.get("fetchedAt") or "")
    snapshot_count = int(previous.get("snapshotCount", 0) or 0)
    if not previous or previous_snapshot_at != fetched_at:
        snapshot_count += 1

    summary = text or "Bluesky post"
    if len(summary) > 400:
        summary = summary[:399].rstrip() + "..."

    return {
        **previous,
        "id": uri,
        "source": "bluesky",
        "sourceType": "bluesky",
        "sourceName": author.get("displayName") or author.get("handle") or "Bluesky",
        "postType": previous.get("postType") or _post_type(record),
        "rootUri": previous.get("rootUri") or root_uri,
        "parentUri": previous.get("parentUri") or parent_uri,
        "quotedUri": previous.get("quotedUri") or quoted_uri,
        "title": _truncate_text(summary.split("\n", 1)[0], 220) if summary else "Bluesky post",
        "summary": summary,
        "author": author.get("displayName") or author.get("handle") or author.get("did") or "Bluesky",
        "authorDid": author.get("did"),
        "authorHandle": author.get("handle"),
        "authorDisplayName": author.get("displayName"),
        "authorAvatar": author.get("avatar"),
        "authorDescription": author.get("description"),
        "url": _build_post_url(author.get("handle"), uri),
        "createdUtc": created_utc,
        "score": _score_post(item),
        "numComments": reply_count,
        "replyCount": reply_count,
        "repostCount": repost_count,
        "quoteCount": quote_count,
        "likeCount": like_count,
        "interactionCounts": _interaction_counts(posts=1, reposts=repost_count, comments=reply_count, likes=like_count),
        "discoveryLanes": _merge_unique_strings(previous.get("discoveryLanes"), discovery_lanes, [lane]),
        "discoveredQueries": _merge_unique_strings(previous.get("discoveredQueries"), discovered_queries),
        "topicHints": _merge_unique_strings(
            previous.get("topicHints"),
            _extract_query_phrases(text.split("\n", 1)[0] if text else ""),
            _extract_query_phrases(text),
            [author.get("handle")] if author.get("handle") else None,
        )[:8],
        "firstObservedAt": previous.get("firstObservedAt") or fetched_at,
        "lastObservedAt": fetched_at,
        "lastSnapshotAt": fetched_at,
        "snapshotCount": snapshot_count,
        "initialLikeCount": previous.get("initialLikeCount", like_count),
        "initialRepostCount": previous.get("initialRepostCount", repost_count),
        "initialReplyCount": previous.get("initialReplyCount", reply_count),
        "initialQuoteCount": previous.get("initialQuoteCount", quote_count),
        "deltaLikeCount": max(0, like_count - prev_like) if previous else 0,
        "deltaRepostCount": max(0, repost_count - prev_repost) if previous else 0,
        "deltaCommentCount": max(0, reply_count - prev_reply) if previous else 0,
        "deltaQuoteCount": max(0, quote_count - prev_quote) if previous else 0,
        "priorityScore": round(
            5
            + math.log10(like_count + 1) * 5
            + math.log10(repost_count + 1) * 7
            + math.log10(reply_count + 1) * 8
            + math.log10(quote_count + 1) * 6,
            1,
        ),
        "fetchedAt": fetched_at,
    }


def _normalize_interaction_post(
    item: Dict[str, Any],
    *,
    fetched_at: str,
    source_lane: str,
) -> Dict[str, Any] | None:
    record = _get_record(item)
    uri = _extract_uri(item)
    if not uri:
        return None
    author = _get_author(item)
    text = _extract_text(record)
    created_at = _get_created_at(item) or fetched_at
    created_utc = _to_utc_timestamp(created_at)
    if created_utc is None:
        return None
    root_uri = _extract_reply_root_uri(record) or uri
    parent_uri = _extract_reply_parent_uri(record)
    quoted_uri = _extract_quoted_uri(record)
    target_uri = root_uri if parent_uri else quoted_uri or uri
    kind = "reply" if parent_uri else "quote"
    return {
        "id": f"{kind}:{uri}",
        "interactionType": kind,
        "postUri": target_uri,
        "eventUri": uri,
        "rootUri": root_uri,
        "parentUri": parent_uri,
        "actorDid": author.get("did"),
        "actorHandle": author.get("handle"),
        "actorDisplayName": author.get("displayName"),
        "text": text,
        "createdUtc": created_utc,
        "fetchedAt": fetched_at,
        "sourceLane": source_lane,
        "likeCount": _parse_int(item.get("likeCount")),
        "repostCount": _parse_int(item.get("repostCount")),
        "replyCount": _parse_int(item.get("replyCount")),
        "quoteCount": _parse_int(item.get("quoteCount")),
        "url": _build_post_url(author.get("handle"), uri),
    }


def _normalize_repost_interaction(
    *,
    target_uri: str,
    actor: Dict[str, Any],
    fetched_at: str,
    source_lane: str,
) -> Dict[str, Any]:
    actor_did = str(actor.get("did") or "")
    actor_handle = str(actor.get("handle") or "")
    actor_name = str(actor.get("displayName") or actor_handle or actor_did or "Bluesky")
    return {
        "id": f"repost:{target_uri}:{actor_did or actor_handle}",
        "interactionType": "repost",
        "postUri": target_uri,
        "eventUri": None,
        "rootUri": target_uri,
        "parentUri": None,
        "actorDid": actor_did or None,
        "actorHandle": actor_handle or None,
        "actorDisplayName": actor_name,
        "text": None,
        "createdUtc": _to_utc_timestamp(str(actor.get("indexedAt") or fetched_at)) or int(datetime.now(timezone.utc).timestamp()),
        "fetchedAt": fetched_at,
        "sourceLane": source_lane,
        "url": None,
    }


def _normalize_snapshot(
    item: Dict[str, Any],
    *,
    fetched_at: str,
    previous: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    uri = _extract_uri(item)
    created_at = _get_created_at(item) or fetched_at
    created_utc = _to_utc_timestamp(created_at) or _to_utc_timestamp(fetched_at) or int(datetime.now(timezone.utc).timestamp())
    like_count = _parse_int(item.get("likeCount"))
    repost_count = _parse_int(item.get("repostCount"))
    reply_count = _parse_int(item.get("replyCount"))
    quote_count = _parse_int(item.get("quoteCount"))
    previous = previous or {}
    previous_fetched = _parse_iso_datetime(str(previous.get("fetchedAt") or "")) or _parse_iso_datetime(str(previous.get("lastFetchedAt") or ""))
    current_fetched = _parse_iso_datetime(fetched_at)
    delta_window_minutes = 0.0
    if previous_fetched and current_fetched and current_fetched > previous_fetched:
        delta_window_minutes = round((current_fetched - previous_fetched).total_seconds() / 60.0, 1)
    return {
        "id": f"bluesky-snapshot:{hashlib.sha256(f'{uri}|{fetched_at}'.encode('utf-8')).hexdigest()[:16]}",
        "postUri": uri,
        "fetchedAt": fetched_at,
        "createdUtc": created_utc,
        "likeCount": like_count,
        "repostCount": repost_count,
        "replyCount": reply_count,
        "quoteCount": quote_count,
        "deltaLikeCount": max(0, like_count - _parse_int(previous.get("likeCount"))) if previous else 0,
        "deltaRepostCount": max(0, repost_count - _parse_int(previous.get("repostCount"))) if previous else 0,
        "deltaCommentCount": max(0, reply_count - _parse_int(previous.get("replyCount"))) if previous else 0,
        "deltaQuoteCount": max(0, quote_count - _parse_int(previous.get("quoteCount"))) if previous else 0,
        "deltaWindowMinutes": delta_window_minutes,
    }


def _merge_posts(existing_rows: Iterable[Dict[str, Any]], incoming_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        str(row.get("id", "")): dict(row) for row in existing_rows if str(row.get("id", ""))
    }
    for row in incoming_rows:
        record_id = str(row.get("id", ""))
        if not record_id:
            continue
        existing = merged.get(record_id, {})
        merged[record_id] = {
            **existing,
            **row,
            "firstObservedAt": existing.get("firstObservedAt") or row.get("firstObservedAt"),
            "snapshotCount": max(int(existing.get("snapshotCount", 0) or 0), int(row.get("snapshotCount", 0) or 0)),
        }
    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("id", "")),
        ),
        reverse=True,
    )


def _merge_profiles(existing_rows: Iterable[Dict[str, Any]], incoming_rows: Iterable[Dict[str, Any]], *, fetched_at: str) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        str(row.get("did") or row.get("handle") or ""): dict(row)
        for row in existing_rows
        if str(row.get("did") or row.get("handle") or "")
    }
    for row in incoming_rows:
        record_id = str(row.get("did") or row.get("handle") or "")
        if not record_id:
            continue
        existing = merged.get(record_id, {})
        merged[record_id] = {
            **existing,
            **row,
            "firstSeenAt": existing.get("firstSeenAt") or fetched_at,
            "lastSeenAt": fetched_at,
            "discoveryLanes": _merge_unique_strings(existing.get("discoveryLanes"), row.get("discoveryLanes")),
            "observedPostCount": int(existing.get("observedPostCount", 0) or 0) + int(row.get("observedPostCount", 0) or 0),
            "amplificationScore": int(existing.get("amplificationScore", 0) or 0) + int(row.get("amplificationScore", 0) or 0),
            "sourcePostUris": _merge_unique_strings(existing.get("sourcePostUris"), row.get("sourcePostUris")),
            "lastObservedPostUri": row.get("lastObservedPostUri") or existing.get("lastObservedPostUri"),
        }
    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("amplificationScore", 0) or 0),
            str(row.get("handle", "")),
        ),
        reverse=True,
    )


def _merge_interactions(existing_rows: Iterable[Dict[str, Any]], incoming_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        str(row.get("id", "")): dict(row) for row in existing_rows if str(row.get("id", ""))
    }
    for row in incoming_rows:
        record_id = str(row.get("id", ""))
        if not record_id:
            continue
        merged[record_id] = {**merged.get(record_id, {}), **row}
    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("id", "")),
        ),
        reverse=True,
    )


def _merge_snapshots(existing_rows: Iterable[Dict[str, Any]], incoming_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        str(row.get("id", "")): dict(row) for row in existing_rows if str(row.get("id", ""))
    }
    for row in incoming_rows:
        record_id = str(row.get("id", ""))
        if not record_id:
            continue
        merged[record_id] = {**merged.get(record_id, {}), **row}
    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get("id", "")),
        ),
        reverse=True,
    )[: BLUESKY_MAX_SNAPSHOTS_PER_POST * 10]


def _consume_budget(budget: Dict[str, int], cost: int = 1) -> bool:
    if budget["remaining"] < cost:
        return False
    budget["remaining"] -= cost
    budget["used"] += cost
    return True


def _search_posts(*, query: str, request_json: RequestJson, limit: int, sort: str) -> List[Dict[str, Any]]:
    payload = request_json(_build_url("app.bsky.feed.searchPosts", {"q": query, "limit": limit, "sort": sort}))
    return [item for item in (payload.get("posts") if isinstance(payload, dict) else []) if isinstance(item, dict)]


def _get_author_feed(*, actor: str, request_json: RequestJson, limit: int) -> List[Dict[str, Any]]:
    payload = request_json(_build_url("app.bsky.feed.getAuthorFeed", {"actor": actor, "limit": limit}))
    return [item for item in (payload.get("feed") if isinstance(payload, dict) else []) if isinstance(item, dict)]


def _get_posts(*, uris: List[str], request_json: RequestJson) -> List[Dict[str, Any]]:
    payload = request_json(_build_url("app.bsky.feed.getPosts", {"uris": ",".join(uris)}))
    return [item for item in (payload.get("posts") if isinstance(payload, dict) else []) if isinstance(item, dict)]


def _get_post_thread(*, uri: str, request_json: RequestJson) -> Dict[str, Any] | None:
    payload = request_json(_build_url("app.bsky.feed.getPostThread", {"uri": uri, "depth": 6}))
    thread = payload.get("thread") if isinstance(payload, dict) else None
    return thread if isinstance(thread, dict) else None


def _get_quotes(*, uri: str, request_json: RequestJson, limit: int) -> List[Dict[str, Any]]:
    payload = request_json(_build_url("app.bsky.feed.getQuotes", {"uri": uri, "limit": limit}))
    return [item for item in (payload.get("posts") if isinstance(payload, dict) else []) if isinstance(item, dict)]


def _get_reposted_by(*, uri: str, request_json: RequestJson, limit: int) -> List[Dict[str, Any]]:
    payload = request_json(_build_url("app.bsky.feed.getRepostedBy", {"uri": uri, "limit": limit}))
    return [item for item in (payload.get("repostedBy") if isinstance(payload, dict) else []) if isinstance(item, dict)]


def _flatten_thread_replies(replies: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for reply in replies:
        if not isinstance(reply, dict):
            continue
        post = reply.get("post")
        if isinstance(post, dict):
            items.append(post)
        nested = reply.get("replies")
        if isinstance(nested, list):
            items.extend(_flatten_thread_replies(nested))
    return items


def _priority_score(post: Dict[str, Any], *, reference_time: datetime) -> float:
    created_utc = int(post.get("createdUtc", 0) or 0)
    age_minutes = max(1.0, (reference_time.timestamp() - created_utc) / 60.0) if created_utc else 1.0
    counts = post.get("interactionCounts") or {}
    interactions = int(counts.get("reposts", 0) or 0) + int(counts.get("comments", 0) or 0) + int(counts.get("likes", 0) or 0)
    return (
        float(post.get("priorityScore", 0) or 0)
        + math.log10(interactions + 1) * 7
        + math.log10(int(post.get("snapshotCount", 0) or 0) + 1) * 4
        + max(0.0, 48.0 - age_minutes / 60.0)
    )


def _build_profiles_from_posts(posts: Iterable[Dict[str, Any]], *, fetched_at: str, lane: str) -> List[Dict[str, Any]]:
    profile_map: Dict[str, Dict[str, Any]] = {}
    for post in posts:
        did = str(post.get("authorDid") or "")
        handle = str(post.get("authorHandle") or "")
        key = did or handle
        if not key:
            continue
        source_post_uri = str(post.get("id") or "")
        current = profile_map.get(
            key,
            {
                "did": did or None,
                "handle": handle or None,
                "displayName": post.get("authorDisplayName"),
                "avatar": post.get("authorAvatar"),
                "description": post.get("authorDescription"),
                "firstObservedAt": fetched_at,
                "lastObservedAt": fetched_at,
                "discoveryLanes": [lane],
                "observedPostCount": 0,
                "amplificationScore": 0,
                "sourcePostUris": [],
            },
        )
        current["displayName"] = current.get("displayName") or post.get("authorDisplayName")
        current["avatar"] = current.get("avatar") or post.get("authorAvatar")
        current["description"] = current.get("description") or post.get("authorDescription")
        current["lastObservedAt"] = fetched_at
        current["observedPostCount"] = int(current.get("observedPostCount", 0) or 0) + 1
        current["amplificationScore"] = int(current.get("amplificationScore", 0) or 0) + int(post.get("repostCount", 0) or 0) + int(post.get("replyCount", 0) or 0) + int(post.get("quoteCount", 0) or 0)
        current["discoveryLanes"] = _merge_unique_strings(current.get("discoveryLanes"), [lane], post.get("discoveryLanes"))
        current["sourcePostUris"] = _merge_unique_strings(current.get("sourcePostUris"), [source_post_uri] if source_post_uri else None)
        current["lastObservedPostUri"] = source_post_uri or current.get("lastObservedPostUri")
        profile_map[key] = current
    return list(profile_map.values())


def _build_profiles_from_reposters(
    reposted_by: Iterable[Dict[str, Any]],
    *,
    fetched_at: str,
    lane: str,
    target_uri: str,
) -> List[Dict[str, Any]]:
    profiles: List[Dict[str, Any]] = []
    for actor in reposted_by:
        did = str(actor.get("did") or "")
        handle = str(actor.get("handle") or "")
        profiles.append(
            {
                "did": did or None,
                "handle": handle or None,
                "displayName": actor.get("displayName"),
                "avatar": actor.get("avatar"),
                "description": actor.get("description"),
                "firstObservedAt": fetched_at,
                "lastObservedAt": fetched_at,
                "discoveryLanes": [lane],
                "observedPostCount": 0,
                "amplificationScore": 1,
                "lastAmplifiedUri": target_uri,
                "sourcePostUris": [target_uri],
                "lastObservedPostUri": target_uri,
            }
        )
    return profiles


def _dedupe_profiles_for_run(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        did = str(row.get("did") or "")
        handle = str(row.get("handle") or "")
        source_post_uri = str(
            row.get("sourcePostUris", [None])[0]
            if isinstance(row.get("sourcePostUris"), list) and row.get("sourcePostUris")
            else row.get("sourcePostUri")
            or row.get("lastObservedPostUri")
            or row.get("lastAmplifiedUri")
            or ""
        )
        key = f"{did or handle}:{source_post_uri}"
        if key in merged:
            continue
        merged[key] = dict(row)
    return list(merged.values())


def refresh_bluesky_sources(
    *,
    existing_posts: Iterable[Dict[str, Any]] | None = None,
    existing_snapshots: Iterable[Dict[str, Any]] | None = None,
    existing_profiles: Iterable[Dict[str, Any]] | None = None,
    existing_interactions: Iterable[Dict[str, Any]] | None = None,
    reddit_posts: Iterable[Dict[str, Any]] | None = None,
    public_items: Iterable[Dict[str, Any]] | None = None,
    request_json: RequestJson = _request_json,
    build_url: BuildUrl = _build_url,
    reference_time: datetime | None = None,
    refresh_enabled: bool | None = None,
) -> Dict[str, Any]:
    started_at = perf_counter()
    now = reference_time or datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    cutoff_utc = int((now - timedelta(hours=BLUESKY_DISCOVERY_MAX_AGE_HOURS)).timestamp())
    refresh_enabled = BLUESKY_ENABLED if refresh_enabled is None else refresh_enabled
    budget = {"remaining": BLUESKY_QUOTA_BUDGET_PER_RUN, "used": 0}

    posts = [dict(row) for row in existing_posts or []]
    snapshots = [dict(row) for row in existing_snapshots or []]
    profiles = [dict(row) for row in existing_profiles or []]
    interactions = [dict(row) for row in existing_interactions or []]

    if not refresh_enabled:
        return {
            "posts": posts,
            "snapshots": snapshots,
            "profiles": profiles,
            "interactions": interactions,
            "sourceUpdates": {},
            "fetchedCount": 0,
            "snapshotCount": 0,
            "profileCount": 0,
            "interactionCount": 0,
            "timings": {"totalMs": 0.0, "perSourceMs": {}},
        }

    existing_post_map = {str(row.get("id", "")): dict(row) for row in posts if str(row.get("id", ""))}
    latest_snapshot_map: Dict[str, Dict[str, Any]] = {}
    for snapshot in snapshots:
        post_uri = str(snapshot.get("postUri") or "")
        if not post_uri:
            continue
        current = latest_snapshot_map.get(post_uri)
        if current is None or str(snapshot.get("fetchedAt") or "") > str(current.get("fetchedAt") or ""):
            latest_snapshot_map[post_uri] = dict(snapshot)

    source_updates: Dict[str, Dict[str, Any]] = {}
    source_timings: Dict[str, float] = {}
    pending_posts: Dict[str, Dict[str, Any]] = {}
    pending_snapshots: List[Dict[str, Any]] = []
    pending_profiles: List[Dict[str, Any]] = []
    pending_interactions: List[Dict[str, Any]] = []

    query_pool = _merge_unique_strings(
        BLUESKY_DISCOVERY_QUERIES,
        _breakout_queries(
            reddit_posts=reddit_posts or [],
            public_items=public_items or [],
            existing_posts=posts,
            limit=BLUESKY_MAX_BREAKOUT_QUERIES_PER_RUN,
        ),
    )[:BLUESKY_MAX_SEARCH_QUERIES_PER_RUN]

    for query in query_pool:
        source_key = f"bluesky:query:{_safe_slug(query)}"
        source_started = perf_counter()
        if not _consume_budget(budget, 1):
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": query,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": "quota exhausted",
                "skipped": True,
                "durationMs": 0.0,
            }
            continue
        try:
            results = _search_posts(query=query, request_json=request_json, limit=BLUESKY_SEARCH_RESULTS_PER_QUERY, sort="top")
            seen_count = 0
            for item in results:
                uri = _extract_uri(item)
                if not uri:
                    continue
                created_utc = _to_utc_timestamp(_get_created_at(item))
                if created_utc is not None and created_utc < cutoff_utc:
                    continue
                previous = pending_posts.get(uri) or existing_post_map.get(uri)
                normalized = _normalize_post(
                    item,
                    fetched_at=fetched_at,
                    discovery_lanes=previous.get("discoveryLanes") if previous else ["query"],
                    discovered_queries=[query],
                    previous=previous,
                    lane="query",
                )
                pending_posts[uri] = normalized
                pending_snapshots.append(
                    _normalize_snapshot(item, fetched_at=fetched_at, previous=latest_snapshot_map.get(uri))
                )
                pending_profiles.extend(_build_profiles_from_posts([normalized], fetched_at=fetched_at, lane="query"))
                seen_count += 1
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": query,
                "kind": "bluesky",
                "success": True,
                "itemsFetched": seen_count,
                "error": None,
                "durationMs": round((perf_counter() - source_started) * 1000, 1),
                "query": query,
            }
            source_timings[source_key] = source_updates[source_key]["durationMs"]
        except Exception as error:
            duration_ms = round((perf_counter() - source_started) * 1000, 1)
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": query,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
                "query": query,
            }
            source_timings[source_key] = duration_ms

    author_candidates: List[str] = []
    for post in sorted(pending_posts.values(), key=lambda row: _priority_score(row, reference_time=now), reverse=True):
        if len(author_candidates) >= BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN:
            break
        actor = str(post.get("authorDid") or post.get("authorHandle") or "")
        if actor:
            author_candidates.append(actor)

    for actor in _merge_unique_strings(author_candidates)[:BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN]:
        source_key = f"bluesky:author:{_safe_slug(actor)}"
        source_started = perf_counter()
        if not _consume_budget(budget, 1):
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": actor,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": "quota exhausted",
                "skipped": True,
                "durationMs": 0.0,
            }
            continue
        try:
            feed_rows = _get_author_feed(actor=actor, request_json=request_json, limit=BLUESKY_MAX_AUTHOR_FEED_POSTS)
            seen_count = 0
            for row in feed_rows:
                post_view = row.get("post") if isinstance(row.get("post"), dict) else row
                uri = _extract_uri(post_view)
                if not uri:
                    continue
                created_utc = _to_utc_timestamp(_get_created_at(post_view))
                if created_utc is not None and created_utc < cutoff_utc:
                    continue
                previous = pending_posts.get(uri) or existing_post_map.get(uri)
                normalized = _normalize_post(
                    post_view,
                    fetched_at=fetched_at,
                    discovery_lanes=previous.get("discoveryLanes") if previous else ["author-feed"],
                    discovered_queries=previous.get("discoveredQueries") if previous else None,
                    previous=previous,
                    lane="author-feed",
                )
                pending_posts[uri] = normalized
                pending_snapshots.append(
                    _normalize_snapshot(post_view, fetched_at=fetched_at, previous=latest_snapshot_map.get(uri))
                )
                pending_profiles.extend(_build_profiles_from_posts([normalized], fetched_at=fetched_at, lane="author-feed"))
                seen_count += 1
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": actor,
                "kind": "bluesky",
                "success": True,
                "itemsFetched": seen_count,
                "error": None,
                "durationMs": round((perf_counter() - source_started) * 1000, 1),
                "actor": actor,
            }
            source_timings[source_key] = source_updates[source_key]["durationMs"]
        except Exception as error:
            duration_ms = round((perf_counter() - source_started) * 1000, 1)
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": actor,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
                "actor": actor,
            }
            source_timings[source_key] = duration_ms

    tracked_posts = sorted(
        pending_posts.values(),
        key=lambda row: _priority_score(row, reference_time=now),
        reverse=True,
    )[:BLUESKY_MAX_TRACKED_POSTS_PER_RUN]
    tracked_uris = [str(post.get("id", "")) for post in tracked_posts if str(post.get("id", ""))]
    if tracked_uris and _consume_budget(budget, 1):
        source_key = "bluesky:tracked-refresh"
        source_started = perf_counter()
        try:
            hydrated_posts = _get_posts(uris=tracked_uris, request_json=request_json)
            seen_count = 0
            for item in hydrated_posts:
                uri = _extract_uri(item)
                if not uri:
                    continue
                previous = pending_posts.get(uri) or existing_post_map.get(uri)
                pending_posts[uri] = _normalize_post(
                    item,
                    fetched_at=fetched_at,
                    discovery_lanes=previous.get("discoveryLanes") if previous else ["tracked-refresh"],
                    discovered_queries=previous.get("discoveredQueries") if previous else None,
                    previous=previous,
                    lane="tracked-refresh",
                )
                pending_snapshots.append(
                    _normalize_snapshot(item, fetched_at=fetched_at, previous=latest_snapshot_map.get(uri))
                )
                pending_profiles.extend(_build_profiles_from_posts([pending_posts[uri]], fetched_at=fetched_at, lane="tracked-refresh"))
                seen_count += 1
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": "Bluesky tracked refresh",
                "kind": "bluesky",
                "success": True,
                "itemsFetched": seen_count,
                "error": None,
                "durationMs": round((perf_counter() - source_started) * 1000, 1),
            }
            source_timings[source_key] = source_updates[source_key]["durationMs"]
        except Exception as error:
            duration_ms = round((perf_counter() - source_started) * 1000, 1)
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": "Bluesky tracked refresh",
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
            }
            source_timings[source_key] = duration_ms

    comment_source_posts = sorted(
        pending_posts.values(),
        key=lambda row: _priority_score(row, reference_time=now),
        reverse=True,
    )[:BLUESKY_MAX_THREAD_POSTS_PER_RUN]
    for post in comment_source_posts:
        uri = str(post.get("id", ""))
        if not uri:
            continue
        source_key = f"bluesky:thread:{_safe_slug(uri)}"
        source_started = perf_counter()
        if not _consume_budget(budget, 1):
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": uri,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": "quota exhausted",
                "skipped": True,
                "durationMs": 0.0,
            }
            continue
        try:
            thread = _get_post_thread(uri=uri, request_json=request_json)
            quotes = _get_quotes(uri=uri, request_json=request_json, limit=BLUESKY_MAX_QUOTES_PER_POST)
            reposted_by = _get_reposted_by(uri=uri, request_json=request_json, limit=BLUESKY_MAX_REPOSTED_BY_PER_POST)
            thread_reply_count = 0
            if thread:
                for reply_item in _flatten_thread_replies(thread.get("replies") or []):
                    reply_uri = _extract_uri(reply_item)
                    if not reply_uri:
                        continue
                    previous = pending_posts.get(reply_uri) or existing_post_map.get(reply_uri)
                    normalized_reply = _normalize_post(
                        reply_item,
                        fetched_at=fetched_at,
                        discovery_lanes=previous.get("discoveryLanes") if previous else ["thread"],
                        discovered_queries=previous.get("discoveredQueries") if previous else None,
                        previous=previous,
                        lane="thread",
                    )
                    pending_posts[reply_uri] = normalized_reply
                    pending_snapshots.append(
                        _normalize_snapshot(reply_item, fetched_at=fetched_at, previous=latest_snapshot_map.get(reply_uri))
                    )
                    pending_profiles.extend(_build_profiles_from_posts([normalized_reply], fetched_at=fetched_at, lane="thread"))
                    interaction = _normalize_interaction_post(reply_item, fetched_at=fetched_at, source_lane="thread")
                    if interaction:
                        pending_interactions.append(interaction)
                        thread_reply_count += 1
            for quote_item in quotes:
                quote_uri = _extract_uri(quote_item)
                if not quote_uri:
                    continue
                previous = pending_posts.get(quote_uri) or existing_post_map.get(quote_uri)
                normalized_quote = _normalize_post(
                    quote_item,
                    fetched_at=fetched_at,
                    discovery_lanes=previous.get("discoveryLanes") if previous else ["quote"],
                    discovered_queries=previous.get("discoveredQueries") if previous else None,
                    previous=previous,
                    lane="quote",
                )
                pending_posts[quote_uri] = normalized_quote
                pending_snapshots.append(
                    _normalize_snapshot(quote_item, fetched_at=fetched_at, previous=latest_snapshot_map.get(quote_uri))
                )
                pending_profiles.extend(_build_profiles_from_posts([normalized_quote], fetched_at=fetched_at, lane="quote"))
                interaction = _normalize_interaction_post(quote_item, fetched_at=fetched_at, source_lane="quote")
                if interaction:
                    pending_interactions.append(interaction)
            for repost_actor in reposted_by:
                pending_profiles.extend(
                    _build_profiles_from_reposters([repost_actor], fetched_at=fetched_at, lane="repost", target_uri=uri)
                )
                pending_interactions.append(
                    _normalize_repost_interaction(target_uri=uri, actor=repost_actor, fetched_at=fetched_at, source_lane="repost")
                )
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": uri,
                "kind": "bluesky",
                "success": True,
                "itemsFetched": thread_reply_count + len(quotes) + len(reposted_by),
                "error": None,
                "durationMs": round((perf_counter() - source_started) * 1000, 1),
                "threadReplies": thread_reply_count,
                "quotesFetched": len(quotes),
                "repostedByFetched": len(reposted_by),
            }
            source_timings[source_key] = source_updates[source_key]["durationMs"]
        except Exception as error:
            duration_ms = round((perf_counter() - source_started) * 1000, 1)
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": uri,
                "kind": "bluesky",
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
            }
            source_timings[source_key] = duration_ms

    merged_posts = _merge_posts(posts, pending_posts.values())
    merged_snapshots = _merge_snapshots(snapshots, pending_snapshots)
    merged_profiles = _merge_profiles(profiles, _dedupe_profiles_for_run(pending_profiles), fetched_at=fetched_at)
    merged_interactions = _merge_interactions(interactions, pending_interactions)

    return {
        "posts": merged_posts,
        "snapshots": merged_snapshots,
        "profiles": merged_profiles,
        "interactions": merged_interactions,
        "sourceUpdates": source_updates,
        "fetchedCount": len(pending_posts),
        "snapshotCount": len(pending_snapshots),
        "profileCount": len(merged_profiles),
        "interactionCount": len(merged_interactions),
        "timings": {
            "totalMs": round((perf_counter() - started_at) * 1000, 1),
            "perSourceMs": source_timings,
        },
    }
