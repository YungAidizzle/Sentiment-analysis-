from __future__ import annotations

import html
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List

from backend.youtube_config import (
    YOUTUBE_COMMENT_REPLIES_PER_THREAD,
    YOUTUBE_COMMENT_THREADS_PER_VIDEO,
    YOUTUBE_DISCOVERY_MAX_AGE_HOURS,
    YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN,
    YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN,
    YOUTUBE_MAX_COMMENT_VIDEOS_PER_RUN,
    YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN,
    YOUTUBE_MAX_RELATED_RESULTS_PER_SEED,
    YOUTUBE_MAX_RELATED_SEEDS_PER_RUN,
    YOUTUBE_MAX_SNAPSHOTS_PER_VIDEO,
    YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN,
    YOUTUBE_QUOTA_BUDGET_PER_RUN,
    YOUTUBE_SEARCH_RESULTS_PER_QUERY,
)

RequestJson = Callable[[str], Any]
BuildYouTubeUrl = Callable[[str, Dict[str, Any]], str]

YOUTUBE_QUERY_STOP_WORDS = {
    "about",
    "after",
    "again",
    "analysis",
    "breaking",
    "comment",
    "comments",
    "explain",
    "explained",
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


def _to_iso_utc_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def _compact_count(value: int) -> str:
    absolute = abs(value)
    for threshold, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if absolute >= threshold:
            return f"{value / threshold:.1f}".rstrip("0").rstrip(".") + suffix
    return str(value)


def _interaction_counts(*, posts: int = 0, reposts: int = 0, comments: int = 0, likes: int = 0) -> Dict[str, int]:
    return {
        "posts": max(0, int(posts)),
        "reposts": max(0, int(reposts)),
        "comments": max(0, int(comments)),
        "likes": max(0, int(likes)),
    }


def _youtube_score(view_count: int, like_count: int, comment_count: int) -> int:
    if view_count <= 0 and like_count <= 0 and comment_count <= 0:
        return 0
    weighted = (
        6
        + math.log10(view_count + 1) * 2.5
        + math.log10(like_count + 1) * 6.5
        + math.log10(comment_count + 1) * 8
    )
    return max(1, min(100, round(weighted)))


def _batched(values: List[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _extract_youtube_video_id(item: Dict[str, Any]) -> str:
    item_id = str(item.get("id", "") or "")
    if item_id.startswith("youtube:"):
        parts = item_id.split(":")
        return parts[-1]
    video_id = str(item.get("videoId", "") or "")
    return video_id


def _youtube_cost(resource: str) -> int:
    return 100 if resource == "search" else 1


def _consume_budget(budget: Dict[str, int], resource: str) -> bool:
    cost = _youtube_cost(resource)
    if budget["remaining"] < cost:
        return False
    budget["remaining"] -= cost
    budget["used"] += cost
    return True


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


def _normalize_query_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _extract_query_phrases(value: str, *, max_phrases: int = 6) -> List[str]:
    tokens = [_normalize_query_token(part) for part in re.split(r"[^A-Za-z0-9]+", value or "")]
    informative = [
        token
        for token in tokens
        if len(token) >= 3 and token not in YOUTUBE_QUERY_STOP_WORDS and not token.isdigit()
    ]
    phrases: List[str] = []
    seen: set[str] = set()
    for size in (3, 2):
        for index in range(0, max(0, len(informative) - size + 1)):
            phrase = " ".join(informative[index:index + size]).strip()
            if not phrase or phrase in seen:
                continue
            seen.add(phrase)
            phrases.append(phrase)
            if len(phrases) >= max_phrases:
                return phrases
    return phrases


def _rotated_slice(values: List[str], limit: int, *, reference_time: datetime) -> List[str]:
    if limit <= 0 or not values:
        return []
    if len(values) <= limit:
        return values[:]
    offset = int(reference_time.timestamp() // 3600) % len(values)
    return (values[offset:] + values[:offset])[:limit]


def _breakout_queries(
    *,
    reddit_posts: Iterable[Dict[str, Any]],
    public_items: Iterable[Dict[str, Any]],
    limit: int,
) -> List[str]:
    weighted: Dict[str, float] = {}
    support: Dict[str, set[str]] = {}
    for row in list(reddit_posts) + list(public_items):
        title = _clean_text(str(row.get("title", "") or ""))
        if not title:
            continue
        source_key = str(row.get("subreddit") or row.get("sourceName") or row.get("id") or "").lower()
        row_weight = (
            1
            + math.log10(int(row.get("score", 0) or 0) + 1) * 6
            + math.log10(int(row.get("numComments", 0) or 0) + 1) * 4
        )
        for phrase in _extract_query_phrases(title):
            weighted[phrase] = weighted.get(phrase, 0.0) + row_weight
            support.setdefault(phrase, set()).add(source_key)
    ranked = sorted(
        weighted.items(),
        key=lambda entry: (len(support.get(entry[0], set())), entry[1], len(entry[0])),
        reverse=True,
    )
    return [
        phrase
        for phrase, _ in ranked
        if len(support.get(phrase, set())) >= 2 or weighted.get(phrase, 0.0) >= 16
    ][:limit]


def _priority_score(item: Dict[str, Any], *, reference_time: datetime) -> float:
    created_utc = int(item.get("createdUtc", 0) or 0)
    age_hours = max(
        1.0,
        (reference_time.timestamp() - created_utc) / 3600 if created_utc else 24.0,
    )
    freshness_multiplier = 1.55 if age_hours <= 12 else 1.25 if age_hours <= 48 else 0.8 if age_hours <= 96 else 0.4
    base = (
        float(item.get("score", 0) or 0)
        + math.log10(int(item.get("viewCount", 0) or 0) + 1) * 3
        + math.log10(int(item.get("likeCount", 0) or 0) + 1) * 6
        + math.log10(int(item.get("commentCount", 0) or 0) + 1) * 8
    )
    delta = (
        math.log10(int(item.get("deltaViewCount", 0) or 0) + 1) * 4
        + math.log10(int(item.get("deltaLikeCount", 0) or 0) + 1) * 8
        + math.log10(int(item.get("deltaCommentCount", 0) or 0) + 1) * 11
    )
    return round(base * freshness_multiplier + delta, 2)


def _queue_video(pending: Dict[str, Dict[str, Any]], seed: Dict[str, Any]) -> None:
    video_id = str(seed.get("videoId", "") or "").strip()
    if not video_id:
        return
    current = pending.get(video_id, {})
    pending[video_id] = {
        **current,
        **seed,
        "videoId": video_id,
        "createdUtc": max(int(current.get("createdUtc", 0) or 0), int(seed.get("createdUtc", 0) or 0)),
        "subscriberCount": max(int(current.get("subscriberCount", 0) or 0), int(seed.get("subscriberCount", 0) or 0)),
        "viewCount": max(int(current.get("viewCount", 0) or 0), int(seed.get("viewCount", 0) or 0)),
        "likeCount": max(int(current.get("likeCount", 0) or 0), int(seed.get("likeCount", 0) or 0)),
        "commentCount": max(int(current.get("commentCount", 0) or 0), int(seed.get("commentCount", 0) or 0)),
        "discoveryLanes": _merge_unique_strings(current.get("discoveryLanes"), seed.get("discoveryLanes")),
        "discoveredQueries": _merge_unique_strings(current.get("discoveredQueries"), seed.get("discoveredQueries")),
        "topicHints": _merge_unique_strings(current.get("topicHints"), seed.get("topicHints")),
    }


def _queue_search_results(
    *,
    query: str,
    pending: Dict[str, Dict[str, Any]],
    budget: Dict[str, int],
    request_json: RequestJson,
    build_url: BuildYouTubeUrl,
    lane: str,
    active_cutoff: int,
    max_results: int,
    related_to_video_id: str | None = None,
) -> int:
    if max_results <= 0 or not _consume_budget(budget, "search"):
        return 0
    params: Dict[str, Any] = {
        "part": "snippet",
        "type": "video",
        "maxResults": max_results,
        "publishedAfter": datetime.fromtimestamp(active_cutoff, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    if related_to_video_id:
        params["relatedToVideoId"] = related_to_video_id
    else:
        params["q"] = query
        params["order"] = "date"
    payload = request_json(build_url("search", params))
    if not isinstance(payload, dict):
        return 0
    queued = 0
    for item in payload.get("items", []):
        item_id = item.get("id", {})
        snippet = item.get("snippet", {})
        if not isinstance(item_id, dict) or not isinstance(snippet, dict):
            continue
        video_id = str(item_id.get("videoId", "") or "")
        created_utc = _to_iso_utc_timestamp(str(snippet.get("publishedAt") or "")) or 0
        if not video_id or created_utc < active_cutoff:
            continue
        _queue_video(
            pending,
            {
                "videoId": video_id,
                "createdUtc": created_utc,
                "channelId": str(snippet.get("channelId", "") or ""),
                "channelTitle": _clean_text(str(snippet.get("channelTitle", "") or "")),
                "fallbackTitle": _clean_text(str(snippet.get("title", "") or "")),
                "fallbackDescription": _truncate_text(_clean_text(str(snippet.get("description", "") or "")), 280),
                "discoveryLanes": [lane],
                "discoveredQueries": [query] if query else [],
                "topicHints": _merge_unique_strings(
                    _extract_query_phrases(query, max_phrases=3),
                    _extract_query_phrases(f"{snippet.get('title', '')} {snippet.get('description', '')}", max_phrases=6),
                ),
            },
        )
        queued += 1
    return queued


def _queue_channel_uploads(
    channel_refs: Iterable[Dict[str, Any]],
    *,
    pending: Dict[str, Dict[str, Any]],
    budget: Dict[str, int],
    request_json: RequestJson,
    build_url: BuildYouTubeUrl,
    lane: str,
    active_cutoff: int,
    per_channel_limit: int,
    min_subscriber_count: int,
    reference_time: datetime,
) -> tuple[int, List[Dict[str, Any]]]:
    queued = 0
    discovered_channels: List[Dict[str, Any]] = []
    for ref in channel_refs:
        handle = str((ref or {}).get("handle", "") or "").strip().lstrip("@")
        channel_id = str((ref or {}).get("channelId", "") or "").strip()
        params = {"part": "snippet,contentDetails,statistics"}
        if handle:
            params["forHandle"] = handle
        elif channel_id:
            params["id"] = channel_id
        else:
            continue
        if not _consume_budget(budget, "channels"):
            break
        payload = request_json(build_url("channels", params))
        if not isinstance(payload, dict):
            continue
        rows = payload.get("items", [])
        if not isinstance(rows, list) or not rows:
            continue
        row = rows[0]
        if not isinstance(row, dict):
            continue
        snippet = row.get("snippet", {})
        content_details = row.get("contentDetails", {})
        statistics = row.get("statistics", {})
        if not isinstance(snippet, dict) or not isinstance(content_details, dict) or not isinstance(statistics, dict):
            continue
        related_playlists = content_details.get("relatedPlaylists", {})
        if not isinstance(related_playlists, dict):
            continue
        uploads_playlist_id = str(related_playlists.get("uploads", "") or "")
        subscriber_count = _parse_int(statistics.get("subscriberCount"))
        if not uploads_playlist_id or (subscriber_count and subscriber_count < min_subscriber_count):
            continue
        discovered_channels.append(
            {
                "channelId": str(row.get("id", "") or ""),
                "channelHandle": f"@{handle}" if handle else "",
                "title": _clean_text(str(snippet.get("title", "") or "")),
                "subscriberCount": subscriber_count,
                "uploadsPlaylistId": uploads_playlist_id,
                "discoveryLanes": [lane],
                "fetchedAt": reference_time.isoformat(),
                "lastSeenAt": reference_time.isoformat(),
            }
        )
        if not _consume_budget(budget, "playlistItems"):
            continue
        playlist_payload = request_json(
            build_url(
                "playlistItems",
                {
                    "part": "snippet,contentDetails",
                    "playlistId": uploads_playlist_id,
                    "maxResults": per_channel_limit,
                },
            )
        )
        if not isinstance(playlist_payload, dict):
            continue
        for item in playlist_payload.get("items", []):
            item_snippet = item.get("snippet", {})
            item_content_details = item.get("contentDetails", {})
            if not isinstance(item_snippet, dict) or not isinstance(item_content_details, dict):
                continue
            video_id = str(item_content_details.get("videoId", "") or "")
            created_utc = _to_iso_utc_timestamp(str(item_content_details.get("videoPublishedAt") or item_snippet.get("publishedAt") or "")) or 0
            if not video_id or created_utc < active_cutoff:
                continue
            _queue_video(
                pending,
                {
                    "videoId": video_id,
                    "createdUtc": created_utc,
                    "channelId": str(row.get("id", "") or ""),
                    "channelHandle": f"@{handle}" if handle else "",
                    "channelTitle": _clean_text(str(snippet.get("title", "") or "")),
                    "subscriberCount": subscriber_count,
                    "fallbackTitle": _clean_text(str(item_snippet.get("title", "") or "")),
                    "fallbackDescription": _truncate_text(_clean_text(str(item_snippet.get("description", "") or "")), 280),
                    "discoveryLanes": [lane],
                    "topicHints": _extract_query_phrases(f"{item_snippet.get('title', '')} {item_snippet.get('description', '')}", max_phrases=6),
                },
            )
            queued += 1
    return queued, discovered_channels


def _hydrate_videos(
    pending: Dict[str, Dict[str, Any]],
    *,
    budget: Dict[str, int],
    request_json: RequestJson,
    build_url: BuildYouTubeUrl,
    reference_time: datetime,
    active_cutoff: int,
) -> List[Dict[str, Any]]:
    selected = sorted(
        pending.values(),
        key=lambda seed: (
            _priority_score(seed, reference_time=reference_time),
            int(seed.get("createdUtc", 0) or 0),
            str(seed.get("videoId", "")),
        ),
        reverse=True,
    )[:YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN]
    if not selected:
        return []
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    for batch_ids in _batched([str(seed.get("videoId", "") or "") for seed in selected], 50):
        if not _consume_budget(budget, "videos"):
            break
        payload = request_json(build_url("videos", {"part": "snippet,statistics", "id": ",".join(batch_ids)}))
        if not isinstance(payload, dict):
            continue
        for row in payload.get("items", []):
            if isinstance(row, dict):
                rows_by_id[str(row.get("id", "") or "")] = row
    fetched_at = reference_time.isoformat()
    items: List[Dict[str, Any]] = []
    for seed in selected:
        video_id = str(seed.get("videoId", "") or "")
        row = rows_by_id.get(video_id, {})
        snippet = row.get("snippet", {})
        statistics = row.get("statistics", {})
        if not isinstance(snippet, dict) or not isinstance(statistics, dict):
            continue
        title = _clean_text(str(snippet.get("title", "") or seed.get("fallbackTitle", "") or ""))
        created_utc = _to_iso_utc_timestamp(str(snippet.get("publishedAt") or "")) or int(seed.get("createdUtc", 0) or 0)
        if not title or created_utc < active_cutoff:
            continue
        description = _truncate_text(_clean_text(str(snippet.get("description", "") or seed.get("fallbackDescription", "") or "")), 280)
        view_count = _parse_int(statistics.get("viewCount"))
        like_count = _parse_int(statistics.get("likeCount"))
        comment_count = _parse_int(statistics.get("commentCount"))
        items.append(
            {
                "id": f"youtube:{video_id}",
                "source": "news",
                "sourceType": "youtube",
                "sourceName": _clean_text(str(snippet.get("channelTitle", "") or seed.get("channelTitle", "") or "YouTube")),
                "title": title,
                "summary": f"Likes {_compact_count(like_count)}. Comments {_compact_count(comment_count)}. Views {_compact_count(view_count)}. {description}".strip(),
                "description": description,
                "author": _clean_text(str(snippet.get("channelTitle", "") or seed.get("channelTitle", "") or "")),
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "createdUtc": created_utc,
                "score": _youtube_score(view_count, like_count, comment_count),
                "numComments": comment_count,
                "interactionCounts": _interaction_counts(posts=1, comments=comment_count, likes=like_count),
                "channelId": str(snippet.get("channelId", "") or seed.get("channelId", "") or ""),
                "channelHandle": str(seed.get("channelHandle", "") or ""),
                "subscriberCount": int(seed.get("subscriberCount", 0) or 0),
                "viewCount": view_count,
                "likeCount": like_count,
                "commentCount": comment_count,
                "discoveryLanes": _merge_unique_strings(seed.get("discoveryLanes")),
                "discoveredQueries": _merge_unique_strings(seed.get("discoveredQueries")),
                "topicHints": _merge_unique_strings(seed.get("topicHints"), _extract_query_phrases(f"{title} {description}", max_phrases=8)),
                "fetchedAt": fetched_at,
            }
        )
    return items


def _merge_snapshots(
    existing_snapshots: Iterable[Dict[str, Any]],
    incoming_items: Iterable[Dict[str, Any]],
    *,
    active_cutoff: int,
) -> tuple[List[Dict[str, Any]], int]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for snapshot in existing_snapshots:
        video_id = str(snapshot.get("videoId", "") or "")
        created_utc = int(snapshot.get("createdUtc", 0) or 0)
        if not video_id or created_utc < active_cutoff:
            continue
        grouped.setdefault(video_id, []).append(snapshot)

    added = 0
    for item in incoming_items:
        video_id = _extract_youtube_video_id(item)
        if not video_id:
            continue
        observed_at = str(item.get("fetchedAt", "") or "")
        observed_utc = _to_iso_utc_timestamp(observed_at) or int(item.get("createdUtc", 0) or 0)
        current = {
            "id": f"youtube-snapshot:{video_id}:{observed_utc}",
            "videoId": video_id,
            "fetchedAt": observed_at,
            "createdUtc": observed_utc,
            "viewCount": int(item.get("viewCount", 0) or 0),
            "likeCount": int(item.get("likeCount", 0) or 0),
            "commentCount": int(item.get("commentCount", 0) or 0),
        }
        previous = grouped.get(video_id, [])[-1] if grouped.get(video_id) else None
        if previous:
            current["deltaViewCount"] = max(0, current["viewCount"] - int(previous.get("viewCount", 0) or 0))
            current["deltaLikeCount"] = max(0, current["likeCount"] - int(previous.get("likeCount", 0) or 0))
            current["deltaCommentCount"] = max(0, current["commentCount"] - int(previous.get("commentCount", 0) or 0))
            current["deltaWindowMinutes"] = round(max(0.0, (observed_utc - int(previous.get("createdUtc", 0) or 0)) / 60), 1)
        else:
            current["deltaViewCount"] = 0
            current["deltaLikeCount"] = 0
            current["deltaCommentCount"] = 0
            current["deltaWindowMinutes"] = 0.0
        if previous and current["id"] == previous.get("id"):
            grouped[video_id][-1] = current
            continue
        grouped.setdefault(video_id, []).append(current)
        added += 1

    flattened: List[Dict[str, Any]] = []
    for video_id, rows in grouped.items():
        kept = sorted(rows, key=lambda row: (int(row.get("createdUtc", 0) or 0), str(row.get("id", "") or "")))[-YOUTUBE_MAX_SNAPSHOTS_PER_VIDEO:]
        flattened.extend(kept)
    return (
        sorted(flattened, key=lambda row: (int(row.get("createdUtc", 0) or 0), str(row.get("id", "") or "")), reverse=True),
        added,
    )


def _apply_snapshot_context(items: Iterable[Dict[str, Any]], snapshots: Iterable[Dict[str, Any]], *, reference_time: datetime) -> List[Dict[str, Any]]:
    by_video: Dict[str, List[Dict[str, Any]]] = {}
    for snapshot in snapshots:
        by_video.setdefault(str(snapshot.get("videoId", "") or ""), []).append(snapshot)
    enriched: List[Dict[str, Any]] = []
    for item in items:
        video_id = _extract_youtube_video_id(item)
        ordered = sorted(by_video.get(video_id, []), key=lambda row: int(row.get("createdUtc", 0) or 0))
        if not ordered:
            enriched.append(item)
            continue
        first = ordered[0]
        latest = ordered[-1]
        enriched_item = {
            **item,
            "firstObservedAt": first.get("fetchedAt") or item.get("fetchedAt"),
            "lastSnapshotAt": latest.get("fetchedAt") or item.get("fetchedAt"),
            "snapshotCount": len(ordered),
            "initialViewCount": int(first.get("viewCount", item.get("viewCount", 0)) or 0),
            "initialLikeCount": int(first.get("likeCount", item.get("likeCount", 0)) or 0),
            "initialCommentCount": int(first.get("commentCount", item.get("commentCount", 0)) or 0),
            "deltaViewCount": int(latest.get("deltaViewCount", 0) or 0),
            "deltaLikeCount": int(latest.get("deltaLikeCount", 0) or 0),
            "deltaCommentCount": int(latest.get("deltaCommentCount", 0) or 0),
            "deltaWindowMinutes": float(latest.get("deltaWindowMinutes", 0) or 0),
        }
        enriched_item["interactionCounts"] = _interaction_counts(
            posts=1,
            comments=int(enriched_item.get("initialCommentCount", enriched_item.get("commentCount", 0)) or 0),
            likes=int(enriched_item.get("initialLikeCount", enriched_item.get("likeCount", 0)) or 0),
        )
        enriched_item["priorityScore"] = _priority_score(enriched_item, reference_time=reference_time)
        enriched.append(enriched_item)
    return enriched


def _normalize_comment_rows(video_item: Dict[str, Any], payload: Dict[str, Any], *, fetched_at: str) -> List[Dict[str, Any]]:
    video_id = _extract_youtube_video_id(video_item)
    if not video_id:
        return []
    rows: List[Dict[str, Any]] = []
    for thread in payload.get("items", []):
        snippet = thread.get("snippet", {})
        top = snippet.get("topLevelComment", {})
        top_snippet = top.get("snippet", {})
        if not isinstance(snippet, dict) or not isinstance(top, dict) or not isinstance(top_snippet, dict):
            continue
        comment_id = str(top.get("id", "") or "")
        body = _clean_text(str(top_snippet.get("textOriginal") or top_snippet.get("textDisplay") or ""))
        created_utc = _to_iso_utc_timestamp(str(top_snippet.get("publishedAt") or "")) or 0
        if not comment_id or not body or not created_utc:
            continue
        top_level_id = f"youtube-comment:{comment_id}"
        rows.append(
            {
                "id": top_level_id,
                "source": "youtube",
                "sourceType": "youtube_comment",
                "postId": str(video_item.get("id", "") or f"youtube:{video_id}"),
                "parentId": str(video_item.get("id", "") or f"youtube:{video_id}"),
                "videoId": video_id,
                "channelId": str(video_item.get("channelId", "") or ""),
                "sourceName": str(video_item.get("sourceName", "") or "YouTube"),
                "videoTitle": str(video_item.get("title", "") or ""),
                "author": _clean_text(str(top_snippet.get("authorDisplayName", "") or "")),
                "authorChannelId": str((((top_snippet.get("authorChannelId") or {}) or {}).get("value", "")) if isinstance(top_snippet.get("authorChannelId"), dict) else ""),
                "body": body,
                "createdUtc": created_utc,
                "score": _parse_int(top_snippet.get("likeCount")),
                "likeCount": _parse_int(top_snippet.get("likeCount")),
                "replyCount": _parse_int(snippet.get("totalReplyCount")),
                "isReply": False,
                "url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
                "fetchedAt": fetched_at,
            }
        )
        replies = thread.get("replies", {})
        reply_rows = replies.get("comments", []) if isinstance(replies, dict) else []
        for reply in list(reply_rows)[:YOUTUBE_COMMENT_REPLIES_PER_THREAD]:
            reply_snippet = reply.get("snippet", {})
            if not isinstance(reply_snippet, dict):
                continue
            reply_id = str(reply.get("id", "") or "")
            reply_body = _clean_text(str(reply_snippet.get("textOriginal") or reply_snippet.get("textDisplay") or ""))
            reply_created_utc = _to_iso_utc_timestamp(str(reply_snippet.get("publishedAt") or "")) or 0
            if not reply_id or not reply_body or not reply_created_utc:
                continue
            rows.append(
                {
                    "id": f"youtube-comment:{reply_id}",
                    "source": "youtube",
                    "sourceType": "youtube_comment",
                    "postId": str(video_item.get("id", "") or f"youtube:{video_id}"),
                    "parentId": top_level_id,
                    "videoId": video_id,
                    "channelId": str(video_item.get("channelId", "") or ""),
                    "sourceName": str(video_item.get("sourceName", "") or "YouTube"),
                    "videoTitle": str(video_item.get("title", "") or ""),
                    "author": _clean_text(str(reply_snippet.get("authorDisplayName", "") or "")),
                    "authorChannelId": str((((reply_snippet.get("authorChannelId") or {}) or {}).get("value", "")) if isinstance(reply_snippet.get("authorChannelId"), dict) else ""),
                    "body": reply_body,
                    "createdUtc": reply_created_utc,
                    "score": _parse_int(reply_snippet.get("likeCount")),
                    "likeCount": _parse_int(reply_snippet.get("likeCount")),
                    "replyCount": 0,
                    "isReply": True,
                    "url": f"https://www.youtube.com/watch?v={video_id}&lc={reply_id}",
                    "fetchedAt": fetched_at,
                }
            )
    return rows


def _merge_comments(existing_comments: Iterable[Dict[str, Any]], incoming_comments: Iterable[Dict[str, Any]], *, active_cutoff: int, active_video_ids: set[str]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing_comments:
        comment_id = str(row.get("id", "") or "")
        created_utc = int(row.get("createdUtc", 0) or 0)
        post_id = str(row.get("postId", "") or "")
        if comment_id and created_utc >= active_cutoff and post_id in active_video_ids:
            merged[comment_id] = row
    for row in incoming_comments:
        comment_id = str(row.get("id", "") or "")
        if not comment_id:
            continue
        existing = merged.get(comment_id, {})
        fetched_at = row.get("fetchedAt") or existing.get("lastFetchedAt")
        merged[comment_id] = {**existing, **row, "firstSeenAt": existing.get("firstSeenAt") or fetched_at, "lastFetchedAt": fetched_at}
    return sorted(merged.values(), key=lambda row: (int(row.get("createdUtc", 0) or 0), str(row.get("id", "") or "")), reverse=True)


def _merge_channels(existing_channels: Iterable[Dict[str, Any]], incoming_channels: Iterable[Dict[str, Any]], *, active_channel_ids: set[str]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing_channels:
        channel_id = str(row.get("channelId", "") or "")
        if channel_id and (not active_channel_ids or channel_id in active_channel_ids):
            merged[channel_id] = row
    for row in incoming_channels:
        channel_id = str(row.get("channelId", "") or "")
        if not channel_id:
            continue
        current = merged.get(channel_id, {})
        merged[channel_id] = {
            **current,
            **row,
            "firstSeenAt": current.get("firstSeenAt") or row.get("fetchedAt"),
            "lastSeenAt": row.get("lastSeenAt") or current.get("lastSeenAt") or row.get("fetchedAt"),
            "subscriberCount": max(int(current.get("subscriberCount", 0) or 0), int(row.get("subscriberCount", 0) or 0)),
            "discoveryLanes": _merge_unique_strings(current.get("discoveryLanes"), row.get("discoveryLanes")),
        }
    return sorted(merged.values(), key=lambda row: (int(row.get("subscriberCount", 0) or 0), str(row.get("title", "") or "")), reverse=True)


def _merge_public_items(existing_items: Iterable[Dict[str, Any]], incoming_items: Iterable[Dict[str, Any]], *, active_cutoff: int) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        str(item.get("id", "") or ""): item
        for item in existing_items
        if str(item.get("id", "") or "") and int(item.get("createdUtc", 0) or 0) >= active_cutoff
    }
    for item in incoming_items:
        item_id = str(item.get("id", "") or "")
        if not item_id or int(item.get("createdUtc", 0) or 0) < active_cutoff:
            continue
        current = merged.get(item_id, {})
        merged[item_id] = {**current, **item, "discoveryLanes": _merge_unique_strings(current.get("discoveryLanes"), item.get("discoveryLanes")), "topicHints": _merge_unique_strings(current.get("topicHints"), item.get("topicHints"))}
    return sorted(merged.values(), key=lambda item: (int(item.get("createdUtc", 0) or 0), int(item.get("commentCount", 0) or 0), int(item.get("score", 0) or 0), str(item.get("id", "") or "")), reverse=True)


def _select_comment_videos(items: Iterable[Dict[str, Any]], existing_comments: Iterable[Dict[str, Any]], *, reference_time: datetime) -> List[Dict[str, Any]]:
    latest_by_post: Dict[str, datetime] = {}
    for row in existing_comments:
        post_id = str(row.get("postId", "") or "")
        fetched_at = datetime.fromisoformat(str(row.get("lastFetchedAt") or row.get("fetchedAt") or "").replace("Z", "+00:00")) if row.get("fetchedAt") or row.get("lastFetchedAt") else None
        if post_id and fetched_at is not None:
            latest_by_post[post_id] = max(latest_by_post.get(post_id, fetched_at), fetched_at)
    candidates = []
    for item in items:
        if item.get("sourceType") != "youtube" or int(item.get("commentCount", 0) or 0) <= 0:
            continue
        post_id = str(item.get("id", "") or "")
        last_sync = latest_by_post.get(post_id)
        stale_minutes = (reference_time - last_sync).total_seconds() / 60 if last_sync else YOUTUBE_COMMENT_THREADS_PER_VIDEO * 2
        if stale_minutes < 30 and int(item.get("deltaCommentCount", 0) or 0) <= 0:
            continue
        candidates.append({**item, "_commentPriority": _priority_score(item, reference_time=reference_time) + math.log10(int(item.get("commentCount", 0) or 0) + 1) * 6 + (12 if int(item.get("deltaCommentCount", 0) or 0) > 0 else 0)})
    return sorted(candidates, key=lambda row: (float(row.get("_commentPriority", 0) or 0), int(row.get("commentCount", 0) or 0)), reverse=True)[:YOUTUBE_MAX_COMMENT_VIDEOS_PER_RUN]


def fetch_youtube_feed_items(
    feed: Dict[str, Any],
    cutoff_utc: int,
    *,
    request_json: RequestJson,
    build_url: BuildYouTubeUrl,
    reference_time: datetime | None = None,
) -> List[Dict[str, Any]]:
    result = refresh_youtube_sources(
        feeds=[{**feed, "lane": str(feed.get("lane", "curated") or "curated")}],
        cutoff_utc=cutoff_utc,
        existing_items=[],
        existing_comments=[],
        existing_snapshots=[],
        existing_channels=[],
        reddit_posts=[],
        public_items=[],
        request_json=request_json,
        build_url=build_url,
        reference_time=reference_time,
        refresh_enabled=True,
    )
    return result["incomingItems"]


def refresh_youtube_sources(
    *,
    feeds: Iterable[Dict[str, Any]],
    cutoff_utc: int,
    existing_items: Iterable[Dict[str, Any]],
    existing_comments: Iterable[Dict[str, Any]],
    existing_snapshots: Iterable[Dict[str, Any]],
    existing_channels: Iterable[Dict[str, Any]],
    reddit_posts: Iterable[Dict[str, Any]],
    public_items: Iterable[Dict[str, Any]],
    request_json: RequestJson,
    build_url: BuildYouTubeUrl,
    reference_time: datetime | None = None,
    refresh_enabled: bool = True,
) -> Dict[str, Any]:
    now = reference_time or datetime.now(timezone.utc)
    current_items = [item for item in existing_items if item.get("sourceType") == "youtube"]
    active_cutoff = max(cutoff_utc, int((now - timedelta(hours=YOUTUBE_DISCOVERY_MAX_AGE_HOURS)).timestamp()))
    current_public_items = [item for item in public_items if item.get("sourceType") != "youtube"]
    budget = {"remaining": YOUTUBE_QUOTA_BUDGET_PER_RUN, "used": 0}
    source_updates: Dict[str, Dict[str, Any]] = {}

    if not refresh_enabled:
        active_video_ids = {str(item.get("id", "") or "") for item in current_items}
        return {
            "incomingItems": [],
            "comments": _merge_comments(existing_comments, [], active_cutoff=active_cutoff, active_video_ids=active_video_ids),
            "snapshots": _merge_snapshots(existing_snapshots, [], active_cutoff=active_cutoff)[0],
            "channels": _merge_channels(existing_channels, [], active_channel_ids={str(item.get("channelId", "") or "") for item in current_items if str(item.get("channelId", "") or "")}),
            "sourceUpdates": {f"public:{feed['id']}": {"sourceId": f"public:{feed['id']}", "label": feed["label"], "kind": feed["kind"], "success": True, "itemsFetched": 0, "error": None, "skipped": True} for feed in feeds},
            "fetchedCount": 0,
            "commentFetchedCount": 0,
            "snapshotFetchedCount": 0,
        }

    pending: Dict[str, Dict[str, Any]] = {}
    for item in sorted(current_items, key=lambda row: _priority_score(row, reference_time=now), reverse=True)[:YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN]:
        video_id = _extract_youtube_video_id(item)
        if video_id:
            _queue_video(pending, {**item, "videoId": video_id, "discoveryLanes": _merge_unique_strings(item.get("discoveryLanes"), ["tracked-refresh"])})

    breakout = _breakout_queries(reddit_posts=reddit_posts, public_items=current_public_items, limit=YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN)
    discovered_channels: List[Dict[str, Any]] = []

    for feed in feeds:
        source_key = f"public:{feed['id']}"
        lane = str(feed.get("lane", "curated") or "curated")
        try:
            queued = 0
            if lane == "curated":
                queued, new_channels = _queue_channel_uploads(
                    list(feed.get("channels") or []),
                    pending=pending,
                    budget=budget,
                    request_json=request_json,
                    build_url=build_url,
                    lane="curated",
                    active_cutoff=active_cutoff,
                    per_channel_limit=max(1, int(feed.get("recentUploadsPerChannel", 3) or 3)),
                    min_subscriber_count=max(0, int(feed.get("minSubscriberCount", 0) or 0)),
                    reference_time=now,
                )
                discovered_channels.extend(new_channels)
            elif lane == "keyword":
                for query in _rotated_slice(list(feed.get("queries") or []), min(YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN, int(feed.get("maxQueriesPerRun", YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN) or YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN)), reference_time=now):
                    queued += _queue_search_results(query=query, pending=pending, budget=budget, request_json=request_json, build_url=build_url, lane="keyword", active_cutoff=active_cutoff, max_results=max(1, int(feed.get("maxResults", YOUTUBE_SEARCH_RESULTS_PER_QUERY) or YOUTUBE_SEARCH_RESULTS_PER_QUERY)))
            elif lane == "breakout":
                for query in breakout[: int(feed.get("maxQueriesPerRun", YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN) or YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN)]:
                    queued += _queue_search_results(query=query, pending=pending, budget=budget, request_json=request_json, build_url=build_url, lane="breakout", active_cutoff=active_cutoff, max_results=max(1, int(feed.get("maxResults", YOUTUBE_SEARCH_RESULTS_PER_QUERY) or YOUTUBE_SEARCH_RESULTS_PER_QUERY)))
            elif lane == "related":
                seed_ids = [_extract_youtube_video_id(item) for item in sorted(current_items, key=lambda row: _priority_score(row, reference_time=now), reverse=True)[: min(YOUTUBE_MAX_RELATED_SEEDS_PER_RUN, int(feed.get("maxSeedVideos", YOUTUBE_MAX_RELATED_SEEDS_PER_RUN) or YOUTUBE_MAX_RELATED_SEEDS_PER_RUN))]]
                for video_id in [value for value in seed_ids if value]:
                    queued += _queue_search_results(query="", pending=pending, budget=budget, request_json=request_json, build_url=build_url, lane="related", active_cutoff=active_cutoff, max_results=max(1, int(feed.get("maxResultsPerSeed", YOUTUBE_MAX_RELATED_RESULTS_PER_SEED) or YOUTUBE_MAX_RELATED_RESULTS_PER_SEED)), related_to_video_id=video_id)
            elif lane == "channel-expansion":
                channel_refs = []
                for item in sorted(current_items, key=lambda row: _priority_score(row, reference_time=now), reverse=True):
                    channel_id = str(item.get("channelId", "") or "")
                    if not channel_id:
                        continue
                    channel_refs.append({"channelId": channel_id})
                    if len(channel_refs) >= min(YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN, int(feed.get("channelLimit", YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN) or YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN)):
                        break
                queued, new_channels = _queue_channel_uploads(
                    channel_refs,
                    pending=pending,
                    budget=budget,
                    request_json=request_json,
                    build_url=build_url,
                    lane="channel-expansion",
                    active_cutoff=active_cutoff,
                    per_channel_limit=max(1, int(feed.get("recentUploadsPerChannel", 3) or 3)),
                    min_subscriber_count=max(0, int(feed.get("minSubscriberCount", 0) or 0)),
                    reference_time=now,
                )
                discovered_channels.extend(new_channels)
            source_updates[source_key] = {"sourceId": source_key, "label": feed["label"], "kind": feed["kind"], "success": True, "itemsFetched": queued, "error": None, "skipped": False}
        except Exception as error:
            source_updates[source_key] = {"sourceId": source_key, "label": feed["label"], "kind": feed["kind"], "success": False, "itemsFetched": 0, "error": str(error), "skipped": False}

    hydrated_items = _hydrate_videos(pending, budget=budget, request_json=request_json, build_url=build_url, reference_time=now, active_cutoff=active_cutoff)
    merged_snapshots, snapshot_fetched_count = _merge_snapshots(existing_snapshots, hydrated_items, active_cutoff=active_cutoff)
    hydrated_items = _apply_snapshot_context(hydrated_items, merged_snapshots, reference_time=now)
    merged_youtube_items = _merge_public_items(current_items, hydrated_items, active_cutoff=active_cutoff)

    incoming_comments: List[Dict[str, Any]] = []
    for video_item in _select_comment_videos(merged_youtube_items, existing_comments, reference_time=now):
        if not _consume_budget(budget, "commentThreads"):
            break
        payload = request_json(build_url("commentThreads", {"part": "snippet,replies", "videoId": _extract_youtube_video_id(video_item), "order": "time", "textFormat": "plainText", "maxResults": YOUTUBE_COMMENT_THREADS_PER_VIDEO}))
        if isinstance(payload, dict):
            incoming_comments.extend(_normalize_comment_rows(video_item, payload, fetched_at=now.isoformat()))

    active_video_ids = {str(item.get("id", "") or "") for item in merged_youtube_items}
    merged_comments = _merge_comments(existing_comments, incoming_comments, active_cutoff=active_cutoff, active_video_ids=active_video_ids)
    merged_channels = _merge_channels(existing_channels, discovered_channels, active_channel_ids={str(item.get("channelId", "") or "") for item in merged_youtube_items if str(item.get("channelId", "") or "")})

    return {
        "incomingItems": hydrated_items,
        "comments": merged_comments,
        "snapshots": merged_snapshots,
        "channels": merged_channels,
        "sourceUpdates": source_updates,
        "fetchedCount": len(hydrated_items),
        "commentFetchedCount": len(incoming_comments),
        "snapshotFetchedCount": snapshot_fetched_count,
    }
