from __future__ import annotations

import hashlib
import html
import json
import math
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from time import perf_counter
from typing import Any, Dict, Iterable, List
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from backend.public_source_config import (
    PUBLIC_GOOGLE_TRENDS_FEEDS,
    PUBLIC_JSON_FEEDS,
    PUBLIC_RSS_FEEDS,
    PUBLIC_SOURCE_ITEM_LIMIT,
    PUBLIC_YOUTUBE_FEEDS,
)
from backend.reddit_dev_only_config import REDDIT_USER_AGENT
from backend.reddit_window import rolling_window_cutoff_utc
from backend.youtube_refresh import fetch_youtube_feed_items, refresh_youtube_sources
from backend.youtube_config import (
    YOUTUBE_API_BASE_URL,
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
    YOUTUBE_REFRESH_MINUTES,
    YOUTUBE_SEARCH_RESULTS_PER_QUERY,
    get_youtube_api_key,
    youtube_enabled,
)


def _request_json(url: str) -> Any:
    request = Request(url, headers={"User-Agent": REDDIT_USER_AGENT})
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _request_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": REDDIT_USER_AGENT})
    with urlopen(request, timeout=20) as response:
        return response.read().decode("utf-8")


def _clean_text(value: str | None) -> str:
    if not value:
        return ""

    without_tags = re.sub(r"<[^>]+>", " ", value)
    normalized = html.unescape(without_tags)
    return re.sub(r"\s+", " ", normalized).strip()


def _to_utc_timestamp(value: str | None) -> int | None:
    if not value:
        return None

    try:
        return int(parsedate_to_datetime(value).timestamp())
    except Exception:
        return None


def _to_iso_utc_timestamp(value: str | None) -> int | None:
    if not value:
        return None

    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def _item_key(prefix: str, raw_value: str) -> str:
    digest = hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def _safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or _item_key("slug", value)


def _rss_text(element: ET.Element | None, tag_names: Iterable[str]) -> str:
    if element is None:
        return ""

    for tag_name in tag_names:
        match = element.find(tag_name)
        if match is not None and match.text:
            return _clean_text(match.text)

    return ""


def _google_trends_traffic_score(value: str | None) -> int:
    if not value:
        return 0

    normalized = _clean_text(value).replace(",", "").replace("+", "").strip().upper()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMB])?", normalized)
    if not match:
        return 0

    magnitude = float(match.group(1))
    suffix = match.group(2) or ""
    multiplier = {
        "": 1,
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
    }[suffix]
    traffic = magnitude * multiplier
    return max(1, min(100, round(math.log10(traffic + 1) * 20)))


def _compact_count(value: int) -> str:
    absolute = abs(value)
    for threshold, suffix in (
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ):
        if absolute >= threshold:
            return f"{value / threshold:.1f}".rstrip("0").rstrip(".") + suffix

    return str(value)


def _truncate_text(value: str, max_length: int) -> str:
    if not value or len(value) <= max_length:
        return value

    return value[: max_length - 1].rstrip() + "..."


def _parse_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _youtube_api_url(resource: str, params: Dict[str, Any]) -> str:
    payload = {
        key: value
        for key, value in params.items()
        if value not in (None, "")
    }
    payload["key"] = get_youtube_api_key()
    return f"{YOUTUBE_API_BASE_URL}/{resource}?{urlencode(payload)}"


def _youtube_score(view_count: int, like_count: int, comment_count: int) -> int:
    if view_count <= 0 and like_count <= 0 and comment_count <= 0:
        return 0

    weighted = (
        6
        + math.log10(like_count + 1) * 6.5
        + math.log10(comment_count + 1) * 8
        + math.log10(view_count + 1) * 2.5
    )
    return max(1, min(100, round(weighted)))


def _interaction_counts(*, posts: int = 0, reposts: int = 0, comments: int = 0, likes: int = 0) -> Dict[str, int]:
    return {
        "posts": max(0, int(posts)),
        "reposts": max(0, int(reposts)),
        "comments": max(0, int(comments)),
        "likes": max(0, int(likes)),
    }


def _batched(values: List[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_youtube_video_id(item: Dict[str, Any]) -> str | None:
    item_id = str(item.get("id", "") or "")
    if item_id.startswith("youtube:"):
        parts = item_id.split(":")
        if len(parts) >= 3:
            return parts[-1]
        if len(parts) == 2:
            return parts[1]

    url = str(item.get("url", "") or "")
    if not url:
        return None

    parsed = urlparse(url)
    if parsed.netloc.endswith("youtu.be"):
        return parsed.path.strip("/") or None

    query_video_id = parse_qs(parsed.query).get("v")
    if query_video_id:
        return query_video_id[0]

    if parsed.path.startswith("/shorts/"):
        return parsed.path.split("/shorts/", 1)[1].split("/", 1)[0] or None

    return None


def _canonical_public_item_id(item: Dict[str, Any]) -> str:
    if item.get("sourceType") == "youtube":
        video_id = _extract_youtube_video_id(item)
        if video_id:
            return f"youtube:{video_id}"

    return str(item.get("id", ""))


def _interaction_count_dict(value: Dict[str, Any] | None) -> Dict[str, int]:
    return _interaction_counts(
        posts=int((value or {}).get("posts", 0) or 0),
        reposts=int((value or {}).get("reposts", 0) or 0),
        comments=int((value or {}).get("comments", 0) or 0),
        likes=int((value or {}).get("likes", 0) or 0),
    )


def _merge_interaction_counts(current: Dict[str, Any] | None, incoming: Dict[str, Any] | None) -> Dict[str, int]:
    left = _interaction_count_dict(current)
    right = _interaction_count_dict(incoming)
    return _interaction_counts(
        posts=max(left["posts"], right["posts"]),
        reposts=max(left["reposts"], right["reposts"]),
        comments=max(left["comments"], right["comments"]),
        likes=max(left["likes"], right["likes"]),
    )


def _should_refresh_youtube(existing_items: Iterable[Dict[str, Any]]) -> bool:
    latest_refresh: datetime | None = None
    for item in existing_items:
        if item.get("sourceType") != "youtube":
            continue

        parsed = _parse_iso_datetime(str(item.get("fetchedAt") or ""))
        if parsed is None:
            continue

        latest_refresh = parsed if latest_refresh is None else max(latest_refresh, parsed)

    if latest_refresh is None:
        return True

    return datetime.now(timezone.utc) - latest_refresh >= timedelta(minutes=YOUTUBE_REFRESH_MINUTES)


def _fetch_rss_feed_items(feed: Dict[str, str], cutoff_utc: int) -> List[Dict[str, Any]]:
    root = ET.fromstring(_request_text(feed["url"]))
    fetched_at = datetime.now(timezone.utc).isoformat()
    items: List[Dict[str, Any]] = []
    namespaces = {
        "atom": "http://www.w3.org/2005/Atom",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    entries = root.findall(".//item")
    if not entries:
        entries = root.findall(".//atom:entry", namespaces)

    for entry in entries[:PUBLIC_SOURCE_ITEM_LIMIT]:
        title = _rss_text(entry, ["title", "{http://www.w3.org/2005/Atom}title"])
        if not title:
            continue

        summary = _rss_text(
            entry,
            [
                "description",
                "{http://purl.org/rss/1.0/modules/content/}encoded",
                "summary",
                "{http://www.w3.org/2005/Atom}summary",
            ],
        )
        link = _rss_text(entry, ["link"])
        if not link:
            atom_link = entry.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href", "")

        guid = _rss_text(entry, ["guid", "id", "{http://www.w3.org/2005/Atom}id"])
        published_utc = (
            _to_utc_timestamp(_rss_text(entry, ["pubDate", "published", "{http://www.w3.org/2005/Atom}published"]))
            or _to_utc_timestamp(
                _rss_text(entry, ["updated", "{http://www.w3.org/2005/Atom}updated"])
            )
            or int(datetime.now(timezone.utc).timestamp())
        )
        if published_utc < cutoff_utc:
            continue

        items.append(
            {
                "id": f"rss:{feed['id']}:{guid or _item_key(feed['id'], link or title)}",
                "source": "news",
                "sourceType": "rss",
                "sourceName": feed["label"],
                "title": title,
                "summary": summary,
                "author": _rss_text(entry, ["author", "{http://www.w3.org/2005/Atom}author"]),
                "url": link,
                "createdUtc": published_utc,
                "score": 1,
                "numComments": 0,
                "interactionCounts": _interaction_counts(posts=1),
                "fetchedAt": fetched_at,
            }
        )

    return items


def _fetch_hackernews_items(feed: Dict[str, str], cutoff_utc: int) -> List[Dict[str, Any]]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    item_ids = _request_json(feed["url"])
    if not isinstance(item_ids, list):
        return []

    items: List[Dict[str, Any]] = []
    for item_id in item_ids[:PUBLIC_SOURCE_ITEM_LIMIT]:
        payload = _request_json(feed["itemUrlTemplate"].format(id=item_id))
        if not isinstance(payload, dict):
            continue

        created_utc = int(payload.get("time", 0) or 0)
        if created_utc < cutoff_utc:
            continue
        if payload.get("type") != "story":
            continue

        title = _clean_text(str(payload.get("title", "") or ""))
        if not title:
            continue

        url = str(payload.get("url") or f"https://news.ycombinator.com/item?id={item_id}")
        items.append(
            {
                "id": f"hackernews:{feed['id']}:{item_id}",
                "source": "news",
                "sourceType": "hackernews",
                "sourceName": feed["label"],
                "title": title,
                "summary": _clean_text(str(payload.get("text", "") or "")),
                "author": str(payload.get("by", "") or ""),
                "url": url,
                "createdUtc": created_utc,
                "score": int(payload.get("score", 0) or 0),
                "numComments": int(payload.get("descendants", 0) or 0),
                "interactionCounts": _interaction_counts(
                    posts=1,
                    comments=int(payload.get("descendants", 0) or 0),
                    likes=int(payload.get("score", 0) or 0),
                ),
                "fetchedAt": fetched_at,
            }
        )

    return items


def _fetch_lobsters_items(feed: Dict[str, str], cutoff_utc: int) -> List[Dict[str, Any]]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    payload = _request_json(feed["url"])
    if not isinstance(payload, list):
        return []

    items: List[Dict[str, Any]] = []
    for row in payload[:PUBLIC_SOURCE_ITEM_LIMIT]:
        created_utc = _to_utc_timestamp(str(row.get("created_at") or "")) or 0
        if created_utc < cutoff_utc:
            continue

        title = _clean_text(str(row.get("title", "") or ""))
        if not title:
            continue

        short_id = str(row.get("short_id", "") or "")
        comments_url = str(row.get("comments_url", "") or "")
        url = str(row.get("url", "") or comments_url)
        items.append(
            {
                "id": f"lobsters:{feed['id']}:{short_id or _item_key(feed['id'], url or title)}",
                "source": "news",
                "sourceType": "lobsters",
                "sourceName": feed["label"],
                "title": title,
                "summary": _clean_text(str(row.get("description", "") or "")),
                "author": str(row.get("submitter_user", {}).get("username", "") or ""),
                "url": url,
                "createdUtc": created_utc,
                "score": int(row.get("score", 0) or 0),
                "numComments": int(row.get("comment_count", 0) or 0),
                "interactionCounts": _interaction_counts(
                    posts=1,
                    comments=int(row.get("comment_count", 0) or 0),
                    likes=int(row.get("score", 0) or 0),
                ),
                "fetchedAt": fetched_at,
            }
        )

    return items


def _fetch_google_trends_items(feed: Dict[str, str], cutoff_utc: int) -> List[Dict[str, Any]]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    root = ET.fromstring(_request_text(feed["url"]))
    namespaces = {
        "ht": "https://trends.google.com/trending/rss",
    }
    items: List[Dict[str, Any]] = []

    for entry in root.findall(".//item")[:PUBLIC_SOURCE_ITEM_LIMIT]:
        title = _rss_text(entry, ["title"])
        if not title:
            continue

        published_utc = (
            _to_utc_timestamp(_rss_text(entry, ["pubDate"]))
            or int(datetime.now(timezone.utc).timestamp())
        )
        if published_utc < cutoff_utc:
            continue

        approx_traffic = _clean_text(
            entry.findtext("{https://trends.google.com/trending/rss}approx_traffic", "")
        )
        related_titles: List[str] = []
        related_sources: List[str] = []
        related_urls: List[str] = []
        for news_item in entry.findall("ht:news_item", namespaces):
            news_title = _clean_text(
                news_item.findtext("{https://trends.google.com/trending/rss}news_item_title", "")
            )
            news_source = _clean_text(
                news_item.findtext("{https://trends.google.com/trending/rss}news_item_source", "")
            )
            news_url = _clean_text(
                news_item.findtext("{https://trends.google.com/trending/rss}news_item_url", "")
            )

            if news_title:
                related_titles.append(news_title)
            if news_source:
                related_sources.append(news_source)
            if news_url:
                related_urls.append(news_url)

        summary_parts = []
        if approx_traffic:
            summary_parts.append(f"Approx traffic {approx_traffic}.")
        if related_titles:
            coverage = "; ".join(
                f"{related_sources[index]}: {headline}"
                if index < len(related_sources) and related_sources[index]
                else headline
                for index, headline in enumerate(related_titles)
            )
            summary_parts.append(f"Related coverage: {coverage}")

        items.append(
            {
                "id": f"googletrends:{feed['id']}:{_safe_slug(title)}:{published_utc}",
                "source": "news",
                "sourceType": "googletrends",
                "sourceName": feed["label"],
                "title": title,
                "summary": " ".join(summary_parts).strip(),
                "author": related_sources[0] if related_sources else "",
                "url": related_urls[0] if related_urls else feed["url"],
                "createdUtc": published_utc,
                "score": _google_trends_traffic_score(approx_traffic),
                "numComments": len(related_titles),
                "interactionCounts": _interaction_counts(
                    posts=1,
                    reposts=len(related_titles),
                ),
                "trafficLabel": approx_traffic,
                "fetchedAt": fetched_at,
            }
        )

    return items


def _fetch_youtube_items(feed: Dict[str, Any], cutoff_utc: int) -> List[Dict[str, Any]]:
    return fetch_youtube_feed_items(
        feed,
        cutoff_utc,
        request_json=_request_json,
        build_url=_youtube_api_url,
    )


def merge_public_items(
    existing_items: Iterable[Dict[str, Any]],
    incoming_items: Iterable[Dict[str, Any]],
    *,
    cutoff_utc: int | None = None,
) -> List[Dict[str, Any]]:
    active_cutoff = cutoff_utc if cutoff_utc is not None else rolling_window_cutoff_utc()
    merged: Dict[str, Dict[str, Any]] = {
        _canonical_public_item_id(item): {
            **item,
            "id": _canonical_public_item_id(item),
        }
        for item in existing_items
        if _canonical_public_item_id(item) and int(item.get("createdUtc", 0) or 0) >= active_cutoff
    }

    for item in incoming_items:
        item_id = _canonical_public_item_id(item)
        created_utc = int(item.get("createdUtc", 0) or 0)
        if not item_id or created_utc < active_cutoff:
            continue

        current = merged.get(item_id, {})
        merged[item_id] = {
            **current,
            **item,
            "id": item_id,
            "interactionCounts": _merge_interaction_counts(
                current.get("interactionCounts"),
                item.get("interactionCounts"),
            ),
            "fetchedAt": item.get("fetchedAt") or current.get("fetchedAt"),
        }

    return sorted(
        merged.values(),
        key=lambda item: (
            int(item.get("createdUtc", 0) or 0),
            int(item.get("numComments", 0) or 0),
            int(item.get("score", 0) or 0),
            str(item.get("id", "")),
        ),
        reverse=True,
    )


def refresh_public_source_items(
    *,
    existing_items: Iterable[Dict[str, Any]] | None = None,
    existing_youtube_comments: Iterable[Dict[str, Any]] | None = None,
    existing_youtube_snapshots: Iterable[Dict[str, Any]] | None = None,
    existing_youtube_channels: Iterable[Dict[str, Any]] | None = None,
    reddit_posts: Iterable[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    refresh_started_at = perf_counter()
    cutoff_utc = rolling_window_cutoff_utc()
    current_items = list(existing_items or [])
    fetched_items: List[Dict[str, Any]] = []
    source_updates: Dict[str, Dict[str, Any]] = {}
    source_timings: Dict[str, float] = {}

    for feed in PUBLIC_RSS_FEEDS:
        source_key = f"public:{feed['id']}"
        source_started_at = perf_counter()
        try:
            items = _fetch_rss_feed_items(feed, cutoff_utc)
            fetched_items.extend(items)
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": "rss",
                "success": True,
                "itemsFetched": len(items),
                "error": None,
                "durationMs": duration_ms,
            }
        except Exception as error:
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": "rss",
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
            }

    for feed in PUBLIC_JSON_FEEDS:
        source_key = f"public:{feed['id']}"
        source_started_at = perf_counter()
        try:
            if feed["kind"] == "hackernews":
                items = _fetch_hackernews_items(feed, cutoff_utc)
            else:
                items = _fetch_lobsters_items(feed, cutoff_utc)
            fetched_items.extend(items)
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": feed["kind"],
                "success": True,
                "itemsFetched": len(items),
                "error": None,
                "durationMs": duration_ms,
            }
        except Exception as error:
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": feed["kind"],
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
            }

    for feed in PUBLIC_GOOGLE_TRENDS_FEEDS:
        source_key = f"public:{feed['id']}"
        source_started_at = perf_counter()
        try:
            items = _fetch_google_trends_items(feed, cutoff_utc)
            fetched_items.extend(items)
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": feed["kind"],
                "success": True,
                "itemsFetched": len(items),
                "error": None,
                "durationMs": duration_ms,
            }
        except Exception as error:
            duration_ms = round((perf_counter() - source_started_at) * 1000, 1)
            source_timings[source_key] = duration_ms
            source_updates[source_key] = {
                "sourceId": source_key,
                "label": feed["label"],
                "kind": feed["kind"],
                "success": False,
                "itemsFetched": 0,
                "error": str(error),
                "durationMs": duration_ms,
            }

    youtube_comments = list(existing_youtube_comments or [])
    youtube_snapshots = list(existing_youtube_snapshots or [])
    youtube_channels = list(existing_youtube_channels or [])

    if youtube_enabled():
        try:
            youtube_started_at = perf_counter()
            youtube_refresh = refresh_youtube_sources(
                feeds=PUBLIC_YOUTUBE_FEEDS,
                cutoff_utc=cutoff_utc,
                existing_items=current_items,
                existing_comments=youtube_comments,
                existing_snapshots=youtube_snapshots,
                existing_channels=youtube_channels,
                reddit_posts=reddit_posts or [],
                public_items=current_items,
                request_json=_request_json,
                build_url=_youtube_api_url,
                refresh_enabled=_should_refresh_youtube(current_items),
            )
            youtube_duration_ms = round((perf_counter() - youtube_started_at) * 1000, 1)
            fetched_items.extend(youtube_refresh.get("incomingItems", []))
            youtube_comments = list(youtube_refresh.get("comments", youtube_comments))
            youtube_snapshots = list(youtube_refresh.get("snapshots", youtube_snapshots))
            youtube_channels = list(youtube_refresh.get("channels", youtube_channels))
            source_updates.update(dict(youtube_refresh.get("sourceUpdates", {})))
            for source_key, update in dict(youtube_refresh.get("sourceUpdates", {})).items():
                duration_ms = float(update.get("durationMs", youtube_duration_ms))
                source_timings[source_key] = duration_ms
                update["durationMs"] = duration_ms
        except Exception as error:
            for feed in PUBLIC_YOUTUBE_FEEDS:
                source_key = f"public:{feed['id']}"
                source_timings[source_key] = 0.0
                source_updates[source_key] = {
                    "sourceId": source_key,
                    "label": feed["label"],
                    "kind": feed["kind"],
                    "success": False,
                    "itemsFetched": 0,
                    "error": str(error),
                    "skipped": False,
                    "durationMs": 0.0,
                }

    merged_items = merge_public_items(current_items, fetched_items, cutoff_utc=cutoff_utc)
    return {
        "items": merged_items,
        "youtubeComments": youtube_comments,
        "youtubeVideoSnapshots": youtube_snapshots,
        "youtubeChannels": youtube_channels,
        "sourceUpdates": source_updates,
        "fetchedCount": len(fetched_items),
        "storedCount": len(merged_items),
        "successCount": sum(1 for row in source_updates.values() if row["success"]),
        "failureCount": sum(1 for row in source_updates.values() if not row["success"]),
        "timings": {
            "totalMs": round((perf_counter() - refresh_started_at) * 1000, 1),
            "perSourceMs": source_timings,
        },
    }
