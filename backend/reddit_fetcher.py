from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from backend.reddit_client import (
    RedditBlockedResponseError,
    create_reddit_client,
    perform_reddit_request,
    public_json_fallback_allowed,
)
from backend.reddit_dev_only_config import (
    ACTIVE_POST_LOOKBACK_HOURS,
    BACKFILL_MAX_COMMENT_THREADS_PER_SUBREDDIT,
    BACKFILL_MAX_PAGES_PER_SUBREDDIT,
    BACKFILL_MAX_POSTS_PER_SUBREDDIT,
    BACKFILL_PAGE_LIMIT,
    COMMENT_THRESHOLD_FOR_EXPANSION,
    FETCH_LIMIT,
    LIVE_MAX_KNOWN_POSTS_TO_REFRESH,
    MAX_COMMENTS_PER_POST,
    MAX_TRACKED_POSTS,
    REDDIT_USER_AGENT,
    ROLLING_WINDOW_HOURS,
)
from backend.reddit_window import rolling_window_cutoff_utc, within_rolling_window


def _post_id(post: Dict[str, Any]) -> str:
    return str(post.get("id") or "")


def _post_subreddit(post: Dict[str, Any]) -> str:
    return str(post.get("subreddit") or "")


def _post_created_utc(post: Dict[str, Any]) -> int:
    return int(post.get("created_utc", post.get("createdUtc", 0)) or 0)


def _post_num_comments(post: Dict[str, Any]) -> int:
    return int(post.get("num_comments", post.get("numComments", 0)) or 0)


def _post_score(post: Dict[str, Any]) -> int:
    return int(post.get("score", 0) or 0)


def _catalog_entry_for_subreddit(
    subreddit_name: str,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None,
) -> Dict[str, Any] | None:
    if not subreddit_catalog:
        return None
    return subreddit_catalog.get(str(subreddit_name or "").lower())


def _fetch_profile_for_subreddit(
    subreddit_name: str,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None,
) -> Dict[str, Any]:
    entry = _catalog_entry_for_subreddit(subreddit_name, subreddit_catalog)
    return dict((entry or {}).get("fetchProfile") or {})


def _source_metadata_for_subreddit(
    subreddit_name: str,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None,
) -> Dict[str, Any]:
    entry = _catalog_entry_for_subreddit(subreddit_name, subreddit_catalog)
    if not entry:
        return {}
    fetch_profile = dict(entry.get("fetchProfile") or {})
    return {
        "category": entry.get("category"),
        "categoryLabel": entry.get("categoryLabel"),
        "tier": entry.get("tier"),
        "activityTier": entry.get("activityTier"),
        "audienceScale": entry.get("audienceScale"),
        "broadCommunity": bool(entry.get("broadCommunity")),
        "spamProne": bool(entry.get("spamProne")),
        "tags": list(entry.get("tags") or []),
        "priorityScore": int(entry.get("priorityScore", 0) or 0),
        "discoverySource": entry.get("discoverySource"),
        "liveCadenceWeight": int(fetch_profile.get("liveCadenceWeight", 1) or 1),
        "liveLimit": int(fetch_profile.get("liveLimit", 0) or 0),
        "liveListings": list(fetch_profile.get("liveListings") or []),
        "liveMaxPages": int(fetch_profile.get("liveMaxPages", 0) or 0),
        "liveMaxPostsPerRun": int(fetch_profile.get("liveMaxPostsPerRun", 0) or 0),
        "commentThreshold": int(fetch_profile.get("commentThreshold", 0) or 0),
        "maxTrackedPosts": int(fetch_profile.get("maxTrackedPosts", 0) or 0),
        "maxCommentsPerPost": int(fetch_profile.get("maxCommentsPerPost", 0) or 0),
        "knownPostsRefreshLimit": int(fetch_profile.get("knownPostsRefreshLimit", 0) or 0),
        "activePostLookbackHours": int(fetch_profile.get("activePostLookbackHours", 0) or 0),
        "backfillMaxPosts": int(fetch_profile.get("backfillMaxPosts", 0) or 0),
        "backfillMaxPages": int(fetch_profile.get("backfillMaxPages", 0) or 0),
        "backfillMaxCommentThreads": int(fetch_profile.get("backfillMaxCommentThreads", 0) or 0),
    }


def _list_submissions(subreddit: Any, listing: str, limit: int | None):
    if listing == "hot":
        return subreddit.hot(limit=limit)
    if listing == "rising":
        return subreddit.rising(limit=limit)
    return subreddit.new(limit=limit)


def _post_row_from_submission(submission: Any) -> Dict[str, Any]:
    return {
        "id": submission.id,
        "subreddit": str(submission.subreddit),
        "title": submission.title,
        "selftext": submission.selftext or "",
        "author": str(submission.author) if submission.author else "[deleted]",
        "permalink": submission.permalink,
        "created_utc": submission.created_utc,
        "score": submission.score,
        "num_comments": submission.num_comments,
        "url": submission.url,
    }


def _comment_row(
    *,
    comment_id: str,
    parent_id: str,
    post_id: str,
    subreddit: str,
    author: str,
    body: str,
    permalink: str,
    created_utc: float | int,
    score: int,
) -> Dict[str, Any]:
    return {
        "id": comment_id,
        "parent_id": parent_id,
        "post_id": post_id,
        "subreddit": subreddit,
        "author": author,
        "body": body,
        "permalink": permalink,
        "created_utc": created_utc,
        "score": score,
    }


def _looks_like_block_page(text: str) -> bool:
    lowered = text.lower()
    return text.lstrip().startswith("<") and (
        "whoa there" in lowered
        or "blocked" in lowered
        or "too many requests" in lowered
        or "rate limit" in lowered
        or "<html" in lowered
    )


def _fetch_json(url: str) -> Any:
    def execute() -> Any:
        request = Request(url, headers={"User-Agent": REDDIT_USER_AGENT})
        try:
            with urlopen(request, timeout=20) as response:
                payload = response.read().decode("utf-8", errors="replace")
                content_type = str(response.headers.get("Content-Type", "")).lower()
        except HTTPError:
            raise

        if "json" not in content_type and _looks_like_block_page(payload):
            raise RedditBlockedResponseError("reddit public-json returned block/anti-abuse HTML")
        if _looks_like_block_page(payload):
            raise RedditBlockedResponseError("reddit public-json returned block/anti-abuse HTML")
        return json.loads(payload)

    return perform_reddit_request(
        operation=f"public-json:{url}",
        transport="public-json",
        func=execute,
    )


def _post_row_from_public_json_child(
    child: Dict[str, Any],
    *,
    subreddit_name: str,
) -> Dict[str, Any]:
    data = child.get("data", {})
    return {
        "id": str(data.get("id", "")),
        "subreddit": data.get("subreddit", subreddit_name),
        "title": data.get("title", ""),
        "selftext": data.get("selftext", ""),
        "author": data.get("author", "[deleted]"),
        "permalink": data.get("permalink", ""),
        "created_utc": data.get("created_utc", 0),
        "score": data.get("score", 0),
        "num_comments": data.get("num_comments", 0),
        "url": data.get("url", ""),
    }


def _existing_post_state(existing_posts: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    known_ids = {
        _post_id(post)
        for post in existing_posts
        if _post_id(post)
    }
    created_values = [
        _post_created_utc(post)
        for post in existing_posts
        if _post_created_utc(post) > 0
    ]
    return {
        "knownIds": known_ids,
        "newestCreatedUtc": max(created_values, default=0),
        "oldestCreatedUtc": min(created_values, default=0),
    }


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _warm_live_source_limits(
    *,
    source_state: Dict[str, Any] | None,
    page_limit: int,
    max_pages: int,
    max_posts: int,
) -> tuple[int, int]:
    if not source_state:
        return max_pages, max_posts

    reference_time = datetime.now(timezone.utc)
    stored_posts = int(source_state.get("storedPostsInWindow", 0) or 0)
    refresh_limit = int(
        source_state.get("knownPostsRefreshLimit", LIVE_MAX_KNOWN_POSTS_TO_REFRESH)
        or LIVE_MAX_KNOWN_POSTS_TO_REFRESH
    )
    newest_known_created_utc = int(
        source_state.get("newestStoredPostCreatedUtc", source_state.get("lastKnownPostCreatedUtc", 0)) or 0
    )
    last_live_success = _parse_iso_timestamp(source_state.get("lastLiveSuccessAt"))
    live_success_age_seconds = (
        (reference_time - last_live_success).total_seconds()
        if last_live_success is not None
        else None
    )
    is_warm = bool(source_state.get("hasWindowCoverage")) and stored_posts >= max(5, refresh_limit) and (
        (live_success_age_seconds is not None and live_success_age_seconds <= 48 * 60 * 60)
        or newest_known_created_utc >= int(reference_time.timestamp()) - (48 * 60 * 60)
    )
    if not is_warm:
        return max_pages, max_posts

    capped_pages = min(max_pages, 2)
    capped_posts = min(max_posts, page_limit + max(10, min(40, refresh_limit * 4)))
    return capped_pages, max(page_limit, capped_posts)


def _should_attempt_backfill_source(
    *,
    source_state: Dict[str, Any] | None,
) -> bool:
    if not source_state:
        return True

    if not bool(source_state.get("hasWindowCoverage")):
        return True

    if int(source_state.get("storedPostsInWindow", 0) or 0) <= 0:
        return True

    return str(source_state.get("backfillStatus") or "pending") != "complete"


def _scan_new_posts_for_subreddit_praw(
    subreddit_name: str,
    *,
    cutoff_utc: int,
    max_posts: int,
    page_limit: int,
    max_pages: int,
    existing_post_ids: set[str] | None = None,
    skip_known_posts: bool,
    stop_at_known_frontier: bool,
) -> Dict[str, Any]:
    reddit = create_reddit_client()
    if reddit is None:
        return {
            "posts": [],
            "transport": "praw",
            "stopReason": "no-client",
            "reachedCutoff": False,
            "exhaustedListing": False,
            "reachedKnownFrontier": False,
            "pagesFetched": 0,
            "newPostsDiscovered": 0,
            "refreshedExistingPosts": 0,
            "skippedKnownPosts": 0,
        }

    subreddit = reddit.subreddit(subreddit_name)
    rows: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    known_ids = set(existing_post_ids or set())
    pages_fetched = 0
    processed_rows = 0
    new_posts_discovered = 0
    refreshed_existing_posts = 0
    skipped_known_posts = 0
    stop_reason = "exhausted"
    reached_known_frontier = False
    for submission in _list_submissions(subreddit, "new", None):
        row = _post_row_from_submission(submission)
        created_utc = _post_created_utc(row)
        if created_utc < cutoff_utc:
            stop_reason = "cutoff"
            break

        post_id = _post_id(row)
        if not post_id or post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        processed_rows += 1
        if processed_rows % max(1, page_limit) == 1:
            pages_fetched += 1
        if pages_fetched > max_pages:
            stop_reason = "max-pages"
            break

        if post_id in known_ids:
            if not skip_known_posts:
                rows.append(row)
                refreshed_existing_posts += 1
            else:
                skipped_known_posts += 1
            if stop_at_known_frontier:
                stop_reason = "known-frontier"
                reached_known_frontier = True
                break
            continue

        rows.append(row)
        new_posts_discovered += 1

        if len(rows) >= max_posts:
            stop_reason = "max-posts"
            break

    return {
        "posts": rows,
        "transport": "praw",
        "stopReason": stop_reason,
        "reachedCutoff": stop_reason == "cutoff",
        "exhaustedListing": stop_reason == "exhausted",
        "reachedKnownFrontier": reached_known_frontier,
        "pagesFetched": pages_fetched,
        "newPostsDiscovered": new_posts_discovered,
        "refreshedExistingPosts": refreshed_existing_posts,
        "skippedKnownPosts": skipped_known_posts,
    }


def _scan_new_posts_for_subreddit_public_json(
    subreddit_name: str,
    *,
    cutoff_utc: int,
    page_limit: int,
    max_posts: int,
    max_pages: int,
    existing_post_ids: set[str] | None = None,
    skip_known_posts: bool,
    stop_at_known_frontier: bool,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    known_ids = set(existing_post_ids or set())
    after: str | None = None
    stop_reason = "exhausted"
    pages = 0
    reached_known_frontier = False
    new_posts_discovered = 0
    refreshed_existing_posts = 0
    skipped_known_posts = 0

    while True:
        if pages >= max_pages:
            stop_reason = "max-pages"
            break

        query = urlencode({k: v for k, v in {"limit": page_limit, "after": after}.items() if v})
        payload = _fetch_json(f"https://www.reddit.com/r/{subreddit_name}/new.json?{query}")
        data = payload.get("data", {})
        children = data.get("children", [])
        if not children:
            stop_reason = "exhausted"
            break

        pages += 1
        reached_cutoff = False
        for child in children:
            row = _post_row_from_public_json_child(child, subreddit_name=subreddit_name)
            created_utc = _post_created_utc(row)
            if created_utc < cutoff_utc:
                stop_reason = "cutoff"
                reached_cutoff = True
                break

            post_id = _post_id(row)
            if not post_id or post_id in seen_ids:
                continue
            seen_ids.add(post_id)
            if post_id in known_ids:
                if not skip_known_posts:
                    rows.append(row)
                    refreshed_existing_posts += 1
                else:
                    skipped_known_posts += 1
                if stop_at_known_frontier:
                    stop_reason = "known-frontier"
                    reached_cutoff = True
                    reached_known_frontier = True
                    break
                continue

            rows.append(row)
            new_posts_discovered += 1

            if len(rows) >= max_posts:
                stop_reason = "max-posts"
                reached_cutoff = True
                break

        if reached_cutoff:
            break

        after = data.get("after")
        if not after:
            stop_reason = "exhausted"
            break

    return {
        "posts": rows,
        "transport": "public-json",
        "stopReason": stop_reason,
        "reachedCutoff": stop_reason == "cutoff",
        "exhaustedListing": stop_reason == "exhausted",
        "reachedKnownFrontier": reached_known_frontier,
        "pagesFetched": pages,
        "newPostsDiscovered": new_posts_discovered,
        "refreshedExistingPosts": refreshed_existing_posts,
        "skippedKnownPosts": skipped_known_posts,
    }


def fetch_backfill_posts_for_subreddit(
    subreddit_name: str,
    *,
    cutoff_utc: int,
    existing_posts: Iterable[Dict[str, Any]] | None = None,
    page_limit: int = BACKFILL_PAGE_LIMIT,
    max_posts: int = BACKFILL_MAX_POSTS_PER_SUBREDDIT,
    max_pages: int = BACKFILL_MAX_PAGES_PER_SUBREDDIT,
) -> Dict[str, Any]:
    errors: List[str] = []
    existing_state = _existing_post_state(existing_posts or [])

    try:
        result = _scan_new_posts_for_subreddit_praw(
            subreddit_name,
            cutoff_utc=cutoff_utc,
            max_posts=max_posts,
            page_limit=page_limit,
            max_pages=max_pages,
            existing_post_ids=existing_state["knownIds"],
            skip_known_posts=True,
            stop_at_known_frontier=False,
        )
        if result["stopReason"] != "no-client":
            return {
                "subreddit": subreddit_name,
                "posts": result["posts"],
                "success": True,
                "transport": result["transport"],
                "error": None,
                "stopReason": result["stopReason"],
                "reachedCutoff": result["reachedCutoff"],
                "exhaustedListing": result["exhaustedListing"],
                "reachedKnownFrontier": result["reachedKnownFrontier"],
                "pagesFetched": result["pagesFetched"],
                "newPostsDiscovered": result["newPostsDiscovered"],
                "refreshedExistingPosts": result["refreshedExistingPosts"],
                "skippedKnownPosts": result["skippedKnownPosts"],
            }
    except Exception as error:
        errors.append(f"praw: {error}")

    if not public_json_fallback_allowed():
        return {
            "subreddit": subreddit_name,
            "posts": [],
            "success": False,
            "transport": None,
            "error": " | ".join(errors) if errors else "oauth fetch failed",
            "stopReason": "error",
            "reachedCutoff": False,
            "exhaustedListing": False,
            "reachedKnownFrontier": False,
            "pagesFetched": 0,
            "newPostsDiscovered": 0,
            "refreshedExistingPosts": 0,
            "skippedKnownPosts": 0,
        }

    try:
        result = _scan_new_posts_for_subreddit_public_json(
            subreddit_name,
            cutoff_utc=cutoff_utc,
            page_limit=page_limit,
            max_posts=max_posts,
            max_pages=max_pages,
            existing_post_ids=existing_state["knownIds"],
            skip_known_posts=True,
            stop_at_known_frontier=False,
        )
        return {
            "subreddit": subreddit_name,
            "posts": result["posts"],
            "success": True,
            "transport": result["transport"],
            "error": None,
            "stopReason": result["stopReason"],
            "reachedCutoff": result["reachedCutoff"],
            "exhaustedListing": result["exhaustedListing"],
            "reachedKnownFrontier": result["reachedKnownFrontier"],
            "pagesFetched": result["pagesFetched"],
            "newPostsDiscovered": result["newPostsDiscovered"],
            "refreshedExistingPosts": result["refreshedExistingPosts"],
            "skippedKnownPosts": result["skippedKnownPosts"],
        }
    except Exception as error:
        errors.append(f"public-json: {error}")

    return {
        "subreddit": subreddit_name,
        "posts": [],
        "success": False,
        "transport": None,
        "error": " | ".join(errors) if errors else "unknown fetch failure",
        "stopReason": "error",
        "reachedCutoff": False,
        "exhaustedListing": False,
        "reachedKnownFrontier": False,
        "pagesFetched": 0,
        "newPostsDiscovered": 0,
        "refreshedExistingPosts": 0,
        "skippedKnownPosts": 0,
    }


def _fetch_live_posts_for_subreddit_praw(
    subreddit_name: str,
    *,
    cutoff_utc: int,
    page_limit: int,
    max_pages: int,
    max_posts: int,
    existing_post_ids: set[str],
) -> Dict[str, Any]:
    return _scan_new_posts_for_subreddit_praw(
        subreddit_name,
        cutoff_utc=cutoff_utc,
        max_posts=max_posts,
        page_limit=page_limit,
        max_pages=max_pages,
        existing_post_ids=existing_post_ids,
        skip_known_posts=False,
        stop_at_known_frontier=True,
    )


def fetch_live_posts_for_subreddit(
    subreddit_name: str,
    *,
    existing_posts: Iterable[Dict[str, Any]] | None = None,
    source_state: Dict[str, Any] | None = None,
    page_limit: int = FETCH_LIMIT,
    max_pages: int = 3,
    max_posts: int = FETCH_LIMIT * 3,
    cutoff_utc: int | None = None,
) -> Dict[str, Any]:
    errors: List[str] = []
    effective_cutoff = cutoff_utc or rolling_window_cutoff_utc()
    existing_state = _existing_post_state(existing_posts or [])
    effective_max_pages, effective_max_posts = _warm_live_source_limits(
        source_state=source_state,
        page_limit=page_limit,
        max_pages=max_pages,
        max_posts=max_posts,
    )

    try:
        result = _fetch_live_posts_for_subreddit_praw(
            subreddit_name,
            cutoff_utc=effective_cutoff,
            page_limit=page_limit,
            max_pages=effective_max_pages,
            max_posts=effective_max_posts,
            existing_post_ids=existing_state["knownIds"],
        )
        if result["stopReason"] != "no-client":
            return {
                "subreddit": subreddit_name,
                "posts": result["posts"],
                "success": True,
                "transport": result["transport"],
                "error": None,
                "stopReason": result["stopReason"],
                "reachedKnownFrontier": result["reachedKnownFrontier"],
                "reachedCutoff": result["reachedCutoff"],
                "exhaustedListing": result["exhaustedListing"],
                "pagesFetched": result["pagesFetched"],
                "newPostsDiscovered": result["newPostsDiscovered"],
                "refreshedExistingPosts": result["refreshedExistingPosts"],
                "skippedKnownPosts": result["skippedKnownPosts"],
            }
    except Exception as error:
        errors.append(f"praw: {error}")

    if not public_json_fallback_allowed():
        return {
            "subreddit": subreddit_name,
            "posts": [],
            "success": False,
            "transport": None,
            "error": " | ".join(errors) if errors else "oauth fetch failed",
            "stopReason": "error",
            "reachedKnownFrontier": False,
            "reachedCutoff": False,
            "exhaustedListing": False,
            "pagesFetched": 0,
            "newPostsDiscovered": 0,
            "refreshedExistingPosts": 0,
            "skippedKnownPosts": 0,
        }

    try:
        result = _scan_new_posts_for_subreddit_public_json(
            subreddit_name,
            cutoff_utc=effective_cutoff,
            page_limit=page_limit,
            max_pages=effective_max_pages,
            max_posts=effective_max_posts,
            existing_post_ids=existing_state["knownIds"],
            skip_known_posts=False,
            stop_at_known_frontier=True,
        )
        return {
            "subreddit": subreddit_name,
            "posts": result["posts"],
            "success": True,
            "transport": result["transport"],
            "error": None,
            "stopReason": result["stopReason"],
            "reachedKnownFrontier": result["reachedKnownFrontier"],
            "reachedCutoff": result["reachedCutoff"],
            "exhaustedListing": result["exhaustedListing"],
            "pagesFetched": result["pagesFetched"],
            "newPostsDiscovered": result["newPostsDiscovered"],
            "refreshedExistingPosts": result["refreshedExistingPosts"],
            "skippedKnownPosts": result["skippedKnownPosts"],
        }
    except Exception as error:
        errors.append(f"public-json: {error}")

    return {
        "subreddit": subreddit_name,
        "posts": [],
        "success": False,
        "transport": None,
        "error": " | ".join(errors) if errors else "unknown fetch failure",
        "stopReason": "error",
        "reachedKnownFrontier": False,
        "reachedCutoff": False,
        "exhaustedListing": False,
        "pagesFetched": 0,
        "newPostsDiscovered": 0,
        "refreshedExistingPosts": 0,
        "skippedKnownPosts": 0,
    }


def _rank_posts(posts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        posts,
        key=lambda post: (
            _post_num_comments(post),
            _post_score(post),
            _post_created_utc(post),
        ),
        reverse=True,
    )


def _select_posts_for_comment_expansion(
    posts: List[Dict[str, Any]],
    *,
    max_tracked_posts: int,
    comment_threshold: int,
) -> List[Dict[str, Any]]:
    ranked = _rank_posts(posts)
    selected = [
        post
        for post in ranked
        if _post_num_comments(post) >= comment_threshold
    ][:max_tracked_posts]

    if len(selected) >= max_tracked_posts:
        return selected

    selected_ids = {_post_id(post) for post in selected}
    for post in ranked:
        post_id = _post_id(post)
        if post_id in selected_ids:
            continue
        selected.append(post)
        selected_ids.add(post_id)
        if len(selected) >= max_tracked_posts:
            break

    return selected


def _select_backfill_comment_targets(
    posts: List[Dict[str, Any]],
    *,
    max_threads_per_subreddit: int,
    comment_threshold: int,
) -> List[Dict[str, Any]]:
    by_subreddit: Dict[str, List[Dict[str, Any]]] = {}
    for post in posts:
        by_subreddit.setdefault(_post_subreddit(post), []).append(post)

    targets: List[Dict[str, Any]] = []
    for subreddit_posts in by_subreddit.values():
        targets.extend(
            _select_posts_for_comment_expansion(
                subreddit_posts,
                max_tracked_posts=max_threads_per_subreddit,
                comment_threshold=comment_threshold,
            )
        )

    deduped: Dict[str, Dict[str, Any]] = {}
    for post in targets:
        post_id = _post_id(post)
        if post_id:
            deduped[post_id] = post
    return list(deduped.values())


def _select_known_posts_for_comment_refresh(
    existing_posts: List[Dict[str, Any]],
    *,
    subreddits: List[str],
    max_posts: int,
    lookback_hours: int,
    comment_threshold: int,
) -> List[Dict[str, Any]]:
    subreddit_keys = {subreddit.lower() for subreddit in subreddits}
    reference_time = datetime.now(timezone.utc)
    cutoff_seconds = int(reference_time.timestamp()) - lookback_hours * 60 * 60
    candidates = [
        post
        for post in existing_posts
        if _post_subreddit(post).lower() in subreddit_keys
        and _post_created_utc(post) >= cutoff_seconds
        and _post_num_comments(post) >= max(1, comment_threshold // 2)
        and within_rolling_window(_post_created_utc(post), reference_time=reference_time)
    ]
    return _select_posts_for_comment_expansion(
        candidates,
        max_tracked_posts=max_posts,
        comment_threshold=comment_threshold,
    )


def _select_live_comment_targets(
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
    *,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None,
    default_max_tracked_posts: int = MAX_TRACKED_POSTS,
    default_comment_threshold: int = COMMENT_THRESHOLD_FOR_EXPANSION,
) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for subreddit, subreddit_posts in posts_by_subreddit.items():
        fetch_profile = _fetch_profile_for_subreddit(subreddit, subreddit_catalog)
        max_tracked_posts = int(
            fetch_profile.get("maxTrackedPosts", default_max_tracked_posts) or default_max_tracked_posts
        )
        comment_threshold = int(
            fetch_profile.get("commentThreshold", default_comment_threshold) or default_comment_threshold
        )
        for post in _select_posts_for_comment_expansion(
            subreddit_posts,
            max_tracked_posts=max_tracked_posts,
            comment_threshold=comment_threshold,
        ):
            post_id = _post_id(post)
            if post_id:
                deduped[post_id] = post
    return list(deduped.values())


def _select_known_posts_for_comment_refresh_by_subreddit(
    existing_posts: List[Dict[str, Any]],
    *,
    subreddits: List[str],
    subreddit_catalog: Dict[str, Dict[str, Any]] | None,
    default_lookback_hours: int = ACTIVE_POST_LOOKBACK_HOURS,
    default_comment_threshold: int = COMMENT_THRESHOLD_FOR_EXPANSION,
    default_max_posts: int = LIVE_MAX_KNOWN_POSTS_TO_REFRESH,
) -> List[Dict[str, Any]]:
    by_subreddit: Dict[str, List[Dict[str, Any]]] = {}
    for post in existing_posts:
        by_subreddit.setdefault(_post_subreddit(post).lower(), []).append(post)

    reference_time = datetime.now(timezone.utc)
    deduped: Dict[str, Dict[str, Any]] = {}
    for subreddit in subreddits:
        subreddit_key = subreddit.lower()
        fetch_profile = _fetch_profile_for_subreddit(subreddit, subreddit_catalog)
        lookback_hours = int(
            fetch_profile.get("activePostLookbackHours", default_lookback_hours) or default_lookback_hours
        )
        comment_threshold = int(
            fetch_profile.get("commentThreshold", default_comment_threshold) or default_comment_threshold
        )
        max_posts = int(
            fetch_profile.get("knownPostsRefreshLimit", default_max_posts) or default_max_posts
        )
        cutoff_seconds = int(reference_time.timestamp()) - lookback_hours * 60 * 60
        candidates = [
            post
            for post in by_subreddit.get(subreddit_key, [])
            if _post_created_utc(post) >= cutoff_seconds
            and _post_num_comments(post) >= max(1, comment_threshold // 2)
            and within_rolling_window(_post_created_utc(post), reference_time=reference_time)
        ]
        for post in _select_posts_for_comment_expansion(
            candidates,
            max_tracked_posts=max_posts,
            comment_threshold=comment_threshold,
        ):
            post_id = _post_id(post)
            if post_id:
                deduped[post_id] = post
    return list(deduped.values())


def _fetch_comments_for_submission_praw(
    post: Dict[str, Any],
    *,
    max_comments_per_post: int,
) -> Dict[str, Any]:
    reddit = create_reddit_client()
    if reddit is None:
        raise RuntimeError("no Reddit client configured")

    submission = reddit.submission(id=_post_id(post))
    submission.comment_sort = "new"
    submission.comments.replace_more(limit=0)
    rows: List[Dict[str, Any]] = []

    for comment in sorted(
        submission.comments.list(),
        key=lambda item: float(getattr(item, "created_utc", 0) or 0),
        reverse=True,
    )[:max_comments_per_post]:
        comment_id = str(getattr(comment, "id", ""))
        body = str(getattr(comment, "body", "") or "").strip()
        if not comment_id or not body or body in {"[deleted]", "[removed]"}:
            continue

        rows.append(
            _comment_row(
                comment_id=comment_id,
                parent_id=str(getattr(comment, "parent_id", "")),
                post_id=_post_id(post),
                subreddit=_post_subreddit(post),
                author=str(getattr(comment, "author", None) or "[deleted]"),
                body=body,
                permalink=str(
                    getattr(comment, "permalink", "")
                    or f"/r/{_post_subreddit(post)}/comments/{_post_id(post)}/_/{comment_id}"
                ),
                created_utc=float(getattr(comment, "created_utc", 0) or 0),
                score=int(getattr(comment, "score", 0) or 0),
            )
        )

    return {
        "post": _post_row_from_submission(submission),
        "comments": rows,
    }


def _extract_public_json_comments(
    children: Iterable[Dict[str, Any]],
    *,
    post: Dict[str, Any],
    target: List[Dict[str, Any]],
    max_comments: int,
) -> None:
    if len(target) >= max_comments:
        return

    for child in children:
        if len(target) >= max_comments:
            return

        if child.get("kind") != "t1":
            continue

        data = child.get("data", {})
        comment_id = str(data.get("id", ""))
        body = str(data.get("body", "") or "").strip()
        if comment_id and body and body not in {"[deleted]", "[removed]"}:
            target.append(
                _comment_row(
                    comment_id=comment_id,
                    parent_id=str(data.get("parent_id", "")),
                    post_id=_post_id(post),
                    subreddit=str(data.get("subreddit", _post_subreddit(post))),
                    author=str(data.get("author", "[deleted]")),
                    body=body,
                    permalink=str(data.get("permalink", "")),
                    created_utc=float(data.get("created_utc", 0) or 0),
                    score=int(data.get("score", 0) or 0),
                )
            )

        replies = data.get("replies")
        if isinstance(replies, dict):
            _extract_public_json_comments(
                replies.get("data", {}).get("children", []),
                post=post,
                target=target,
                max_comments=max_comments,
            )


def _fetch_comments_for_submission_public_json(
    post: Dict[str, Any],
    *,
    max_comments_per_post: int,
) -> Dict[str, Any]:
    payload = _fetch_json(
        f"https://www.reddit.com/comments/{_post_id(post)}.json?limit={max_comments_per_post}&sort=new"
    )
    post_row = None
    if payload and isinstance(payload, list) and payload:
        post_children = payload[0].get("data", {}).get("children", [])
        if post_children:
            post_row = _post_row_from_public_json_child(post_children[0], subreddit_name=_post_subreddit(post))
    children = payload[1].get("data", {}).get("children", []) if len(payload) > 1 else []
    rows: List[Dict[str, Any]] = []
    _extract_public_json_comments(children, post=post, target=rows, max_comments=max_comments_per_post)
    return {
        "post": post_row or dict(post),
        "comments": rows[:max_comments_per_post],
    }


def fetch_comments_for_post(
    post: Dict[str, Any],
    *,
    max_comments_per_post: int = MAX_COMMENTS_PER_POST,
) -> Dict[str, Any]:
    errors: List[str] = []

    try:
        result = _fetch_comments_for_submission_praw(
            post,
            max_comments_per_post=max_comments_per_post,
        )
        return {
            "postId": _post_id(post),
            "subreddit": _post_subreddit(post),
            "post": result["post"],
            "comments": result["comments"],
            "success": True,
            "transport": "praw",
            "error": None,
        }
    except Exception as error:
        errors.append(f"praw: {error}")

    if not public_json_fallback_allowed():
        return {
            "postId": _post_id(post),
            "subreddit": _post_subreddit(post),
            "post": dict(post),
            "comments": [],
            "success": False,
            "transport": None,
            "error": " | ".join(errors) if errors else "oauth comment fetch failed",
        }

    try:
        result = _fetch_comments_for_submission_public_json(
            post,
            max_comments_per_post=max_comments_per_post,
        )
        return {
            "postId": _post_id(post),
            "subreddit": _post_subreddit(post),
            "post": result["post"],
            "comments": result["comments"],
            "success": True,
            "transport": "public-json",
            "error": None,
        }
    except Exception as error:
        errors.append(f"public-json: {error}")

    return {
        "postId": _post_id(post),
        "subreddit": _post_subreddit(post),
        "post": dict(post),
        "comments": [],
        "success": False,
        "transport": None,
        "error": " | ".join(errors) if errors else "unknown comment fetch failure",
    }


def _summarize_source_update(
    *,
    subreddit: str,
    mode: str,
    success: bool,
    transport: str | None,
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    error: str | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    oldest_post_created_utc = min((_post_created_utc(post) for post in posts), default=None)
    newest_post_created_utc = max((_post_created_utc(post) for post in posts), default=None)
    payload = {
        "subreddit": subreddit,
        "mode": mode,
        "success": success,
        "transport": transport,
        "postsFetched": len(posts),
        "commentsFetched": len(comments),
        "commentRefreshCount": len(comments),
        "oldestPostCreatedUtc": oldest_post_created_utc,
        "newestPostCreatedUtc": newest_post_created_utc,
        "error": error,
    }
    if extra:
        payload.update(extra)
    return payload


def fetch_reddit_backfill_bucket(
    subreddits: List[str],
    *,
    existing_posts: List[Dict[str, Any]] | None = None,
    existing_sources: Dict[str, Dict[str, Any]] | None = None,
    cutoff_utc: int | None = None,
    max_comments_per_post: int = MAX_COMMENTS_PER_POST,
    comment_threshold: int = COMMENT_THRESHOLD_FOR_EXPANSION,
    max_threads_per_subreddit: int = BACKFILL_MAX_COMMENT_THREADS_PER_SUBREDDIT,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    cutoff = cutoff_utc or rolling_window_cutoff_utc()
    existing_posts_by_subreddit: Dict[str, List[Dict[str, Any]]] = {}
    for post in existing_posts or []:
        existing_posts_by_subreddit.setdefault(_post_subreddit(post).lower(), []).append(post)

    post_rows_by_id: Dict[str, Dict[str, Any]] = {}
    comments: List[Dict[str, Any]] = []
    succeeded_subreddits: List[str] = []
    failed_subreddits: List[str] = []
    subreddit_errors: Dict[str, str] = {}
    posts_by_subreddit: Dict[str, int] = {}
    comments_by_subreddit: Dict[str, int] = {}
    source_updates: Dict[str, Dict[str, Any]] = {}
    subreddit_outcomes: Dict[str, Dict[str, Any]] = {}
    attempted_subreddits: List[str] = []
    skipped_warm_subreddits: List[str] = []
    total_pages_fetched = 0
    total_new_posts_discovered = 0
    total_refreshed_existing_posts = 0
    total_skipped_known_posts = 0
    frontier_stop_count = 0

    for subreddit in subreddits:
        source_state = (existing_sources or {}).get(subreddit.lower())
        if not _should_attempt_backfill_source(source_state=source_state):
            skipped_warm_subreddits.append(subreddit)
            continue

        attempted_subreddits.append(subreddit)
        fetch_profile = _fetch_profile_for_subreddit(subreddit, subreddit_catalog)
        source_metadata = _source_metadata_for_subreddit(subreddit, subreddit_catalog)
        result = fetch_backfill_posts_for_subreddit(
            subreddit,
            cutoff_utc=cutoff,
            existing_posts=existing_posts_by_subreddit.get(subreddit.lower(), []),
            page_limit=BACKFILL_PAGE_LIMIT,
            max_posts=int(fetch_profile.get("backfillMaxPosts", BACKFILL_MAX_POSTS_PER_SUBREDDIT) or BACKFILL_MAX_POSTS_PER_SUBREDDIT),
            max_pages=int(fetch_profile.get("backfillMaxPages", BACKFILL_MAX_PAGES_PER_SUBREDDIT) or BACKFILL_MAX_PAGES_PER_SUBREDDIT),
        )
        subreddit_posts = result["posts"]
        if result["success"]:
            succeeded_subreddits.append(subreddit)
            for post in subreddit_posts:
                post_id = _post_id(post)
                if post_id:
                    post_rows_by_id[post_id] = post
        else:
            failed_subreddits.append(subreddit)
            subreddit_errors[subreddit] = result["error"]

        comment_targets = _select_backfill_comment_targets(
            subreddit_posts,
            max_threads_per_subreddit=int(
                fetch_profile.get("backfillMaxCommentThreads", max_threads_per_subreddit)
                or max_threads_per_subreddit
            ),
            comment_threshold=int(
                fetch_profile.get("commentThreshold", comment_threshold)
                or comment_threshold
            ),
        )
        subreddit_comments: List[Dict[str, Any]] = []
        seen_comment_ids: set[str] = set()
        for post in comment_targets:
            comment_result = fetch_comments_for_post(
                post,
                max_comments_per_post=int(
                    fetch_profile.get("maxCommentsPerPost", max_comments_per_post)
                    or max_comments_per_post
                ),
            )
            if not comment_result["success"]:
                subreddit_errors.setdefault(subreddit, comment_result["error"])
                continue
            refreshed_post = comment_result.get("post")
            refreshed_post_id = _post_id(refreshed_post or {})
            if refreshed_post_id:
                post_rows_by_id[refreshed_post_id] = dict(refreshed_post)
            for comment in comment_result["comments"]:
                comment_id = str(comment.get("id", ""))
                if not comment_id or comment_id in seen_comment_ids:
                    continue
                seen_comment_ids.add(comment_id)
                subreddit_comments.append(comment)

        comments.extend(subreddit_comments)
        posts_by_subreddit[subreddit] = len(subreddit_posts)
        comments_by_subreddit[subreddit] = len(subreddit_comments)
        total_pages_fetched += int(result.get("pagesFetched", 0) or 0)
        total_new_posts_discovered += int(result.get("newPostsDiscovered", 0) or 0)
        total_refreshed_existing_posts += int(result.get("refreshedExistingPosts", 0) or 0)
        total_skipped_known_posts += int(result.get("skippedKnownPosts", 0) or 0)
        if result.get("reachedKnownFrontier", False):
            frontier_stop_count += 1
        source_updates[subreddit.lower()] = _summarize_source_update(
            subreddit=subreddit,
            mode="backfill",
            success=result["success"],
            transport=result.get("transport"),
            posts=subreddit_posts,
            comments=subreddit_comments,
            error=result.get("error"),
            extra={
                **source_metadata,
                "backfillReachedCutoff": result.get("reachedCutoff", False),
                "backfillExhaustedListing": result.get("exhaustedListing", False),
                "backfillReachedKnownFrontier": result.get("reachedKnownFrontier", False),
                "backfillStopReason": result.get("stopReason"),
                "backfillPagesFetched": int(result.get("pagesFetched", 0) or 0),
                "backfillSkippedKnownPosts": int(result.get("skippedKnownPosts", 0) or 0),
                "newPostsDiscovered": int(result.get("newPostsDiscovered", 0) or 0),
                "refreshedExistingPosts": int(result.get("refreshedExistingPosts", 0) or 0),
                "backfillComplete": result["success"]
                and (result.get("reachedCutoff", False) or result.get("exhaustedListing", False)),
            },
        )
        subreddit_outcomes[subreddit.lower()] = {
            "subreddit": subreddit,
            "mode": "backfill",
            "success": bool(result["success"]),
            "transport": result.get("transport"),
            "stopReason": result.get("stopReason"),
            "reachedCutoff": bool(result.get("reachedCutoff", False)),
            "reachedKnownFrontier": bool(result.get("reachedKnownFrontier", False)),
            "pagesFetched": int(result.get("pagesFetched", 0) or 0),
            "postsFetched": len(subreddit_posts),
            "commentsFetched": len(subreddit_comments),
            "newPostsDiscovered": int(result.get("newPostsDiscovered", 0) or 0),
            "refreshedExistingPosts": int(result.get("refreshedExistingPosts", 0) or 0),
            "skippedKnownPosts": int(result.get("skippedKnownPosts", 0) or 0),
            "knownPostsRefreshed": 0,
            "error": result.get("error"),
        }

    known_targets = _select_known_posts_for_comment_refresh_by_subreddit(
        existing_posts or [],
        subreddits=attempted_subreddits,
        subreddit_catalog=subreddit_catalog,
        default_lookback_hours=ACTIVE_POST_LOOKBACK_HOURS,
        default_comment_threshold=comment_threshold,
        default_max_posts=2,
    )
    known_refresh_counts: Dict[str, int] = {}
    seen_comment_ids = {
        str(comment.get("id", ""))
        for comment in comments
        if str(comment.get("id", ""))
    }
    for post in known_targets:
        subreddit = _post_subreddit(post)
        fetch_profile = _fetch_profile_for_subreddit(subreddit, subreddit_catalog)
        comment_result = fetch_comments_for_post(
            post,
            max_comments_per_post=int(
                fetch_profile.get("maxCommentsPerPost", max_comments_per_post)
                or max_comments_per_post
            ),
        )
        if not comment_result["success"]:
            subreddit_errors.setdefault(subreddit, comment_result["error"])
            continue

        refreshed_post = comment_result.get("post")
        refreshed_post_id = _post_id(refreshed_post or {})
        if refreshed_post_id:
            post_rows_by_id[refreshed_post_id] = dict(refreshed_post)
            known_refresh_counts[subreddit] = known_refresh_counts.get(subreddit, 0) + 1

        for comment in comment_result["comments"]:
            comment_id = str(comment.get("id", ""))
            if not comment_id or comment_id in seen_comment_ids:
                continue
            seen_comment_ids.add(comment_id)
            comments.append(comment)
            comments_by_subreddit[subreddit] = comments_by_subreddit.get(subreddit, 0) + 1

    total_refreshed_existing_posts += sum(known_refresh_counts.values())
    for subreddit in attempted_subreddits:
        key = subreddit.lower()
        if key not in source_updates:
            continue
        source_updates[key] = {
            **source_updates[key],
            "commentsFetched": comments_by_subreddit.get(subreddit, 0),
            "commentRefreshCount": comments_by_subreddit.get(subreddit, 0),
            "knownPostsRefreshed": known_refresh_counts.get(subreddit, 0),
            "refreshedExistingPosts": int(source_updates[key].get("refreshedExistingPosts", 0) or 0)
            + known_refresh_counts.get(subreddit, 0),
        }
        outcome = subreddit_outcomes.get(key)
        if outcome is not None:
            outcome["commentsFetched"] = comments_by_subreddit.get(subreddit, 0)
            outcome["knownPostsRefreshed"] = known_refresh_counts.get(subreddit, 0)
            outcome["refreshedExistingPosts"] = int(outcome.get("refreshedExistingPosts", 0) or 0) + known_refresh_counts.get(subreddit, 0)

    if failed_subreddits and succeeded_subreddits:
        status = "partial"
    elif failed_subreddits:
        status = "failed"
    else:
        status = "success"

    return {
        "mode": "backfill",
        "fetchedAt": fetched_at,
        "status": status,
        "attemptedSubreddits": attempted_subreddits,
        "skippedWarmSubreddits": skipped_warm_subreddits,
        "succeededSubreddits": succeeded_subreddits,
        "failedSubreddits": failed_subreddits,
        "subredditErrors": subreddit_errors,
        "posts": list(post_rows_by_id.values()),
        "comments": comments,
        "postsBySubreddit": posts_by_subreddit,
        "commentsBySubreddit": comments_by_subreddit,
        "pagesFetched": total_pages_fetched,
        "newPostsDiscovered": total_new_posts_discovered,
        "refreshedExistingPosts": total_refreshed_existing_posts,
        "skippedKnownPosts": total_skipped_known_posts,
        "frontierStops": frontier_stop_count,
        "subredditOutcomes": subreddit_outcomes,
        "sourceUpdates": source_updates,
    }


def fetch_reddit_live_bucket(
    subreddits: List[str],
    *,
    existing_posts: List[Dict[str, Any]],
    existing_sources: Dict[str, Dict[str, Any]] | None = None,
    limit: int = FETCH_LIMIT,
    max_tracked_posts: int = MAX_TRACKED_POSTS,
    max_comments_per_post: int = MAX_COMMENTS_PER_POST,
    comment_threshold: int = COMMENT_THRESHOLD_FOR_EXPANSION,
    known_posts_refresh_limit: int = LIVE_MAX_KNOWN_POSTS_TO_REFRESH,
    active_post_lookback_hours: int = ACTIVE_POST_LOOKBACK_HOURS,
    subreddit_catalog: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    comments: List[Dict[str, Any]] = []
    succeeded_subreddits: List[str] = []
    failed_subreddits: List[str] = []
    subreddit_errors: Dict[str, str] = {}
    posts_by_subreddit: Dict[str, int] = {}
    comments_by_subreddit: Dict[str, int] = {}
    source_updates: Dict[str, Dict[str, Any]] = {}
    fetched_posts_by_subreddit: Dict[str, List[Dict[str, Any]]] = {}
    existing_posts_by_subreddit: Dict[str, List[Dict[str, Any]]] = {}
    for post in existing_posts:
        existing_posts_by_subreddit.setdefault(_post_subreddit(post).lower(), []).append(post)
    post_rows_by_id: Dict[str, Dict[str, Any]] = {}
    subreddit_outcomes: Dict[str, Dict[str, Any]] = {}
    total_pages_fetched = 0
    total_new_posts_discovered = 0
    total_refreshed_existing_posts = 0
    total_skipped_known_posts = 0
    frontier_stop_count = 0

    for subreddit in subreddits:
        fetch_profile = _fetch_profile_for_subreddit(subreddit, subreddit_catalog)
        source_metadata = _source_metadata_for_subreddit(subreddit, subreddit_catalog)
        result = fetch_live_posts_for_subreddit(
            subreddit,
            existing_posts=existing_posts_by_subreddit.get(subreddit.lower(), []),
            source_state=(existing_sources or {}).get(subreddit.lower()),
            page_limit=int(fetch_profile.get("liveLimit", limit) or limit),
            max_pages=int(fetch_profile.get("liveMaxPages", 3) or 3),
            max_posts=int(fetch_profile.get("liveMaxPostsPerRun", FETCH_LIMIT * 3) or (FETCH_LIMIT * 3)),
            cutoff_utc=rolling_window_cutoff_utc(),
        )
        subreddit_posts = result["posts"]
        fetched_posts_by_subreddit[subreddit] = subreddit_posts
        if result["success"]:
            succeeded_subreddits.append(subreddit)
            for post in subreddit_posts:
                post_id = _post_id(post)
                if post_id:
                    post_rows_by_id[post_id] = post
        else:
            failed_subreddits.append(subreddit)
            subreddit_errors[subreddit] = result["error"]

        posts_by_subreddit[subreddit] = len(subreddit_posts)
        total_pages_fetched += int(result.get("pagesFetched", 0) or 0)
        total_new_posts_discovered += int(result.get("newPostsDiscovered", 0) or 0)
        total_refreshed_existing_posts += int(result.get("refreshedExistingPosts", 0) or 0)
        total_skipped_known_posts += int(result.get("skippedKnownPosts", 0) or 0)
        if result.get("reachedKnownFrontier", False):
            frontier_stop_count += 1
        source_updates[subreddit.lower()] = _summarize_source_update(
            subreddit=subreddit,
            mode="live",
            success=result["success"],
            transport=result.get("transport"),
            posts=subreddit_posts,
            comments=[],
            error=result.get("error"),
            extra={
                **source_metadata,
                "liveStopReason": result.get("stopReason"),
                "liveReachedKnownFrontier": result.get("reachedKnownFrontier", False),
                "liveReachedCutoff": result.get("reachedCutoff", False),
                "livePagesFetched": int(result.get("pagesFetched", 0) or 0),
                "liveNewPostsDiscovered": int(result.get("newPostsDiscovered", 0) or 0),
                "liveRefreshedExistingPosts": int(result.get("refreshedExistingPosts", 0) or 0),
                "liveSkippedKnownPosts": int(result.get("skippedKnownPosts", 0) or 0),
            },
        )
        subreddit_outcomes[subreddit.lower()] = {
            "subreddit": subreddit,
            "mode": "live",
            "success": bool(result["success"]),
            "transport": result.get("transport"),
            "stopReason": result.get("stopReason"),
            "reachedCutoff": bool(result.get("reachedCutoff", False)),
            "reachedKnownFrontier": bool(result.get("reachedKnownFrontier", False)),
            "pagesFetched": int(result.get("pagesFetched", 0) or 0),
            "postsFetched": len(subreddit_posts),
            "commentsFetched": 0,
            "newPostsDiscovered": int(result.get("newPostsDiscovered", 0) or 0),
            "refreshedExistingPosts": int(result.get("refreshedExistingPosts", 0) or 0),
            "skippedKnownPosts": int(result.get("skippedKnownPosts", 0) or 0),
            "knownPostsRefreshed": 0,
            "error": result.get("error"),
        }

    fresh_targets = _select_live_comment_targets(
        fetched_posts_by_subreddit,
        subreddit_catalog=subreddit_catalog,
        default_max_tracked_posts=max_tracked_posts,
        default_comment_threshold=comment_threshold,
    )
    known_targets = _select_known_posts_for_comment_refresh_by_subreddit(
        existing_posts,
        subreddits=subreddits,
        subreddit_catalog=subreddit_catalog,
        default_lookback_hours=active_post_lookback_hours,
        default_comment_threshold=comment_threshold,
        default_max_posts=known_posts_refresh_limit,
    )
    target_posts: Dict[str, Dict[str, Any]] = {}
    for post in [*fresh_targets, *known_targets]:
        post_id = _post_id(post)
        if post_id:
            target_posts[post_id] = post

    seen_comment_ids: set[str] = set()
    known_post_ids = {_post_id(post) for post in known_targets}
    known_refresh_counts: Dict[str, int] = {}
    for post in target_posts.values():
        post_id = _post_id(post)
        fetch_profile = _fetch_profile_for_subreddit(_post_subreddit(post), subreddit_catalog)
        comment_result = fetch_comments_for_post(
            post,
            max_comments_per_post=int(
                fetch_profile.get("maxCommentsPerPost", max_comments_per_post)
                or max_comments_per_post
            ),
        )
        subreddit = _post_subreddit(post)

        if not comment_result["success"]:
            subreddit_errors.setdefault(subreddit, comment_result["error"])
            continue
        refreshed_post = comment_result.get("post")
        refreshed_post_id = _post_id(refreshed_post or {})
        if refreshed_post_id:
            post_rows_by_id[refreshed_post_id] = dict(refreshed_post)
            if post_id in known_post_ids:
                known_refresh_counts[subreddit] = known_refresh_counts.get(subreddit, 0) + 1

        for comment in comment_result["comments"]:
            comment_id = str(comment.get("id", ""))
            if not comment_id or comment_id in seen_comment_ids:
                continue
            seen_comment_ids.add(comment_id)
            comments.append(comment)
            comments_by_subreddit[subreddit] = comments_by_subreddit.get(subreddit, 0) + 1

    for subreddit in subreddits:
        key = subreddit.lower()
        current = source_updates.get(key) or _summarize_source_update(
            subreddit=subreddit,
            mode="live",
            success=False,
            transport=None,
            posts=[],
            comments=[],
        )
        source_updates[key] = {
            **current,
            "commentsFetched": comments_by_subreddit.get(subreddit, 0),
            "commentRefreshCount": comments_by_subreddit.get(subreddit, 0),
            "knownPostsRefreshed": known_refresh_counts.get(subreddit, 0),
        }
        outcome = subreddit_outcomes.get(key)
        if outcome is not None:
            outcome["commentsFetched"] = comments_by_subreddit.get(subreddit, 0)
            outcome["knownPostsRefreshed"] = known_refresh_counts.get(subreddit, 0)

    if failed_subreddits and succeeded_subreddits:
        status = "partial"
    elif failed_subreddits:
        status = "failed"
    else:
        status = "success"

    return {
        "mode": "live",
        "fetchedAt": fetched_at,
        "status": status,
        "attemptedSubreddits": subreddits,
        "succeededSubreddits": succeeded_subreddits,
        "failedSubreddits": failed_subreddits,
        "subredditErrors": subreddit_errors,
        "posts": list(post_rows_by_id.values()),
        "comments": comments,
        "postsBySubreddit": posts_by_subreddit,
        "commentsBySubreddit": comments_by_subreddit,
        "pagesFetched": total_pages_fetched,
        "newPostsDiscovered": total_new_posts_discovered,
        "refreshedExistingPosts": total_refreshed_existing_posts,
        "skippedKnownPosts": total_skipped_known_posts,
        "frontierStops": frontier_stop_count,
        "subredditOutcomes": subreddit_outcomes,
        "sourceUpdates": source_updates,
    }
