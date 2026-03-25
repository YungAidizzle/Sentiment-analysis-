from __future__ import annotations

import errno
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from backend.reddit_dev_only_config import (
    BLUESKY_INTERACTIONS_PATH,
    BLUESKY_FIREHOSE_STATE_PATH,
    BLUESKY_POSTS_PATH,
    BLUESKY_POST_SNAPSHOTS_PATH,
    BLUESKY_PROFILES_PATH,
    COMMENTS_PATH,
    POSTS_PATH,
    PUBLIC_ITEMS_PATH,
    RUNS_PATH,
    RUN_RETENTION,
    SCHEDULE_PATH,
    SOURCES_PATH,
    STORE_ROOT,
    REDDIT_SOURCE_STATE_VERSION,
    REDDIT_STORE_MODEL,
    YOUTUBE_CHANNELS_PATH,
    YOUTUBE_COMMENTS_PATH,
    YOUTUBE_VIDEO_SNAPSHOTS_PATH,
)


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp_path.write_text(serialized, encoding="utf-8")

    retryable_errors = {
        errno.EACCES,
        errno.EBUSY,
        errno.ENOENT,
        errno.EPERM,
    }

    fallback_error: OSError | None = None
    try:
        for attempt in range(6):
            try:
                os.replace(temp_path, path)
                return
            except OSError as error:
                fallback_error = error
                if error.errno not in retryable_errors or attempt >= 5:
                    break

                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass

                time.sleep(0.05 * (attempt + 1))

        try:
            path.write_text(serialized, encoding="utf-8")
        except OSError:
            if fallback_error is not None:
                raise fallback_error
            raise
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def ensure_store_root() -> None:
    STORE_ROOT.mkdir(parents=True, exist_ok=True)


def load_posts() -> List[Dict[str, Any]]:
    return _read_json(POSTS_PATH, [])


def load_comments() -> List[Dict[str, Any]]:
    return _read_json(COMMENTS_PATH, [])


def load_runs() -> List[Dict[str, Any]]:
    return _read_json(RUNS_PATH, [])


def load_sources() -> Dict[str, Dict[str, Any]]:
    return _read_json(SOURCES_PATH, {})


def load_public_items() -> List[Dict[str, Any]]:
    return _read_json(PUBLIC_ITEMS_PATH, [])


def load_youtube_comments() -> List[Dict[str, Any]]:
    return _read_json(YOUTUBE_COMMENTS_PATH, [])


def load_youtube_video_snapshots() -> List[Dict[str, Any]]:
    return _read_json(YOUTUBE_VIDEO_SNAPSHOTS_PATH, [])


def load_youtube_channels() -> List[Dict[str, Any]]:
    return _read_json(YOUTUBE_CHANNELS_PATH, [])


def load_bluesky_posts() -> List[Dict[str, Any]]:
    return _read_json(BLUESKY_POSTS_PATH, [])


def load_bluesky_post_snapshots() -> List[Dict[str, Any]]:
    return _read_json(BLUESKY_POST_SNAPSHOTS_PATH, [])


def load_bluesky_profiles() -> List[Dict[str, Any]]:
    return _read_json(BLUESKY_PROFILES_PATH, [])


def load_bluesky_interactions() -> List[Dict[str, Any]]:
    return _read_json(BLUESKY_INTERACTIONS_PATH, [])


def load_bluesky_firehose_state() -> Dict[str, Any]:
    return _read_json(BLUESKY_FIREHOSE_STATE_PATH, {})


def load_schedule() -> Dict[str, Any]:
    return _read_json(SCHEDULE_PATH, {})


def save_posts(posts: List[Dict[str, Any]]) -> None:
    _write_json(POSTS_PATH, posts)


def save_comments(comments: List[Dict[str, Any]]) -> None:
    _write_json(COMMENTS_PATH, comments)


def save_runs(runs: List[Dict[str, Any]]) -> None:
    _write_json(RUNS_PATH, runs[-RUN_RETENTION:])


def save_sources(sources: Dict[str, Dict[str, Any]]) -> None:
    _write_json(SOURCES_PATH, sources)


def save_public_items(items: List[Dict[str, Any]]) -> None:
    _write_json(PUBLIC_ITEMS_PATH, items)


def save_youtube_comments(items: List[Dict[str, Any]]) -> None:
    _write_json(YOUTUBE_COMMENTS_PATH, items)


def save_youtube_video_snapshots(items: List[Dict[str, Any]]) -> None:
    _write_json(YOUTUBE_VIDEO_SNAPSHOTS_PATH, items)


def save_youtube_channels(items: List[Dict[str, Any]]) -> None:
    _write_json(YOUTUBE_CHANNELS_PATH, items)


def save_bluesky_posts(items: List[Dict[str, Any]]) -> None:
    _write_json(BLUESKY_POSTS_PATH, items)


def save_bluesky_post_snapshots(items: List[Dict[str, Any]]) -> None:
    _write_json(BLUESKY_POST_SNAPSHOTS_PATH, items)


def save_bluesky_profiles(items: List[Dict[str, Any]]) -> None:
    _write_json(BLUESKY_PROFILES_PATH, items)


def save_bluesky_interactions(items: List[Dict[str, Any]]) -> None:
    _write_json(BLUESKY_INTERACTIONS_PATH, items)


def save_bluesky_firehose_state(state: Dict[str, Any]) -> None:
    _write_json(BLUESKY_FIREHOSE_STATE_PATH, state)


def save_schedule(schedule: Dict[str, Any]) -> None:
    _write_json(SCHEDULE_PATH, schedule)


def normalize_sources_for_hot_store(
    existing_sources: Dict[str, Dict[str, Any]],
    *,
    schedule_metadata: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Dict[str, Any]]:
    current_sources = {
        str((value or {}).get("subreddit") or key or "").lower(): dict(value or {})
        for key, value in dict(existing_sources or {}).items()
        if str((value or {}).get("subreddit") or key or "").strip()
    }
    metadata = {
        str(key or "").lower(): dict(value or {})
        for key, value in dict(schedule_metadata or {}).items()
        if str(key or "").strip()
    }
    normalized: Dict[str, Dict[str, Any]] = {}
    all_keys = set(metadata.keys()) if metadata else set(current_sources.keys())

    for key in all_keys:
        current = dict(current_sources.get(key, {}) or {})
        source_metadata = dict(metadata.get(key, {}) or {})
        fetch_profile = dict(source_metadata.get("fetchProfile", {}) or {})
        subreddit_name = (
            str(current.get("subreddit") or source_metadata.get("subreddit") or key).strip()
        )
        if not subreddit_name:
            continue

        source_key = subreddit_name.lower()
        normalized[source_key] = {
            **current,
            **{k: v for k, v in source_metadata.items() if k != "fetchProfile"},
            "subreddit": subreddit_name,
            "storeModel": REDDIT_STORE_MODEL,
            "sourceStateVersion": REDDIT_SOURCE_STATE_VERSION,
            "liveCadenceWeight": int(
                fetch_profile.get("liveCadenceWeight", current.get("liveCadenceWeight", 1)) or 1
            ),
            "liveLimit": int(fetch_profile.get("liveLimit", current.get("liveLimit", 0)) or 0),
            "liveListings": list(fetch_profile.get("liveListings") or ["new"]),
            "liveMaxPages": int(fetch_profile.get("liveMaxPages", current.get("liveMaxPages", 0)) or 0),
            "liveMaxPostsPerRun": int(
                fetch_profile.get("liveMaxPostsPerRun", current.get("liveMaxPostsPerRun", 0)) or 0
            ),
            "commentThreshold": int(
                fetch_profile.get("commentThreshold", current.get("commentThreshold", 0)) or 0
            ),
            "maxTrackedPosts": int(
                fetch_profile.get("maxTrackedPosts", current.get("maxTrackedPosts", 0)) or 0
            ),
            "maxCommentsPerPost": int(
                fetch_profile.get("maxCommentsPerPost", current.get("maxCommentsPerPost", 0)) or 0
            ),
            "knownPostsRefreshLimit": int(
                fetch_profile.get("knownPostsRefreshLimit", current.get("knownPostsRefreshLimit", 0)) or 0
            ),
            "activePostLookbackHours": int(
                fetch_profile.get("activePostLookbackHours", current.get("activePostLookbackHours", 0)) or 0
            ),
            "backfillMaxPosts": int(
                fetch_profile.get("backfillMaxPosts", current.get("backfillMaxPosts", 0)) or 0
            ),
            "backfillMaxPages": int(
                fetch_profile.get("backfillMaxPages", current.get("backfillMaxPages", 0)) or 0
            ),
            "backfillMaxCommentThreads": int(
                fetch_profile.get("backfillMaxCommentThreads", current.get("backfillMaxCommentThreads", 0)) or 0
            ),
            "liveStopReason": current.get("liveStopReason"),
            "liveReachedKnownFrontier": bool(current.get("liveReachedKnownFrontier", False)),
            "liveReachedCutoff": bool(current.get("liveReachedCutoff", False)),
            "livePagesFetched": int(current.get("livePagesFetched", 0) or 0),
            "liveNewPostsDiscovered": int(current.get("liveNewPostsDiscovered", 0) or 0),
            "liveRefreshedExistingPosts": int(current.get("liveRefreshedExistingPosts", 0) or 0),
            "liveSkippedKnownPosts": int(current.get("liveSkippedKnownPosts", 0) or 0),
            "backfillReachedKnownFrontier": bool(current.get("backfillReachedKnownFrontier", False)),
            "backfillPagesFetched": int(current.get("backfillPagesFetched", 0) or 0),
            "backfillSkippedKnownPosts": int(current.get("backfillSkippedKnownPosts", 0) or 0),
            "backfillNewPostsDiscovered": int(current.get("backfillNewPostsDiscovered", 0) or 0),
            "backfillRefreshedExistingPosts": int(current.get("backfillRefreshedExistingPosts", 0) or 0),
        }

    return normalized


def _upsert_records(
    existing_rows: Iterable[Dict[str, Any]],
    incoming_rows: Iterable[Dict[str, Any]],
    *,
    run_id: str,
    bucket_id: str | None,
    id_key: str,
) -> List[Dict[str, Any]]:
    merged = {
        str(row.get(id_key, "")): row
        for row in existing_rows
        if str(row.get(id_key, ""))
    }

    for row in incoming_rows:
        record_id = str(row.get(id_key, ""))
        if not record_id:
            continue

        existing = merged.get(record_id, {})
        fetched_at = row.get("fetchedAt") or existing.get("lastFetchedAt")
        merged[record_id] = {
            **existing,
            **row,
            "firstSeenAt": existing.get("firstSeenAt") or fetched_at,
            "lastFetchedAt": fetched_at,
            "lastRunId": run_id,
            "lastBucketId": bucket_id,
        }

    return sorted(
        merged.values(),
        key=lambda row: (
            int(row.get("createdUtc", 0) or 0),
            str(row.get(id_key, "")),
        ),
        reverse=True,
    )


def upsert_posts(
    existing_posts: Iterable[Dict[str, Any]],
    incoming_posts: Iterable[Dict[str, Any]],
    *,
    run_id: str,
    bucket_id: str | None,
) -> List[Dict[str, Any]]:
    return _upsert_records(
        existing_posts,
        incoming_posts,
        run_id=run_id,
        bucket_id=bucket_id,
        id_key="id",
    )


def upsert_comments(
    existing_comments: Iterable[Dict[str, Any]],
    incoming_comments: Iterable[Dict[str, Any]],
    *,
    run_id: str,
    bucket_id: str | None,
) -> List[Dict[str, Any]]:
    return _upsert_records(
        existing_comments,
        incoming_comments,
        run_id=run_id,
        bucket_id=bucket_id,
        id_key="id",
    )


def update_source_health(
    existing_sources: Dict[str, Dict[str, Any]],
    *,
    source_updates: Dict[str, Dict[str, Any]],
    attempted_at: str,
    bucket_id: str | None,
    mode: str,
) -> Dict[str, Dict[str, Any]]:
    updated = {**existing_sources}

    for subreddit, payload in source_updates.items():
        key = subreddit.lower()
        current = updated.get(key, {})
        was_success = bool(payload.get("success"))
        posts_fetched = int(payload.get("postsFetched", 0) or 0)
        comments_fetched = int(payload.get("commentsFetched", 0) or 0)
        oldest_post_created_utc = payload.get("oldestPostCreatedUtc")
        newest_post_created_utc = payload.get("newestPostCreatedUtc")
        merged = {
            **current,
            "subreddit": subreddit,
            "storeModel": REDDIT_STORE_MODEL,
            "sourceStateVersion": REDDIT_SOURCE_STATE_VERSION,
            "category": payload.get("category") or current.get("category"),
            "categoryLabel": payload.get("categoryLabel") or current.get("categoryLabel"),
            "tier": payload.get("tier") or current.get("tier"),
            "activityTier": payload.get("activityTier") or current.get("activityTier"),
            "audienceScale": payload.get("audienceScale") or current.get("audienceScale"),
            "broadCommunity": (
                bool(payload.get("broadCommunity"))
                if "broadCommunity" in payload
                else bool(current.get("broadCommunity", False))
            ),
            "spamProne": (
                bool(payload.get("spamProne"))
                if "spamProne" in payload
                else bool(current.get("spamProne", False))
            ),
            "tags": list(payload.get("tags") or current.get("tags") or []),
            "priorityScore": int(payload.get("priorityScore", current.get("priorityScore", 0)) or 0),
            "discoverySource": payload.get("discoverySource") or current.get("discoverySource"),
            "liveCadenceWeight": int(payload.get("liveCadenceWeight", current.get("liveCadenceWeight", 1)) or 1),
            "liveLimit": int(payload.get("liveLimit", current.get("liveLimit", 0)) or 0),
            "liveListings": list(payload.get("liveListings") or current.get("liveListings") or []),
            "liveMaxPages": int(payload.get("liveMaxPages", current.get("liveMaxPages", 0)) or 0),
            "liveMaxPostsPerRun": int(
                payload.get("liveMaxPostsPerRun", current.get("liveMaxPostsPerRun", 0)) or 0
            ),
            "commentThreshold": int(payload.get("commentThreshold", current.get("commentThreshold", 0)) or 0),
            "maxTrackedPosts": int(payload.get("maxTrackedPosts", current.get("maxTrackedPosts", 0)) or 0),
            "maxCommentsPerPost": int(payload.get("maxCommentsPerPost", current.get("maxCommentsPerPost", 0)) or 0),
            "knownPostsRefreshLimit": int(
                payload.get("knownPostsRefreshLimit", current.get("knownPostsRefreshLimit", 0)) or 0
            ),
            "activePostLookbackHours": int(
                payload.get("activePostLookbackHours", current.get("activePostLookbackHours", 0)) or 0
            ),
            "lastAttemptedAt": attempted_at,
            "lastSuccessAt": attempted_at if was_success else current.get("lastSuccessAt"),
            "lastFailureAt": attempted_at if not was_success else current.get("lastFailureAt"),
            "consecutiveFailures": 0
            if was_success
            else int(current.get("consecutiveFailures", 0) or 0) + 1,
            "lastPostsFetched": posts_fetched,
            "lastCommentsFetched": comments_fetched,
            "lastRunMode": mode,
            "lastBucketId": bucket_id,
            "lastTransport": payload.get("transport") or current.get("lastTransport"),
            "healthStatus": current.get("healthStatus", "unknown"),
            "lastKnownPostCreatedUtc": max(
                int(current.get("lastKnownPostCreatedUtc", 0) or 0),
                int(newest_post_created_utc or 0),
            )
            or current.get("lastKnownPostCreatedUtc"),
        }

        if oldest_post_created_utc:
            previous_oldest = int(current.get("backfillOldestPostCreatedUtc", 0) or 0)
            merged["backfillOldestPostCreatedUtc"] = (
                min(previous_oldest, int(oldest_post_created_utc))
                if previous_oldest
                else int(oldest_post_created_utc)
            )

        if newest_post_created_utc:
            merged["backfillNewestPostCreatedUtc"] = max(
                int(current.get("backfillNewestPostCreatedUtc", 0) or 0),
                int(newest_post_created_utc),
            )

        if mode == "backfill":
            backfill_complete = bool(payload.get("backfillComplete"))
            merged.update(
                {
                    "lastBackfillAttemptedAt": attempted_at,
                    "lastBackfillSuccessAt": attempted_at
                    if was_success
                    else current.get("lastBackfillSuccessAt"),
                    "backfillStatus": (
                        "complete"
                        if was_success and backfill_complete
                        else "incomplete"
                        if was_success
                        else current.get("backfillStatus", "pending")
                    ),
                    "backfillCompletedAt": attempted_at
                    if was_success and backfill_complete
                    else current.get("backfillCompletedAt"),
                    "backfillReachedCutoff": bool(payload.get("backfillReachedCutoff")),
                    "backfillExhaustedListing": bool(payload.get("backfillExhaustedListing")),
                    "backfillReachedKnownFrontier": bool(payload.get("backfillReachedKnownFrontier")),
                    "backfillStopReason": payload.get("backfillStopReason")
                    or current.get("backfillStopReason"),
                    "backfillPagesFetched": int(
                        payload.get("backfillPagesFetched", current.get("backfillPagesFetched", 0)) or 0
                    ),
                    "backfillSkippedKnownPosts": int(
                        payload.get("backfillSkippedKnownPosts", current.get("backfillSkippedKnownPosts", 0)) or 0
                    ),
                    "backfillNewPostsDiscovered": int(
                        payload.get("newPostsDiscovered", current.get("backfillNewPostsDiscovered", 0)) or 0
                    ),
                    "backfillRefreshedExistingPosts": int(
                        payload.get("refreshedExistingPosts", current.get("backfillRefreshedExistingPosts", 0)) or 0
                    ),
                    "backfillMaxPosts": int(payload.get("backfillMaxPosts", current.get("backfillMaxPosts", 0)) or 0),
                    "backfillMaxPages": int(payload.get("backfillMaxPages", current.get("backfillMaxPages", 0)) or 0),
                    "backfillMaxCommentThreads": int(
                        payload.get("backfillMaxCommentThreads", current.get("backfillMaxCommentThreads", 0)) or 0
                    ),
                }
            )
        else:
            merged.update(
                {
                    "lastLiveAttemptedAt": attempted_at,
                    "lastLiveSuccessAt": attempted_at
                    if was_success
                    else current.get("lastLiveSuccessAt"),
                    "liveCommentRefreshCount": int(payload.get("commentRefreshCount", 0) or 0),
                    "liveKnownPostsRefreshed": int(payload.get("knownPostsRefreshed", 0) or 0),
                    "liveStopReason": payload.get("liveStopReason") or current.get("liveStopReason"),
                    "liveReachedKnownFrontier": bool(payload.get("liveReachedKnownFrontier")),
                    "liveReachedCutoff": bool(payload.get("liveReachedCutoff")),
                    "livePagesFetched": int(payload.get("livePagesFetched", current.get("livePagesFetched", 0)) or 0),
                    "liveNewPostsDiscovered": int(
                        payload.get("liveNewPostsDiscovered", current.get("liveNewPostsDiscovered", 0)) or 0
                    ),
                    "liveRefreshedExistingPosts": int(
                        payload.get("liveRefreshedExistingPosts", current.get("liveRefreshedExistingPosts", 0)) or 0
                    ),
                    "liveSkippedKnownPosts": int(
                        payload.get("liveSkippedKnownPosts", current.get("liveSkippedKnownPosts", 0)) or 0
                    ),
                }
            )

        updated[key] = merged

    return updated


def recompute_source_storage_metrics(
    posts: Iterable[Dict[str, Any]],
    comments: Iterable[Dict[str, Any]],
    existing_sources: Dict[str, Dict[str, Any]],
    *,
    tracked_subreddits: Iterable[str] | None = None,
) -> Dict[str, Dict[str, Any]]:
    updated = {**existing_sources}
    tracked_keys = {
        str(subreddit or "").lower()
        for subreddit in list(tracked_subreddits or [])
        if str(subreddit or "").strip()
    }
    post_counts: Dict[str, int] = {}
    comment_counts: Dict[str, int] = {}
    oldest_post_utc: Dict[str, int] = {}
    newest_post_utc: Dict[str, int] = {}

    for row in posts:
        subreddit = str(row.get("subreddit", "")).lower()
        if not subreddit or (tracked_keys and subreddit not in tracked_keys):
            continue
        created_utc = int(row.get("createdUtc", 0) or 0)
        post_counts[subreddit] = post_counts.get(subreddit, 0) + 1
        oldest_post_utc[subreddit] = (
            min(oldest_post_utc.get(subreddit, created_utc), created_utc)
            if subreddit in oldest_post_utc
            else created_utc
        )
        newest_post_utc[subreddit] = max(newest_post_utc.get(subreddit, 0), created_utc)

    for row in comments:
        subreddit = str(row.get("subreddit", "")).lower()
        if not subreddit or (tracked_keys and subreddit not in tracked_keys):
            continue
        comment_counts[subreddit] = comment_counts.get(subreddit, 0) + 1

    all_keys = tracked_keys or (set(updated.keys()) | set(post_counts.keys()) | set(comment_counts.keys()))
    for key in all_keys:
        current = updated.get(key, {"subreddit": key})
        updated[key] = {
            **current,
            "storedPostsInWindow": post_counts.get(key, 0),
            "storedCommentsInWindow": comment_counts.get(key, 0),
            "oldestStoredPostCreatedUtc": oldest_post_utc.get(key),
            "newestStoredPostCreatedUtc": newest_post_utc.get(key),
            "hasWindowCoverage": post_counts.get(key, 0) > 0,
        }

    return updated


def append_run(existing_runs: List[Dict[str, Any]], run_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [*existing_runs, run_row]
