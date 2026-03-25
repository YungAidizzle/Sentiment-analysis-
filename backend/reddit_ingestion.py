from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Literal

from backend.bluesky_firehose import sync_bluesky_firehose
from backend.bluesky_refresh import refresh_bluesky_sources
from backend.public_source_fetcher import refresh_public_source_items
from backend.reddit_dev_only_config import (
    BUCKET_SIZE,
    COMMENT_THRESHOLD_FOR_EXPANSION,
    DASHBOARD_SNAPSHOT_PATH,
    DEFAULT_SUBREDDITS,
    LIVE_FETCH_LIMIT,
    MAX_COMMENTS_PER_POST,
    MAX_SUBREDDITS_PER_RUN,
    MAX_TRACKED_POSTS,
    REDDIT_ENABLED,
    SUBREDDIT_CATALOG,
    SUBREDDIT_LOOKUP,
)
from backend.reddit_fetcher import fetch_reddit_backfill_bucket, fetch_reddit_live_bucket
from backend.reddit_normalizer import normalize_comment, normalize_submission
from backend.reddit_persistence import (
    append_run,
    ensure_store_root,
    load_comments,
    load_posts,
    load_public_items,
    load_runs,
    load_schedule,
    load_sources,
    load_bluesky_interactions,
    load_bluesky_firehose_state,
    load_bluesky_post_snapshots,
    load_bluesky_posts,
    load_bluesky_profiles,
    load_youtube_channels,
    load_youtube_comments,
    load_youtube_video_snapshots,
    normalize_sources_for_hot_store,
    recompute_source_storage_metrics,
    save_comments,
    save_posts,
    save_public_items,
    save_bluesky_interactions,
    save_bluesky_firehose_state,
    save_bluesky_post_snapshots,
    save_bluesky_posts,
    save_bluesky_profiles,
    save_runs,
    save_schedule,
    save_sources,
    save_youtube_channels,
    save_youtube_comments,
    save_youtube_video_snapshots,
    update_source_health,
    upsert_comments,
    upsert_posts,
)
from backend.reddit_scheduler import advance_schedule_state, get_current_bucket, sync_schedule_state
from backend.reddit_snapshot import build_dashboard_snapshot, write_dashboard_snapshot
from backend.reddit_window import prune_records_to_window

IngestionMode = Literal["backfill", "live"]


def _failure_source_updates(subreddits: list[str]) -> dict[str, dict[str, object]]:
    return {
        subreddit.lower(): {
            "subreddit": subreddit,
            "success": False,
            "transport": None,
            "postsFetched": 0,
            "commentsFetched": 0,
            "commentRefreshCount": 0,
            "error": "ingestion failed before subreddit fetch completed",
        }
        for subreddit in subreddits
    }


def _row_id(row: dict[str, object]) -> str:
    return str(row.get("id", "") or "")


def _row_subreddit(row: dict[str, object]) -> str:
    return str(row.get("subreddit", "") or "").lower()


def _reconcile_post_update_metrics(
    *,
    mode: IngestionMode,
    raw_result: dict[str, object],
    normalized_posts: list[dict[str, object]],
    existing_posts: list[dict[str, object]],
) -> dict[str, object]:
    existing_ids = {
        _row_id(row)
        for row in existing_posts
        if _row_id(row)
    }
    actual_posts_by_subreddit: dict[str, int] = {}
    actual_new_by_subreddit: dict[str, int] = {}
    actual_refreshed_by_subreddit: dict[str, int] = {}

    for row in normalized_posts:
        record_id = _row_id(row)
        subreddit = _row_subreddit(row)
        if not record_id or not subreddit:
            continue
        actual_posts_by_subreddit[subreddit] = actual_posts_by_subreddit.get(subreddit, 0) + 1
        if record_id in existing_ids:
            actual_refreshed_by_subreddit[subreddit] = actual_refreshed_by_subreddit.get(subreddit, 0) + 1
        else:
            actual_new_by_subreddit[subreddit] = actual_new_by_subreddit.get(subreddit, 0) + 1

    source_updates = dict(raw_result.get("sourceUpdates", {}) or {})
    subreddit_outcomes = dict(raw_result.get("subredditOutcomes", {}) or {})
    attempted_subreddits = [
        str(subreddit or "")
        for subreddit in list(raw_result.get("attemptedSubreddits", []) or [])
        if str(subreddit or "").strip()
    ]
    for subreddit in attempted_subreddits:
        key = subreddit.lower()
        posts_fetched = int(actual_posts_by_subreddit.get(key, 0) or 0)
        new_posts = int(actual_new_by_subreddit.get(key, 0) or 0)
        refreshed_posts = int(actual_refreshed_by_subreddit.get(key, 0) or 0)
        if key in source_updates:
            mode_specific_payload = (
                {
                    "liveNewPostsDiscovered": new_posts,
                    "liveRefreshedExistingPosts": refreshed_posts,
                }
                if mode == "live"
                else {
                    "newPostsDiscovered": new_posts,
                    "refreshedExistingPosts": refreshed_posts,
                }
            )
            source_updates[key] = {
                **source_updates[key],
                "postsFetched": posts_fetched,
                **mode_specific_payload,
            }
        if key in subreddit_outcomes:
            subreddit_outcomes[key] = {
                **subreddit_outcomes[key],
                "postsFetched": posts_fetched,
                "newPostsDiscovered": new_posts,
                "refreshedExistingPosts": refreshed_posts,
            }

    return {
        **raw_result,
        "newPostsDiscovered": sum(actual_new_by_subreddit.values()),
        "refreshedExistingPosts": sum(actual_refreshed_by_subreddit.values()),
        "sourceUpdates": source_updates,
        "subredditOutcomes": subreddit_outcomes,
    }


def _persist_and_snapshot(
    *,
    posts: list[dict[str, object]],
    comments: list[dict[str, object]],
    runs: list[dict[str, object]],
    sources: dict[str, dict[str, object]],
    schedule: dict[str, object],
) -> dict[str, object]:
    save_posts(posts)
    save_comments(comments)
    save_runs(runs)
    save_sources(sources)
    save_schedule(schedule)

    snapshot = build_dashboard_snapshot(
        posts=posts,
        comments=comments,
        runs=runs,
        sources=sources,
        schedule=schedule,
    )
    write_dashboard_snapshot(snapshot, DASHBOARD_SNAPSHOT_PATH)
    return snapshot


def _load_ingestion_state() -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, dict[str, object]],
]:
    posts = load_posts()
    comments = load_comments()
    runs = load_runs()
    sources = load_sources()
    posts, comments = prune_records_to_window(posts, comments)
    return posts, comments, runs, sources


def _run_reddit_ingestion_bucket(
    mode: IngestionMode,
    *,
    posts: list[dict[str, object]],
    comments: list[dict[str, object]],
    runs: list[dict[str, object]],
    sources: dict[str, dict[str, object]],
    schedule: dict[str, object],
) -> tuple[
    int,
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, dict[str, object]],
    dict[str, object],
    dict[str, object],
]:
    bucket_started_at = perf_counter()
    schedule = sync_schedule_state(
        schedule,
        subreddits=DEFAULT_SUBREDDITS,
        bucket_size=BUCKET_SIZE,
        max_subreddits_per_run=MAX_SUBREDDITS_PER_RUN,
        catalog_entries=SUBREDDIT_CATALOG,
    )
    current_bucket = get_current_bucket(schedule, mode=mode)
    run_started_at = datetime.now(timezone.utc).isoformat()
    run_id = f"reddit-{mode}-run-{uuid.uuid4().hex[:10]}"
    bucket_id = current_bucket.get("id")
    bucket_tier = current_bucket.get("tier")
    bucket_categories = list(current_bucket.get("categories", []))
    bucket_subreddits = list(current_bucket.get("subreddits", []))

    exit_code = 0
    raw_result: dict[str, object] | None = None
    normalized_posts: list[dict[str, object]] = []
    normalized_comments: list[dict[str, object]] = []

    try:
        pre_upsert_post_count = len(posts)
        pre_upsert_comment_count = len(comments)
        if mode == "backfill":
            raw_result = fetch_reddit_backfill_bucket(
                bucket_subreddits,
                existing_posts=posts,
                existing_sources=sources,
                max_comments_per_post=MAX_COMMENTS_PER_POST,
                comment_threshold=COMMENT_THRESHOLD_FOR_EXPANSION,
                subreddit_catalog=SUBREDDIT_LOOKUP,
            )
        else:
            raw_result = fetch_reddit_live_bucket(
                bucket_subreddits,
                existing_posts=posts,
                existing_sources=sources,
                limit=LIVE_FETCH_LIMIT,
                max_tracked_posts=MAX_TRACKED_POSTS,
                max_comments_per_post=MAX_COMMENTS_PER_POST,
                comment_threshold=COMMENT_THRESHOLD_FOR_EXPANSION,
                subreddit_catalog=SUBREDDIT_LOOKUP,
            )

        fetched_at = str(raw_result.get("fetchedAt", run_started_at))
        normalized_posts = [
            normalize_submission(post, fetched_at)
            for post in raw_result.get("posts", [])
        ]
        normalized_comments = [
            normalize_comment(comment, fetched_at)
            for comment in raw_result.get("comments", [])
        ]
        raw_result = _reconcile_post_update_metrics(
            mode=mode,
            raw_result=raw_result,
            normalized_posts=normalized_posts,
            existing_posts=posts,
        )

        posts = upsert_posts(
            posts,
            normalized_posts,
            run_id=run_id,
            bucket_id=bucket_id,
        )
        comments = upsert_comments(
            comments,
            normalized_comments,
            run_id=run_id,
            bucket_id=bucket_id,
        )
        post_count_before_prune = len(posts)
        comment_count_before_prune = len(comments)
        posts, comments = prune_records_to_window(posts, comments)
        sources = update_source_health(
            sources,
            source_updates=raw_result.get("sourceUpdates", {}),
            attempted_at=fetched_at,
            bucket_id=bucket_id,
            mode=mode,
        )
        sources = recompute_source_storage_metrics(
            posts,
            comments,
            sources,
            tracked_subreddits=schedule.get("subredditMetadata", {}).keys(),
        )
        runs = append_run(
            runs,
            {
                "runId": run_id,
                "mode": mode,
                "startedAt": run_started_at,
                "completedAt": datetime.now(timezone.utc).isoformat(),
                "status": raw_result.get("status", "failed"),
                "bucketId": bucket_id,
                "bucketTier": bucket_tier,
                "bucketCategories": bucket_categories,
                "subredditsAttempted": len(raw_result.get("attemptedSubreddits", [])),
                "subredditsSucceeded": len(raw_result.get("succeededSubreddits", [])),
                "subredditsFailed": len(raw_result.get("failedSubreddits", [])),
                "subreddits": raw_result.get("attemptedSubreddits", []),
                "failedSubreddits": raw_result.get("failedSubreddits", []),
                "postsFetched": len(normalized_posts),
                "commentsFetched": len(normalized_comments),
                "newPostsDiscovered": int(raw_result.get("newPostsDiscovered", 0) or 0),
                "refreshedExistingPosts": int(raw_result.get("refreshedExistingPosts", 0) or 0),
                "skippedKnownPosts": int(raw_result.get("skippedKnownPosts", 0) or 0),
                "pagesFetched": int(raw_result.get("pagesFetched", 0) or 0),
                "frontierStops": int(raw_result.get("frontierStops", 0) or 0),
                "knownPostsRefreshed": sum(
                    int((row or {}).get("knownPostsRefreshed", 0) or 0)
                    for row in dict(raw_result.get("subredditOutcomes", {})).values()
                ),
                "storePostsBeforeRun": pre_upsert_post_count,
                "storeCommentsBeforeRun": pre_upsert_comment_count,
                "storePostsAfterRun": len(posts),
                "storeCommentsAfterRun": len(comments),
                "postsPruned": max(0, post_count_before_prune - len(posts)),
                "commentsPruned": max(0, comment_count_before_prune - len(comments)),
                "errorSummary": " | ".join(
                    f"{name}: {message}"
                    for name, message in dict(raw_result.get("subredditErrors", {})).items()
                )
                or None,
                "rateLimitInfo": None,
                "subredditOutcomes": dict(raw_result.get("subredditOutcomes", {})),
            },
        )
        schedule = advance_schedule_state(schedule, mode=mode)
    except Exception as error:
        exit_code = 1
        failure_time = datetime.now(timezone.utc).isoformat()
        runs = append_run(
            runs,
            {
                "runId": run_id,
                "mode": mode,
                "startedAt": run_started_at,
                "completedAt": failure_time,
                "status": "failed",
                "bucketId": bucket_id,
                "bucketTier": bucket_tier,
                "bucketCategories": bucket_categories,
                "subredditsAttempted": len(bucket_subreddits),
                "subredditsSucceeded": 0,
                "subredditsFailed": len(bucket_subreddits),
                "subreddits": bucket_subreddits,
                "failedSubreddits": bucket_subreddits,
                "postsFetched": 0,
                "commentsFetched": 0,
                "newPostsDiscovered": 0,
                "refreshedExistingPosts": 0,
                "skippedKnownPosts": 0,
                "pagesFetched": 0,
                "frontierStops": 0,
                "knownPostsRefreshed": 0,
                "storePostsBeforeRun": len(posts),
                "storeCommentsBeforeRun": len(comments),
                "storePostsAfterRun": len(posts),
                "storeCommentsAfterRun": len(comments),
                "postsPruned": 0,
                "commentsPruned": 0,
                "errorSummary": str(error),
                "rateLimitInfo": None,
                "subredditOutcomes": {},
            },
        )
        sources = update_source_health(
            sources,
            source_updates=_failure_source_updates(bucket_subreddits),
            attempted_at=failure_time,
            bucket_id=bucket_id,
            mode=mode,
        )
        sources = recompute_source_storage_metrics(
            posts,
            comments,
            sources,
            tracked_subreddits=schedule.get("subredditMetadata", {}).keys(),
        )
        schedule = advance_schedule_state(schedule, mode=mode)
        print(f"Reddit {mode} ingestion failed: {error}")

    snapshot = _persist_and_snapshot(
        posts=posts,
        comments=comments,
        runs=runs,
        sources=sources,
        schedule=schedule,
    )

    latest_run = snapshot.get("latestRun") or {}
    summary = {
        "snapshotPath": str(DASHBOARD_SNAPSHOT_PATH),
        "mode": mode,
        "status": latest_run.get("status"),
        "bucketId": latest_run.get("bucketId"),
        "bucketTier": latest_run.get("bucketTier"),
        "bucketCategories": latest_run.get("bucketCategories", []),
        "attempted": latest_run.get("subredditsAttempted", 0),
        "succeeded": latest_run.get("subredditsSucceeded", 0),
        "failed": latest_run.get("subredditsFailed", 0),
        "postsFetched": latest_run.get("postsFetched", 0),
        "commentsFetched": latest_run.get("commentsFetched", 0),
        "freshnessState": snapshot.get("health", {}).get("freshnessState"),
        "coverageScore": snapshot.get("health", {}).get("coverageScore"),
        "backfillCompletenessPct": snapshot.get("health", {}).get("backfillCompletenessPct"),
        "timings": {
            "bucketTotalMs": round((perf_counter() - bucket_started_at) * 1000, 1),
        },
    }

    return exit_code, summary, posts, comments, runs, sources, schedule, snapshot


def _aggregate_run_summaries(
    mode: IngestionMode,
    run_summaries: list[dict[str, object]],
    snapshot: dict[str, object],
    schedule: dict[str, object],
) -> dict[str, object]:
    mode_state = schedule.get("backfillState" if mode == "backfill" else "liveState", {})

    if not run_summaries:
        return {
            "snapshotPath": str(DASHBOARD_SNAPSHOT_PATH),
            "mode": mode,
            "status": "empty",
            "processedBuckets": 0,
            "successfulBuckets": 0,
            "partialBuckets": 0,
            "failedBuckets": 0,
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "postsFetched": 0,
            "commentsFetched": 0,
            "newPostsDiscovered": 0,
            "knownPostsRefreshed": 0,
            "frontierStops": 0,
            "pagesFetched": 0,
            "postsPruned": 0,
            "commentsPruned": 0,
            "freshnessState": snapshot.get("health", {}).get("freshnessState"),
            "coverageScore": snapshot.get("health", {}).get("coverageScore"),
            "backfillCompletenessPct": snapshot.get("health", {}).get("backfillCompletenessPct"),
            "currentBucketId": mode_state.get("currentBucketId"),
            "bucketSummaries": [],
        }

    successful_buckets = sum(1 for row in run_summaries if row.get("status") == "success")
    partial_buckets = sum(1 for row in run_summaries if row.get("status") == "partial")
    failed_buckets = sum(1 for row in run_summaries if row.get("status") == "failed")
    if failed_buckets and (successful_buckets or partial_buckets):
        status = "partial"
    elif failed_buckets:
        status = "failed"
    else:
        status = "success"

    return {
        "snapshotPath": str(DASHBOARD_SNAPSHOT_PATH),
        "mode": mode,
        "status": status,
        "processedBuckets": len(run_summaries),
        "successfulBuckets": successful_buckets,
        "partialBuckets": partial_buckets,
        "failedBuckets": failed_buckets,
        "attempted": sum(int(row.get("attempted", 0) or 0) for row in run_summaries),
        "succeeded": sum(int(row.get("succeeded", 0) or 0) for row in run_summaries),
        "failed": sum(int(row.get("failed", 0) or 0) for row in run_summaries),
        "postsFetched": sum(int(row.get("postsFetched", 0) or 0) for row in run_summaries),
        "commentsFetched": sum(int(row.get("commentsFetched", 0) or 0) for row in run_summaries),
        "newPostsDiscovered": sum(int(row.get("newPostsDiscovered", 0) or 0) for row in run_summaries),
        "knownPostsRefreshed": sum(int(row.get("knownPostsRefreshed", 0) or 0) for row in run_summaries),
        "frontierStops": sum(int(row.get("frontierStops", 0) or 0) for row in run_summaries),
        "pagesFetched": sum(int(row.get("pagesFetched", 0) or 0) for row in run_summaries),
        "postsPruned": sum(int(row.get("postsPruned", 0) or 0) for row in run_summaries),
        "commentsPruned": sum(int(row.get("commentsPruned", 0) or 0) for row in run_summaries),
        "freshnessState": snapshot.get("health", {}).get("freshnessState"),
        "coverageScore": snapshot.get("health", {}).get("coverageScore"),
        "backfillCompletenessPct": snapshot.get("health", {}).get("backfillCompletenessPct"),
        "currentBucketId": mode_state.get("currentBucketId"),
        "bucketSummaries": run_summaries,
    }


def _run_auxiliary_refreshes(
    *,
    posts: list[dict[str, object]],
    overall_exit_code: int,
) -> tuple[int, dict[str, object], dict[str, object]]:
    public_refresh_summary = {
        "fetchedCount": 0,
        "storedCount": 0,
        "successCount": 0,
        "failureCount": 0,
        "youtubeCommentCount": 0,
        "youtubeSnapshotCount": 0,
        "timings": {
            "totalMs": 0.0,
            "perSourceMs": {},
        },
        "skipped": False,
    }
    public_items = load_public_items()
    youtube_comments = load_youtube_comments()
    youtube_snapshots = load_youtube_video_snapshots()
    youtube_channels = load_youtube_channels()
    try:
        public_refresh_started_at = perf_counter()
        public_refresh = refresh_public_source_items(
            existing_items=public_items,
            existing_youtube_comments=youtube_comments,
            existing_youtube_snapshots=youtube_snapshots,
            existing_youtube_channels=youtube_channels,
            reddit_posts=posts,
        )
        save_public_items(public_refresh["items"])
        save_youtube_comments(public_refresh.get("youtubeComments", youtube_comments))
        save_youtube_video_snapshots(public_refresh.get("youtubeVideoSnapshots", youtube_snapshots))
        save_youtube_channels(public_refresh.get("youtubeChannels", youtube_channels))
        public_items = public_refresh.get("items", public_items)
        public_refresh_summary = {
            "fetchedCount": int(public_refresh.get("fetchedCount", 0) or 0),
            "storedCount": int(public_refresh.get("storedCount", 0) or 0),
            "successCount": int(public_refresh.get("successCount", 0) or 0),
            "failureCount": int(public_refresh.get("failureCount", 0) or 0),
            "youtubeCommentCount": len(public_refresh.get("youtubeComments", youtube_comments)),
            "youtubeSnapshotCount": len(public_refresh.get("youtubeVideoSnapshots", youtube_snapshots)),
            "timings": {
                **dict(public_refresh.get("timings", {})),
                "totalMs": round((perf_counter() - public_refresh_started_at) * 1000, 1),
            },
            "skipped": False,
        }
    except Exception as error:
        print(f"Public source refresh failed: {error}")
        overall_exit_code = max(overall_exit_code, 1)

    bluesky_refresh_summary = {
        "fetchedCount": 0,
        "snapshotCount": 0,
        "profileCount": 0,
        "interactionCount": 0,
        "timings": {
            "totalMs": 0.0,
            "perSourceMs": {},
        },
        "skipped": False,
    }
    try:
        bluesky_refresh_started_at = perf_counter()
        existing_bluesky_posts = load_bluesky_posts()
        existing_bluesky_snapshots = load_bluesky_post_snapshots()
        existing_bluesky_profiles = load_bluesky_profiles()
        existing_bluesky_interactions = load_bluesky_interactions()
        existing_bluesky_state = load_bluesky_firehose_state()
        try:
            bluesky_refresh = sync_bluesky_firehose(
                existing_posts=existing_bluesky_posts,
                existing_snapshots=existing_bluesky_snapshots,
                existing_profiles=existing_bluesky_profiles,
                existing_interactions=existing_bluesky_interactions,
                existing_state=existing_bluesky_state,
            )
        except Exception as firehose_error:
            print(f"Bluesky firehose sync failed, falling back to legacy refresh: {firehose_error}")
            bluesky_refresh = refresh_bluesky_sources(
                existing_posts=existing_bluesky_posts,
                existing_snapshots=existing_bluesky_snapshots,
                existing_profiles=existing_bluesky_profiles,
                existing_interactions=existing_bluesky_interactions,
                reddit_posts=posts,
                public_items=public_items,
            )
        save_bluesky_posts(bluesky_refresh.get("posts", []))
        save_bluesky_post_snapshots(bluesky_refresh.get("snapshots", []))
        save_bluesky_profiles(bluesky_refresh.get("profiles", []))
        save_bluesky_interactions(bluesky_refresh.get("interactions", []))
        save_bluesky_firehose_state(dict(bluesky_refresh.get("state", existing_bluesky_state)))
        bluesky_refresh_summary = {
            "fetchedCount": int(bluesky_refresh.get("fetchedCount", 0) or 0),
            "snapshotCount": int(bluesky_refresh.get("snapshotCount", 0) or 0),
            "profileCount": int(bluesky_refresh.get("profileCount", 0) or 0),
            "interactionCount": int(bluesky_refresh.get("interactionCount", 0) or 0),
            "timings": {
                **dict(bluesky_refresh.get("timings", {})),
                "totalMs": round((perf_counter() - bluesky_refresh_started_at) * 1000, 1),
            },
            "skipped": False,
        }
    except Exception as error:
        print(f"Bluesky refresh failed: {error}")
        overall_exit_code = max(overall_exit_code, 1)

    return overall_exit_code, public_refresh_summary, bluesky_refresh_summary


def run_reddit_ingestion(
    mode: IngestionMode,
    *,
    run_all_buckets: bool = False,
    refresh_auxiliary: bool = True,
) -> tuple[int, dict[str, object]]:
    ingestion_started_at = perf_counter()
    ensure_store_root()
    schedule = sync_schedule_state(
        load_schedule(),
        subreddits=DEFAULT_SUBREDDITS,
        bucket_size=BUCKET_SIZE,
        max_subreddits_per_run=MAX_SUBREDDITS_PER_RUN,
        catalog_entries=SUBREDDIT_CATALOG,
    )
    posts, comments, runs, sources = _load_ingestion_state()
    sources = normalize_sources_for_hot_store(
        sources,
        schedule_metadata=schedule.get("subredditMetadata", {}),
    )
    sources = recompute_source_storage_metrics(
        posts,
        comments,
        sources,
        tracked_subreddits=schedule.get("subredditMetadata", {}).keys(),
    )
    if not REDDIT_ENABLED:
        snapshot = _persist_and_snapshot(
            posts=posts,
            comments=comments,
            runs=runs,
            sources=sources,
            schedule=schedule,
        )
        return 0, {
            "snapshotPath": str(DASHBOARD_SNAPSHOT_PATH),
            "mode": mode,
            "status": "disabled",
            "reason": "REDDIT_ENABLED=0",
            "processedBuckets": 0,
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "postsFetched": 0,
            "commentsFetched": 0,
            "auxiliaryRefreshSkipped": True,
            "freshnessState": snapshot.get("health", {}).get("freshnessState"),
            "coverageScore": snapshot.get("health", {}).get("coverageScore"),
            "backfillCompletenessPct": snapshot.get("health", {}).get("backfillCompletenessPct"),
            "timings": {
                "totalIngestionMs": round((perf_counter() - ingestion_started_at) * 1000, 1),
            },
        }

    schedule_buckets = schedule.get("backfillBuckets" if mode == "backfill" else "liveBuckets", []) or schedule.get("buckets", [])
    bucket_count = len(schedule_buckets) if run_all_buckets else 1
    run_summaries: list[dict[str, object]] = []
    overall_exit_code = 0
    snapshot: dict[str, object] = {}

    for _ in range(bucket_count):
        (
            exit_code,
            summary,
            posts,
            comments,
            runs,
            sources,
            schedule,
            snapshot,
        ) = _run_reddit_ingestion_bucket(
            mode,
            posts=posts,
            comments=comments,
            runs=runs,
            sources=sources,
            schedule=schedule,
        )
        overall_exit_code = max(overall_exit_code, exit_code)
        run_summaries.append(summary)

    if refresh_auxiliary:
        (
            overall_exit_code,
            public_refresh_summary,
            bluesky_refresh_summary,
        ) = _run_auxiliary_refreshes(
            posts=posts,
            overall_exit_code=overall_exit_code,
        )
    else:
        public_refresh_summary = {
            "fetchedCount": 0,
            "storedCount": 0,
            "successCount": 0,
            "failureCount": 0,
            "youtubeCommentCount": 0,
            "youtubeSnapshotCount": 0,
            "timings": {
                "totalMs": 0.0,
                "perSourceMs": {},
            },
            "skipped": True,
        }
        bluesky_refresh_summary = {
            "fetchedCount": 0,
            "snapshotCount": 0,
            "profileCount": 0,
            "interactionCount": 0,
            "timings": {
                "totalMs": 0.0,
                "perSourceMs": {},
            },
            "skipped": True,
        }

    if run_all_buckets:
        aggregate = _aggregate_run_summaries(mode, run_summaries, snapshot, schedule)
        aggregate.update(
            {
                "publicItemsFetched": public_refresh_summary["fetchedCount"],
                "publicItemsStored": public_refresh_summary["storedCount"],
                "publicSourcesSucceeded": public_refresh_summary["successCount"],
                "publicSourcesFailed": public_refresh_summary["failureCount"],
                "youtubeCommentsStored": public_refresh_summary["youtubeCommentCount"],
                "youtubeSnapshotsStored": public_refresh_summary["youtubeSnapshotCount"],
                "blueskyPostsStored": bluesky_refresh_summary["fetchedCount"],
                "blueskySnapshotsStored": bluesky_refresh_summary["snapshotCount"],
                "blueskyProfilesStored": bluesky_refresh_summary["profileCount"],
                "blueskyInteractionsStored": bluesky_refresh_summary["interactionCount"],
                "auxiliaryRefreshSkipped": not refresh_auxiliary,
                "timings": {
                    "totalIngestionMs": round((perf_counter() - ingestion_started_at) * 1000, 1),
                    "publicRefresh": public_refresh_summary["timings"],
                    "blueskyRefresh": bluesky_refresh_summary["timings"],
                    "bucketDurationsMs": [
                        float(row.get("timings", {}).get("bucketTotalMs", 0) or 0)
                        for row in run_summaries
                    ],
                },
            }
        )
        return overall_exit_code, aggregate

    summary = run_summaries[0] if run_summaries else {}
    summary.update(
        {
            "publicItemsFetched": public_refresh_summary["fetchedCount"],
            "publicItemsStored": public_refresh_summary["storedCount"],
            "publicSourcesSucceeded": public_refresh_summary["successCount"],
            "publicSourcesFailed": public_refresh_summary["failureCount"],
            "youtubeCommentsStored": public_refresh_summary["youtubeCommentCount"],
            "youtubeSnapshotsStored": public_refresh_summary["youtubeSnapshotCount"],
            "blueskyPostsStored": bluesky_refresh_summary["fetchedCount"],
            "blueskySnapshotsStored": bluesky_refresh_summary["snapshotCount"],
            "blueskyProfilesStored": bluesky_refresh_summary["profileCount"],
            "blueskyInteractionsStored": bluesky_refresh_summary["interactionCount"],
            "auxiliaryRefreshSkipped": not refresh_auxiliary,
            "timings": {
                "totalIngestionMs": round((perf_counter() - ingestion_started_at) * 1000, 1),
                "bucketTotalMs": float(summary.get("timings", {}).get("bucketTotalMs", 0) or 0),
                "publicRefresh": public_refresh_summary["timings"],
                "blueskyRefresh": bluesky_refresh_summary["timings"],
            },
        }
    )
    return overall_exit_code, summary


def print_summary(summary: dict[str, object]) -> None:
    print(json.dumps(summary, indent=2))
