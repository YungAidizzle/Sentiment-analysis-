from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from backend.reddit_dev_only_config import DASHBOARD_SNAPSHOT_PATH, ROTATION_WORKER_STATE_PATH
from backend.reddit_scheduler import summarize_rotation
from backend.reddit_window import prune_records_to_window


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _minutes_since(timestamp: str | None, reference_time: datetime) -> float | None:
    parsed = _parse_iso(timestamp)
    if parsed is None:
        return None
    return max(0.0, (reference_time - parsed).total_seconds() / 60)


def _process_exists(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _read_worker_state(path: Path = ROTATION_WORKER_STATE_PATH) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _rotation_worker_runtime(schedule: Dict[str, Any]) -> Dict[str, Any]:
    rotation_state = dict(schedule.get("rotationState") or {})
    worker_state = _read_worker_state()
    worker_pid = int(worker_state.get("pid", 0) or 0) or int(rotation_state.get("workerPid", 0) or 0) or None
    worker_alive = _process_exists(worker_pid)
    return {
        "alive": worker_alive,
        "pid": worker_pid if worker_alive else None,
        "status": "running" if worker_alive else "stopped",
        "runtimeVersion": worker_state.get("runtimeVersion") or rotation_state.get("runtimeVersion"),
        "startedAt": worker_state.get("startedAt") or rotation_state.get("workerStartedAt"),
        "heartbeatAt": worker_state.get("updatedAt") or rotation_state.get("workerHeartbeatAt"),
        "stoppedAt": worker_state.get("stoppedAt") or rotation_state.get("workerStoppedAt"),
        "lastPid": worker_state.get("lastPid") or worker_state.get("pid") or rotation_state.get("workerPid"),
    }


def _compute_source_status(
    source_row: Dict[str, Any],
    *,
    reference_time: datetime,
) -> Dict[str, Any]:
    age_minutes = _minutes_since(source_row.get("lastSuccessAt"), reference_time)
    consecutive_failures = int(source_row.get("consecutiveFailures", 0) or 0)

    if age_minutes is None:
        status = "unknown"
        freshness_score = 0
    elif age_minutes <= 60 and consecutive_failures == 0:
        status = "healthy"
        freshness_score = 100
    elif age_minutes <= 6 * 60 and consecutive_failures <= 1:
        status = "delayed"
        freshness_score = 82
    elif age_minutes <= 24 * 60:
        status = "degraded"
        freshness_score = 58
    else:
        status = "stale"
        freshness_score = 24

    freshness_score = max(0, freshness_score - min(consecutive_failures, 4) * 8)

    return {
        **source_row,
        "healthStatus": status,
        "freshnessScore": freshness_score,
        "sourceAgeMinutes": round(age_minutes, 1) if age_minutes is not None else None,
    }


def _fetch_success_rate(runs: List[Dict[str, Any]]) -> float:
    recent_runs = runs[-20:]
    if not recent_runs:
        return 0.0

    weighted_success = 0.0
    for run in recent_runs:
        status = run.get("status")
        if status == "success":
            weighted_success += 1.0
        elif status == "partial":
            weighted_success += 0.5

    return round((weighted_success / len(recent_runs)) * 100, 1)


def _oldest_post_at(posts: List[Dict[str, Any]]) -> str | None:
    if not posts:
        return None
    created_utc = min(int(row.get("createdUtc", 0) or 0) for row in posts)
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def _schedule_summary(
    schedule: Dict[str, Any],
    *,
    sources: Dict[str, Dict[str, Any]],
    reference_time: datetime,
) -> Dict[str, Any]:
    rotation = summarize_rotation(schedule, sources, reference_time=reference_time)
    live_state = rotation.get("live", {})
    backfill_state = rotation.get("backfill", {})
    rotation_state = schedule.get("rotationState", {})
    worker_runtime = _rotation_worker_runtime(schedule)
    return {
        "live": {
            "currentBucketId": live_state.get("currentBucketId"),
            "currentBucketIndex": live_state.get("currentBucketIndex", 0),
            "lastCompletedBucketId": live_state.get("lastCompletedBucketId"),
            "completedBucketCount": live_state.get("completedBucketCount", 0),
            "remainingBucketCount": live_state.get("remainingBucketCount", 0),
            "completionPct": live_state.get("completionPct", 0.0),
            "completedCycleCount": live_state.get("completedCycleCount", 0),
            "lastCycleCompletedAt": live_state.get("lastCycleCompletedAt"),
            "lastRunAt": live_state.get("lastRunAt"),
        },
        "backfill": {
            "currentBucketId": backfill_state.get("currentBucketId"),
            "currentBucketIndex": backfill_state.get("currentBucketIndex", 0),
            "lastCompletedBucketId": backfill_state.get("lastCompletedBucketId"),
            "completedBucketCount": backfill_state.get("completedBucketCount", 0),
            "remainingBucketCount": backfill_state.get("remainingBucketCount", 0),
            "completionPct": backfill_state.get("completionPct", 0.0),
            "completedCycleCount": backfill_state.get("completedCycleCount", 0),
            "lastCycleCompletedAt": backfill_state.get("lastCycleCompletedAt"),
            "lastRunAt": backfill_state.get("lastRunAt"),
        },
        "totalBuckets": schedule.get("totalBuckets", 0),
        "liveTotalBuckets": schedule.get("totalLiveBuckets", schedule.get("totalBuckets", 0)),
        "backfillTotalBuckets": schedule.get("totalBackfillBuckets", schedule.get("totalBuckets", 0)),
        "trackedSubreddits": schedule.get("trackedSubreddits", 0),
        "rotation": {
            "phase": rotation.get("phase"),
            "liveDueSources": rotation.get("liveDueSources", 0),
            "backfillPendingSources": rotation.get("backfillPendingSources", 0),
            "backfillDueSources": rotation.get("backfillDueSources", 0),
            "liveCoveragePct": rotation.get("liveCoveragePct", 0.0),
            "backfillCompletePct": rotation.get("backfillCompletePct", 0.0),
            "windowCoveragePct": rotation.get("windowCoveragePct", 0.0),
            "lastMode": rotation_state.get("lastMode"),
            "modeStreak": rotation_state.get("modeStreak", 0),
            "runtimeVersion": worker_runtime.get("runtimeVersion") or rotation_state.get("runtimeVersion"),
            "workerPid": worker_runtime.get("pid"),
            "workerAlive": worker_runtime.get("alive", False),
            "workerStatus": worker_runtime.get("status"),
            "workerHeartbeatAt": worker_runtime.get("heartbeatAt"),
            "workerStartedAt": worker_runtime.get("startedAt"),
            "workerStoppedAt": worker_runtime.get("stoppedAt"),
            "lastAuxiliaryRefreshAt": rotation_state.get("lastAuxiliaryRefreshAt"),
            "phaseChangedAt": rotation_state.get("phaseChangedAt"),
            "populationCompletedAt": rotation_state.get("populationCompletedAt"),
        },
    }


def _tracked_subreddits_from_schedule(schedule: Dict[str, Any]) -> List[str]:
    live_buckets = schedule.get("liveBuckets", []) or schedule.get("buckets", [])
    backfill_buckets = schedule.get("backfillBuckets", []) or schedule.get("buckets", [])
    tracked = [
        subreddit
        for bucket in [*live_buckets, *backfill_buckets]
        for subreddit in bucket.get("subreddits", [])
    ]
    return list(dict.fromkeys(tracked))


def _schedule_metadata(schedule: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    metadata = schedule.get("subredditMetadata", {})
    return metadata if isinstance(metadata, dict) else {}


def _build_health_summary(
    *,
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    runs: List[Dict[str, Any]],
    sources: Dict[str, Dict[str, Any]],
    schedule: Dict[str, Any],
    reference_time: datetime,
) -> Dict[str, Any]:
    rotation_summary = summarize_rotation(schedule, sources, reference_time=reference_time)
    worker_runtime = _rotation_worker_runtime(schedule)
    schedule_subreddits = _tracked_subreddits_from_schedule(schedule)
    tracked_subreddits = schedule_subreddits or [row.get("subreddit") for row in sources.values()]
    source_rows = []
    schedule_metadata = _schedule_metadata(schedule)
    for subreddit in tracked_subreddits:
        schedule_row = schedule_metadata.get(str(subreddit).lower(), {})
        row = {
            **schedule_row,
            **sources.get(str(subreddit).lower(), {}),
        }
        row.setdefault("subreddit", subreddit)
        row.setdefault("consecutiveFailures", 0)
        source_rows.append(_compute_source_status(row, reference_time=reference_time))

    total_sources = max(len(source_rows), 1)
    refreshed_1h = sum(
        1 for row in source_rows if (row.get("sourceAgeMinutes") or 10**9) <= 60
    )
    refreshed_6h = sum(
        1 for row in source_rows if (row.get("sourceAgeMinutes") or 10**9) <= 6 * 60
    )
    refreshed_24h = sum(
        1 for row in source_rows if (row.get("sourceAgeMinutes") or 10**9) <= 24 * 60
    )
    latest_successful_run = next(
        (run for run in reversed(runs) if run.get("status") in {"success", "partial"}),
        None,
    )
    latest_run = runs[-1] if runs else None
    latest_success_age = _minutes_since(
        latest_successful_run.get("completedAt") if latest_successful_run else None,
        reference_time,
    )
    average_source_age = round(
        sum(row.get("sourceAgeMinutes") or 0 for row in source_rows if row.get("sourceAgeMinutes") is not None)
        / max(1, sum(1 for row in source_rows if row.get("sourceAgeMinutes") is not None)),
        1,
    ) if source_rows else None
    coverage_score = round((refreshed_24h / total_sources) * 100, 1)
    freshness_score = round(
        (sum(row.get("freshnessScore", 0) for row in source_rows) / total_sources),
        1,
    ) if source_rows else 0.0
    sources_covered_7d = sum(1 for row in source_rows if row.get("hasWindowCoverage"))
    backfill_completed_sources = sum(1 for row in source_rows if row.get("backfillStatus") == "complete")
    sources_covered_7d_pct = round((sources_covered_7d / total_sources) * 100, 1)
    backfill_coverage_pct = round((backfill_completed_sources / total_sources) * 100, 1)
    backfill_completeness_pct = round(
        min(100.0, backfill_coverage_pct * 0.7 + sources_covered_7d_pct * 0.3),
        1,
    )
    category_breakdown: Dict[str, int] = {}
    tier_breakdown: Dict[str, int] = {}
    for row in source_rows:
        category = str(row.get("category") or "uncategorized")
        tier = str(row.get("tier") or "unknown")
        category_breakdown[category] = category_breakdown.get(category, 0) + 1
        tier_breakdown[tier] = tier_breakdown.get(tier, 0) + 1

    if not posts and not comments:
        freshness_state = "empty"
    elif latest_run and latest_run.get("status") == "failed":
        freshness_state = "degraded" if (latest_success_age or 10**9) <= 24 * 60 else "stale"
    elif (latest_success_age or 10**9) <= 60 and coverage_score >= 55:
        freshness_state = "fresh"
    elif (latest_success_age or 10**9) <= 6 * 60 and coverage_score >= 30:
        freshness_state = "delayed"
    elif (latest_success_age or 10**9) <= 24 * 60:
        freshness_state = "degraded"
    else:
        freshness_state = "stale"

    if freshness_state == "fresh" and float(rotation_summary.get("liveCoveragePct", 0.0) or 0.0) < 50.0:
        freshness_state = "delayed"

    if not worker_runtime.get("alive", False):
        if (latest_success_age or 10**9) > 60 or int(rotation_summary.get("liveDueSources", 0) or 0) > 0:
            freshness_state = "stale"
        elif freshness_state == "fresh":
            freshness_state = "delayed"

    return {
        "freshnessState": freshness_state,
        "coverageScore": coverage_score,
        "freshnessScore": freshness_score,
        "fetchSuccessRate": _fetch_success_rate(runs),
        "activeSubredditCount": refreshed_24h,
        "refreshed1hPct": round((refreshed_1h / total_sources) * 100, 1),
        "refreshed6hPct": round((refreshed_6h / total_sources) * 100, 1),
        "refreshed24hPct": round((refreshed_24h / total_sources) * 100, 1),
        "averageSourceAgeMinutes": average_source_age,
        "latestSuccessfulRunAt": latest_successful_run.get("completedAt") if latest_successful_run else None,
        "latestRunId": latest_run.get("runId") if latest_run else None,
        "latestRunMode": latest_run.get("mode") if latest_run else None,
        "latestRunStatus": latest_run.get("status") if latest_run else "empty",
        "latestRunBucketId": latest_run.get("bucketId") if latest_run else None,
        "latestRunAttempted": latest_run.get("subredditsAttempted", 0) if latest_run else 0,
        "latestRunSucceeded": latest_run.get("subredditsSucceeded", 0) if latest_run else 0,
        "latestRunFailed": latest_run.get("subredditsFailed", 0) if latest_run else 0,
        "sourcesCovered7dPct": sources_covered_7d_pct,
        "backfillCompletedSources": backfill_completed_sources,
        "backfillCoveragePct": backfill_coverage_pct,
        "backfillCompletenessPct": backfill_completeness_pct,
        "postsInWindow": len(posts),
        "commentsInWindow": len(comments),
        "oldestStoredPostAt": _oldest_post_at(posts),
        "totalTrackedSources": len(source_rows),
        "workerAlive": worker_runtime.get("alive", False),
        "workerStatus": worker_runtime.get("status"),
        "workerPid": worker_runtime.get("pid"),
        "workerHeartbeatAt": worker_runtime.get("heartbeatAt"),
        "categoryCount": len(category_breakdown),
        "categoriesTracked": category_breakdown,
        "tiersTracked": tier_breakdown,
        "schedule": _schedule_summary(
            schedule,
            sources=source_rows,
            reference_time=reference_time,
        ),
    }


def build_dashboard_snapshot(
    *,
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    runs: List[Dict[str, Any]],
    sources: Dict[str, Dict[str, Any]],
    schedule: Dict[str, Any],
) -> Dict[str, Any]:
    reference_time = datetime.now(timezone.utc)
    filtered_posts, filtered_comments = prune_records_to_window(
        posts,
        comments,
        reference_time=reference_time,
    )
    schedule_metadata = _schedule_metadata(schedule)
    source_rows = {}
    all_source_keys = set(schedule_metadata.keys()) if schedule_metadata else (set(schedule_metadata.keys()) | set(sources.keys()))
    for key in all_source_keys:
        merged = {
            **schedule_metadata.get(str(key).lower(), {}),
            **sources.get(str(key).lower(), {}),
        }
        merged.setdefault("subreddit", merged.get("subreddit") or key)
        merged.setdefault("consecutiveFailures", 0)
        source_rows[str(merged.get("subreddit") or key).lower()] = _compute_source_status(
            merged,
            reference_time=reference_time,
        )
    health = _build_health_summary(
        posts=filtered_posts,
        comments=filtered_comments,
        runs=runs,
        sources=source_rows,
        schedule=schedule,
        reference_time=reference_time,
    )
    latest_run = runs[-1] if runs else None

    return {
        "source": "reddit",
        "storeModel": schedule.get("storeModel"),
        "sourceStateVersion": schedule.get("sourceStateVersion"),
        "runtimeVersion": schedule.get("runtimeVersion"),
        "generatedAt": reference_time.isoformat(),
        "fetchedAt": health.get("latestSuccessfulRunAt"),
        "posts": filtered_posts,
        "comments": filtered_comments,
        "runs": runs[-20:],
        "sourceHealth": source_rows,
        "health": health,
        "latestRun": latest_run,
        "error": latest_run.get("errorSummary") if latest_run and latest_run.get("status") == "failed" else None,
    }


def write_dashboard_snapshot(payload: Dict[str, Any], path: Path = DASHBOARD_SNAPSHOT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
