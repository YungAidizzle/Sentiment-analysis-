from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Literal

from backend.reddit_dev_only_config import REDDIT_ROTATION_RUNTIME_VERSION, REDDIT_SOURCE_STATE_VERSION, REDDIT_STORE_MODEL
from backend.reddit_subreddit_catalog import (
    BACKFILL_BUCKET_SIZE_BY_TIER,
    BACKFILL_BUCKET_TIER_PATTERN,
    LIVE_BUCKET_SIZE_BY_TIER,
    LIVE_BUCKET_TIER_PATTERN,
)

ScheduleMode = Literal["live", "backfill"]
RotationPhase = Literal["initial_population", "maintenance"]


def _mode_bucket_key(mode: ScheduleMode) -> str:
    return "backfillBuckets" if mode == "backfill" else "liveBuckets"


def _legacy_build_buckets(
    subreddits: List[str],
    bucket_size: int,
    max_subreddits_per_run: int | None = None,
) -> List[Dict[str, Any]]:
    effective_size = max(1, min(bucket_size, max_subreddits_per_run or bucket_size))
    return [
        {
            "id": f"bucket-{index // effective_size + 1:03d}",
            "tier": "tier2",
            "subreddits": subreddits[index:index + effective_size],
            "categories": [],
            "sourceCount": len(subreddits[index:index + effective_size]),
        }
        for index in range(0, len(subreddits), effective_size)
    ]


def build_buckets(
    subreddits: List[str],
    bucket_size: int,
    max_subreddits_per_run: int | None = None,
) -> List[Dict[str, Any]]:
    return _legacy_build_buckets(subreddits, bucket_size, max_subreddits_per_run)


def _group_entries_by_category(entries: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in entries:
        grouped.setdefault(str(entry.get("category", "general")), []).append(entry)

    for category_entries in grouped.values():
        category_entries.sort(
            key=lambda item: (
                -int(item.get("priorityScore", 0) or 0),
                str(item.get("subreddit", "")),
            )
        )
    return grouped


def _round_robin_entries(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = _group_entries_by_category(entries)
    categories = sorted(grouped.keys())
    ordered: List[Dict[str, Any]] = []
    while True:
        progressed = False
        for category in categories:
            bucket = grouped[category]
            if not bucket:
                continue
            ordered.append(bucket.pop(0))
            progressed = True
        if not progressed:
            break
    return ordered


def _expand_live_sequence(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = _round_robin_entries(entries)
    max_weight = max(
        (
            int((entry.get("fetchProfile") or {}).get("liveCadenceWeight", 1) or 1)
            for entry in ordered
        ),
        default=1,
    )
    expanded: List[Dict[str, Any]] = []
    for pass_index in range(max_weight):
        for entry in ordered:
            weight = int((entry.get("fetchProfile") or {}).get("liveCadenceWeight", 1) or 1)
            if weight > pass_index:
                expanded.append(entry)
    return expanded


def _chunk_catalog_entries(
    entries: List[Dict[str, Any]],
    *,
    mode: ScheduleMode,
    tier: str,
    size: int,
) -> List[Dict[str, Any]]:
    buckets: List[Dict[str, Any]] = []
    for index in range(0, len(entries), size):
        slice_entries = entries[index:index + size]
        buckets.append(
            {
                "id": f"{mode}-{tier}-{len(buckets) + 1:03d}",
                "mode": mode,
                "tier": tier,
                "subreddits": [str(entry.get("subreddit", "")) for entry in slice_entries],
                "categories": sorted(
                    {
                        str(entry.get("category", "general"))
                        for entry in slice_entries
                        if str(entry.get("category", "general"))
                    }
                ),
                "sourceCount": len(slice_entries),
            }
        )
    return buckets


def _interleave_bucket_groups(
    grouped_buckets: Dict[str, List[Dict[str, Any]]],
    *,
    pattern: List[str],
) -> List[Dict[str, Any]]:
    queues = {tier: list(grouped_buckets.get(tier, [])) for tier in grouped_buckets}
    ordered: List[Dict[str, Any]] = []
    while any(queues.values()):
        progressed = False
        for tier in pattern:
            queue = queues.get(tier, [])
            if not queue:
                continue
            ordered.append(queue.pop(0))
            progressed = True
        if progressed:
            continue
        for tier in sorted(queues.keys()):
            queue = queues[tier]
            if not queue:
                continue
            ordered.append(queue.pop(0))
    return ordered


def _build_catalog_buckets(
    catalog_entries: Iterable[Dict[str, Any]],
    *,
    mode: ScheduleMode,
) -> List[Dict[str, Any]]:
    tiered: Dict[str, List[Dict[str, Any]]] = {
        "tier1": [],
        "tier2": [],
        "tier3": [],
    }
    for entry in catalog_entries:
        tier = str(entry.get("tier", "tier3"))
        tiered.setdefault(tier, []).append(entry)

    grouped_buckets: Dict[str, List[Dict[str, Any]]] = {}
    for tier, entries in tiered.items():
        if not entries:
            grouped_buckets[tier] = []
            continue
        sequence = _expand_live_sequence(entries) if mode == "live" else _round_robin_entries(entries)
        size = (
            LIVE_BUCKET_SIZE_BY_TIER.get(tier, 12)
            if mode == "live"
            else BACKFILL_BUCKET_SIZE_BY_TIER.get(tier, 10)
        )
        grouped_buckets[tier] = _chunk_catalog_entries(sequence, mode=mode, tier=tier, size=size)

    pattern = LIVE_BUCKET_TIER_PATTERN if mode == "live" else BACKFILL_BUCKET_TIER_PATTERN
    return _interleave_bucket_groups(grouped_buckets, pattern=pattern)


def _legacy_mode_state(current_state: Dict[str, Any] | None) -> Dict[str, Any]:
    state = current_state or {}
    return {
        "currentBucketIndex": int(state.get("currentBucketIndex", 0) or 0),
        "currentBucketId": state.get("currentBucketId"),
        "lastCompletedBucketId": state.get("lastCompletedBucketId"),
        "completedBucketIds": list(dict.fromkeys(state.get("completedBucketIds", []))),
        "completedCycleCount": int(state.get("completedCycleCount", 0) or 0),
        "lastCycleCompletedAt": state.get("lastCycleCompletedAt"),
        "lastRunAt": state.get("lastRunAt"),
        "updatedAt": state.get("updatedAt"),
    }


def _normalize_mode_state(
    current_state: Dict[str, Any] | None,
    *,
    buckets: List[Dict[str, Any]],
) -> Dict[str, Any]:
    current_index = int((current_state or {}).get("currentBucketIndex", 0) or 0)
    if buckets:
        current_index %= len(buckets)
    else:
        current_index = 0

    bucket_ids = {bucket["id"] for bucket in buckets}
    completed_bucket_ids = [
        bucket_id
        for bucket_id in list(dict.fromkeys((current_state or {}).get("completedBucketIds", [])))
        if bucket_id in bucket_ids
    ]
    return {
        "currentBucketIndex": current_index,
        "currentBucketId": buckets[current_index]["id"] if buckets else None,
        "lastCompletedBucketId": (current_state or {}).get("lastCompletedBucketId"),
        "completedBucketIds": completed_bucket_ids,
        "completedCycleCount": int((current_state or {}).get("completedCycleCount", 0) or 0),
        "lastCycleCompletedAt": (current_state or {}).get("lastCycleCompletedAt"),
        "lastRunAt": (current_state or {}).get("lastRunAt"),
        "updatedAt": (current_state or {}).get("updatedAt"),
    }


def _normalize_rotation_state(current_state: Dict[str, Any] | None) -> Dict[str, Any]:
    state = current_state or {}
    phase = str(state.get("phase") or "initial_population")
    if phase not in {"initial_population", "maintenance"}:
        phase = "initial_population"

    last_mode = state.get("lastMode")
    if last_mode not in {"live", "backfill"}:
        last_mode = None

    worker_status = str(state.get("workerStatus") or ("running" if state.get("workerPid") else "stopped"))
    if worker_status not in {"running", "stopped"}:
        worker_status = "running" if state.get("workerPid") else "stopped"

    return {
        "phase": phase,
        "lastMode": last_mode,
        "modeStreak": int(state.get("modeStreak", 0) or 0),
        "runtimeVersion": state.get("runtimeVersion") or REDDIT_ROTATION_RUNTIME_VERSION,
        "workerPid": int(state.get("workerPid", 0) or 0) or None,
        "workerStatus": worker_status,
        "workerHeartbeatAt": state.get("workerHeartbeatAt") or state.get("updatedAt"),
        "workerStartedAt": state.get("workerStartedAt"),
        "workerStoppedAt": state.get("workerStoppedAt"),
        "lastLiveRunAt": state.get("lastLiveRunAt"),
        "lastBackfillRunAt": state.get("lastBackfillRunAt"),
        "lastAuxiliaryRefreshAt": state.get("lastAuxiliaryRefreshAt"),
        "phaseChangedAt": state.get("phaseChangedAt"),
        "populationCompletedAt": state.get("populationCompletedAt"),
        "updatedAt": state.get("updatedAt"),
    }


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _seconds_since(value: str | None, *, reference_time: datetime) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return max(0.0, (reference_time - parsed).total_seconds())


def _target_live_age_hours_for_tier(tier: str) -> int:
    if tier == "tier1":
        return 6
    if tier == "tier2":
        return 12
    return 24


def _target_backfill_age_hours_for_tier(tier: str) -> int:
    if tier == "tier1":
        return 72
    if tier == "tier2":
        return 120
    return 168


def _get_schedule_source_rows(
    schedule_state: Dict[str, Any],
    sources: Dict[str, Dict[str, Any]] | List[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    metadata = schedule_state.get("subredditMetadata", {})
    if isinstance(sources, list):
        source_map = {
            str((row or {}).get("subreddit", "")).lower(): dict(row or {})
            for row in sources
            if str((row or {}).get("subreddit", "")).strip()
        }
    else:
        source_map = dict(sources or {})
    source_rows: List[Dict[str, Any]] = []
    tracked_keys = set(metadata.keys()) if metadata else set(source_map.keys())
    for key in tracked_keys:
        merged = {
            **dict(metadata.get(key, {})),
            **dict(source_map.get(key, {})),
        }
        merged.setdefault("subreddit", merged.get("subreddit") or key)
        merged.setdefault("tier", merged.get("tier") or "tier3")
        source_rows.append(merged)
    return source_rows


def get_mode_progress(
    schedule_state: Dict[str, Any],
    *,
    mode: ScheduleMode,
) -> Dict[str, Any]:
    buckets = schedule_state.get(_mode_bucket_key(mode), []) or schedule_state.get("buckets", [])
    mode_state = schedule_state.get("backfillState" if mode == "backfill" else "liveState", {})
    completed_bucket_ids = [
        bucket_id
        for bucket_id in list(dict.fromkeys(mode_state.get("completedBucketIds", [])))
        if bucket_id in {bucket["id"] for bucket in buckets}
    ]
    completed_bucket_count = min(len(completed_bucket_ids), len(buckets))
    total_buckets = len(buckets)
    remaining_bucket_count = max(0, total_buckets - completed_bucket_count)
    completion_pct = round((completed_bucket_count / total_buckets) * 100, 1) if total_buckets else 0.0
    return {
        "currentBucketId": mode_state.get("currentBucketId"),
        "currentBucketIndex": int(mode_state.get("currentBucketIndex", 0) or 0),
        "lastCompletedBucketId": mode_state.get("lastCompletedBucketId"),
        "completedBucketCount": completed_bucket_count,
        "remainingBucketCount": remaining_bucket_count,
        "completionPct": completion_pct,
        "completedCycleCount": int(mode_state.get("completedCycleCount", 0) or 0),
        "lastCycleCompletedAt": mode_state.get("lastCycleCompletedAt"),
        "lastRunAt": mode_state.get("lastRunAt"),
        "totalBuckets": total_buckets,
    }


def summarize_rotation(
    schedule_state: Dict[str, Any],
    sources: Dict[str, Dict[str, Any]] | None,
    *,
    reference_time: datetime | None = None,
    initial_population_backfill_target_pct: float = 85.0,
    initial_population_live_target_pct: float = 65.0,
) -> Dict[str, Any]:
    reference = reference_time or datetime.now(timezone.utc)
    source_rows = _get_schedule_source_rows(schedule_state, sources)
    tracked_sources = max(len(source_rows), 1)

    live_due_sources = 0
    backfill_pending_sources = 0
    backfill_due_sources = 0
    sources_with_window = 0

    for row in source_rows:
        tier = str(row.get("tier") or "tier3")
        if row.get("hasWindowCoverage"):
            sources_with_window += 1

        live_age_seconds = _seconds_since(row.get("lastLiveSuccessAt"), reference_time=reference)
        live_target_seconds = _target_live_age_hours_for_tier(tier) * 3600
        if live_age_seconds is None or live_age_seconds > live_target_seconds:
            live_due_sources += 1

        backfill_status = str(row.get("backfillStatus") or "pending")
        if backfill_status != "complete":
            backfill_pending_sources += 1
            backfill_due_sources += 1

    backfill_complete_pct = round(((tracked_sources - backfill_pending_sources) / tracked_sources) * 100, 1)
    live_coverage_pct = round(((tracked_sources - live_due_sources) / tracked_sources) * 100, 1)
    window_coverage_pct = round((sources_with_window / tracked_sources) * 100, 1)
    phase: RotationPhase = (
        "maintenance"
        if backfill_complete_pct >= initial_population_backfill_target_pct
        and live_coverage_pct >= initial_population_live_target_pct
        and window_coverage_pct >= initial_population_live_target_pct
        else "initial_population"
    )

    return {
        "phase": phase,
        "trackedSources": len(source_rows),
        "liveDueSources": live_due_sources,
        "backfillPendingSources": backfill_pending_sources,
        "backfillDueSources": backfill_due_sources,
        "backfillCompletePct": backfill_complete_pct,
        "liveCoveragePct": live_coverage_pct,
        "windowCoveragePct": window_coverage_pct,
        "live": get_mode_progress(schedule_state, mode="live"),
        "backfill": get_mode_progress(schedule_state, mode="backfill"),
    }


def get_rotation_plan(
    schedule_state: Dict[str, Any],
    sources: Dict[str, Dict[str, Any]] | None,
    *,
    reference_time: datetime | None = None,
    phase_live_interval_seconds: Dict[RotationPhase, int] | None = None,
    phase_backfill_interval_seconds: Dict[RotationPhase, int] | None = None,
    phase_backfill_streak_limit: Dict[RotationPhase, int] | None = None,
    initial_population_backfill_target_pct: float = 85.0,
    initial_population_live_target_pct: float = 65.0,
) -> Dict[str, Any]:
    reference = reference_time or datetime.now(timezone.utc)
    rotation = _normalize_rotation_state(schedule_state.get("rotationState"))
    summary = summarize_rotation(
        schedule_state,
        sources,
        reference_time=reference,
        initial_population_backfill_target_pct=initial_population_backfill_target_pct,
        initial_population_live_target_pct=initial_population_live_target_pct,
    )
    phase = summary["phase"]
    live_intervals = phase_live_interval_seconds or {
        "initial_population": 12 * 60,
        "maintenance": 10 * 60,
    }
    backfill_intervals = phase_backfill_interval_seconds or {
        "initial_population": 4 * 60,
        "maintenance": 45 * 60,
    }
    backfill_streak_limit = phase_backfill_streak_limit or {
        "initial_population": 2,
        "maintenance": 1,
    }

    live_wait_seconds = max(
        0,
        int(live_intervals[phase] - (_seconds_since(rotation.get("lastLiveRunAt"), reference_time=reference) or 10**9)),
    )
    backfill_wait_seconds = max(
        0,
        int(
            backfill_intervals[phase]
            - (_seconds_since(rotation.get("lastBackfillRunAt"), reference_time=reference) or 10**9)
        ),
    )
    live_due = summary["liveDueSources"] > 0 and live_wait_seconds <= 0
    backfill_due = summary["backfillDueSources"] > 0 and backfill_wait_seconds <= 0

    selected_mode: ScheduleMode | None = None
    if live_due and backfill_due:
        if phase == "initial_population":
            live_gap = max(
                0.0,
                initial_population_live_target_pct - float(summary.get("liveCoveragePct", 0.0) or 0.0),
            )
            backfill_gap = max(
                0.0,
                initial_population_backfill_target_pct - float(summary.get("backfillCompletePct", 0.0) or 0.0),
            )
            if (
                live_gap > backfill_gap
                or int(summary.get("liveDueSources", 0) or 0) > int(summary.get("backfillDueSources", 0) or 0)
                or rotation.get("lastLiveRunAt") is None
            ):
                selected_mode = "live"
            elif rotation.get("lastMode") == "backfill" and int(rotation.get("modeStreak", 0) or 0) >= backfill_streak_limit[phase]:
                selected_mode = "live"
            else:
                selected_mode = "backfill"
        else:
            if rotation.get("lastMode") == "live" and int(rotation.get("modeStreak", 0) or 0) >= 3:
                selected_mode = "backfill"
            else:
                selected_mode = "live"
    elif live_due:
        selected_mode = "live"
    elif backfill_due:
        selected_mode = "backfill"

    next_wait_candidates = [
        wait
        for wait in [live_wait_seconds, backfill_wait_seconds]
        if wait is not None
    ]
    next_wait_seconds = min(next_wait_candidates) if next_wait_candidates else 0
    if selected_mode is None and next_wait_seconds <= 0:
        next_wait_seconds = min(live_intervals[phase], backfill_intervals[phase])

    return {
        "phase": phase,
        "mode": selected_mode,
        "waitSeconds": next_wait_seconds,
        "rotationState": rotation,
        "summary": summary,
        "liveDue": live_due,
        "backfillDue": backfill_due,
        "liveWaitSeconds": live_wait_seconds,
        "backfillWaitSeconds": backfill_wait_seconds,
    }


def update_rotation_state(
    schedule_state: Dict[str, Any],
    *,
    mode: ScheduleMode,
    phase: RotationPhase,
    auxiliary_refreshed: bool = False,
    runtime_version: str = REDDIT_ROTATION_RUNTIME_VERSION,
    worker_pid: int | None = None,
    reference_time: datetime | None = None,
) -> Dict[str, Any]:
    reference = reference_time or datetime.now(timezone.utc)
    now_iso = reference.isoformat()
    rotation = _normalize_rotation_state(schedule_state.get("rotationState"))
    previous_phase = rotation.get("phase")
    last_mode = rotation.get("lastMode")
    mode_streak = int(rotation.get("modeStreak", 0) or 0)
    if last_mode == mode:
        mode_streak += 1
    else:
        mode_streak = 1

    updated_rotation = {
        **rotation,
        "phase": phase,
        "lastMode": mode,
        "modeStreak": mode_streak,
        "runtimeVersion": runtime_version,
        "workerPid": worker_pid,
        "workerStatus": "running" if worker_pid else rotation.get("workerStatus", "stopped"),
        "workerHeartbeatAt": now_iso if worker_pid else rotation.get("workerHeartbeatAt"),
        "workerStartedAt": rotation.get("workerStartedAt") or (now_iso if worker_pid else None),
        "workerStoppedAt": None if worker_pid else rotation.get("workerStoppedAt"),
        "lastLiveRunAt": now_iso if mode == "live" else rotation.get("lastLiveRunAt"),
        "lastBackfillRunAt": now_iso if mode == "backfill" else rotation.get("lastBackfillRunAt"),
        "lastAuxiliaryRefreshAt": now_iso if auxiliary_refreshed else rotation.get("lastAuxiliaryRefreshAt"),
        "phaseChangedAt": now_iso if previous_phase != phase else rotation.get("phaseChangedAt"),
        "populationCompletedAt": (
            now_iso
            if phase == "maintenance" and previous_phase != "maintenance"
            else rotation.get("populationCompletedAt")
        ),
        "updatedAt": now_iso,
    }

    return {
        **schedule_state,
        "rotationState": updated_rotation,
        "updatedAt": now_iso,
    }


def sync_schedule_state(
    current_state: Dict[str, Any] | None,
    *,
    subreddits: List[str],
    bucket_size: int,
    max_subreddits_per_run: int | None = None,
    catalog_entries: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    live_buckets = (
        _build_catalog_buckets(catalog_entries or [], mode="live")
        if catalog_entries
        else build_buckets(subreddits, bucket_size, max_subreddits_per_run)
    )
    backfill_buckets = (
        _build_catalog_buckets(catalog_entries or [], mode="backfill")
        if catalog_entries
        else build_buckets(subreddits, bucket_size, max_subreddits_per_run)
    )
    state = current_state or {}
    legacy_state = _legacy_mode_state(state)
    live_state = _normalize_mode_state(
        state.get("liveState", legacy_state),
        buckets=live_buckets,
    )
    backfill_state = _normalize_mode_state(
        state.get("backfillState", {"currentBucketIndex": 0}),
        buckets=backfill_buckets,
    )
    rotation_state = _normalize_rotation_state(state.get("rotationState"))

    tracked_subreddits = sorted(
        {
            subreddit
            for bucket in [*live_buckets, *backfill_buckets]
            for subreddit in bucket.get("subreddits", [])
        }
    )
    subreddit_metadata = {
        str(entry.get("key", "") or "").lower(): {
            "subreddit": entry.get("subreddit"),
            "category": entry.get("category"),
            "categoryLabel": entry.get("categoryLabel"),
            "tier": entry.get("tier"),
            "activityTier": entry.get("activityTier"),
            "audienceScale": entry.get("audienceScale"),
            "broadCommunity": bool(entry.get("broadCommunity", False)),
            "spamProne": bool(entry.get("spamProne", False)),
            "tags": list(entry.get("tags") or []),
            "priorityScore": int(entry.get("priorityScore", 0) or 0),
            "discoverySource": entry.get("discoverySource"),
            "fetchProfile": dict(entry.get("fetchProfile") or {}),
        }
        for entry in (catalog_entries or [])
        if str(entry.get("key", "") or "").strip()
    }

    return {
        "version": 4,
        "storeModel": REDDIT_STORE_MODEL,
        "sourceStateVersion": REDDIT_SOURCE_STATE_VERSION,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "bucketSize": bucket_size,
        "maxSubredditsPerRun": max_subreddits_per_run or bucket_size,
        "totalBuckets": len(live_buckets),
        "totalLiveBuckets": len(live_buckets),
        "totalBackfillBuckets": len(backfill_buckets),
        "trackedSubreddits": len(tracked_subreddits),
        "subredditMetadata": subreddit_metadata,
        "buckets": live_buckets,
        "liveBuckets": live_buckets,
        "backfillBuckets": backfill_buckets,
        "liveState": live_state,
        "backfillState": backfill_state,
        "rotationState": rotation_state,
        "updatedAt": state.get("updatedAt"),
    }


def get_current_bucket(schedule_state: Dict[str, Any], mode: ScheduleMode = "live") -> Dict[str, Any]:
    buckets = schedule_state.get(_mode_bucket_key(mode), []) or schedule_state.get("buckets", [])
    if not buckets:
        return {"id": None, "subreddits": [], "categories": [], "tier": None}

    mode_state = schedule_state.get("backfillState" if mode == "backfill" else "liveState", {})
    current_index = int(mode_state.get("currentBucketIndex", 0) or 0) % len(buckets)
    return buckets[current_index]


def advance_schedule_state(
    schedule_state: Dict[str, Any],
    mode: ScheduleMode = "live",
) -> Dict[str, Any]:
    buckets = schedule_state.get(_mode_bucket_key(mode), []) or schedule_state.get("buckets", [])
    if not buckets:
        return {
            **schedule_state,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }

    mode_key = "backfillState" if mode == "backfill" else "liveState"
    mode_state = schedule_state.get(mode_key, {})
    current_index = int(mode_state.get("currentBucketIndex", 0) or 0)
    next_index = (current_index + 1) % len(buckets)
    completed_bucket = buckets[current_index]
    completed_bucket_ids = [
        bucket_id
        for bucket_id in list(
            dict.fromkeys([*(mode_state.get("completedBucketIds", [])), completed_bucket["id"]])
        )
        if bucket_id in {bucket["id"] for bucket in buckets}
    ]
    cycle_completed = len(completed_bucket_ids) >= len(buckets)
    if cycle_completed:
        completed_cycle_count = int(mode_state.get("completedCycleCount", 0) or 0) + 1
        next_completed_bucket_ids: List[str] = []
        last_cycle_completed_at = datetime.now(timezone.utc).isoformat()
    else:
        completed_cycle_count = int(mode_state.get("completedCycleCount", 0) or 0)
        next_completed_bucket_ids = completed_bucket_ids
        last_cycle_completed_at = mode_state.get("lastCycleCompletedAt")
    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        **schedule_state,
        mode_key: {
            **mode_state,
            "currentBucketIndex": next_index,
            "currentBucketId": buckets[next_index]["id"],
            "lastCompletedBucketId": completed_bucket["id"],
            "completedBucketIds": next_completed_bucket_ids,
            "completedCycleCount": completed_cycle_count,
            "lastCycleCompletedAt": last_cycle_completed_at,
            "lastRunAt": now_iso,
            "updatedAt": now_iso,
        },
        "updatedAt": now_iso,
    }
