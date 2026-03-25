from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from backend.reddit_dev_only_config import (
    BACKFILL_BUCKET_SIZE_BY_TIER,
    BUCKET_SIZE,
    DEFAULT_SUBREDDITS,
    LIVE_BUCKET_SIZE_BY_TIER,
    MAX_SUBREDDITS_PER_RUN,
    REDDIT_ENABLED,
    REDDIT_ROTATION_RUNTIME_VERSION,
    REDDIT_ROTATION_AUXILIARY_REFRESH_SECONDS,
    REDDIT_ROTATION_INITIAL_BACKFILL_INTERVAL_SECONDS,
    REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
    REDDIT_ROTATION_INITIAL_LIVE_INTERVAL_SECONDS,
    REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
    REDDIT_ROTATION_LOOP_SLEEP_SECONDS,
    REDDIT_ROTATION_MAINTENANCE_BACKFILL_INTERVAL_SECONDS,
    REDDIT_ROTATION_MAINTENANCE_LIVE_INTERVAL_SECONDS,
    ROTATION_WORKER_STATE_PATH,
    SUBREDDIT_CATALOG,
)
from backend.reddit_ingestion import run_reddit_ingestion
from backend.reddit_client import get_reddit_backoff_wait_seconds, get_reddit_runtime_state
from backend.reddit_persistence import (
    ensure_store_root,
    load_comments,
    load_posts,
    load_runs,
    load_schedule,
    load_sources,
    normalize_sources_for_hot_store,
    recompute_source_storage_metrics,
    save_comments,
    save_posts,
    save_schedule,
    save_sources,
)
from backend.reddit_scheduler import (
    RotationPhase,
    _normalize_rotation_state,
    get_rotation_plan,
    summarize_rotation,
    sync_schedule_state,
    update_rotation_state,
)
from backend.reddit_snapshot import build_dashboard_snapshot, write_dashboard_snapshot
from backend.reddit_window import prune_records_to_window

RotationMode = Literal["auto", "live", "backfill"]


def _read_worker_state(path: Path = ROTATION_WORKER_STATE_PATH) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_worker_state(payload: dict[str, Any], path: Path = ROTATION_WORKER_STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _process_exists(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _terminate_process(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        capture_output=True,
        text=True,
        check=False,
    )


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _latest_iso_timestamp(*values: str | None) -> str | None:
    parsed_values = [
        (parsed, value)
        for value in values
        for parsed in [_parse_iso_timestamp(value)]
        if parsed is not None and value
    ]
    if not parsed_values:
        return None
    return max(parsed_values, key=lambda item: item[0])[1]


def _normalize_worker_state(worker_state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(worker_state or {})
    pid = int(state.get("pid", 0) or 0) or None
    running = bool(pid and _process_exists(pid))
    status = str(state.get("status") or ("running" if pid else "stopped")).lower()
    if status not in {"running", "stopped"}:
        status = "running" if pid else "stopped"
    if not running:
        pid = None
        status = "stopped"
    last_pid = int(state.get("lastPid", 0) or 0) or int(state.get("pid", 0) or 0) or None
    stopped_at = state.get("stoppedAt")
    if status == "stopped" and not stopped_at and last_pid:
        stopped_at = datetime.now(timezone.utc).isoformat()
    return {
        "pid": pid,
        "lastPid": last_pid,
        "status": status,
        "running": running,
        "runtimeVersion": state.get("runtimeVersion") or REDDIT_ROTATION_RUNTIME_VERSION,
        "storeModel": state.get("storeModel") or "reddit-hot-store-v2",
        "startedAt": state.get("startedAt"),
        "updatedAt": state.get("updatedAt"),
        "stoppedAt": stopped_at,
        "stopReason": state.get("stopReason"),
    }


def _apply_runtime_metadata(
    schedule: dict[str, Any],
    *,
    worker_state: dict[str, Any],
    reference_time: datetime,
) -> dict[str, Any]:
    rotation = _normalize_rotation_state(schedule.get("rotationState"))
    now_iso = reference_time.isoformat()
    return {
        **schedule,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "rotationState": {
            **rotation,
            "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
            "workerPid": worker_state.get("pid"),
            "workerStatus": worker_state.get("status", "stopped"),
            "workerHeartbeatAt": worker_state.get("updatedAt") or rotation.get("workerHeartbeatAt") or now_iso,
            "workerStartedAt": worker_state.get("startedAt") or rotation.get("workerStartedAt"),
            "workerStoppedAt": (
                None
                if worker_state.get("status") == "running"
                else worker_state.get("stoppedAt") or rotation.get("workerStoppedAt")
            ),
            "lastLiveRunAt": _latest_iso_timestamp(
                rotation.get("lastLiveRunAt"),
                dict(schedule.get("liveState", {})).get("lastRunAt"),
            ),
            "lastBackfillRunAt": _latest_iso_timestamp(
                rotation.get("lastBackfillRunAt"),
                dict(schedule.get("backfillState", {})).get("lastRunAt"),
            ),
            "updatedAt": now_iso,
        },
        "updatedAt": now_iso,
    }


def _persist_runtime_metadata(
    *,
    worker_state: dict[str, Any],
    reference_time: datetime,
    persist_snapshot: bool,
) -> None:
    ensure_store_root()
    original_schedule = load_schedule()
    schedule = sync_schedule_state(
        original_schedule,
        subreddits=DEFAULT_SUBREDDITS,
        bucket_size=BUCKET_SIZE,
        max_subreddits_per_run=MAX_SUBREDDITS_PER_RUN,
        catalog_entries=SUBREDDIT_CATALOG,
    )
    schedule = _apply_runtime_metadata(
        schedule,
        worker_state=worker_state,
        reference_time=reference_time,
    )
    if schedule != original_schedule:
        save_schedule(schedule)

    if not persist_snapshot:
        return

    posts = load_posts()
    comments = load_comments()
    runs = load_runs()
    sources = normalize_sources_for_hot_store(
        load_sources(),
        schedule_metadata=schedule.get("subredditMetadata", {}),
    )
    posts, comments = prune_records_to_window(posts, comments, reference_time=reference_time)
    sources = recompute_source_storage_metrics(
        posts,
        comments,
        sources,
        tracked_subreddits=schedule.get("subredditMetadata", {}).keys(),
    )
    save_posts(posts)
    save_comments(comments)
    save_sources(sources)
    snapshot = build_dashboard_snapshot(
        posts=posts,
        comments=comments,
        runs=runs,
        sources=sources,
        schedule=schedule,
    )
    write_dashboard_snapshot(snapshot)


def _stopped_worker_state(
    *,
    previous_state: dict[str, Any] | None = None,
    current_pid: int | None = None,
    stop_reason: str,
    reference_time: datetime | None = None,
) -> dict[str, Any]:
    worker_state = _normalize_worker_state(previous_state or {})
    current_time = reference_time or datetime.now(timezone.utc)
    return {
        "pid": None,
        "lastPid": current_pid or int(worker_state.get("lastPid", 0) or worker_state.get("pid", 0) or 0) or None,
        "status": "stopped",
        "running": False,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "storeModel": "reddit-hot-store-v2",
        "startedAt": worker_state.get("startedAt") or current_time.isoformat(),
        "updatedAt": current_time.isoformat(),
        "stoppedAt": worker_state.get("stoppedAt") or current_time.isoformat(),
        "stopReason": stop_reason,
    }


def _acquire_rotation_worker_lease(*, replace_stale_worker: bool) -> dict[str, Any]:
    current_pid = os.getpid()
    worker_state = _normalize_worker_state(_read_worker_state())
    existing_pid = int(worker_state.get("pid", 0) or 0) or None
    existing_version = str(worker_state.get("runtimeVersion") or "")

    if existing_pid and existing_pid != current_pid and _process_exists(existing_pid):
        if replace_stale_worker and existing_version != REDDIT_ROTATION_RUNTIME_VERSION:
            _terminate_process(existing_pid)
            time.sleep(1)
        else:
            raise RuntimeError(
                f"reddit rotation worker already running with pid {existing_pid}"
            )

    now_iso = datetime.now(timezone.utc).isoformat()
    lease = {
        "pid": current_pid,
        "lastPid": current_pid,
        "status": "running",
        "running": True,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "storeModel": "reddit-hot-store-v2",
        "startedAt": now_iso,
        "updatedAt": now_iso,
        "stoppedAt": None,
        "stopReason": None,
    }
    _write_worker_state(lease)
    try:
        _persist_runtime_metadata(
            worker_state=lease,
            reference_time=datetime.now(timezone.utc),
            persist_snapshot=True,
        )
    except BaseException:
        rollback_time = datetime.now(timezone.utc)
        released_state = _stopped_worker_state(
            previous_state=lease,
            current_pid=current_pid,
            stop_reason="startup-aborted",
            reference_time=rollback_time,
        )
        try:
            _write_worker_state(released_state)
            _persist_runtime_metadata(
                worker_state=released_state,
                reference_time=rollback_time,
                persist_snapshot=False,
            )
        except Exception:
            pass
        raise
    return lease


def _refresh_rotation_worker_lease() -> None:
    current_pid = os.getpid()
    current_time = datetime.now(timezone.utc)
    worker_state = _normalize_worker_state(_read_worker_state())
    refreshed_state = {
        "pid": current_pid,
        "lastPid": current_pid,
        "status": "running",
        "running": True,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "storeModel": "reddit-hot-store-v2",
        "startedAt": worker_state.get("startedAt") or current_time.isoformat(),
        "updatedAt": current_time.isoformat(),
        "stoppedAt": None,
        "stopReason": None,
    }
    _write_worker_state(refreshed_state)
    _persist_runtime_metadata(
        worker_state=refreshed_state,
        reference_time=current_time,
        persist_snapshot=False,
    )


def _release_rotation_worker_lease(stop_reason: str = "stopped") -> None:
    current_pid = os.getpid()
    current_time = datetime.now(timezone.utc)
    worker_state = _normalize_worker_state(_read_worker_state())
    worker_pid = int(worker_state.get("pid", 0) or 0) or None
    last_pid = int(worker_state.get("lastPid", 0) or 0) or None
    if worker_pid not in {None, current_pid} and last_pid not in {None, current_pid}:
        return

    released_state = _stopped_worker_state(
        previous_state=worker_state,
        current_pid=current_pid,
        stop_reason=stop_reason,
        reference_time=current_time,
    )
    _write_worker_state(released_state)
    _persist_runtime_metadata(
        worker_state=released_state,
        reference_time=current_time,
        persist_snapshot=True,
    )


def _current_process_holds_worker_lease() -> bool:
    worker_state = _normalize_worker_state(_read_worker_state())
    return (
        int(worker_state.get("pid", 0) or 0) == os.getpid()
        and worker_state.get("status") == "running"
    )


def _phase_live_interval_seconds() -> dict[RotationPhase, int]:
    return {
        "initial_population": REDDIT_ROTATION_INITIAL_LIVE_INTERVAL_SECONDS,
        "maintenance": REDDIT_ROTATION_MAINTENANCE_LIVE_INTERVAL_SECONDS,
    }


def _phase_backfill_interval_seconds() -> dict[RotationPhase, int]:
    return {
        "initial_population": REDDIT_ROTATION_INITIAL_BACKFILL_INTERVAL_SECONDS,
        "maintenance": REDDIT_ROTATION_MAINTENANCE_BACKFILL_INTERVAL_SECONDS,
    }


def _seconds_since(value: str | None, *, reference_time: datetime) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
    return max(0.0, (reference_time - parsed).total_seconds())


def _auxiliary_due(rotation_state: dict[str, Any], *, reference_time: datetime) -> tuple[bool, int]:
    elapsed = _seconds_since(rotation_state.get("lastAuxiliaryRefreshAt"), reference_time=reference_time)
    if elapsed is None:
        return True, 0
    wait_seconds = max(0, int(REDDIT_ROTATION_AUXILIARY_REFRESH_SECONDS - elapsed))
    return wait_seconds <= 0, wait_seconds


def _load_rotation_inputs() -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    ensure_store_root()
    original_schedule = load_schedule()
    schedule = sync_schedule_state(
        original_schedule,
        subreddits=DEFAULT_SUBREDDITS,
        bucket_size=BUCKET_SIZE,
        max_subreddits_per_run=MAX_SUBREDDITS_PER_RUN,
        catalog_entries=SUBREDDIT_CATALOG,
    )
    raw_worker_state = _read_worker_state()
    worker_state = _normalize_worker_state(raw_worker_state)
    if worker_state != raw_worker_state:
        _write_worker_state(worker_state)
    schedule = _apply_runtime_metadata(
        schedule,
        worker_state=worker_state,
        reference_time=datetime.now(timezone.utc),
    )
    posts = load_posts()
    comments = load_comments()
    runs = load_runs()
    pruned_posts, pruned_comments = prune_records_to_window(posts, comments)
    original_sources = load_sources()
    sources = normalize_sources_for_hot_store(
        original_sources,
        schedule_metadata=schedule.get("subredditMetadata", {}),
    )
    sources = recompute_source_storage_metrics(
        pruned_posts,
        pruned_comments,
        sources,
        tracked_subreddits=schedule.get("subredditMetadata", {}).keys(),
    )

    if (
        pruned_posts != posts
        or pruned_comments != comments
        or sources != original_sources
        or schedule != original_schedule
    ):
        save_posts(pruned_posts)
        save_comments(pruned_comments)
        save_sources(sources)
        save_schedule(schedule)
        snapshot = build_dashboard_snapshot(
            posts=pruned_posts,
            comments=pruned_comments,
            runs=runs,
            sources=sources,
            schedule=schedule,
        )
        write_dashboard_snapshot(snapshot)
    return schedule, sources


def reconcile_reddit_runtime_state(
    *,
    stop_reason: str = "reconciled",
    persist_snapshot: bool = True,
) -> dict[str, Any]:
    current_time = datetime.now(timezone.utc)
    raw_worker_state = _read_worker_state()
    worker_state = _normalize_worker_state(raw_worker_state)
    if worker_state.get("status") != "running":
        worker_state = _stopped_worker_state(
            previous_state=worker_state,
            stop_reason=stop_reason,
            reference_time=current_time,
        )
    if worker_state != raw_worker_state:
        _write_worker_state(worker_state)
    _persist_runtime_metadata(
        worker_state=worker_state,
        reference_time=current_time,
        persist_snapshot=persist_snapshot,
    )
    return worker_state


def _persist_phase_only(
    schedule: dict[str, Any],
    *,
    phase: RotationPhase,
    reference_time: datetime,
) -> dict[str, Any]:
    rotation = _normalize_rotation_state(schedule.get("rotationState"))
    previous_phase = rotation.get("phase")
    now_iso = reference_time.isoformat()
    worker_pid = os.getpid() if _current_process_holds_worker_lease() else None
    schedule["rotationState"] = {
        **rotation,
        "phase": phase,
        "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        "workerPid": worker_pid,
        "workerStatus": "running" if worker_pid else rotation.get("workerStatus", "stopped"),
        "workerHeartbeatAt": now_iso if worker_pid else rotation.get("workerHeartbeatAt"),
        "workerStartedAt": rotation.get("workerStartedAt") or (now_iso if worker_pid else None),
        "workerStoppedAt": None if worker_pid else rotation.get("workerStoppedAt"),
        "phaseChangedAt": now_iso if previous_phase != phase else rotation.get("phaseChangedAt"),
        "populationCompletedAt": (
            now_iso
            if phase == "maintenance" and previous_phase != "maintenance"
            else rotation.get("populationCompletedAt")
        ),
        "updatedAt": now_iso,
    }
    schedule["updatedAt"] = now_iso
    save_schedule(schedule)
    return schedule


def run_reddit_rotation_once(
    *,
    mode: RotationMode = "auto",
    refresh_auxiliary: bool | None = None,
) -> tuple[int, dict[str, Any]]:
    if not REDDIT_ENABLED:
        reconcile_reddit_runtime_state(stop_reason="disabled", persist_snapshot=True)
        return 0, {
            "status": "disabled",
            "reason": "REDDIT_ENABLED=0",
            "mode": None,
            "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
        }

    reference_time = datetime.now(timezone.utc)
    schedule, sources = _load_rotation_inputs()
    plan = get_rotation_plan(
        schedule,
        sources,
        reference_time=reference_time,
        phase_live_interval_seconds=_phase_live_interval_seconds(),
        phase_backfill_interval_seconds=_phase_backfill_interval_seconds(),
        initial_population_backfill_target_pct=REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
        initial_population_live_target_pct=REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
    )

    selected_mode = plan.get("mode")
    if mode in {"live", "backfill"}:
        selected_mode = mode

    auxiliary_due, auxiliary_wait_seconds = _auxiliary_due(
        plan.get("rotationState", {}),
        reference_time=reference_time,
    )
    should_refresh_auxiliary = auxiliary_due if refresh_auxiliary is None else refresh_auxiliary
    reddit_runtime = get_reddit_runtime_state()
    reddit_backoff_wait_seconds = get_reddit_backoff_wait_seconds(transport="praw")

    if selected_mode is None:
        schedule = _persist_phase_only(
            schedule,
            phase=plan["phase"],
            reference_time=reference_time,
        )
        rotation_summary = summarize_rotation(
            schedule,
            sources,
            reference_time=reference_time,
            initial_population_backfill_target_pct=REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
            initial_population_live_target_pct=REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
        )
        return 0, {
            "status": "idle",
            "mode": None,
            "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
            "rotationPhase": plan["phase"],
            "waitSeconds": int(plan.get("waitSeconds", 0) or 0),
            "auxiliaryRefreshDue": auxiliary_due,
            "trackedSubreddits": rotation_summary["trackedSources"],
            "liveCoveragePct": rotation_summary["liveCoveragePct"],
            "backfillCompletePct": rotation_summary["backfillCompletePct"],
            "windowCoveragePct": rotation_summary["windowCoveragePct"],
            "liveDueSources": rotation_summary["liveDueSources"],
            "backfillPendingSources": rotation_summary["backfillPendingSources"],
            "liveCycleCompletionPct": rotation_summary["live"]["completionPct"],
            "backfillCycleCompletionPct": rotation_summary["backfill"]["completionPct"],
            "liveCompletedCycles": rotation_summary["live"]["completedCycleCount"],
            "backfillCompletedCycles": rotation_summary["backfill"]["completedCycleCount"],
            "redditCooldownActive": reddit_runtime.get("cooldownActive", False),
            "redditAuthBackoffActive": reddit_runtime.get("authBackoffActive", False),
            "redditNextAllowedInSeconds": reddit_runtime.get("nextAllowedInSeconds", 0.0),
        }

    if reddit_backoff_wait_seconds > 0:
        schedule = _persist_phase_only(
            schedule,
            phase=plan["phase"],
            reference_time=reference_time,
        )
        rotation_summary = summarize_rotation(
            schedule,
            sources,
            reference_time=reference_time,
            initial_population_backfill_target_pct=REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
            initial_population_live_target_pct=REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
        )
        return 0, {
            "status": "idle",
            "reason": "reddit_backoff",
            "mode": None,
            "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
            "rotationPhase": plan["phase"],
            "waitSeconds": max(int(plan.get("waitSeconds", 0) or 0), reddit_backoff_wait_seconds),
            "auxiliaryRefreshDue": auxiliary_due,
            "trackedSubreddits": rotation_summary["trackedSources"],
            "liveCoveragePct": rotation_summary["liveCoveragePct"],
            "backfillCompletePct": rotation_summary["backfillCompletePct"],
            "windowCoveragePct": rotation_summary["windowCoveragePct"],
            "liveDueSources": rotation_summary["liveDueSources"],
            "backfillPendingSources": rotation_summary["backfillPendingSources"],
            "liveCycleCompletionPct": rotation_summary["live"]["completionPct"],
            "backfillCycleCompletionPct": rotation_summary["backfill"]["completionPct"],
            "liveCompletedCycles": rotation_summary["live"]["completedCycleCount"],
            "backfillCompletedCycles": rotation_summary["backfill"]["completedCycleCount"],
            "redditCooldownActive": reddit_runtime.get("cooldownActive", False),
            "redditAuthBackoffActive": reddit_runtime.get("authBackoffActive", False),
            "redditNextAllowedInSeconds": reddit_runtime.get("nextAllowedInSeconds", 0.0),
            "redditLastFailureReason": reddit_runtime.get("lastFailureReason"),
        }

    exit_code, summary = run_reddit_ingestion(
        selected_mode,
        refresh_auxiliary=should_refresh_auxiliary,
    )

    completion_time = datetime.now(timezone.utc)
    schedule_after, sources_after = _load_rotation_inputs()
    schedule_after = update_rotation_state(
        schedule_after,
        mode=selected_mode,
        phase=plan["phase"],
        auxiliary_refreshed=should_refresh_auxiliary,
        runtime_version=REDDIT_ROTATION_RUNTIME_VERSION,
        worker_pid=os.getpid() if _current_process_holds_worker_lease() else None,
        reference_time=completion_time,
    )
    save_schedule(schedule_after)

    rotation_summary = summarize_rotation(
        schedule_after,
        sources_after,
        reference_time=completion_time,
        initial_population_backfill_target_pct=REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
        initial_population_live_target_pct=REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
    )
    reddit_runtime_after = get_reddit_runtime_state()
    summary.update(
        {
            "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
            "rotationPhase": rotation_summary["phase"],
            "auxiliaryRefreshDue": auxiliary_due,
            "auxiliaryRefreshTriggered": should_refresh_auxiliary,
            "rotationWaitSeconds": int(plan.get("waitSeconds", 0) or 0),
            "trackedSubreddits": rotation_summary["trackedSources"],
            "liveCoveragePct": rotation_summary["liveCoveragePct"],
            "backfillCompletePct": rotation_summary["backfillCompletePct"],
            "windowCoveragePct": rotation_summary["windowCoveragePct"],
            "liveDueSources": rotation_summary["liveDueSources"],
            "backfillPendingSources": rotation_summary["backfillPendingSources"],
            "backfillDueSources": rotation_summary["backfillDueSources"],
            "liveCycleCompletionPct": rotation_summary["live"]["completionPct"],
            "backfillCycleCompletionPct": rotation_summary["backfill"]["completionPct"],
            "liveCompletedCycles": rotation_summary["live"]["completedCycleCount"],
            "backfillCompletedCycles": rotation_summary["backfill"]["completedCycleCount"],
            "redditCooldownActive": reddit_runtime_after.get("cooldownActive", False),
            "redditAuthBackoffActive": reddit_runtime_after.get("authBackoffActive", False),
            "redditNextAllowedInSeconds": reddit_runtime_after.get("nextAllowedInSeconds", 0.0),
            "redditLastFailureReason": reddit_runtime_after.get("lastFailureReason"),
        }
    )
    return exit_code, summary


def run_reddit_rotation_loop(
    *,
    mode: RotationMode = "auto",
    sleep_seconds: int = REDDIT_ROTATION_LOOP_SLEEP_SECONDS,
    max_iterations: int | None = None,
    refresh_auxiliary: bool | None = None,
    replace_stale_worker: bool = True,
) -> int:
    if not REDDIT_ENABLED:
        reconcile_reddit_runtime_state(stop_reason="disabled", persist_snapshot=True)
        print(
            json.dumps(
                {
                    "status": "disabled",
                    "reason": "REDDIT_ENABLED=0",
                    "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
                },
                indent=2,
            )
        )
        return 0

    iteration = 0
    overall_exit_code = 0
    stop_reason = "stopped"
    lease_acquired = False
    try:
        _acquire_rotation_worker_lease(replace_stale_worker=replace_stale_worker)
        lease_acquired = True
        initial_schedule, initial_sources = _load_rotation_inputs()
        initial_summary = summarize_rotation(
            initial_schedule,
            initial_sources,
            reference_time=datetime.now(timezone.utc),
            initial_population_backfill_target_pct=REDDIT_ROTATION_INITIAL_BACKFILL_TARGET_PCT,
            initial_population_live_target_pct=REDDIT_ROTATION_INITIAL_LIVE_TARGET_PCT,
        )
        _persist_phase_only(
            initial_schedule,
            phase=initial_summary["phase"],
            reference_time=datetime.now(timezone.utc),
        )
        while True:
            iteration += 1
            _refresh_rotation_worker_lease()
            try:
                exit_code, summary = run_reddit_rotation_once(
                    mode=mode,
                    refresh_auxiliary=refresh_auxiliary,
                )
            except Exception as error:
                exit_code = 1
                summary = {
                    "status": "loop-error",
                    "mode": mode if mode in {"live", "backfill"} else None,
                    "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
                    "error": str(error),
                }
            overall_exit_code = max(overall_exit_code, exit_code)
            print(json.dumps(summary, indent=2))

            if max_iterations is not None and iteration >= max_iterations:
                break

            if summary.get("status") == "idle":
                wait_seconds = int(summary.get("waitSeconds", sleep_seconds) or sleep_seconds)
                time.sleep(max(1, min(wait_seconds, sleep_seconds)))
                continue

            time.sleep(max(1, sleep_seconds))
    except KeyboardInterrupt:
        stop_reason = "keyboard_interrupt"
        overall_exit_code = max(overall_exit_code, 130)
        print(json.dumps({"status": "stopped", "reason": stop_reason}, indent=2))
    except Exception as error:
        stop_reason = "loop-error"
        overall_exit_code = max(overall_exit_code, 1)
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": stop_reason,
                    "runtimeVersion": REDDIT_ROTATION_RUNTIME_VERSION,
                    "error": str(error),
                },
                indent=2,
            )
        )
    finally:
        if lease_acquired or _current_process_holds_worker_lease():
            _release_rotation_worker_lease(stop_reason=stop_reason)

    return overall_exit_code
