import "server-only";

import { requestDashboardBackgroundRefresh } from "@/lib/dashboard/background-refresh";
import {
  DashboardProfiler,
  formatDashboardProfileLog,
  writeProfileSnapshot,
} from "@/lib/dashboard/profiling";
import { clipTimeSeriesToLiveWindow } from "@/lib/dashboard/time-window";
import { getSupabaseTrendDashboardState, shouldUseSupabaseTrendSource } from "@/lib/dashboard/supabase-trends";
import {
  bootstrapRuntimeBundleFromLegacyCache,
  buildAndPersistDashboardRuntimeBundle,
  getDashboardSourceManifest,
  getRuntimeBaseState,
  hasPersistedRuntimeRawSnapshot,
  isRuntimeBundleStale,
  loadDashboardRefreshState,
  loadLatestDashboardRuntimeBundle,
} from "@/lib/dashboard/runtime-store";
import { applyTrendDashboardSelection, pinTrendIntoRows } from "@/lib/dashboard/selection";
import { createZeroTrendDashboardVM } from "@/lib/dashboard/zero-state";
import { RUNTIME_REQUEST_PROFILE_PATH } from "@/lib/runtime/paths";
import { allowLegacyTrendFallbackOnSupabaseError } from "@/lib/supabase/server";
import {
  DashboardDataStatus,
  DashboardRuntimeBundleOrigin,
  DashboardRuntimeSource,
  RankedTrend,
  TrendDashboardQuery,
  TrendDashboardVM,
} from "@/types/view-models";

const variantDashboardStateCache = new Map<string, TrendDashboardVM>();
const DASHBOARD_VARIANT_CACHE_LIMIT = 24;
const DASHBOARD_RESPONSE_LEADERBOARD_LIMIT = 250;
const DASHBOARD_DEBUG = process.env.NODE_ENV !== "production";

function buildRuntimeVariantCacheKey(
  query: TrendDashboardQuery,
  generatedAt: string,
  stateSource: DashboardRuntimeSource,
  requestTime: Date,
) {
  const mode = query.mode ?? "established";
  return JSON.stringify({
    generatedAt,
    liveWindowEnd: requestTime.toISOString(),
    stateSource,
    scope: query.scope,
    range: query.range,
    mode,
    sort: query.sort,
    selectedId: query.selectedId ?? null,
  });
}

function writeVariantDashboardStateCache(key: string, value: TrendDashboardVM) {
  variantDashboardStateCache.delete(key);
  variantDashboardStateCache.set(key, value);

  if (variantDashboardStateCache.size <= DASHBOARD_VARIANT_CACHE_LIMIT) {
    return;
  }

  const oldestKey = variantDashboardStateCache.keys().next().value;
  if (oldestKey) {
    variantDashboardStateCache.delete(oldestKey);
  }
}

function trimLeaderboardTrend(trend: RankedTrend): RankedTrend {
  return {
    ...trend,
    attentionHistory: [],
    platformBreakdown: [],
    topPosts: [],
    attentionDrivers: [],
    platformMigrationPath: [],
    blueskyDetail: null,
  };
}

function compactDashboardState(vm: TrendDashboardVM): TrendDashboardVM {
  const selectedTrend = vm.detail?.trend ?? null;
  const mode = vm.query.mode ?? "established";
  const leaderboard = pinTrendIntoRows(
    vm.leaderboard,
    selectedTrend,
    DASHBOARD_RESPONSE_LEADERBOARD_LIMIT,
  ).map(trimLeaderboardTrend);
  const established = (
    mode === "established"
      ? pinTrendIntoRows(vm.leaderboards.established, selectedTrend, DASHBOARD_RESPONSE_LEADERBOARD_LIMIT)
      : vm.leaderboards.established.slice(0, DASHBOARD_RESPONSE_LEADERBOARD_LIMIT)
  ).map(trimLeaderboardTrend);
  const emerging = (
    mode === "emerging"
      ? pinTrendIntoRows(vm.leaderboards.emerging, selectedTrend, DASHBOARD_RESPONSE_LEADERBOARD_LIMIT)
      : vm.leaderboards.emerging.slice(0, DASHBOARD_RESPONSE_LEADERBOARD_LIMIT)
  ).map(trimLeaderboardTrend);

  return {
    ...vm,
    leaderboards: {
      established,
      emerging,
    },
    leaderboard,
  };
}

function buildLegacyLeaderboards(rows: RankedTrend[]) {
  const established = [...rows].map((trend, index) => ({
    ...trend,
    rank: index + 1,
  }));
  const emerging = [...rows]
    .filter((trend) => trend.isEarlyTrend || (trend.emergingScore ?? 0) > 0)
    .sort(compareBySort("emerging", "breakout"))
    .map((trend, index) => ({
      ...trend,
      rank: index + 1,
    }));

  return {
    established,
    emerging,
  };
}

function normalizeBaseStateLeaderboards(
  baseState: TrendDashboardVM,
  context: string,
): TrendDashboardVM {
  if (baseState.leaderboards) {
    return baseState;
  }

  const leaderboards = buildLegacyLeaderboards(baseState.leaderboard ?? []);
  if (DASHBOARD_DEBUG) {
    console.warn("[dashboard-service] normalizing legacy runtime base state", {
      context,
      establishedCount: leaderboards.established.length,
      emergingCount: leaderboards.emerging.length,
      stateKeys: Object.keys(baseState ?? {}),
    });
  }

  return {
    ...baseState,
    leaderboards,
  };
}

function compareBySort(
  mode: TrendDashboardQuery["mode"],
  sort: TrendDashboardQuery["sort"],
) {
  const interactionCount = (row: RankedTrend) =>
    row.totalInteractions24h ?? row.mentions ?? row.attentionInteractions ?? 0;
  const tierWeight = (row: RankedTrend) => {
    switch (row.leaderboardTier) {
      case "primary_grouped":
        return 4;
      case "secondary_singleton":
        return 3;
      case "audit_low_information":
        return 2;
      case "audit_template":
        return 1;
      case "audit_fallback":
      default:
        return 0;
    }
  };
  const byTierAndQuality = (left: RankedTrend, right: RankedTrend) =>
    tierWeight(right) - tierWeight(left) ||
    (right.qualityAdjustedScore ?? right.totalInteractions24h ?? right.attentionInteractions) -
      (left.qualityAdjustedScore ?? left.totalInteractions24h ?? left.attentionInteractions) ||
    (right.totalInteractions24h ?? right.attentionInteractions) -
      (left.totalInteractions24h ?? left.attentionInteractions) ||
    left.id.localeCompare(right.id);
  const byEstablishedInteractions = (left: RankedTrend, right: RankedTrend) =>
    interactionCount(right) - interactionCount(left) ||
    right.attentionInteractions - left.attentionInteractions ||
    left.id.localeCompare(right.id);

  if (mode === "emerging") {
    if (sort === "velocity") {
      return (left: RankedTrend, right: RankedTrend) =>
        byTierAndQuality(left, right) ||
        (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
        (right.breakoutScore ?? right.emergingScore ?? 0) -
          (left.breakoutScore ?? left.emergingScore ?? 0) ||
        right.attentionInteractions - left.attentionInteractions ||
        left.id.localeCompare(right.id);
    }

    if (sort === "novelty") {
      return (left: RankedTrend, right: RankedTrend) =>
        byTierAndQuality(left, right) ||
        (right.noveltyScore ?? 0) - (left.noveltyScore ?? 0) ||
        (right.breakoutScore ?? right.emergingScore ?? 0) -
          (left.breakoutScore ?? left.emergingScore ?? 0) ||
        right.attentionInteractions - left.attentionInteractions ||
        left.id.localeCompare(right.id);
    }

    if (sort === "confirmation") {
      return (left: RankedTrend, right: RankedTrend) =>
        byTierAndQuality(left, right) ||
        (right.confirmationScore ?? 0) - (left.confirmationScore ?? 0) ||
        (right.breakoutScore ?? right.emergingScore ?? 0) -
          (left.breakoutScore ?? left.emergingScore ?? 0) ||
        right.attentionInteractions - left.attentionInteractions ||
        left.id.localeCompare(right.id);
    }

    return (left: RankedTrend, right: RankedTrend) =>
      byTierAndQuality(left, right) ||
      (right.breakoutScore ?? right.emergingScore ?? 0) -
        (left.breakoutScore ?? left.emergingScore ?? 0) ||
      (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
      left.id.localeCompare(right.id);
  }

  if (sort === "growth") {
    return (left: RankedTrend, right: RankedTrend) =>
      byEstablishedInteractions(left, right) ||
      right.growthRate - left.growthRate ||
      right.attentionScore - left.attentionScore ||
      left.id.localeCompare(right.id);
  }

  if (sort === "mentions") {
    return (left: RankedTrend, right: RankedTrend) =>
      byEstablishedInteractions(left, right) ||
      right.mentions - left.mentions ||
      right.attentionScore - left.attentionScore ||
      left.id.localeCompare(right.id);
  }

  if (sort === "strength") {
    return (left: RankedTrend, right: RankedTrend) =>
      byEstablishedInteractions(left, right) ||
      right.trendStrengthScore - left.trendStrengthScore ||
      right.attentionScore - left.attentionScore ||
      left.id.localeCompare(right.id);
  }

  return byEstablishedInteractions;
}

function getTrendDisplayPriority(row: RankedTrend) {
  if (row.leaderboardTier) {
    switch (row.leaderboardTier) {
      case "primary_grouped":
        return 0;
      case "secondary_singleton":
        return 1;
      case "audit_low_information":
        return 2;
      case "audit_template":
        return 3;
      case "audit_fallback":
      default:
        return 4;
    }
  }

  const normalizedName = (row.name ?? "").trim().toLowerCase();

  if (
    row.lowInformation ||
    normalizedName.includes("empty or no-content") ||
    normalizedName.includes("empty post") ||
    normalizedName.includes("placeholder")
  ) {
    return 5;
  }

  if (row.templateSeries || (row.spamLikelihood ?? 0) >= 0.72) {
    return 4;
  }

  if (row.labelType === "fallback_generated") {
    return 3;
  }

  if (row.lowQualityLabel) {
    return 2;
  }

  if (row.isSingleton) {
    return 1;
  }

  return 0;
}

function prioritizeDisplayRows(
  rows: RankedTrend[],
  mode: TrendDashboardQuery["mode"],
  sort: TrendDashboardQuery["sort"],
) {
  const compare = compareBySort(mode, sort);
  if (mode === "established") {
    return [...rows].sort(compare);
  }
  return [...rows].sort((left, right) => {
    const leftPriority = getTrendDisplayPriority(left);
    const rightPriority = getTrendDisplayPriority(right);
    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }

    return compare(left, right);
  });
}

function alignDashboardChartsToLiveWindow(
  vm: TrendDashboardVM,
  range: TrendDashboardQuery["range"],
  requestTime: Date,
) {
  const overviewSeries = vm.overviewSeries.map((series) => {
    const clipped = clipTimeSeriesToLiveWindow({
      points: series.points,
      range,
      now: requestTime,
    });

    return {
      ...series,
      points: clipped.points,
      window: clipped.window,
    };
  });
  const detail = vm.detail
    ? (() => {
        const clipped = clipTimeSeriesToLiveWindow({
          points: vm.detail.attentionGraph,
          range,
          now: requestTime,
        });

        return {
          ...vm.detail,
          attentionGraph: clipped.points,
          attentionWindow: clipped.window,
        };
      })()
    : null;
  const blueskyOverview = vm.blueskyOverview
    ? (() => {
        const clipped = clipTimeSeriesToLiveWindow({
          points: vm.blueskyOverview.replay,
          range,
          now: requestTime,
        });

        return {
          ...vm.blueskyOverview,
          replay: clipped.points,
          replayWindow: clipped.window,
        };
      })()
    : null;

  return {
    ...vm,
    overviewSeries,
    detail,
    blueskyOverview,
  };
}

function buildDashboardVariant(
  baseState: TrendDashboardVM,
  query: TrendDashboardQuery,
  requestTime: Date,
) {
  const mode = query.mode ?? "established";
  const normalizedBaseState = normalizeBaseStateLeaderboards(
    baseState,
    `variant:${query.scope}:${query.range}:${mode}:${query.sort}`,
  );
  const baseRows = normalizedBaseState.leaderboards[mode] ?? [];
  const leaderboard = prioritizeDisplayRows(baseRows, mode, query.sort)
    .map((trend, index) => ({
      ...trend,
      rank: index + 1,
    }));

  if (DASHBOARD_DEBUG) {
    console.info("[dashboard-service] variant counts", {
      scope: query.scope,
      range: query.range,
      mode,
      sort: query.sort,
      establishedCount: normalizedBaseState.leaderboards.established.length,
      emergingCount: normalizedBaseState.leaderboards.emerging.length,
      selectedCount: leaderboard.length,
    });
  }

  const selectedVariant = applyTrendDashboardSelection(
    {
      ...normalizedBaseState,
      query: {
        ...query,
        mode,
        selectedId: undefined,
      },
      leaderboard,
    },
    query.selectedId,
  );

  if (DASHBOARD_DEBUG) {
    console.info("[dashboard-service] selection resolution", {
      scope: query.scope,
      range: query.range,
      mode,
      sort: query.sort,
      requestedSelectedId: query.selectedId ?? null,
      resolvedSelectedId: selectedVariant.detail?.trend.id ?? null,
      leaderboardContainsSelected: Boolean(
        query.selectedId && selectedVariant.leaderboard.some((trend) => trend.id === query.selectedId),
      ),
      selectedWasFallback:
        Boolean(query.selectedId) &&
        Boolean(selectedVariant.detail?.trend.id) &&
        query.selectedId !== selectedVariant.detail?.trend.id,
    });
  }

  return alignDashboardChartsToLiveWindow(selectedVariant, query.range, requestTime);
}

function attachDataStatus(vm: TrendDashboardVM, dataStatus: DashboardDataStatus): TrendDashboardVM {
  return {
    ...vm,
    dataStatus,
  };
}

function isRefreshActive(refresh: DashboardDataStatus["refresh"] | null) {
  return refresh?.status === "scheduled" || refresh?.status === "running";
}

function toTimestamp(value: string | null | undefined) {
  if (!value) {
    return Number.NaN;
  }

  return Date.parse(value);
}

function getLatestMaterializedSourceUpdatedAt(
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
) {
  return (
    sourceManifest.files
      .filter((file) => file.exists && file.sizeBytes > 0 && Boolean(file.updatedAt))
      .map((file) => file.updatedAt as string)
      .sort()
      .at(-1) ?? null
  );
}

function isRuntimeBundleBehindSourceData(
  runtimeBundle: NonNullable<Awaited<ReturnType<typeof loadLatestDashboardRuntimeBundle>>>,
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
) {
  const latestSourceUpdatedAt = getLatestMaterializedSourceUpdatedAt(sourceManifest);
  const latestSourceUpdatedMs = toTimestamp(latestSourceUpdatedAt);
  const runtimeBundleGeneratedMs = toTimestamp(runtimeBundle.generatedAt);

  if (!Number.isFinite(latestSourceUpdatedMs) || !Number.isFinite(runtimeBundleGeneratedMs)) {
    return false;
  }

  return latestSourceUpdatedMs > runtimeBundleGeneratedMs;
}

function getInlineRebuildReason(
  runtimeBundle: NonNullable<Awaited<ReturnType<typeof loadLatestDashboardRuntimeBundle>>>,
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
  localRawDataAvailable: boolean,
  refreshState: DashboardDataStatus["refresh"] | null,
) {
  if (!localRawDataAvailable || isRefreshActive(refreshState)) {
    return null;
  }

  if (!isRuntimeBundleBehindSourceData(runtimeBundle, sourceManifest)) {
    return null;
  }

  return `runtime snapshot ${runtimeBundle.origin} is behind fresher materialized source data`;
}

function hasMaterializedSourceFileData(
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
  id: string,
) {
  const file = sourceManifest.files.find((entry) => entry.id === id);
  return Boolean(file?.exists && file.sizeBytes > 2);
}

function hasBlueskyRawDataOnDisk(
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
) {
  return (
    hasMaterializedSourceFileData(sourceManifest, "bluesky_posts") ||
    hasMaterializedSourceFileData(sourceManifest, "bluesky_interactions") ||
    hasMaterializedSourceFileData(sourceManifest, "bluesky_post_snapshots") ||
    hasMaterializedSourceFileData(sourceManifest, "bluesky_profiles") ||
    hasMaterializedSourceFileData(sourceManifest, "bluesky_firehose_state")
  );
}

function getRuntimeBundleRecoveryReason(
  runtimeBundle: NonNullable<Awaited<ReturnType<typeof loadLatestDashboardRuntimeBundle>>>,
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
) {
  if (!hasBlueskyRawDataOnDisk(sourceManifest)) {
    return null;
  }

  const hasBlueskyCounts =
    runtimeBundle.rawCounts.blueskyPosts > 0 ||
    runtimeBundle.rawCounts.blueskyInteractions > 0 ||
    runtimeBundle.rawCounts.blueskyPostSnapshots > 0 ||
    runtimeBundle.rawCounts.blueskyProfiles > 0;

  if (hasBlueskyCounts) {
    return null;
  }

  return `runtime snapshot ${runtimeBundle.origin} is missing Bluesky counts while fresher Bluesky raw files exist on disk`;
}

function buildDataStatus(params: {
  stateSource: DashboardRuntimeSource;
  bundleOrigin: DashboardRuntimeBundleOrigin | null;
  runtimeSnapshotGeneratedAt: string | null;
  sourceSnapshotGeneratedAt: string | null;
  latestFetchedAt: string | null;
  runtimeSnapshotAvailable: boolean;
  localRawDataAvailable: boolean;
  runtimeSnapshotStale: boolean;
  sourceFreshness: DashboardDataStatus["sourceFreshness"];
  refresh: DashboardDataStatus["refresh"];
  timings: DashboardDataStatus["timings"];
}): DashboardDataStatus {
  return {
    stateSource: params.stateSource,
    bundleOrigin: params.bundleOrigin,
    showing:
      params.stateSource === "supabase_live"
        ? "supabase_live"
        : params.stateSource === "runtime_snapshot"
        ? "cached_local"
        : params.stateSource === "local_raw_rebuild"
          ? "fresh_local_rebuild"
          : "zero_state",
    serverNow: new Date().toISOString(),
    runtimeSnapshotGeneratedAt: params.runtimeSnapshotGeneratedAt,
    sourceSnapshotGeneratedAt: params.sourceSnapshotGeneratedAt,
    latestFetchedAt: params.latestFetchedAt,
    runtimeSnapshotAvailable: params.runtimeSnapshotAvailable,
    localRawDataAvailable: params.localRawDataAvailable,
    runtimeSnapshotStale: params.runtimeSnapshotStale,
    sourceFreshness: params.sourceFreshness,
    refresh: params.refresh,
    timings: params.timings,
  };
}

function getAgeMinutesFromIso(isoValue: string | null | undefined) {
  if (!isoValue) {
    return null;
  }

  const timestamp = Date.parse(isoValue);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return Math.max(0, Math.round((Date.now() - timestamp) / 60_000));
}

function getBlueskyFirehoseFreshness(
  runtimeBundle: NonNullable<Awaited<ReturnType<typeof loadLatestDashboardRuntimeBundle>>>,
  sourceManifest: Awaited<ReturnType<typeof getDashboardSourceManifest>>,
) {
  const freshnessRow = runtimeBundle.sourceFreshness.find(
    (entry) => entry.sourceId === "bluesky:firehose:state",
  );
  const fileSignature = sourceManifest.files.find((entry) => entry.id === "bluesky_firehose_state");
  const manifestAgeMinutes = getAgeMinutesFromIso(fileSignature?.updatedAt ?? null);
  const freshnessAgeMinutes = freshnessRow?.ageMinutes ?? null;
  const sourceStatus = String(freshnessRow?.sourceStatus ?? "").toLowerCase();
  const stale =
    sourceStatus === "stale" ||
    sourceStatus === "disconnected" ||
    (freshnessAgeMinutes !== null && freshnessAgeMinutes > 15) ||
    (manifestAgeMinutes !== null && manifestAgeMinutes > 15);

  return {
    stale,
    sourceFreshness: runtimeBundle.sourceFreshness.map((entry) =>
      entry.sourceId !== "bluesky:firehose:state"
        ? entry
        : {
            ...entry,
            sourceStatus: stale ? "stale" : entry.sourceStatus,
            ageMinutes:
              manifestAgeMinutes !== null
                ? Math.max(entry.ageMinutes ?? 0, manifestAgeMinutes)
                : entry.ageMinutes,
          },
    ),
  };
}

function applyBlueskyReplayFreshness(
  vm: TrendDashboardVM,
  freshness: ReturnType<typeof getBlueskyFirehoseFreshness> | null,
) {
  if (!freshness?.stale) {
    return vm;
  }

  return {
    ...vm,
    blueskyOverview: vm.blueskyOverview
      ? {
          ...vm.blueskyOverview,
          replay: [],
          replayWindow: null,
        }
      : vm.blueskyOverview,
    dataStatus: vm.dataStatus
      ? {
          ...vm.dataStatus,
          sourceFreshness: freshness.sourceFreshness,
        }
      : vm.dataStatus,
  };
}

type DashboardStateOptions = {
  forceRebuild?: boolean;
};

export async function getTrendDashboardState(
  query: TrendDashboardQuery,
  options: DashboardStateOptions = {},
) {
  // Supabase-backed dashboard source is the primary path when enabled.
  // The runtime-store path below is retained as an explicit legacy fallback.
  if (shouldUseSupabaseTrendSource()) {
    try {
      return await getSupabaseTrendDashboardState(query);
    } catch (error) {
      const allowFallback = allowLegacyTrendFallbackOnSupabaseError();
      console.error("[dashboard] Supabase trend source failed", {
        query,
        allowFallback,
        error,
      });
      if (!allowFallback) {
        return attachDataStatus(createZeroTrendDashboardVM(query), {
          stateSource: "zero_state",
          bundleOrigin: null,
          showing: "zero_state",
          serverNow: new Date().toISOString(),
          runtimeSnapshotGeneratedAt: null,
          sourceSnapshotGeneratedAt: null,
          latestFetchedAt: null,
          runtimeSnapshotAvailable: false,
          localRawDataAvailable: false,
          runtimeSnapshotStale: false,
          sourceFreshness: [],
          refresh: null,
          timings: null,
        });
      }
    }
  }

  const profiler = new DashboardProfiler();

  try {
    const sourceManifestPromise = profiler.measure("source_manifest_read", () =>
      getDashboardSourceManifest(),
    );
    const refreshStatePromise = loadDashboardRefreshState();
    const persistedRuntimeRawPromise = hasPersistedRuntimeRawSnapshot();
    const runtimeBundle = options.forceRebuild
      ? null
      : await profiler.measure("runtime_snapshot_read", () => loadLatestDashboardRuntimeBundle());
    if (options.forceRebuild) {
      profiler.record("runtime_snapshot_read", 0);
    }

    const [sourceManifest, refreshState, persistedRuntimeRawAvailable] = await Promise.all([
      sourceManifestPromise,
      refreshStatePromise,
      persistedRuntimeRawPromise,
    ]);
    const localRawDataAvailable = sourceManifest.hasAnyData || persistedRuntimeRawAvailable;
    const hasBlueskyRawData = hasBlueskyRawDataOnDisk(sourceManifest);
    const runtimeBundleRecoveryReason = runtimeBundle
      ? getRuntimeBundleRecoveryReason(runtimeBundle, sourceManifest)
      : null;
    const runtimeSnapshotStale = runtimeBundle
      ? (sourceManifest.hasAnyData && isRuntimeBundleStale(runtimeBundle, sourceManifest)) ||
        (runtimeBundle.origin === "legacy_bootstrap" && localRawDataAvailable) ||
        Boolean(runtimeBundleRecoveryReason)
      : false;
    const inlineRebuildReason =
      runtimeBundle && runtimeSnapshotStale
        ? getInlineRebuildReason(runtimeBundle, sourceManifest, localRawDataAvailable, refreshState)
        : null;
    const runtimeBundleFreshness = runtimeBundle
      ? getBlueskyFirehoseFreshness(runtimeBundle, sourceManifest)
      : null;

    if (runtimeBundle) {
      const baseState = getRuntimeBaseState(runtimeBundle, query);
      if (baseState) {
        const immediateRebuildReason = runtimeBundleRecoveryReason ?? inlineRebuildReason;
        if (immediateRebuildReason && localRawDataAvailable) {
          console.warn("[dashboard] runtime snapshot invalid, rebuilding from local raw data", {
            reason: immediateRebuildReason,
            bundleOrigin: runtimeBundle.origin,
            runtimeSnapshotGeneratedAt: runtimeBundle.generatedAt,
            sourceSnapshotGeneratedAt: runtimeBundle.sourceSnapshotGeneratedAt,
          });

          const { bundle } = await buildAndPersistDashboardRuntimeBundle("local_rebuild", {
            profiler,
            refreshState,
            requestedQueries: [query],
            existingBundle: runtimeBundle,
          });
          const recoveredBaseState =
            getRuntimeBaseState(bundle, query) ?? createZeroTrendDashboardVM(query);
          const recoveredVariant = await profiler.measure("variant_build", () =>
            buildDashboardVariant(recoveredBaseState, query, new Date()),
          );
          const recoveredResult = attachDataStatus(
            compactDashboardState(recoveredVariant),
            buildDataStatus({
              stateSource: "local_raw_rebuild",
              bundleOrigin: bundle.origin,
              runtimeSnapshotGeneratedAt: bundle.generatedAt,
              sourceSnapshotGeneratedAt: bundle.sourceSnapshotGeneratedAt,
              latestFetchedAt: bundle.latestFetchedAt,
              runtimeSnapshotAvailable: true,
              localRawDataAvailable: true,
              runtimeSnapshotStale: false,
              sourceFreshness: bundle.sourceFreshness,
              refresh: refreshState,
              timings: profiler.buildSummary(),
            }),
          );

          const timings = recoveredResult.dataStatus?.timings ?? profiler.buildSummary();
          await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
            generatedAt: new Date().toISOString(),
            label: "dashboard-request",
            timings,
            metadata: {
              query,
              stateSource: "local_raw_rebuild",
              bundleOrigin: bundle.origin,
              runtimeSnapshotGeneratedAt: bundle.generatedAt,
              recoveryReason: immediateRebuildReason,
            },
          });
          console.info(
            formatDashboardProfileLog("request", timings, {
              stateSource: "local_raw_rebuild",
              bundleOrigin: bundle.origin,
              recovered: true,
              range: query.range,
              scope: query.scope,
            }),
          );
          return applyBlueskyReplayFreshness(recoveredResult, getBlueskyFirehoseFreshness(bundle, sourceManifest));
        }

        const requestTime = new Date();
        const variantCacheKey = buildRuntimeVariantCacheKey(
          query,
          runtimeBundle.generatedAt,
          "runtime_snapshot",
          requestTime,
        );
        const cachedVariant = variantDashboardStateCache.get(variantCacheKey);
        const variant = cachedVariant
          ? cachedVariant
          : await profiler.measure("variant_build", () =>
              buildDashboardVariant(baseState, query, requestTime),
            );

        if (!cachedVariant) {
          writeVariantDashboardStateCache(variantCacheKey, variant);
        }

        if (runtimeSnapshotStale) {
          void requestDashboardBackgroundRefresh("local_rebuild", "runtime-snapshot-stale", {
            queries: [query],
          });
        }

        const result = attachDataStatus(
          compactDashboardState(variant),
          buildDataStatus({
            stateSource: "runtime_snapshot",
            bundleOrigin: runtimeBundle.origin,
            runtimeSnapshotGeneratedAt: runtimeBundle.generatedAt,
            sourceSnapshotGeneratedAt: runtimeBundle.sourceSnapshotGeneratedAt,
            latestFetchedAt: runtimeBundle.latestFetchedAt,
            runtimeSnapshotAvailable: true,
            localRawDataAvailable,
            runtimeSnapshotStale,
            sourceFreshness: runtimeBundle.sourceFreshness,
            refresh: refreshState,
            timings: profiler.buildSummary(),
          }),
        );

        const timings = result.dataStatus?.timings ?? profiler.buildSummary();
        await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
          generatedAt: new Date().toISOString(),
          label: "dashboard-request",
          timings,
          metadata: {
            query,
            stateSource: "runtime_snapshot",
            bundleOrigin: runtimeBundle.origin,
            runtimeSnapshotGeneratedAt: runtimeBundle.generatedAt,
            runtimeSnapshotStale,
          },
        });
        console.info(
          formatDashboardProfileLog("request", timings, {
            stateSource: "runtime_snapshot",
            bundleOrigin: runtimeBundle.origin,
            stale: runtimeSnapshotStale,
            range: query.range,
            scope: query.scope,
          }),
        );
        return applyBlueskyReplayFreshness(result, runtimeBundleFreshness);
      }
    }

    if (localRawDataAvailable && (hasBlueskyRawData || persistedRuntimeRawAvailable)) {
      const { bundle } = await buildAndPersistDashboardRuntimeBundle(
        options.forceRebuild ? "startup_rebuild" : "local_rebuild",
        {
          profiler,
          refreshState,
          requestedQueries: [query],
          existingBundle: runtimeBundle,
        },
      );

      const baseState = getRuntimeBaseState(bundle, query) ?? createZeroTrendDashboardVM(query);
      const variant = await profiler.measure("variant_build", () =>
        buildDashboardVariant(baseState, query, new Date()),
      );
      const result = attachDataStatus(
        compactDashboardState(variant),
        buildDataStatus({
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          runtimeSnapshotGeneratedAt: bundle.generatedAt,
          sourceSnapshotGeneratedAt: bundle.sourceSnapshotGeneratedAt,
          latestFetchedAt: bundle.latestFetchedAt,
          runtimeSnapshotAvailable: true,
          localRawDataAvailable: true,
          runtimeSnapshotStale: false,
          sourceFreshness: bundle.sourceFreshness,
          refresh: refreshState,
          timings: profiler.buildSummary(),
        }),
      );

      const timings = result.dataStatus?.timings ?? profiler.buildSummary();
      await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
        generatedAt: new Date().toISOString(),
        label: "dashboard-request",
        timings,
        metadata: {
          query,
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          runtimeSnapshotGeneratedAt: bundle.generatedAt,
          bootstrapSkipped: true,
          hasBlueskyRawData,
        },
      });
      console.info(
        formatDashboardProfileLog("request", timings, {
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          range: query.range,
          scope: query.scope,
        }),
      );
      return applyBlueskyReplayFreshness(result, getBlueskyFirehoseFreshness(bundle, sourceManifest));
    }

    const bootstrappedBundle = await bootstrapRuntimeBundleFromLegacyCache();
    if (bootstrappedBundle) {
      const baseState =
        getRuntimeBaseState(bootstrappedBundle, query) ?? createZeroTrendDashboardVM(query);
      const variant = await profiler.measure("variant_build", () =>
        buildDashboardVariant(baseState, query, new Date()),
      );
      if (localRawDataAvailable) {
        void requestDashboardBackgroundRefresh("local_rebuild", "legacy-bootstrap", {
          queries: [query],
        });
      }

      const result = attachDataStatus(
        compactDashboardState(variant),
        buildDataStatus({
          stateSource: "runtime_snapshot",
          bundleOrigin: bootstrappedBundle.origin,
          runtimeSnapshotGeneratedAt: bootstrappedBundle.generatedAt,
          sourceSnapshotGeneratedAt: bootstrappedBundle.sourceSnapshotGeneratedAt,
          latestFetchedAt: bootstrappedBundle.latestFetchedAt,
          runtimeSnapshotAvailable: true,
          localRawDataAvailable,
          runtimeSnapshotStale: localRawDataAvailable,
          sourceFreshness: bootstrappedBundle.sourceFreshness,
          refresh: refreshState,
          timings: profiler.buildSummary(),
        }),
      );

      const timings = result.dataStatus?.timings ?? profiler.buildSummary();
      await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
        generatedAt: new Date().toISOString(),
        label: "dashboard-request",
        timings,
        metadata: {
          query,
          stateSource: "runtime_snapshot",
          bundleOrigin: bootstrappedBundle.origin,
          runtimeSnapshotGeneratedAt: bootstrappedBundle.generatedAt,
          runtimeSnapshotStale: localRawDataAvailable,
        },
      });
      console.info(
        formatDashboardProfileLog("request", timings, {
          stateSource: "runtime_snapshot",
          bundleOrigin: bootstrappedBundle.origin,
          stale: localRawDataAvailable,
          range: query.range,
          scope: query.scope,
        }),
      );
      return applyBlueskyReplayFreshness(result, getBlueskyFirehoseFreshness(bootstrappedBundle, sourceManifest));
    }

    if (localRawDataAvailable) {
      const { bundle } = await buildAndPersistDashboardRuntimeBundle(
        options.forceRebuild ? "startup_rebuild" : "local_rebuild",
        {
          profiler,
          refreshState,
          requestedQueries: [query],
          existingBundle: runtimeBundle,
        },
      );

      const baseState = getRuntimeBaseState(bundle, query) ?? createZeroTrendDashboardVM(query);
      const variant = await profiler.measure("variant_build", () =>
        buildDashboardVariant(baseState, query, new Date()),
      );
      const result = attachDataStatus(
        compactDashboardState(variant),
        buildDataStatus({
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          runtimeSnapshotGeneratedAt: bundle.generatedAt,
          sourceSnapshotGeneratedAt: bundle.sourceSnapshotGeneratedAt,
          latestFetchedAt: bundle.latestFetchedAt,
          runtimeSnapshotAvailable: true,
          localRawDataAvailable: true,
          runtimeSnapshotStale: false,
          sourceFreshness: bundle.sourceFreshness,
          refresh: refreshState,
          timings: profiler.buildSummary(),
        }),
      );

      const timings = result.dataStatus?.timings ?? profiler.buildSummary();
      await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
        generatedAt: new Date().toISOString(),
        label: "dashboard-request",
        timings,
        metadata: {
          query,
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          runtimeSnapshotGeneratedAt: bundle.generatedAt,
        },
      });
      console.info(
        formatDashboardProfileLog("request", timings, {
          stateSource: "local_raw_rebuild",
          bundleOrigin: bundle.origin,
          range: query.range,
          scope: query.scope,
        }),
      );
      return applyBlueskyReplayFreshness(result, getBlueskyFirehoseFreshness(bundle, sourceManifest));
    }

    const zeroState = attachDataStatus(
      createZeroTrendDashboardVM(query),
      buildDataStatus({
        stateSource: "zero_state",
        bundleOrigin: null,
        runtimeSnapshotGeneratedAt: null,
        sourceSnapshotGeneratedAt: null,
        latestFetchedAt: null,
        runtimeSnapshotAvailable: false,
        localRawDataAvailable: false,
        runtimeSnapshotStale: false,
        sourceFreshness: [],
        refresh: refreshState,
        timings: profiler.buildSummary(),
      }),
    );
    const timings = zeroState.dataStatus?.timings ?? profiler.buildSummary();
    await writeProfileSnapshot(RUNTIME_REQUEST_PROFILE_PATH, {
      generatedAt: new Date().toISOString(),
      label: "dashboard-request",
      timings,
      metadata: {
        query,
        stateSource: "zero_state",
      },
    });
    console.info(
      formatDashboardProfileLog("request", timings, {
        stateSource: "zero_state",
        range: query.range,
        scope: query.scope,
      }),
    );
    return zeroState;
  } catch (error) {
    console.error("[dashboard] failed to build Reddit-backed dashboard state", error);
    return attachDataStatus(createZeroTrendDashboardVM(query), {
      stateSource: "zero_state",
      bundleOrigin: null,
      showing: "zero_state",
      runtimeSnapshotGeneratedAt: null,
      sourceSnapshotGeneratedAt: null,
      latestFetchedAt: null,
      runtimeSnapshotAvailable: false,
      localRawDataAvailable: false,
      runtimeSnapshotStale: false,
      sourceFreshness: [],
      refresh: null,
      timings: profiler.buildSummary(),
    });
  }
}
