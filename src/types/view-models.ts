import {
  DateRangePreset,
  PlatformId,
  TimeSeriesPoint,
  TrendFreshnessState,
  TrendLifecycleStage,
  TrendScope,
} from "@/types/domain";
import { RedditIngestionHealth } from "@/lib/reddit/types";

export type AccentTone = "cyan" | "emerald" | "amber" | "rose" | "violet";

export type TrendLeaderboardMode = "emerging" | "established";

export type TrendGroupingSource =
  | "canonical_url_anchor"
  | "ai_semantic_cluster"
  | "template_anchor"
  | "singleton_ai"
  | "fallback_singleton";

export type TrendLeaderboardTier =
  | "primary_grouped"
  | "secondary_singleton"
  | "audit_low_information"
  | "audit_template"
  | "audit_fallback";

export type EstablishedTrendSort = "attention" | "growth" | "mentions" | "strength";

export type EmergingTrendSort = "breakout" | "velocity" | "novelty" | "confirmation";

export type TrendSort = EstablishedTrendSort | EmergingTrendSort;

export type TrendTopPost = {
  id: string;
  platformId: PlatformId;
  title: string;
  subtitle?: string | null;
  author?: string | null;
  authorHandle?: string | null;
  postType?: string | null;
  documentKind?: string | null;
  rootDocumentId?: string | null;
  interactionBreakdown?: string | null;
  engagement: number;
  engagementVelocity: number;
  ageMinutes: number;
  url: string;
};

export type TrendAttentionDriver = {
  platformId: PlatformId;
  contributionPct: number;
  deltaPct: number;
};

export type TrendAiEnrichment = {
  rawLabel: string;
  canonicalName: string;
  shortDescription: string;
  contextParagraph: string;
  keyEntities: string[];
  trendCategory: string | null;
  summaryConfidence: number;
  modelName: string | null;
  promptVersion: string | null;
  generatedAt: string | null;
  refreshedAt: string | null;
};

export type TrendPlatformBreakdown = Array<{
  platformId: PlatformId;
  interactions: number;
  sharePct: number;
}>;

export type TrendGoogleSearchInterest = {
  score: number;
  approxTrafficLabel: string | null;
  matchedQueries: string[];
  queryCount: number;
};

export type FirehoseNetworkNode = {
  id: string;
  label: string;
  x: number;
  y: number;
  size: number;
  color: [number, number, number];
};

export type FirehoseNetworkEdge = {
  source: [number, number];
  target: [number, number];
  width: number;
  color: [number, number, number];
};

export type TimeSeriesWindow = {
  range: DateRangePreset;
  windowStart: string;
  windowEnd: string;
  latestPointAt: string | null;
  latestDataAt: string | null;
  staleGapMinutes: number | null;
  trailingGapBucketCount: number;
  hasTrailingGap: boolean;
  bucketIntervalMinutes: number;
};

export type TrendBlueskySummary = {
  attentionSharePct: number;
  postCount: number;
  uniqueAuthorCount: number;
  amplifierCount: number;
  topAmplifierHandle: string | null;
  topAmplifier?: string | null;
  repostVelocity: number;
  replyVelocity: number;
  quoteVelocity: number;
  likeVelocity?: number;
  amplificationScore?: number;
  engagementIntensity?: number;
  accountSpread?: number;
  cascadeCount?: number;
  accelerationScore?: number;
  narrativeCount?: number;
  postsPerMinute?: number;
  repostsPerMinute?: number;
  repliesPerMinute?: number;
  quotesPerMinute?: number;
  likesPerMinute?: number;
  meaningfulAttentionScore?: number;
  noiseRatioPct?: number;
  leadingSignalLabel: string;
  firehoseLagMinutes: number | null;
};

export type TrendBlueskyAmplifier = {
  did?: string | null;
  handle: string;
  displayName?: string | null;
  followersCount?: number | null;
  interactions: number;
  documents: number;
  contributionPct: number;
};

export type TrendBlueskyCascade = {
  rootDocumentId: string;
  title: string;
  authorHandle: string | null;
  interactions: number;
  velocity: number;
  sharePct: number;
  uniqueParticipants: number;
};

export type TrendBlueskyDetail = {
  summary: TrendBlueskySummary;
  topAmplifiers: TrendBlueskyAmplifier[];
  cascadeLeaders?: TrendBlueskyCascade[];
  engagementBreakdown?: Array<{
    label: string;
    count: number;
    velocityPerHour: number;
  }>;
  postTypeMix: Array<{
    type: string;
    count: number;
    sharePct: number;
  }>;
  accountSpreadLabel: string;
  propagationSummary: string;
  noiseSummary?: string | null;
  meaningfulAttentionScore?: number | null;
  network?: {
    nodes: FirehoseNetworkNode[];
    edges: FirehoseNetworkEdge[];
  };
  earliestOriginAt: string | null;
  earliestOriginHandle: string | null;
};

export type BlueskyFirehoseLeader = {
  id: string;
  label: string;
  trendId?: string | null;
  attentionScore: number;
  velocity: number;
  accelerationScore: number;
  attentionSharePct: number;
  uniqueAuthors: number;
  amplificationScore: number;
  topAmplifierHandle: string | null;
};

export type BlueskyFirehoseCluster = {
  id: string;
  label: string;
  narratives: number;
  attentionScore: number;
  velocity: number;
  uniqueAuthors: number;
  sharePct: number;
};

export type BlueskyFirehoseOverview = {
  generatedAt: string | null;
  firehoseLagMinutes: number | null;
  attentionSharePct: number;
  engagementIntensity: number;
  meaningfulAttentionScore: number;
  narrativeCount: number;
  accountSpread: number;
  postsPerMinute: number;
  likesPerMinute: number;
  repostsPerMinute: number;
  repliesPerMinute: number;
  quotesPerMinute: number;
  accelerationScore: number;
  noiseRatioPct: number;
  leaders: BlueskyFirehoseLeader[];
  emerging: BlueskyFirehoseLeader[];
  topAmplifiers: TrendBlueskyAmplifier[];
  cascades: TrendBlueskyCascade[];
  clusters: BlueskyFirehoseCluster[];
  network: {
    nodes: FirehoseNetworkNode[];
    edges: FirehoseNetworkEdge[];
  };
  replay: TimeSeriesPoint[];
  replayWindow?: TimeSeriesWindow | null;
};

export type RankedTrend = {
  id: string;
  rank: number;
  name: string;
  trendDescription?: string | null;
  trendContextParagraph?: string | null;
  trendRawLabel?: string | null;
  trendSummaryConfidence?: number | null;
  trendKeyEntities?: string[] | null;
  trendEnrichment?: TrendAiEnrichment | null;
  scope: TrendScope;
  source?: "bluesky" | "mixed";
  labelType?: TrendLabelType;
  groupingSource?: TrendGroupingSource;
  leaderboardTier?: TrendLeaderboardTier;
  canonicalKeySummary?: string | null;
  labelQualityScore?: number;
  lowQualityLabel?: boolean;
  aiAssisted?: boolean;
  leaderboardMode: TrendLeaderboardMode;
  attentionScore: number;
  emergingScore?: number;
  breakoutScore?: number;
  velocityScore?: number;
  noveltyScore?: number;
  confirmationScore?: number;
  totalInteractions24h?: number;
  qualityAdjustedScore?: number;
  attentionInteractions: number;
  rootsCount24h?: number;
  uniqueAuthors24h?: number;
  positiveMentions24h?: number;
  neutralMentions24h?: number;
  negativeMentions24h?: number;
  sentimentBalance?: number;
  singleAuthorShare?: number;
  firstSeenAt?: string | null;
  lastSeenAt?: string | null;
  isSingleton?: boolean;
  trendCategory?: string | null;
  contentType?: string | null;
  spamLikelihood?: number;
  templateLikelihood?: number;
  contextualCoherence?: number;
  lowInformation?: boolean;
  templateSeries?: boolean;
  confidenceScore: number;
  freshnessScore: number;
  freshnessState: TrendFreshnessState;
  sampleSize: number;
  supportingThreadCount: number;
  lowDataWarning: boolean;
  growthRate: number;
  attentionAcceleration: number;
  mentions: number;
  platforms: PlatformId[];
  platformSpread: number;
  confirmedPlatformSpread: number;
  attentionHistory: TimeSeriesPoint[];
  platformBreakdown: TrendPlatformBreakdown;
  topPosts: TrendTopPost[];
  lifecycleStage: TrendLifecycleStage;
  originPlatform: PlatformId;
  platformMigrationPath: PlatformId[];
  attentionDrivers: TrendAttentionDriver[];
  hasSpike: boolean;
  spikeMagnitude?: number;
  clusterId: string;
  clusterName: string;
  trendStrengthScore: number;
  persistenceScore: number;
  isEarlyTrend: boolean;
  positionChange24h: number;
  googleSearchInterest: TrendGoogleSearchInterest | null;
  blueskySummary?: TrendBlueskySummary | null;
  blueskyDetail?: TrendBlueskyDetail | null;
};

export type TrendLabelType =
  | "canonical_url_title"
  | "entity_label"
  | "repeated_phrase"
  | "hashtag_label"
  | "cleaned_singleton_text"
  | "ai_generated"
  | "fallback_generated";

export type FastestGrowingTrend = {
  id: string;
  name: string;
  growthRate: number;
  attentionAcceleration: number;
  attentionScore: number;
  emergingScore: number;
  attentionInteractions: number;
  confidenceScore: number;
  lowDataWarning: boolean;
  platformSpread: number;
  supportingThreadCount: number;
  lifecycleStage: TrendLifecycleStage;
  hasSpike: boolean;
  deltaLabel: string;
  positionChange24h: number;
  blueskySummary?: TrendBlueskySummary | null;
};

export type RelatedTrend = {
  id: string;
  name: string;
  attentionScore: number;
  attentionInteractions: number;
  growthRate: number;
  lifecycleStage: TrendLifecycleStage;
};

export type TrendDetailVM = {
  trend: RankedTrend;
  attentionGraph: TimeSeriesPoint[];
  attentionWindow: TimeSeriesWindow | null;
  platformBreakdown: TrendPlatformBreakdown;
  topPosts: TrendTopPost[];
  relatedTrends: RelatedTrend[];
  blueskyDetail?: TrendBlueskyDetail | null;
};

export type TrendDashboardQuery = {
  scope: TrendScope;
  range: DateRangePreset;
  sort: TrendSort;
  mode?: TrendLeaderboardMode;
  selectedId?: string;
  selectedKey?: string;
};

export type DashboardRuntimeSource =
  | "supabase_live"
  | "runtime_snapshot"
  | "local_raw_rebuild"
  | "zero_state";

export type DashboardRuntimeBundleOrigin =
  | "startup_rebuild"
  | "local_rebuild"
  | "background_refresh"
  | "manual_full_regroup"
  | "legacy_bootstrap";

export type DashboardRefreshMode =
  | "local_rebuild"
  | "manual_full_regroup"
  | "external_refresh";

export type DashboardRefreshStatus =
  | "idle"
  | "scheduled"
  | "running"
  | "succeeded"
  | "failed";

export type DashboardRefreshState = {
  status: DashboardRefreshStatus;
  mode: DashboardRefreshMode;
  trigger: string;
  requestedAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  lastError: string | null;
  latestBundleGeneratedAt?: string | null;
  latestSourceSnapshotGeneratedAt?: string | null;
  ownerPid?: number | null;
  ownerSessionId?: string | null;
  staleClearedAt?: string | null;
  staleReason?: string | null;
};

export type DashboardSourceFreshness = {
  sourceId: string;
  sourceLabel: string;
  platformId: PlatformId;
  sourceStatus?: string | null;
  itemCount: number;
  lastFetchedAt: string | null;
  latestCreatedAt: string | null;
  ageMinutes: number | null;
};

export type DashboardTimingEntry = {
  name: string;
  durationMs: number;
  count?: number;
};

export type DashboardRequestTimings = {
  totalMs: number;
  runtimeSnapshotReadMs: number;
  sourceManifestReadMs: number;
  localRawReadMs: number;
  analyticsBuildMs: number;
  variantBuildMs: number;
  sqliteWriteMs: number;
  steps: DashboardTimingEntry[];
  perQueryBuilds: DashboardTimingEntry[];
};

export type DashboardFreshnessDiagnostics = {
  latestIngestionAt: string | null;
  latestProcessedAt: string | null;
  latestMentionEventAt: string | null;
  latestReadModelFinalizeAt: string | null;
  latestReadModelRollingWriteAt: string | null;
  latestReadModelSeriesWriteAt: string | null;
  latestReadModelWindowEndAt: string | null;
  latestSeriesNonZeroBucketAt: string | null;
  workerRunStartedAt: string | null;
  workerRunStatus: string | null;
  workerLastEventAt: string | null;
  workerRowsInserted: number | null;
  apiResponseAt: string | null;
  sourceSnapshotAt: string | null;
  selectedTrendLatestDataAt: string | null;
  selectedTrendLatestPointAt: string | null;
  renderedStaleReferenceAt: string | null;
  renderedStaleReferenceSource: "source_snapshot" | "latest_point" | "latest_data" | null;
  chainBreakStage:
    | "ingestion"
    | "processing"
    | "read_model_refresh"
    | "api_cache"
    | "render_stale_field"
    | "none"
    | null;
  agesMinutes: {
    ingestion: number | null;
    processed: number | null;
    mentionEvent: number | null;
    readModelFinalize: number | null;
    readModelWrite: number | null;
    readModelWindowEnd: number | null;
    workerLastEvent: number | null;
    sourceSnapshot: number | null;
    selectedLatestPoint: number | null;
    selectedLatestData: number | null;
    renderedStaleReference: number | null;
  };
};

export type DashboardDataStatus = {
  stateSource: DashboardRuntimeSource;
  bundleOrigin: DashboardRuntimeBundleOrigin | null;
  showing: "supabase_live" | "cached_local" | "fresh_local_rebuild" | "zero_state";
  serverNow?: string | null;
  responseVersion?: string | null;
  runtimeSnapshotGeneratedAt: string | null;
  sourceSnapshotGeneratedAt: string | null;
  latestFetchedAt: string | null;
  runtimeSnapshotAvailable: boolean;
  localRawDataAvailable: boolean;
  runtimeSnapshotStale: boolean;
  sourceFreshness: DashboardSourceFreshness[];
  freshnessDiagnostics?: DashboardFreshnessDiagnostics | null;
  refresh: DashboardRefreshState | null;
  timings: DashboardRequestTimings | null;
};

export type TrendCoverageDebug = {
  source: "bluesky";
  rankingSource: "ai_grouped_clusters";
  windowHours: number;
  totalInteractionsInWindow: number;
  totalInteractionsAssignedToTrends: number;
  unassignedInteractionsCount: number;
  totalRootsInWindow: number;
  eligibleRootsCount: number;
  assignedRootsCount: number;
  unassignedRootCount: number;
  nonAiAssignedRootCount: number;
  groupedRootsCount: number;
  singletonRootsCount: number;
  totalTrendsReturned: number;
  groupedTrendCount: number;
  singletonTrendCount: number;
  urlAnchorGroupCount: number;
  aiClusterGroupCount: number;
  templateSeriesCount: number;
  lowInformationTrendCount: number;
  fallbackLabelCount: number;
  lowQualityLabelCount: number;
  aiLabeledCount: number;
  aiAttempted: boolean;
  aiClientInitialized: boolean;
  aiCredentialSource: string | null;
  aiCredentialFingerprint: string | null;
  aiModel: string | null;
  aiProcessedRootCount: number;
  aiProcessedFreshRootCount: number;
  aiCacheHitCount: number;
  aiFreshCallCount: number;
  aiBatchCount: number;
  aiBatchFailureCount: number;
  aiFailedRootCount: number;
  aiAssignmentCoveragePct: number;
  aiIncomplete: boolean;
  aiIncompleteReason: string | null;
  groupingRunMode: "incremental_live" | "manual_full_regroup";
  lastAiGroupingRunAt: string | null;
  lastSuccessfulFullRegroupAt: string | null;
  leaderboardSource: "ai_grouped_clusters";
  displayedRowsCount: number;
  displayedAiGroupedRowsCount: number;
  displayedSingletonRowsCount: number;
  displayedFallbackRowsCount: number;
  displayedLowInformationRowsCount: number;
  latestInteractionAt: string | null;
  firehoseLagMinutes: number | null;
};

export type TrendDashboardVM = {
  query: TrendDashboardQuery;
  ingestionHealth: RedditIngestionHealth | null;
  dataStatus?: DashboardDataStatus | null;
  blueskyOverview?: BlueskyFirehoseOverview | null;
  trendCoverage?: TrendCoverageDebug | null;
  leaderboards: Record<TrendLeaderboardMode, RankedTrend[]>;
  leaderboard: RankedTrend[];
  overviewSeries: Array<{
    id: string;
    name: string;
    selected: boolean;
    color: string;
    points: TimeSeriesPoint[];
    window: TimeSeriesWindow | null;
  }>;
  detail: TrendDetailVM | null;
};
