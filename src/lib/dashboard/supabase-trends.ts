import "server-only";

import { PostgrestError } from "@supabase/supabase-js";
import { Pool } from "pg";
import { getServerPostgresPool, hasDatabaseUrl } from "@/lib/db/server-postgres";
import { applyTrendDashboardSelection } from "@/lib/dashboard/selection";
import { createZeroRankedTrend, createZeroTrendDashboardVM } from "@/lib/dashboard/zero-state";
import { getSupabaseServerClient, hasSupabaseServerCredentials } from "@/lib/supabase/server";
import { DateRangePreset, PlatformId, TimeSeriesPoint, TrendFreshnessState, TrendLifecycleStage } from "@/types/domain";
import { DashboardDataStatus, RankedTrend, TimeSeriesWindow, TrendDashboardQuery, TrendDashboardVM, TrendLeaderboardMode, TrendSort } from "@/types/view-models";

type SupabaseTopicRow = Record<string, unknown>;

type ParsedTopicBucketRow = {
  bucketMinute: string;
  platform: PlatformId;
  topicText: string;
  normalizedTopic: string;
  topicType: string;
  mentionCount: number;
  postCount: number;
  positiveCount: number;
  neutralCount: number;
  negativeCount: number;
  tags: string[];
};

type TopicAggregate = {
  topicText: string;
  normalizedTopic: string;
  mentionCount: number;
  postCount: number;
  positiveCount: number;
  neutralCount: number;
  negativeCount: number;
  platforms: Map<PlatformId, number>;
  bucketCounts: Map<string, number>;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
};

type RangeConfig = {
  points: number;
  bucketMinutes: number;
};

const RANGE_CONFIG: Record<DateRangePreset, RangeConfig> = {
  "1h": { points: 12, bucketMinutes: 5 },
  "6h": { points: 72, bucketMinutes: 5 },
  "24h": { points: 288, bucketMinutes: 5 },
  "7d": { points: 336, bucketMinutes: 30 },
};

const TOPIC_SOURCE_VIEW = "v_topic_trends_1m";
const TOPIC_SOURCE_TABLE = "topic_buckets_1m";
const MAX_QUERY_ROWS = 50_000;
const PAGE_SIZE = 1_000;
const MAX_LEADERBOARD_ROWS = 250;

const MEME_SCOPE_TERMS = [
  "meme",
  "memecoin",
  "token",
  "coin",
  "crypto",
  "solana",
  "ethereum",
  "bitcoin",
  "doge",
  "pepe",
  "nft",
  "cashtag",
  "pump",
  "bullish",
  "bearish",
];

const PLATFORM_MAP: Record<string, PlatformId> = {
  bluesky: "bluesky",
  x: "x",
  twitter: "x",
  reddit: "reddit",
  telegram: "telegram",
  youtube: "youtube",
  tiktok: "tiktok",
  google: "google",
  news: "news",
};

function readBooleanEnv(value: string | undefined, fallback: boolean) {
  if (!value) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function readStringValue(value: unknown) {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
}

function pickFirstString(row: SupabaseTopicRow, keys: string[]) {
  for (const key of keys) {
    const value = readStringValue(row[key]);
    if (value.length > 0) {
      return value;
    }
  }
  return "";
}

function pickFirstNumber(row: SupabaseTopicRow, keys: string[], fallback = 0) {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return fallback;
}

function pickStringArray(row: SupabaseTopicRow, keys: string[]) {
  for (const key of keys) {
    const value = row[key];
    if (Array.isArray(value)) {
      return value
        .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
        .filter((entry) => entry.length > 0);
    }
  }

  return [];
}

function normalizeTopic(topic: string) {
  const normalized = topic
    .replace(/\s+/g, " ")
    .replace(/^[\s\p{P}]+|[\s\p{P}]+$/gu, "")
    .trim();

  if (!normalized) {
    return "";
  }

  const isUppercaseAcronym = /^[A-Z0-9$#]{2,10}$/.test(normalized);
  if (isUppercaseAcronym) {
    return normalized;
  }

  return normalized
    .split(" ")
    .map((part) => {
      if (part.length <= 1) {
        return part.toUpperCase();
      }
      const lower = part.toLowerCase();
      return `${lower[0].toUpperCase()}${lower.slice(1)}`;
    })
    .join(" ");
}

function normalizePlatform(value: unknown): PlatformId {
  const normalized = readStringValue(value).toLowerCase();
  if (normalized in PLATFORM_MAP) {
    return PLATFORM_MAP[normalized];
  }

  return "bluesky";
}

function toIsoMinute(value: unknown) {
  let timestamp = Number.NaN;
  if (value instanceof Date) {
    timestamp = value.getTime();
  } else if (typeof value === "number" && Number.isFinite(value)) {
    timestamp = value;
  } else {
    const raw = readStringValue(value);
    if (raw) {
      timestamp = Date.parse(raw);
    }
  }

  if (!Number.isFinite(timestamp)) {
    return "";
  }

  const date = new Date(timestamp);
  date.setSeconds(0, 0);
  return date.toISOString();
}

function floorToBucket(date: Date, bucketMinutes: number) {
  const ms = date.getTime();
  const bucketMs = bucketMinutes * 60_000;
  return new Date(Math.floor(ms / bucketMs) * bucketMs);
}

function bucketKeyForIso(isoTimestamp: string, bucketMinutes: number) {
  const timestamp = Date.parse(isoTimestamp);
  if (!Number.isFinite(timestamp)) {
    return "";
  }

  const bucketMs = bucketMinutes * 60_000;
  return new Date(Math.floor(timestamp / bucketMs) * bucketMs).toISOString();
}

function buildWindowBuckets(range: DateRangePreset, now = new Date()) {
  const config = RANGE_CONFIG[range];
  const bucketMs = config.bucketMinutes * 60_000;
  const windowEnd = floorToBucket(now, config.bucketMinutes);
  const windowStart = new Date(windowEnd.getTime() - (config.points - 1) * bucketMs);

  const buckets = Array.from({ length: config.points }, (_, index) => {
    return new Date(windowStart.getTime() + index * bucketMs).toISOString();
  });

  return {
    bucketMinutes: config.bucketMinutes,
    bucketMs,
    windowStart,
    windowEnd,
    buckets,
  };
}

function isSupabaseMissingRelation(error: PostgrestError | null) {
  if (!error) {
    return false;
  }

  const message = `${error.message} ${error.details ?? ""} ${error.hint ?? ""}`.toLowerCase();
  return (
    message.includes("does not exist") ||
    message.includes("relation") ||
    error.code === "42P01"
  );
}

function isPostgresMissingRelation(error: unknown) {
  const databaseError = error as { code?: string; message?: string };
  const message = String(databaseError?.message ?? "").toLowerCase();
  return databaseError?.code === "42P01" || message.includes("does not exist");
}

async function fetchRowsWithBucketColumn(params: {
  source: string;
  bucketColumn: string;
  windowStartIso: string;
  windowEndIso: string;
}) {
  const client = getSupabaseServerClient();
  const rows: SupabaseTopicRow[] = [];

  for (let offset = 0; offset < MAX_QUERY_ROWS; offset += PAGE_SIZE) {
    const end = offset + PAGE_SIZE - 1;
    const { data, error } = await client
      .from(params.source)
      .select("*")
      .gte(params.bucketColumn, params.windowStartIso)
      .lte(params.bucketColumn, params.windowEndIso)
      .order(params.bucketColumn, { ascending: true })
      .range(offset, end);

    if (error) {
      throw error;
    }

    if (!data || data.length === 0) {
      break;
    }

    rows.push(...data);

    if (data.length < PAGE_SIZE) {
      break;
    }
  }

  return rows;
}

async function fetchTopicRowsFromSupabaseSource(
  source: string,
  windowStartIso: string,
  windowEndIso: string,
) {
  const bucketColumns = ["bucket_minute", "bucket_start", "minute_bucket"];
  const attemptErrors: PostgrestError[] = [];

  for (const bucketColumn of bucketColumns) {
    try {
      return await fetchRowsWithBucketColumn({
        source,
        bucketColumn,
        windowStartIso,
        windowEndIso,
      });
    } catch (error) {
      const pgError = error as PostgrestError;
      attemptErrors.push(pgError);
      if (!isSupabaseMissingRelation(pgError)) {
        throw error;
      }
    }
  }

  throw attemptErrors.at(-1) ?? new Error(`Unable to query ${source}`);
}

async function resolveBucketColumnFromPostgres(pool: Pool, source: string) {
  const columnQuery = await pool.query<{
    column_name: string;
  }>(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
    `,
    [source],
  );

  if (columnQuery.rows.length === 0) {
    const error = new Error(`Relation public.${source} not found`);
    (error as Error & { code?: string }).code = "42P01";
    throw error;
  }

  const columns = new Set(columnQuery.rows.map((row) => row.column_name));
  const candidates = ["bucket_minute", "bucket_start", "minute_bucket", "created_at"];
  const resolved = candidates.find((name) => columns.has(name));
  if (!resolved) {
    throw new Error(`No supported bucket column found on public.${source}`);
  }

  return resolved;
}

async function fetchTopicRowsFromPostgresSource(
  source: string,
  windowStartIso: string,
  windowEndIso: string,
) {
  const pool = getServerPostgresPool();
  const bucketColumn = await resolveBucketColumnFromPostgres(pool, source);
  const query = `
    SELECT *
    FROM public.${source}
    WHERE ${bucketColumn} >= $1::timestamptz
      AND ${bucketColumn} <= $2::timestamptz
    ORDER BY ${bucketColumn} ASC
    LIMIT $3
  `;
  const result = await pool.query<Record<string, unknown>>(query, [
    windowStartIso,
    windowEndIso,
    MAX_QUERY_ROWS,
  ]);
  return result.rows as SupabaseTopicRow[];
}

async function fetchTopicRows(windowStartIso: string, windowEndIso: string) {
  if (hasSupabaseServerCredentials()) {
    try {
      const rows = await fetchTopicRowsFromSupabaseSource(
        TOPIC_SOURCE_VIEW,
        windowStartIso,
        windowEndIso,
      );
      return {
        rows,
        source: TOPIC_SOURCE_VIEW,
      };
    } catch (error) {
      const pgError = error as PostgrestError;
      if (!isSupabaseMissingRelation(pgError)) {
        throw error;
      }
    }

    const rows = await fetchTopicRowsFromSupabaseSource(
      TOPIC_SOURCE_TABLE,
      windowStartIso,
      windowEndIso,
    );
    return {
      rows,
      source: TOPIC_SOURCE_TABLE,
    };
  }

  if (hasDatabaseUrl()) {
    try {
      const rows = await fetchTopicRowsFromPostgresSource(
        TOPIC_SOURCE_VIEW,
        windowStartIso,
        windowEndIso,
      );
      return {
        rows,
        source: TOPIC_SOURCE_VIEW,
      };
    } catch (error) {
      if (!isPostgresMissingRelation(error)) {
        throw error;
      }
    }

    const rows = await fetchTopicRowsFromPostgresSource(
      TOPIC_SOURCE_TABLE,
      windowStartIso,
      windowEndIso,
    );
    return {
      rows,
      source: TOPIC_SOURCE_TABLE,
    };
  }

  throw new Error(
    "No Supabase trend query credentials available. Set SUPABASE_SERVICE_ROLE_KEY (+ SUPABASE_URL) or DATABASE_URL.",
  );
}

function parseTopicRow(row: SupabaseTopicRow): ParsedTopicBucketRow | null {
  const topicText = pickFirstString(row, [
    "topic_text",
    "topic",
    "normalized_topic",
    "topic_key_candidate",
    "trend_name",
    "name",
  ]);
  const normalizedTopic = normalizeTopic(
    pickFirstString(row, ["normalized_topic", "topic", "topic_key_candidate"]) || topicText,
  );
  const bucketMinute = toIsoMinute(
    row.bucket_minute ?? row.bucket_start ?? row.minute_bucket ?? row.created_at,
  );
  const platform = normalizePlatform(row.platform);
  const topicType = pickFirstString(row, ["topic_type"]) || "entity";

  if (!topicText || !normalizedTopic || !bucketMinute) {
    return null;
  }

  const mentionCount = Math.max(
    0,
    pickFirstNumber(row, [
      "mention_count",
      "mentions",
      "topic_mentions",
      "topic_count",
      "count",
      "total_mentions",
      "count_mentions",
    ], 0),
  );
  const postCount = Math.max(
    0,
    pickFirstNumber(row, ["post_count", "posts", "root_count", "unique_posts", "count_posts"], 0),
  );

  let positiveCount = Math.max(
    0,
    pickFirstNumber(row, [
      "positive_count",
      "sentiment_positive_count",
      "positive_mentions",
      "sentiment_positive",
    ], 0),
  );
  let neutralCount = Math.max(
    0,
    pickFirstNumber(row, [
      "neutral_count",
      "sentiment_neutral_count",
      "neutral_mentions",
      "sentiment_neutral",
    ], 0),
  );
  let negativeCount = Math.max(
    0,
    pickFirstNumber(row, [
      "negative_count",
      "sentiment_negative_count",
      "negative_mentions",
      "sentiment_negative",
    ], 0),
  );

  const rowSentimentLabel = pickFirstString(row, ["sentiment_label"]).toLowerCase();
  const effectiveMentions = mentionCount > 0 ? mentionCount : Math.max(1, postCount);
  if (positiveCount + neutralCount + negativeCount === 0 && rowSentimentLabel) {
    if (rowSentimentLabel === "positive") {
      positiveCount = effectiveMentions;
    } else if (rowSentimentLabel === "negative") {
      negativeCount = effectiveMentions;
    } else {
      neutralCount = effectiveMentions;
    }
  }

  return {
    bucketMinute,
    platform,
    topicText,
    normalizedTopic,
    topicType,
    mentionCount: effectiveMentions,
    postCount: Math.max(postCount, effectiveMentions > 0 ? 1 : 0),
    positiveCount,
    neutralCount,
    negativeCount,
    tags: pickStringArray(row, ["tags", "topic_tags"]),
  };
}

function matchesScope(row: ParsedTopicBucketRow, scope: TrendDashboardQuery["scope"]) {
  if (scope !== "memes") {
    return true;
  }

  const haystack = `${row.topicText} ${row.normalizedTopic} ${row.tags.join(" ")}`.toLowerCase();
  return MEME_SCOPE_TERMS.some((term) => haystack.includes(term));
}

function buildSlug(value: string) {
  const slug = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  if (slug.length > 0) {
    return slug;
  }

  const fallback = Array.from(value)
    .map((char) => char.charCodeAt(0).toString(16))
    .join("")
    .slice(0, 16);
  return `topic-${fallback || "unknown"}`;
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function percentDelta(current: number, previous: number) {
  if (previous <= 0) {
    return current > 0 ? 100 : 0;
  }
  return ((current - previous) / previous) * 100;
}

function getFreshnessState(ageMinutes: number): TrendFreshnessState {
  if (ageMinutes <= 12) {
    return "fresh";
  }
  if (ageMinutes <= 30) {
    return "mixed";
  }
  if (ageMinutes <= 90) {
    return "delayed";
  }
  return "stale";
}

function getLifecycleStage(growthRate: number, freshnessState: TrendFreshnessState): TrendLifecycleStage {
  if (freshnessState === "stale" && growthRate < -20) {
    return "Declining";
  }
  if (growthRate >= 60) {
    return "Emerging";
  }
  if (growthRate >= 20) {
    return "Expanding";
  }
  if (growthRate <= -35) {
    return "Fading";
  }
  return "Established";
}

function sortRowsByMode(rows: RankedTrend[], mode: TrendLeaderboardMode, sort: TrendSort) {
  const sortKey = mode === "emerging" ? sort : sort;
  const sorted = [...rows];
  const interactionCount = (row: RankedTrend) =>
    row.totalInteractions24h ?? row.mentions ?? row.attentionInteractions ?? 0;

  sorted.sort((left, right) => {
    if (mode === "emerging") {
      if (sortKey === "velocity") {
        return (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          right.attentionInteractions - left.attentionInteractions;
      }
      if (sortKey === "novelty") {
        return (right.noveltyScore ?? 0) - (left.noveltyScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          right.attentionInteractions - left.attentionInteractions;
      }
      if (sortKey === "confirmation") {
        return (right.confirmationScore ?? 0) - (left.confirmationScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          right.attentionInteractions - left.attentionInteractions;
      }

      return (right.breakoutScore ?? right.emergingScore ?? 0) -
        (left.breakoutScore ?? left.emergingScore ?? 0) ||
        (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
        right.attentionInteractions - left.attentionInteractions;
    }

    const interactionDelta = interactionCount(right) - interactionCount(left);
    if (interactionDelta !== 0) {
      return interactionDelta;
    }

    if (sortKey === "growth") {
      return right.growthRate - left.growthRate ||
        right.attentionInteractions - left.attentionInteractions;
    }

    if (sortKey === "mentions") {
      return right.mentions - left.mentions ||
        right.attentionInteractions - left.attentionInteractions;
    }

    if (sortKey === "strength") {
      return right.trendStrengthScore - left.trendStrengthScore ||
        right.attentionInteractions - left.attentionInteractions;
    }

    return right.attentionScore - left.attentionScore ||
      right.attentionInteractions - left.attentionInteractions;
  });

  return sorted;
}

function buildWindow(points: TimeSeriesPoint[], range: DateRangePreset, bucketMinutes: number): TimeSeriesWindow | null {
  if (points.length === 0) {
    return null;
  }

  const windowStart = points[0]?.timestamp;
  const windowEnd = points.at(-1)?.timestamp;
  if (!windowStart || !windowEnd) {
    return null;
  }

  const latestDataPoint = [...points].reverse().find((point) => point.value > 0) ?? null;
  const endMs = Date.parse(windowEnd);
  const latestDataMs = latestDataPoint ? Date.parse(latestDataPoint.timestamp) : Number.NaN;
  const staleGapMinutes =
    Number.isFinite(endMs) && Number.isFinite(latestDataMs)
      ? Math.max(0, Math.round((endMs - latestDataMs) / 60_000))
      : null;
  const trailingGapBucketCount =
    staleGapMinutes !== null ? Math.max(0, Math.floor(staleGapMinutes / bucketMinutes)) : 0;

  return {
    range,
    windowStart,
    windowEnd,
    latestPointAt: latestDataPoint?.timestamp ?? null,
    latestDataAt: latestDataPoint?.timestamp ?? null,
    staleGapMinutes,
    trailingGapBucketCount,
    hasTrailingGap: trailingGapBucketCount > 0,
    bucketIntervalMinutes: bucketMinutes,
  };
}

function toSupabaseDataStatus(params: {
  rowCount: number;
  source: string;
  latestBucketAt: string | null;
}): DashboardDataStatus {
  const nowIso = new Date().toISOString();
  const latestBucketMs = params.latestBucketAt ? Date.parse(params.latestBucketAt) : Number.NaN;
  const ageMinutes =
    Number.isFinite(latestBucketMs)
      ? Math.max(0, Math.round((Date.now() - latestBucketMs) / 60_000))
      : null;

  return {
    stateSource: "supabase_live",
    bundleOrigin: null,
    showing: "supabase_live",
    serverNow: nowIso,
    runtimeSnapshotGeneratedAt: null,
    sourceSnapshotGeneratedAt: params.latestBucketAt,
    latestFetchedAt: nowIso,
    runtimeSnapshotAvailable: false,
    localRawDataAvailable: params.rowCount > 0,
    runtimeSnapshotStale: false,
    sourceFreshness: [
      {
        sourceId: `supabase:${params.source}`,
        sourceLabel: params.source,
        platformId: "bluesky",
        sourceStatus: params.rowCount > 0 ? "fresh" : "empty",
        itemCount: params.rowCount,
        lastFetchedAt: nowIso,
        latestCreatedAt: params.latestBucketAt,
        ageMinutes,
      },
    ],
    refresh: null,
    timings: null,
  };
}

function attachSeriesWindows(vm: TrendDashboardVM, range: DateRangePreset, bucketMinutes: number) {
  const withOverviewWindows = vm.overviewSeries.map((series) => ({
    ...series,
    window: buildWindow(series.points, range, bucketMinutes),
  }));

  const detail = vm.detail
    ? {
        ...vm.detail,
        attentionWindow: buildWindow(vm.detail.attentionGraph, range, bucketMinutes),
      }
    : null;

  return {
    ...vm,
    overviewSeries: withOverviewWindows,
    detail,
  };
}

function buildBlueskyOverview(
  rows: RankedTrend[],
  allPoints: TimeSeriesPoint[],
  range: DateRangePreset,
  bucketMinutes: number,
): TrendDashboardVM["blueskyOverview"] {
  const totalInteractions = rows.reduce(
    (sum, row) => sum + Math.max(0, row.attentionInteractions),
    0,
  );
  const replayWindow = buildWindow(allPoints, range, bucketMinutes);

  return {
    generatedAt: new Date().toISOString(),
    firehoseLagMinutes: replayWindow?.staleGapMinutes ?? null,
    attentionSharePct: 100,
    engagementIntensity: totalInteractions,
    meaningfulAttentionScore: totalInteractions,
    narrativeCount: rows.length,
    accountSpread: rows.reduce((sum, row) => sum + (row.uniqueAuthors24h ?? 0), 0),
    postsPerMinute: bucketMinutes > 0 ? totalInteractions / (bucketMinutes * Math.max(1, allPoints.length)) : 0,
    likesPerMinute: 0,
    repostsPerMinute: 0,
    repliesPerMinute: 0,
    quotesPerMinute: 0,
    accelerationScore: rows.length > 0
      ? rows.reduce((sum, row) => sum + row.attentionAcceleration, 0) / rows.length
      : 0,
    noiseRatioPct: 0,
    leaders: rows.slice(0, 8).map((row) => ({
      id: row.id,
      label: row.name,
      trendId: row.id,
      attentionScore: row.attentionScore,
      velocity: row.velocityScore ?? 0,
      accelerationScore: row.attentionAcceleration,
      attentionSharePct: totalInteractions > 0
        ? (row.attentionInteractions / totalInteractions) * 100
        : 0,
      uniqueAuthors: row.uniqueAuthors24h ?? 0,
      amplificationScore: row.breakoutScore ?? row.emergingScore ?? 0,
      topAmplifierHandle: null,
    })),
    emerging: rows
      .filter((row) => (row.breakoutScore ?? 0) > 0 || row.growthRate > 0)
      .slice(0, 8)
      .map((row) => ({
        id: row.id,
        label: row.name,
        trendId: row.id,
        attentionScore: row.attentionScore,
        velocity: row.velocityScore ?? 0,
        accelerationScore: row.attentionAcceleration,
        attentionSharePct: totalInteractions > 0
          ? (row.attentionInteractions / totalInteractions) * 100
          : 0,
        uniqueAuthors: row.uniqueAuthors24h ?? 0,
        amplificationScore: row.breakoutScore ?? row.emergingScore ?? 0,
        topAmplifierHandle: null,
      })),
    topAmplifiers: [],
    cascades: [],
    clusters: [],
    network: {
      nodes: [],
      edges: [],
    },
    replay: allPoints,
    replayWindow,
  };
}

export async function getSupabaseTrendDashboardState(query: TrendDashboardQuery): Promise<TrendDashboardVM> {
  const window = buildWindowBuckets(query.range);
  const { rows, source } = await fetchTopicRows(
    window.windowStart.toISOString(),
    window.windowEnd.toISOString(),
  );

  const parsedRows = rows
    .map((row) => parseTopicRow(row))
    .filter((row): row is ParsedTopicBucketRow => Boolean(row))
    .filter((row) => matchesScope(row, query.scope));

  if (parsedRows.length === 0) {
    const zero = createZeroTrendDashboardVM(query);
    return {
      ...zero,
      dataStatus: toSupabaseDataStatus({
        rowCount: rows.length,
        source,
        latestBucketAt: null,
      }),
    };
  }

  const bucketSet = new Set(window.buckets);
  const aggregateMap = new Map<string, TopicAggregate>();

  for (const row of parsedRows) {
    const bucketKey = bucketKeyForIso(row.bucketMinute, window.bucketMinutes);
    if (!bucketSet.has(bucketKey)) {
      continue;
    }

    const topicKey = row.normalizedTopic.toLowerCase();
    const existing = aggregateMap.get(topicKey);
    if (!existing) {
      aggregateMap.set(topicKey, {
        topicText: row.topicText,
        normalizedTopic: row.normalizedTopic,
        mentionCount: row.mentionCount,
        postCount: row.postCount,
        positiveCount: row.positiveCount,
        neutralCount: row.neutralCount,
        negativeCount: row.negativeCount,
        platforms: new Map([[row.platform, row.mentionCount]]),
        bucketCounts: new Map([[bucketKey, row.mentionCount]]),
        firstSeenAt: row.bucketMinute,
        lastSeenAt: row.bucketMinute,
      });
      continue;
    }

    existing.mentionCount += row.mentionCount;
    existing.postCount += row.postCount;
    existing.positiveCount += row.positiveCount;
    existing.neutralCount += row.neutralCount;
    existing.negativeCount += row.negativeCount;
    existing.platforms.set(row.platform, (existing.platforms.get(row.platform) ?? 0) + row.mentionCount);
    existing.bucketCounts.set(bucketKey, (existing.bucketCounts.get(bucketKey) ?? 0) + row.mentionCount);

    if (!existing.firstSeenAt || Date.parse(row.bucketMinute) < Date.parse(existing.firstSeenAt)) {
      existing.firstSeenAt = row.bucketMinute;
    }
    if (!existing.lastSeenAt || Date.parse(row.bucketMinute) > Date.parse(existing.lastSeenAt)) {
      existing.lastSeenAt = row.bucketMinute;
    }
  }

  const rankedBaseRows: RankedTrend[] = [];
  const allTrendTotalsByBucket = new Map<string, number>();
  const nowMs = Date.now();

  for (const topic of aggregateMap.values()) {
    const points: TimeSeriesPoint[] = window.buckets.map((bucketIso) => {
      const value = topic.bucketCounts.get(bucketIso) ?? 0;
      allTrendTotalsByBucket.set(bucketIso, (allTrendTotalsByBucket.get(bucketIso) ?? 0) + value);
      return {
        timestamp: bucketIso,
        value,
      };
    });

    const values = points.map((point) => point.value);
    const midpoint = Math.max(1, Math.floor(values.length / 2));
    const quarter = Math.max(1, Math.floor(values.length / 4));
    const previousHalf = values.slice(0, midpoint).reduce((sum, value) => sum + value, 0);
    const recentHalf = values.slice(midpoint).reduce((sum, value) => sum + value, 0);
    const previousQuarter = values.slice(Math.max(0, values.length - quarter * 2), values.length - quarter)
      .reduce((sum, value) => sum + value, 0);
    const recentQuarter = values.slice(values.length - quarter).reduce((sum, value) => sum + value, 0);
    const growthRate = clamp(percentDelta(recentHalf, previousHalf), -100, 400);
    const acceleration = clamp(percentDelta(recentQuarter, previousQuarter), -100, 400);

    const latestDataPoint = [...points].reverse().find((point) => point.value > 0);
    const latestDataMs = latestDataPoint ? Date.parse(latestDataPoint.timestamp) : Number.NaN;
    const ageMinutes = Number.isFinite(latestDataMs)
      ? Math.max(0, Math.round((nowMs - latestDataMs) / 60_000))
      : 10_000;
    const freshnessState = getFreshnessState(ageMinutes);
    const freshnessScore = clamp(100 - ageMinutes * 1.6, 0, 100);

    const platforms = [...topic.platforms.keys()];
    const platformTotal = Math.max(1, topic.mentionCount);
    const platformBreakdown = [...topic.platforms.entries()].map(([platform, interactions]) => ({
      platformId: platform,
      interactions,
      sharePct: (interactions / platformTotal) * 100,
    }));
    platformBreakdown.sort((left, right) => right.interactions - left.interactions);

    const lowDataWarning = topic.mentionCount < 8;
    const confidenceScore = clamp(
      25 +
        Math.log2(Math.max(1, topic.mentionCount + topic.postCount)) * 16 +
        Math.min(20, platforms.length * 6) +
        Math.max(-10, Math.min(10, growthRate / 8)),
      0,
      100,
    );
    const velocityScore = clamp(recentQuarter * (60 / window.bucketMinutes), 0, 1000);
    const noveltyScore = clamp(
      growthRate > 0 ? growthRate * 0.65 + (lowDataWarning ? 12 : 0) : growthRate * 0.35,
      0,
      100,
    );
    const sentimentTotal = topic.positiveCount + topic.neutralCount + topic.negativeCount;
    const sentimentBalance = sentimentTotal > 0
      ? (topic.positiveCount - topic.negativeCount) / sentimentTotal
      : 0;
    const confirmationScore = clamp(
      Math.min(100, topic.postCount * 2.4 + platforms.length * 7 + confidenceScore * 0.25),
      0,
      100,
    );
    const breakoutScore = clamp(
      velocityScore * 0.38 +
        noveltyScore * 0.36 +
        confirmationScore * 0.26 +
        sentimentBalance * 12,
      0,
      100,
    );
    const trendStrengthScore = clamp(
      topic.mentionCount * 0.14 +
        confidenceScore * 0.35 +
        Math.max(0, growthRate) * 0.12 +
        freshnessScore * 0.28,
      0,
      100,
    );

    const row = createZeroRankedTrend(query.scope, query.range);
    const id = `trend-${buildSlug(topic.normalizedTopic)}`;
    row.id = id;
    row.name = topic.normalizedTopic;
    row.clusterId = id;
    row.clusterName = topic.normalizedTopic;
    row.scope = query.scope;
    row.source = "bluesky";
    row.labelType = "entity_label";
    row.groupingSource = "fallback_singleton";
    row.leaderboardTier = lowDataWarning ? "secondary_singleton" : "primary_grouped";
    row.canonicalKeySummary = topic.normalizedTopic;
    row.labelQualityScore = clamp(0.55 + (topic.mentionCount > 20 ? 0.3 : 0.15), 0, 1);
    row.lowQualityLabel = lowDataWarning;
    row.aiAssisted = false;
    row.attentionInteractions = topic.mentionCount;
    row.totalInteractions24h = topic.mentionCount;
    row.qualityAdjustedScore = trendStrengthScore;
    row.attentionScore = clamp(
      topic.mentionCount * 0.2 + trendStrengthScore * 0.35 + Math.max(0, growthRate) * 0.2,
      0,
      1000,
    );
    row.emergingScore = breakoutScore;
    row.breakoutScore = breakoutScore;
    row.velocityScore = velocityScore;
    row.noveltyScore = noveltyScore;
    row.confirmationScore = confirmationScore;
    row.rootsCount24h = topic.postCount;
    row.uniqueAuthors24h = 0;
    row.singleAuthorShare = 0;
    row.firstSeenAt = topic.firstSeenAt;
    row.lastSeenAt = topic.lastSeenAt;
    row.isSingleton = topic.postCount <= 1;
    row.trendCategory = null;
    row.contentType = null;
    row.spamLikelihood = 0;
    row.templateLikelihood = 0;
    row.contextualCoherence = undefined;
    row.lowInformation = lowDataWarning;
    row.templateSeries = false;
    row.confidenceScore = confidenceScore;
    row.freshnessScore = freshnessScore;
    row.freshnessState = freshnessState;
    row.sampleSize = topic.mentionCount;
    row.supportingThreadCount = topic.postCount;
    row.lowDataWarning = lowDataWarning;
    row.growthRate = growthRate;
    row.attentionAcceleration = acceleration;
    row.mentions = topic.mentionCount;
    row.platforms = platforms.length > 0 ? platforms : ["bluesky"];
    row.platformSpread = row.platforms.length;
    row.confirmedPlatformSpread = row.platforms.length;
    row.attentionHistory = points;
    row.platformBreakdown = platformBreakdown;
    row.topPosts = [];
    row.lifecycleStage = getLifecycleStage(growthRate, freshnessState);
    row.originPlatform = row.platforms[0] ?? "bluesky";
    row.platformMigrationPath = [...row.platforms];
    row.attentionDrivers = platformBreakdown.map((entry) => ({
      platformId: entry.platformId,
      contributionPct: entry.sharePct,
      deltaPct: 0,
    }));
    row.hasSpike = acceleration >= 60;
    row.spikeMagnitude = row.hasSpike ? clamp(acceleration / 100, 0, 4) : 0;
    row.trendStrengthScore = trendStrengthScore;
    row.persistenceScore = clamp(confidenceScore * 0.6 + topic.postCount * 0.9, 0, 100);
    row.isEarlyTrend = topic.postCount <= 5 && growthRate >= 20;
    row.positionChange24h = 0;
    row.googleSearchInterest = null;
    row.blueskySummary = {
      attentionSharePct: 100,
      postCount: topic.postCount,
      uniqueAuthorCount: 0,
      amplifierCount: 0,
      topAmplifierHandle: null,
      leadingSignalLabel: topic.topicText,
      firehoseLagMinutes: ageMinutes,
      repostVelocity: 0,
      replyVelocity: 0,
      quoteVelocity: 0,
    };
    row.blueskyDetail = null;

    rankedBaseRows.push(row);
  }

  const establishedRows = sortRowsByMode(
    rankedBaseRows.map((row) => ({
      ...row,
      leaderboardMode: "established",
    })),
    "established",
    query.sort,
  )
    .slice(0, MAX_LEADERBOARD_ROWS)
    .map((row, index) => ({
      ...row,
      rank: index + 1,
    }));

  const emergingCandidates = rankedBaseRows
    .filter((row) => row.isEarlyTrend || row.growthRate > 10 || (row.breakoutScore ?? 0) > 18);
  const emergingSeedRows = emergingCandidates.length > 0 ? emergingCandidates : rankedBaseRows;
  const emergingRows = sortRowsByMode(
    emergingSeedRows.map((row) => ({
      ...row,
      leaderboardMode: "emerging",
    })),
    "emerging",
    query.sort,
  )
    .slice(0, MAX_LEADERBOARD_ROWS)
    .map((row, index) => ({
      ...row,
      rank: index + 1,
    }));

  const selectedMode = query.mode ?? "established";
  const selectedLeaderboard = selectedMode === "emerging" ? emergingRows : establishedRows;
  const aggregatePoints = window.buckets.map((bucket) => ({
    timestamp: bucket,
    value: allTrendTotalsByBucket.get(bucket) ?? 0,
  }));
  const latestBucketAt = aggregatePoints
    .slice()
    .reverse()
    .find((point) => point.value > 0)?.timestamp ?? null;

  const vm: TrendDashboardVM = {
    query: {
      ...query,
      mode: selectedMode,
      selectedId: query.selectedId,
    },
    ingestionHealth: null,
    dataStatus: toSupabaseDataStatus({
      rowCount: parsedRows.length,
      source,
      latestBucketAt,
    }),
    blueskyOverview: buildBlueskyOverview(
      selectedLeaderboard,
      aggregatePoints,
      query.range,
      window.bucketMinutes,
    ),
    trendCoverage: null,
    leaderboards: {
      established: establishedRows,
      emerging: emergingRows,
    },
    leaderboard: selectedLeaderboard,
    overviewSeries: [],
    detail: null,
  };

  const selectedVm = applyTrendDashboardSelection(vm, query.selectedId);
  return attachSeriesWindows(selectedVm, query.range, window.bucketMinutes);
}

export function shouldUseSupabaseTrendSource() {
  return readBooleanEnv(process.env.USE_SUPABASE_TRENDS, false);
}
