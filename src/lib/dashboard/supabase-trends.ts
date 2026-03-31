import "server-only";

import { PostgrestError } from "@supabase/supabase-js";
import { Pool } from "pg";
import { getServerPostgresPool, hasDatabaseUrl } from "@/lib/db/server-postgres";
import { applyTrendDashboardSelection } from "@/lib/dashboard/selection";
import { createZeroRankedTrend, createZeroTrendDashboardVM } from "@/lib/dashboard/zero-state";
import { getSupabaseServerClient, hasSupabaseServerCredentials } from "@/lib/supabase/server";
import { DateRangePreset, PlatformId, TimeSeriesPoint, TrendFreshnessState, TrendLifecycleStage } from "@/types/domain";
import { DashboardDataStatus, DashboardFreshnessDiagnostics, RankedTrend, TimeSeriesWindow, TrendDashboardQuery, TrendDashboardVM, TrendLeaderboardMode, TrendSort } from "@/types/view-models";

type SupabaseTopicRow = Record<string, unknown>;

export type SupabaseTrendReadProfile = "summary" | "detail";

type ParsedTopicBucketRow = {
  bucketMinute: string;
  updatedAt: string | null;
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

type StableTopicDayTotalRow = {
  day: string;
  topicKey: string;
  topicLabel: string;
  totalMentions: number;
  uniquePosts: number;
  uniqueAuthors: number;
  positiveCount: number;
  neutralCount: number;
  negativeCount: number;
  platformCount: number;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
};

type StableTopicDayTotalAggregateRow = StableTopicDayTotalRow & {
  rawTopicKeys: string[];
};

type StableTopicSeriesRow = {
  day: string;
  topicKey: string;
  topicLabel: string;
  bucket5m: string;
  interactions: number;
  cumulativeInteractions: number;
  updatedAt: string | null;
};

type TopicEnrichmentRow = {
  topicKey: string;
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
  asOfWindowEnd: string | null;
};

type SupabaseFreshnessProbeRow = {
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
};

const RANGE_CONFIG: Record<DateRangePreset, RangeConfig> = {
  "1h": { points: 12, bucketMinutes: 5 },
  "6h": { points: 72, bucketMinutes: 5 },
  "24h": { points: 288, bucketMinutes: 5 },
  "7d": { points: 336, bucketMinutes: 30 },
};

const TOPIC_SOURCE_VIEW = "v_topic_trends_1m";
const TOPIC_SOURCE_TABLE = "topic_buckets_1m";
const STABLE_TOPIC_ROLLING_TOTALS_TABLE = "topic_rolling_24h";
const STABLE_TOPIC_ROLLING_TOTALS_VIEW = "v_topic_leaderboard_rolling_24h";
const STABLE_TOPIC_DAY_SERIES_TABLE = "topic_day_series_5m";
const STABLE_TOPIC_DAY_SERIES_VIEW = "v_topic_series_day_5m";
const MAX_QUERY_ROWS = 50_000;
const PAGE_SIZE = 1_000;
const MAX_LEADERBOARD_ROWS = 250;
const STABLE_SERIES_TOPIC_LIMIT = readIntegerEnv(
  process.env.DASHBOARD_SERIES_TOPIC_LIMIT,
  12,
  0,
  MAX_LEADERBOARD_ROWS,
);
const STABLE_DETAIL_SERIES_TOPIC_LIMIT = readIntegerEnv(
  process.env.DASHBOARD_DETAIL_SERIES_TOPIC_LIMIT,
  0,
  0,
  MAX_LEADERBOARD_ROWS,
);
const FRESHNESS_PROBE_CACHE_TTL_MS = readIntegerEnv(
  process.env.DASHBOARD_FRESHNESS_PROBE_CACHE_MS,
  60_000,
  0,
  300_000,
);
const STABLE_ROLLING_TOTALS_LIMIT = MAX_LEADERBOARD_ROWS;
const STABLE_ROLLING_TOTALS_COLUMNS = [
  "topic_key",
  "topic_label",
  "platform_count",
  "total_mentions",
  "unique_posts",
  "unique_authors",
  "positive_count",
  "neutral_count",
  "negative_count",
  "first_seen_at",
  "last_seen_at",
  "window_end",
  "updated_at",
].join(",");
const STABLE_DAY_SERIES_COLUMNS = [
  "day",
  "bucket_5m",
  "topic_key",
  "topic_label",
  "interactions",
  "updated_at",
].join(",");
const TOPIC_ENRICHMENT_TABLE = "topic_ai_enrichments";
const TOPIC_ENRICHMENT_COLUMNS = [
  "topic_key",
  "as_of_window_end",
  "raw_label",
  "canonical_name",
  "short_description",
  "context_paragraph",
  "key_entities",
  "trend_category",
  "summary_confidence",
  "model_name",
  "prompt_version",
  "generated_at",
  "refreshed_at",
].join(",");
const TOPIC_ENRICHMENT_LOOKBACK_HOURS = readIntegerEnv(
  process.env.TOPIC_ENRICHMENT_LOOKBACK_HOURS,
  96,
  1,
  24 * 30,
);
const TOPIC_ENRICHMENT_MAX_ROWS = readIntegerEnv(
  process.env.TOPIC_ENRICHMENT_MAX_ROWS,
  MAX_LEADERBOARD_ROWS * 8,
  MAX_LEADERBOARD_ROWS,
  MAX_LEADERBOARD_ROWS * 30,
);
const BLUESKY_WORKER_SOURCE = (() => {
  const value = (process.env.BLUESKY_WORKER_SOURCE ?? "bluesky_firehose_worker").trim();
  return value.length > 0 ? value : "bluesky_firehose_worker";
})();

let freshnessProbeCache: {
  value: SupabaseFreshnessProbeRow | null;
  expiresAt: number;
  inFlight: Promise<SupabaseFreshnessProbeRow | null> | null;
} = {
  value: null,
  expiresAt: 0,
  inFlight: null,
};

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

function readIntegerEnv(value: string | undefined, fallback: number, min: number, max: number) {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value.trim(), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  return Math.min(max, Math.max(min, parsed));
}

function readStringValue(value: unknown) {
  if (typeof value === "string") {
    return value.trim();
  }

  if (value instanceof Date) {
    const timestamp = value.getTime();
    if (Number.isFinite(timestamp)) {
      return value.toISOString();
    }
    return "";
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "";
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  return "";
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function parseJsonObject(value: unknown) {
  if (isRecord(value)) {
    return value;
  }

  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function parseOptionalInteger(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.trunc(value));
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return Math.max(0, parsed);
    }
  }

  return null;
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

function toIsoTimestamp(value: unknown) {
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
    return null;
  }

  return new Date(timestamp).toISOString();
}

function ageMinutesFromIso(isoTimestamp: string | null | undefined, nowMs: number) {
  const timestamp = isoTimestamp ? Date.parse(isoTimestamp) : Number.NaN;
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return Math.max(0, Math.round((nowMs - timestamp) / 60_000));
}

function resolveWorkerLastEventAt(notesValue: unknown) {
  const notes = parseJsonObject(notesValue);
  if (!notes) {
    return null;
  }

  const state = parseJsonObject(notes.state);
  const stateLastEventAt = toIsoTimestamp(state?.lastEventAt);
  if (stateLastEventAt) {
    return stateLastEventAt;
  }

  const noteLastEventAt = toIsoTimestamp(notes.lastEventAt);
  if (noteLastEventAt) {
    return noteLastEventAt;
  }

  return toIsoTimestamp(notes.updated_at);
}

function maxIsoTimestampFrom(values: Array<string | null | undefined>) {
  let latest: string | null = null;
  let latestMs = Number.NaN;

  for (const value of values) {
    if (!value) {
      continue;
    }

    const timestampMs = Date.parse(value);
    if (!Number.isFinite(timestampMs)) {
      continue;
    }

    if (!Number.isFinite(latestMs) || timestampMs > latestMs) {
      latest = value;
      latestMs = timestampMs;
    }
  }

  return latest;
}

function resolveCanonicalSourceSnapshotAt(
  latestSourceAt: string | null,
  freshnessProbe: SupabaseFreshnessProbeRow | null,
) {
  return maxIsoTimestampFrom([
    latestSourceAt,
    freshnessProbe?.latestReadModelRollingWriteAt,
    freshnessProbe?.latestReadModelSeriesWriteAt,
    freshnessProbe?.latestReadModelFinalizeAt,
    freshnessProbe?.latestReadModelWindowEndAt,
  ]);
}

async function fetchSupabaseFreshnessProbeFromPostgres(): Promise<SupabaseFreshnessProbeRow | null> {
  const pool = getServerPostgresPool();
  const query = `
    WITH latest_run AS (
      SELECT
        started_at,
        status,
        rows_inserted,
        notes
      FROM public.ingestion_runs
      WHERE source = $1
      ORDER BY started_at DESC
      LIMIT 1
    )
    SELECT
      (SELECT MAX(ingested_at) FROM public.post_topic_mentions)::timestamptz AS "latestIngestionAt",
      (SELECT MAX(processed_at) FROM public.processed_posts)::timestamptz AS "latestProcessedAt",
      (SELECT MAX(event_timestamp) FROM public.post_topic_mentions)::timestamptz AS "latestMentionEventAt",
      (SELECT last_finalize_before FROM public.topic_read_model_state WHERE id = 1)::timestamptz AS "latestReadModelFinalizeAt",
      (SELECT MAX(updated_at) FROM public.topic_rolling_24h)::timestamptz AS "latestReadModelRollingWriteAt",
      (SELECT MAX(updated_at) FROM public.topic_day_series_5m)::timestamptz AS "latestReadModelSeriesWriteAt",
      (SELECT MAX(window_end) FROM public.topic_rolling_24h)::timestamptz AS "latestReadModelWindowEndAt",
      (SELECT MAX(bucket_5m) FROM public.topic_day_series_5m WHERE interactions > 0)::timestamptz AS "latestSeriesNonZeroBucketAt",
      (SELECT started_at FROM latest_run)::timestamptz AS "workerRunStartedAt",
      (SELECT status FROM latest_run)::text AS "workerRunStatus",
      (SELECT rows_inserted FROM latest_run)::bigint AS "workerRowsInserted",
      (SELECT notes FROM latest_run) AS "workerRunNotes"
  `;
  const result = await pool.query<Record<string, unknown>>(query, [BLUESKY_WORKER_SOURCE]);
  const row = result.rows[0];
  if (!row) {
    return null;
  }

  const workerRunStatusRaw = readStringValue(row.workerRunStatus);

  return {
    latestIngestionAt: toIsoTimestamp(row.latestIngestionAt),
    latestProcessedAt: toIsoTimestamp(row.latestProcessedAt),
    latestMentionEventAt: toIsoTimestamp(row.latestMentionEventAt),
    latestReadModelFinalizeAt: toIsoTimestamp(row.latestReadModelFinalizeAt),
    latestReadModelRollingWriteAt: toIsoTimestamp(row.latestReadModelRollingWriteAt),
    latestReadModelSeriesWriteAt: toIsoTimestamp(row.latestReadModelSeriesWriteAt),
    latestReadModelWindowEndAt: toIsoTimestamp(row.latestReadModelWindowEndAt),
    latestSeriesNonZeroBucketAt: toIsoTimestamp(row.latestSeriesNonZeroBucketAt),
    workerRunStartedAt: toIsoTimestamp(row.workerRunStartedAt),
    workerRunStatus: workerRunStatusRaw || null,
    workerLastEventAt: resolveWorkerLastEventAt(row.workerRunNotes),
    workerRowsInserted: parseOptionalInteger(row.workerRowsInserted),
  };
}

async function fetchSupabaseFreshnessProbeFromSupabase(): Promise<SupabaseFreshnessProbeRow | null> {
  const client = getSupabaseServerClient();
  const safeHead = async (fn: () => Promise<string | null>) => {
    try {
      return await fn();
    } catch {
      return null;
    }
  };
  const latestIngestionAt = await safeHead(async () => {
    const { data, error } = await client
      .from("post_topic_mentions")
      .select("ingested_at")
      .order("ingested_at", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.ingested_at ?? null);
  });
  const latestProcessedAt = await safeHead(async () => {
    const { data, error } = await client
      .from("processed_posts")
      .select("processed_at")
      .order("processed_at", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.processed_at ?? null);
  });
  const latestMentionEventAt = await safeHead(async () => {
    const { data, error } = await client
      .from("post_topic_mentions")
      .select("event_timestamp")
      .order("event_timestamp", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.event_timestamp ?? null);
  });
  const latestReadModelFinalizeAt = await safeHead(async () => {
    const { data, error } = await client
      .from("topic_read_model_state")
      .select("last_finalize_before")
      .eq("id", 1)
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.last_finalize_before ?? null);
  });
  const latestReadModelRollingWriteAt = await safeHead(async () => {
    const { data, error } = await client
      .from("topic_rolling_24h")
      .select("updated_at")
      .order("updated_at", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.updated_at ?? null);
  });
  const latestReadModelSeriesWriteAt = await safeHead(async () => {
    const { data, error } = await client
      .from("topic_day_series_5m")
      .select("updated_at")
      .order("updated_at", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.updated_at ?? null);
  });
  const latestReadModelWindowEndAt = await safeHead(async () => {
    const { data, error } = await client
      .from("topic_rolling_24h")
      .select("window_end")
      .order("window_end", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.window_end ?? null);
  });
  const latestSeriesNonZeroBucketAt = await safeHead(async () => {
    const { data, error } = await client
      .from("topic_day_series_5m")
      .select("bucket_5m")
      .gt("interactions", 0)
      .order("bucket_5m", { ascending: false })
      .limit(1);
    if (error) {
      throw error;
    }
    return toIsoTimestamp(data?.[0]?.bucket_5m ?? null);
  });
  const latestWorkerRun = await (async () => {
    try {
      const { data, error } = await client
        .from("ingestion_runs")
        .select("started_at,status,rows_inserted,notes")
        .eq("source", BLUESKY_WORKER_SOURCE)
        .order("started_at", { ascending: false })
        .limit(1);

      if (error) {
        throw error;
      }

      return data?.[0] ?? null;
    } catch {
      return null;
    }
  })();
  const workerRunStatusRaw = readStringValue(latestWorkerRun?.status);

  return {
    latestIngestionAt,
    latestProcessedAt,
    latestMentionEventAt,
    latestReadModelFinalizeAt,
    latestReadModelRollingWriteAt,
    latestReadModelSeriesWriteAt,
    latestReadModelWindowEndAt,
    latestSeriesNonZeroBucketAt,
    workerRunStartedAt: toIsoTimestamp(latestWorkerRun?.started_at ?? null),
    workerRunStatus: workerRunStatusRaw || null,
    workerLastEventAt: resolveWorkerLastEventAt(latestWorkerRun?.notes ?? null),
    workerRowsInserted: parseOptionalInteger(latestWorkerRun?.rows_inserted ?? null),
  };
}

async function fetchSupabaseFreshnessProbe() {
  const now = Date.now();
  if (FRESHNESS_PROBE_CACHE_TTL_MS > 0 && freshnessProbeCache.expiresAt > now) {
    return freshnessProbeCache.value;
  }

  if (FRESHNESS_PROBE_CACHE_TTL_MS > 0 && freshnessProbeCache.inFlight) {
    return freshnessProbeCache.inFlight;
  }

  const probePromise = (async () => {
    try {
      if (hasDatabaseUrl()) {
        return await fetchSupabaseFreshnessProbeFromPostgres();
      }
      if (hasSupabaseServerCredentials()) {
        return await fetchSupabaseFreshnessProbeFromSupabase();
      }
    } catch (error) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[supabase-trends] freshness probe failed", { error });
      }
    }

    return null;
  })();

  if (FRESHNESS_PROBE_CACHE_TTL_MS <= 0) {
    return probePromise;
  }

  freshnessProbeCache = {
    ...freshnessProbeCache,
    inFlight: probePromise,
  };

  const value = await probePromise;
  freshnessProbeCache = {
    value,
    expiresAt: Date.now() + FRESHNESS_PROBE_CACHE_TTL_MS,
    inFlight: null,
  };

  return value;
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

function isSupabaseMissingStructure(error: PostgrestError | null) {
  if (!error) {
    return false;
  }

  const message = `${error.message} ${error.details ?? ""} ${error.hint ?? ""}`.toLowerCase();
  return (
    isSupabaseMissingRelation(error) ||
    error.code === "42703" ||
    message.includes("column") ||
    message.includes("schema cache")
  );
}

function isPostgresMissingStructure(error: unknown) {
  const databaseError = error as { code?: string; message?: string };
  const message = String(databaseError?.message ?? "").toLowerCase();
  return (
    isPostgresMissingRelation(error) ||
    databaseError?.code === "42703" ||
    message.includes("column")
  );
}

function chunkArray<T>(items: T[], size: number): T[][] {
  if (size <= 0 || items.length === 0) {
    return items.length > 0 ? [items] : [];
  }

  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
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
        TOPIC_SOURCE_TABLE,
        windowStartIso,
        windowEndIso,
      );
      return {
        rows,
        source: TOPIC_SOURCE_TABLE,
      };
    } catch (error) {
      const pgError = error as PostgrestError;
      if (!isSupabaseMissingRelation(pgError)) {
        throw error;
      }
    }

    const rows = await fetchTopicRowsFromSupabaseSource(
      TOPIC_SOURCE_VIEW,
      windowStartIso,
      windowEndIso,
    );
    return {
      rows,
      source: TOPIC_SOURCE_VIEW,
    };
  }

  if (hasDatabaseUrl()) {
    try {
      const rows = await fetchTopicRowsFromPostgresSource(
        TOPIC_SOURCE_TABLE,
        windowStartIso,
        windowEndIso,
      );
      return {
        rows,
        source: TOPIC_SOURCE_TABLE,
      };
    } catch (error) {
      if (!isPostgresMissingRelation(error)) {
        throw error;
      }
    }

    const rows = await fetchTopicRowsFromPostgresSource(
      TOPIC_SOURCE_VIEW,
      windowStartIso,
      windowEndIso,
    );
    return {
      rows,
      source: TOPIC_SOURCE_VIEW,
    };
  }

  throw new Error(
    "No Supabase trend query credentials available. Set SUPABASE_SERVICE_ROLE_KEY (+ SUPABASE_URL) or DATABASE_URL.",
  );
}

async function fetchStableRollingTotalsFromSupabaseSource(source: string) {
  const client = getSupabaseServerClient();
  const { data, error } = await client
    .from(source)
    .select(STABLE_ROLLING_TOTALS_COLUMNS)
    .order("total_mentions", { ascending: false })
    .order("topic_key", { ascending: true })
    .limit(STABLE_ROLLING_TOTALS_LIMIT);

  if (error) {
    throw error;
  }

  return (data ?? []) as unknown as SupabaseTopicRow[];
}

async function fetchStableRollingTotalsFromPostgresSource(source: string) {
  const pool = getServerPostgresPool();
  const query = `
    SELECT
      topic_key,
      topic_label,
      platform_count,
      total_mentions,
      unique_posts,
      unique_authors,
      positive_count,
      neutral_count,
      negative_count,
      first_seen_at,
      last_seen_at,
      window_end,
      updated_at
    FROM public.${source}
    ORDER BY total_mentions DESC, topic_key ASC
    LIMIT $1
  `;
  const result = await pool.query<Record<string, unknown>>(query, [STABLE_ROLLING_TOTALS_LIMIT]);
  return result.rows as SupabaseTopicRow[];
}

async function fetchStableRollingTotals() {
  const sources = [STABLE_TOPIC_ROLLING_TOTALS_TABLE, STABLE_TOPIC_ROLLING_TOTALS_VIEW];
  if (hasSupabaseServerCredentials()) {
    let lastError: PostgrestError | null = null;
    for (const source of sources) {
      try {
        return {
          rows: await fetchStableRollingTotalsFromSupabaseSource(source),
          source,
        };
      } catch (error) {
        const pgError = error as PostgrestError;
        if (!isSupabaseMissingStructure(pgError)) {
          throw error;
        }
        lastError = pgError;
      }
    }
    if (lastError) {
      throw lastError;
    }
  }

  if (hasDatabaseUrl()) {
    let lastError: unknown = null;
    for (const source of sources) {
      try {
        return {
          rows: await fetchStableRollingTotalsFromPostgresSource(source),
          source,
        };
      } catch (error) {
        if (!isPostgresMissingStructure(error)) {
          throw error;
        }
        lastError = error;
      }
    }
    if (lastError) {
      throw lastError;
    }
  }

  throw new Error(
    "Stable rolling 24h source not found. Create topic_rolling_24h or v_topic_leaderboard_rolling_24h.",
  );
}

async function fetchStableDaySeriesFromSupabaseSource(
  source: string,
  dayIso: string,
  windowStartIso: string,
  windowEndIso: string,
  topicKeys: string[],
) {
  if (topicKeys.length === 0) {
    return [] as SupabaseTopicRow[];
  }

  const client = getSupabaseServerClient();
  const rows: SupabaseTopicRow[] = [];
  const keyChunks = chunkArray(topicKeys, 75);

  for (const topicKeyChunk of keyChunks) {
    const { data, error } = await client
      .from(source)
      .select(STABLE_DAY_SERIES_COLUMNS)
      .eq("day", dayIso)
      .gte("bucket_5m", windowStartIso)
      .lte("bucket_5m", windowEndIso)
      .in("topic_key", topicKeyChunk)
      .order("bucket_5m", { ascending: true })
      .range(0, MAX_QUERY_ROWS - 1);

    if (error) {
      throw error;
    }

    if (data && data.length > 0) {
      rows.push(...(data as unknown as SupabaseTopicRow[]));
    }
  }

  return rows;
}

async function fetchStableDaySeriesFromPostgresSource(
  source: string,
  dayIso: string,
  windowStartIso: string,
  windowEndIso: string,
  topicKeys: string[],
) {
  if (topicKeys.length === 0) {
    return [] as SupabaseTopicRow[];
  }

  const pool = getServerPostgresPool();
  const rows: SupabaseTopicRow[] = [];
  const keyChunks = chunkArray(topicKeys, 75);

  for (const topicKeyChunk of keyChunks) {
    const query = `
      SELECT
        day,
        bucket_5m,
        topic_key,
        topic_label,
        interactions,
        updated_at
      FROM public.${source}
      WHERE day = $1::date
        AND bucket_5m >= $2::timestamptz
        AND bucket_5m <= $3::timestamptz
        AND topic_key = ANY($4::text[])
      ORDER BY bucket_5m ASC
    `;
    const result = await pool.query<Record<string, unknown>>(query, [
      dayIso,
      windowStartIso,
      windowEndIso,
      topicKeyChunk,
    ]);
    if (result.rows.length > 0) {
      rows.push(...(result.rows as SupabaseTopicRow[]));
    }
  }

  return rows;
}

async function fetchStableDaySeries(
  dayIso: string,
  windowStartIso: string,
  windowEndIso: string,
  topicKeys: string[],
) {
  const sources = [STABLE_TOPIC_DAY_SERIES_TABLE, STABLE_TOPIC_DAY_SERIES_VIEW];
  if (hasSupabaseServerCredentials()) {
    let lastError: PostgrestError | null = null;
    for (const source of sources) {
      try {
        return {
          rows: await fetchStableDaySeriesFromSupabaseSource(
            source,
            dayIso,
            windowStartIso,
            windowEndIso,
            topicKeys,
          ),
          source,
        };
      } catch (error) {
        const pgError = error as PostgrestError;
        if (!isSupabaseMissingStructure(pgError)) {
          throw error;
        }
        lastError = pgError;
      }
    }
    if (lastError) {
      throw lastError;
    }
  }

  if (hasDatabaseUrl()) {
    let lastError: unknown = null;
    for (const source of sources) {
      try {
        return {
          rows: await fetchStableDaySeriesFromPostgresSource(
            source,
            dayIso,
            windowStartIso,
            windowEndIso,
            topicKeys,
          ),
          source,
        };
      } catch (error) {
        if (!isPostgresMissingStructure(error)) {
          throw error;
        }
        lastError = error;
      }
    }
    if (lastError) {
      throw lastError;
    }
  }

  throw new Error(
    "Stable topic day series source not found. Create topic_day_series_5m or v_topic_series_day_5m.",
  );
}

function parseTopicEnrichmentRow(row: SupabaseTopicRow): TopicEnrichmentRow | null {
  const topicKey = readStringValue(row.topic_key);
  if (!topicKey) {
    return null;
  }

  const canonicalName = normalizeTopic(
    pickFirstString(row, ["canonical_name", "raw_label", "topic_key"]) || topicKey,
  );
  const rawLabel = normalizeTopic(
    pickFirstString(row, ["raw_label", "canonical_name", "topic_key"]) || topicKey,
  );

  return {
    topicKey,
    rawLabel: rawLabel || topicKey,
    canonicalName: canonicalName || rawLabel || topicKey,
    shortDescription: pickFirstString(row, ["short_description"]),
    contextParagraph: pickFirstString(row, ["context_paragraph"]),
    keyEntities: pickStringArray(row, ["key_entities"]),
    trendCategory: pickFirstString(row, ["trend_category"]) || null,
    summaryConfidence: clamp(pickFirstNumber(row, ["summary_confidence"], 0), 0, 1),
    modelName: pickFirstString(row, ["model_name"]) || null,
    promptVersion: pickFirstString(row, ["prompt_version"]) || null,
    generatedAt: toIsoTimestamp(row.generated_at),
    refreshedAt: toIsoTimestamp(row.refreshed_at),
    asOfWindowEnd: toIsoTimestamp(row.as_of_window_end),
  };
}

function selectLatestTopicEnrichment(rows: TopicEnrichmentRow[]) {
  const byTopicKey = new Map<string, TopicEnrichmentRow>();
  for (const row of rows) {
    const existing = byTopicKey.get(row.topicKey);
    if (!existing) {
      byTopicKey.set(row.topicKey, row);
      continue;
    }

    const rowWindowMs = parseIsoTimestamp(row.asOfWindowEnd);
    const existingWindowMs = parseIsoTimestamp(existing.asOfWindowEnd);
    if (Number.isFinite(rowWindowMs) && (!Number.isFinite(existingWindowMs) || rowWindowMs > existingWindowMs)) {
      byTopicKey.set(row.topicKey, row);
      continue;
    }
    if (Number.isFinite(rowWindowMs) && Number.isFinite(existingWindowMs) && rowWindowMs < existingWindowMs) {
      continue;
    }

    const rowRefreshMs = parseIsoTimestamp(row.refreshedAt ?? row.generatedAt);
    const existingRefreshMs = parseIsoTimestamp(existing.refreshedAt ?? existing.generatedAt);
    if (
      Number.isFinite(rowRefreshMs) &&
      (!Number.isFinite(existingRefreshMs) || rowRefreshMs > existingRefreshMs)
    ) {
      byTopicKey.set(row.topicKey, row);
    }
  }

  return byTopicKey;
}

async function fetchTopicEnrichmentRowsFromSupabase(
  topicKeys: string[],
  minAsOfWindowEndIso: string,
) {
  const client = getSupabaseServerClient();
  let cursor = 0;
  const rows: SupabaseTopicRow[] = [];
  while (cursor < TOPIC_ENRICHMENT_MAX_ROWS) {
    const upper = Math.min(cursor + PAGE_SIZE - 1, TOPIC_ENRICHMENT_MAX_ROWS - 1);
    const { data, error } = await client
      .from(TOPIC_ENRICHMENT_TABLE)
      .select(TOPIC_ENRICHMENT_COLUMNS)
      .in("topic_key", topicKeys)
      .gte("as_of_window_end", minAsOfWindowEndIso)
      .order("as_of_window_end", { ascending: false })
      .order("generated_at", { ascending: false })
      .range(cursor, upper);

    if (error) {
      throw error;
    }

    const pageRows = (data ?? []) as unknown as SupabaseTopicRow[];
    if (pageRows.length === 0) {
      break;
    }
    rows.push(...pageRows);
    if (pageRows.length < PAGE_SIZE) {
      break;
    }
    cursor += PAGE_SIZE;
  }

  return rows;
}

async function fetchTopicEnrichmentRowsFromPostgres(
  topicKeys: string[],
  minAsOfWindowEndIso: string,
) {
  const pool = getServerPostgresPool();
  const result = await pool.query<Record<string, unknown>>(
    `
      SELECT
        topic_key,
        as_of_window_end,
        raw_label,
        canonical_name,
        short_description,
        context_paragraph,
        key_entities,
        trend_category,
        summary_confidence,
        model_name,
        prompt_version,
        generated_at,
        refreshed_at
      FROM public.${TOPIC_ENRICHMENT_TABLE}
      WHERE topic_key = ANY($1::text[])
        AND as_of_window_end >= $2::timestamptz
      ORDER BY topic_key ASC, as_of_window_end DESC, generated_at DESC
      LIMIT $3::int
    `,
    [topicKeys, minAsOfWindowEndIso, TOPIC_ENRICHMENT_MAX_ROWS],
  );
  return result.rows as SupabaseTopicRow[];
}

async function fetchTopicEnrichmentByTopicKeys(
  topicKeys: string[],
  windowEndIso: string,
) {
  const normalizedTopicKeys = [...new Set(topicKeys.map((value) => value.trim()).filter((value) => value.length > 0))];
  if (normalizedTopicKeys.length === 0) {
    return new Map<string, TopicEnrichmentRow>();
  }

  const minWindowEndIso = new Date(
    Date.parse(windowEndIso) - TOPIC_ENRICHMENT_LOOKBACK_HOURS * 60 * 60 * 1_000,
  ).toISOString();

  const parsedRows: TopicEnrichmentRow[] = [];

  if (hasSupabaseServerCredentials()) {
    try {
      const rows = await fetchTopicEnrichmentRowsFromSupabase(normalizedTopicKeys, minWindowEndIso);
      parsedRows.push(...rows.map((row) => parseTopicEnrichmentRow(row)).filter((row): row is TopicEnrichmentRow => Boolean(row)));
      return selectLatestTopicEnrichment(parsedRows);
    } catch (error) {
      const pgError = error as PostgrestError;
      if (!isSupabaseMissingStructure(pgError)) {
        throw error;
      }
    }
  }

  if (!hasDatabaseUrl()) {
    return new Map<string, TopicEnrichmentRow>();
  }

  try {
    const rows = await fetchTopicEnrichmentRowsFromPostgres(normalizedTopicKeys, minWindowEndIso);
    parsedRows.push(...rows.map((row) => parseTopicEnrichmentRow(row)).filter((row): row is TopicEnrichmentRow => Boolean(row)));
    return selectLatestTopicEnrichment(parsedRows);
  } catch (error) {
    if (!isPostgresMissingStructure(error)) {
      throw error;
    }
    return new Map<string, TopicEnrichmentRow>();
  }
}

function parseStableDayTotalRow(row: SupabaseTopicRow): StableTopicDayTotalRow | null {
  const topicKey = readStringValue(row.topic_key ?? row.normalized_topic ?? row.topic);
  const topicLabel = normalizeTopic(
    pickFirstString(row, ["topic_label", "topic_display", "topic", "normalized_topic", "topic_key"]) || topicKey,
  );
  const dayTimestampIso =
    toIsoTimestamp(row.day) ??
    toIsoTimestamp(row.window_end) ??
    toIsoTimestamp(row.updated_at);
  const day = dayTimestampIso ? dayTimestampIso.slice(0, 10) : "";

  if (!topicKey || !topicLabel || !day) {
    return null;
  }

  return {
    day,
    topicKey,
    topicLabel,
    totalMentions: Math.max(
      0,
      pickFirstNumber(row, ["total_mentions", "mention_count", "mentions", "interactions"], 0),
    ),
    uniquePosts: Math.max(0, pickFirstNumber(row, ["unique_posts", "post_count", "roots_count"], 0)),
    uniqueAuthors: Math.max(0, pickFirstNumber(row, ["unique_authors", "author_count"], 0)),
    positiveCount: Math.max(0, pickFirstNumber(row, ["positive_count"], 0)),
    neutralCount: Math.max(0, pickFirstNumber(row, ["neutral_count"], 0)),
    negativeCount: Math.max(0, pickFirstNumber(row, ["negative_count"], 0)),
    platformCount: Math.max(1, pickFirstNumber(row, ["platform_count"], 1)),
    firstSeenAt: toIsoTimestamp(row.first_seen_at ?? row.day),
    lastSeenAt: toIsoTimestamp(row.last_seen_at ?? row.updated_at ?? row.day),
  };
}

function parseStableDaySeriesRow(row: SupabaseTopicRow): StableTopicSeriesRow | null {
  const topicKey = readStringValue(row.topic_key ?? row.normalized_topic ?? row.topic);
  const bucket5m = toIsoMinute(row.bucket_5m ?? row.bucket_start ?? row.bucket_minute);
  const day = readStringValue(row.day || (bucket5m ? bucket5m.slice(0, 10) : ""));
  if (!topicKey || !bucket5m || !day) {
    return null;
  }

  const topicLabel = normalizeTopic(
    pickFirstString(row, ["topic_label", "topic_display", "topic", "normalized_topic", "topic_key"]) || topicKey,
  );
  const interactions = Math.max(0, pickFirstNumber(row, ["interactions", "mention_count", "value"], 0));
  const cumulativeInteractions = Math.max(
    interactions,
    pickFirstNumber(row, ["cumulative_interactions", "running_total"], interactions),
  );

  return {
    day,
    topicKey,
    topicLabel,
    bucket5m,
    interactions,
    cumulativeInteractions,
    updatedAt: toIsoTimestamp(row.updated_at ?? row.bucket_5m ?? row.bucket_start),
  };
}

function normalizeTopicIdentity(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9$#\s-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripTopicSigils(value: string) {
  return value.replace(/^[$#]+/, "");
}

function resolveStableTopicIdentity(topicKey: string, topicLabel: string) {
  const keyIdentity = normalizeTopicIdentity(topicKey);
  const labelIdentity = normalizeTopicIdentity(topicLabel);
  const labelBase = stripTopicSigils(labelIdentity);

  if (!keyIdentity) {
    return labelBase || labelIdentity;
  }

  if (!labelBase) {
    return stripTopicSigils(keyIdentity);
  }

  const strippedKey = stripTopicSigils(keyIdentity);
  if (strippedKey === labelBase) {
    return labelBase;
  }

  // Guard against known malformed key drift like `Trump -> rump`.
  if (
    strippedKey.length >= 2 &&
    strippedKey.length <= 4 &&
    labelBase.length === strippedKey.length + 1 &&
    strippedKey === labelBase.slice(1)
  ) {
    return labelBase;
  }

  return strippedKey;
}

function normalizeRequestedStableTopicKey(value: string | null | undefined) {
  if (!value) {
    return null;
  }

  const normalized = resolveStableTopicIdentity(value, value);
  return normalized.length > 0 ? normalized : null;
}

function parseIsoTimestamp(value: string | null | undefined) {
  if (!value) {
    return Number.NaN;
  }
  return Date.parse(value);
}

function minIsoTimestamp(
  left: string | null | undefined,
  right: string | null | undefined,
) {
  const leftMs = parseIsoTimestamp(left);
  const rightMs = parseIsoTimestamp(right);
  if (!Number.isFinite(leftMs)) {
    return Number.isFinite(rightMs) ? right ?? null : null;
  }
  if (!Number.isFinite(rightMs)) {
    return left ?? null;
  }
  return leftMs <= rightMs ? left ?? null : right ?? null;
}

function maxIsoTimestamp(
  left: string | null | undefined,
  right: string | null | undefined,
) {
  const leftMs = parseIsoTimestamp(left);
  const rightMs = parseIsoTimestamp(right);
  if (!Number.isFinite(leftMs)) {
    return Number.isFinite(rightMs) ? right ?? null : null;
  }
  if (!Number.isFinite(rightMs)) {
    return left ?? null;
  }
  return leftMs >= rightMs ? left ?? null : right ?? null;
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
    updatedAt: toIsoTimestamp(row.updated_at ?? row.created_at ?? row.bucket_minute),
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

function hashTopicKey(value: string) {
  let hash = 2166136261;

  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(36);
}

function buildTrendId(topic: string) {
  const slug = buildSlug(topic).slice(0, 64);
  const hash = hashTopicKey(topic.toLowerCase());
  return `trend-${slug}-${hash}`;
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
  const byInteractions = (left: RankedTrend, right: RankedTrend) =>
    interactionCount(right) - interactionCount(left) ||
    right.attentionInteractions - left.attentionInteractions ||
    left.id.localeCompare(right.id);

  sorted.sort((left, right) => {
    if (mode === "emerging") {
      if (sortKey === "velocity") {
        return byInteractions(left, right) ||
          (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          left.id.localeCompare(right.id);
      }
      if (sortKey === "novelty") {
        return byInteractions(left, right) ||
          (right.noveltyScore ?? 0) - (left.noveltyScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          left.id.localeCompare(right.id);
      }
      if (sortKey === "confirmation") {
        return byInteractions(left, right) ||
          (right.confirmationScore ?? 0) - (left.confirmationScore ?? 0) ||
          (right.breakoutScore ?? 0) - (left.breakoutScore ?? 0) ||
          left.id.localeCompare(right.id);
      }

      return byInteractions(left, right) ||
        (right.breakoutScore ?? right.emergingScore ?? 0) -
        (left.breakoutScore ?? left.emergingScore ?? 0) ||
        (right.velocityScore ?? 0) - (left.velocityScore ?? 0) ||
        left.id.localeCompare(right.id);
    }

    const interactionDelta = byInteractions(left, right);
    if (interactionDelta !== 0) return interactionDelta;

    if (sortKey === "growth") {
      return right.growthRate - left.growthRate ||
        left.id.localeCompare(right.id);
    }

    if (sortKey === "mentions") {
      return right.mentions - left.mentions ||
        left.id.localeCompare(right.id);
    }

    if (sortKey === "strength") {
      return right.trendStrengthScore - left.trendStrengthScore ||
        left.id.localeCompare(right.id);
    }

    return right.attentionScore - left.attentionScore ||
      left.id.localeCompare(right.id);
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

  const latestPoint = [...points]
    .reverse()
    .find((point) => Number.isFinite(Date.parse(point.timestamp))) ?? null;
  const latestDataPoint = [...points].reverse().find((point) => point.value > 0) ?? null;
  const endMs = Date.parse(windowEnd);
  const latestPointMs = latestPoint ? Date.parse(latestPoint.timestamp) : Number.NaN;
  const staleGapMinutes =
    Number.isFinite(endMs) && Number.isFinite(latestPointMs)
      ? Math.max(0, Math.round((endMs - latestPointMs) / 60_000))
      : null;
  const trailingGapBucketCount =
    staleGapMinutes !== null ? Math.max(0, Math.floor(staleGapMinutes / bucketMinutes)) : 0;

  return {
    range,
    windowStart,
    windowEnd,
    latestPointAt: latestPoint?.timestamp ?? null,
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
  latestSourceAt: string | null;
  freshnessProbe: SupabaseFreshnessProbeRow | null;
}): DashboardDataStatus {
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  const sourceSnapshotAt = resolveCanonicalSourceSnapshotAt(
    params.latestSourceAt,
    params.freshnessProbe,
  );
  const latestBucketMs = sourceSnapshotAt ? Date.parse(sourceSnapshotAt) : Number.NaN;
  const ageMinutes =
    Number.isFinite(latestBucketMs)
      ? Math.max(0, Math.round((nowMs - latestBucketMs) / 60_000))
      : null;
  const freshnessDiagnostics: DashboardFreshnessDiagnostics = {
    latestIngestionAt: params.freshnessProbe?.latestIngestionAt ?? null,
    latestProcessedAt: params.freshnessProbe?.latestProcessedAt ?? null,
    latestMentionEventAt: params.freshnessProbe?.latestMentionEventAt ?? null,
    latestReadModelFinalizeAt: params.freshnessProbe?.latestReadModelFinalizeAt ?? null,
    latestReadModelRollingWriteAt: params.freshnessProbe?.latestReadModelRollingWriteAt ?? null,
    latestReadModelSeriesWriteAt: params.freshnessProbe?.latestReadModelSeriesWriteAt ?? null,
    latestReadModelWindowEndAt: params.freshnessProbe?.latestReadModelWindowEndAt ?? null,
    latestSeriesNonZeroBucketAt: params.freshnessProbe?.latestSeriesNonZeroBucketAt ?? null,
    workerRunStartedAt: params.freshnessProbe?.workerRunStartedAt ?? null,
    workerRunStatus: params.freshnessProbe?.workerRunStatus ?? null,
    workerLastEventAt: params.freshnessProbe?.workerLastEventAt ?? null,
    workerRowsInserted: params.freshnessProbe?.workerRowsInserted ?? null,
    apiResponseAt: nowIso,
    sourceSnapshotAt,
    selectedTrendLatestDataAt: null,
    selectedTrendLatestPointAt: null,
    renderedStaleReferenceAt: null,
    renderedStaleReferenceSource: null,
    chainBreakStage: null,
    agesMinutes: {
      ingestion: ageMinutesFromIso(params.freshnessProbe?.latestIngestionAt, nowMs),
      processed: ageMinutesFromIso(params.freshnessProbe?.latestProcessedAt, nowMs),
      mentionEvent: ageMinutesFromIso(params.freshnessProbe?.latestMentionEventAt, nowMs),
      readModelFinalize: ageMinutesFromIso(params.freshnessProbe?.latestReadModelFinalizeAt, nowMs),
      readModelWrite: ageMinutesFromIso(
        params.freshnessProbe?.latestReadModelRollingWriteAt ??
          params.freshnessProbe?.latestReadModelSeriesWriteAt,
        nowMs,
      ),
      readModelWindowEnd: ageMinutesFromIso(params.freshnessProbe?.latestReadModelWindowEndAt, nowMs),
      workerLastEvent: ageMinutesFromIso(params.freshnessProbe?.workerLastEventAt, nowMs),
      sourceSnapshot: ageMinutesFromIso(sourceSnapshotAt, nowMs),
      selectedLatestPoint: null,
      selectedLatestData: null,
      renderedStaleReference: null,
    },
  };

  const workerStatus = String(freshnessDiagnostics.workerRunStatus ?? "").trim().toLowerCase();
  const workerStatusLooksHealthy =
    workerStatus.length === 0 ||
    workerStatus === "running" ||
    workerStatus === "active" ||
    workerStatus === "healthy" ||
    workerStatus === "success";
  const processingAge = freshnessDiagnostics.agesMinutes.processed;
  const ingestionAge = freshnessDiagnostics.agesMinutes.ingestion;
  const finalizeAge = freshnessDiagnostics.agesMinutes.readModelFinalize;
  const sourceAge = freshnessDiagnostics.agesMinutes.sourceSnapshot;
  if (ingestionAge !== null && ingestionAge > 15) {
    freshnessDiagnostics.chainBreakStage = "ingestion";
  } else if (!workerStatusLooksHealthy && ingestionAge === null) {
    freshnessDiagnostics.chainBreakStage = "ingestion";
  } else if (processingAge !== null && processingAge > 15) {
    freshnessDiagnostics.chainBreakStage = "processing";
  } else if (finalizeAge !== null && finalizeAge > 15) {
    freshnessDiagnostics.chainBreakStage = "read_model_refresh";
  } else if (sourceAge !== null && sourceAge > 15) {
    freshnessDiagnostics.chainBreakStage = "api_cache";
  } else {
    freshnessDiagnostics.chainBreakStage = "none";
  }

  return {
    stateSource: "supabase_live",
    bundleOrigin: null,
    showing: "supabase_live",
    serverNow: nowIso,
    runtimeSnapshotGeneratedAt: null,
    sourceSnapshotGeneratedAt: sourceSnapshotAt,
    latestFetchedAt: nowIso,
    runtimeSnapshotAvailable: false,
    localRawDataAvailable: params.rowCount > 0,
    runtimeSnapshotStale: false,
    sourceFreshness: [
      {
        sourceId: `supabase:${params.source}`,
        sourceLabel: params.source,
        platformId: "bluesky",
        sourceStatus:
          params.rowCount <= 0
            ? "empty"
            : ageMinutes === null
              ? "unknown"
              : ageMinutes <= 5
                ? "fresh"
                : ageMinutes <= 15
                  ? "delayed"
                  : "stale",
        itemCount: params.rowCount,
        lastFetchedAt: nowIso,
        latestCreatedAt: sourceSnapshotAt,
        ageMinutes,
      },
    ],
    freshnessDiagnostics,
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

function annotateSelectedFreshnessDiagnostics(vm: TrendDashboardVM): TrendDashboardVM {
  const diagnostics = vm.dataStatus?.freshnessDiagnostics;
  const detailWindow = vm.detail?.attentionWindow;
  if (!vm.dataStatus || !diagnostics || !detailWindow) {
    return vm;
  }

  const nowMs = Date.now();
  const selectedLatestPointAge = ageMinutesFromIso(detailWindow.latestPointAt, nowMs);
  const selectedLatestDataAge = ageMinutesFromIso(detailWindow.latestDataAt, nowMs);
  const sourceSnapshotAge = diagnostics.agesMinutes.sourceSnapshot;
  const renderedReferenceAt = diagnostics.sourceSnapshotAt ?? detailWindow.latestPointAt ?? null;
  const renderedReferenceSource = diagnostics.sourceSnapshotAt ? "source_snapshot" : "latest_point";
  const renderedReferenceAge = diagnostics.sourceSnapshotAt ? sourceSnapshotAge : selectedLatestPointAge;
  const renderMismatch =
    renderedReferenceSource !== "source_snapshot" &&
    sourceSnapshotAge !== null &&
    sourceSnapshotAge <= 6 &&
    selectedLatestDataAge !== null &&
    selectedLatestDataAge >= sourceSnapshotAge + 10;

  return {
    ...vm,
    dataStatus: {
      ...vm.dataStatus,
      freshnessDiagnostics: {
        ...diagnostics,
        selectedTrendLatestPointAt: detailWindow.latestPointAt ?? null,
        selectedTrendLatestDataAt: detailWindow.latestDataAt ?? null,
        renderedStaleReferenceAt: renderedReferenceAt,
        renderedStaleReferenceSource: renderedReferenceSource,
        chainBreakStage: renderMismatch ? "render_stale_field" : diagnostics.chainBreakStage,
        agesMinutes: {
          ...diagnostics.agesMinutes,
          selectedLatestPoint: selectedLatestPointAge,
          selectedLatestData: selectedLatestDataAge,
          renderedStaleReference: renderedReferenceAge,
        },
      },
    },
  };
}

function buildBlueskyOverview(
  rows: RankedTrend[],
  allPoints: TimeSeriesPoint[],
  range: DateRangePreset,
  bucketMinutes: number,
  sourceLagMinutes: number | null,
): TrendDashboardVM["blueskyOverview"] {
  const totalInteractions = rows.reduce(
    (sum, row) => sum + Math.max(0, row.attentionInteractions),
    0,
  );
  const replayWindow = buildWindow(allPoints, range, bucketMinutes);

  return {
    generatedAt: new Date().toISOString(),
    firehoseLagMinutes: sourceLagMinutes,
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

async function getSupabaseTrendDashboardStateLegacy(query: TrendDashboardQuery): Promise<TrendDashboardVM> {
  const window = buildWindowBuckets(query.range);
  const { rows, source } = await fetchTopicRows(
    window.windowStart.toISOString(),
    window.windowEnd.toISOString(),
  );
  const freshnessProbe = await fetchSupabaseFreshnessProbe();

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
        latestSourceAt: null,
        freshnessProbe,
      }),
    };
  }

  const bucketSet = new Set(window.buckets);
  const aggregateMap = new Map<string, TopicAggregate>();
  let latestSourceTimestampMs = Number.NaN;

  for (const row of parsedRows) {
    const bucketKey = bucketKeyForIso(row.bucketMinute, window.bucketMinutes);
    if (!bucketSet.has(bucketKey)) {
      continue;
    }
    const bucketMs = Date.parse(bucketKey);
    const sourceMs = Number.isFinite(bucketMs)
      ? bucketMs
      : row.updatedAt
        ? Date.parse(row.updatedAt)
        : Number.NaN;
    if (Number.isFinite(sourceMs)) {
      latestSourceTimestampMs = Number.isFinite(latestSourceTimestampMs)
        ? Math.max(latestSourceTimestampMs, sourceMs)
        : sourceMs;
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
  const legacyEnrichmentByTopicKey = await fetchTopicEnrichmentByTopicKeys(
    [...aggregateMap.keys()],
    window.windowEnd.toISOString(),
  );

  for (const topic of aggregateMap.values()) {
    const enrichment =
      legacyEnrichmentByTopicKey.get(topic.normalizedTopic) ??
      legacyEnrichmentByTopicKey.get(topic.normalizedTopic.toLowerCase()) ??
      null;
    const displayLabel = normalizeTopic(enrichment?.canonicalName ?? topic.normalizedTopic) || topic.normalizedTopic;
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
    const id = buildTrendId(topic.normalizedTopic);
    row.id = id;
    row.name = displayLabel;
    row.clusterId = id;
    row.clusterName = displayLabel;
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
    row.positiveMentions24h = topic.positiveCount;
    row.neutralMentions24h = topic.neutralCount;
    row.negativeMentions24h = topic.negativeCount;
    row.sentimentBalance = sentimentBalance;
    row.singleAuthorShare = 0;
    row.firstSeenAt = topic.firstSeenAt;
    row.lastSeenAt = topic.lastSeenAt;
    row.isSingleton = topic.postCount <= 1;
    row.trendCategory = enrichment?.trendCategory ?? null;
    row.trendDescription = enrichment?.shortDescription || null;
    row.trendContextParagraph = enrichment?.contextParagraph || null;
    row.trendRawLabel = enrichment?.rawLabel ?? topic.normalizedTopic;
    row.trendSummaryConfidence = enrichment ? enrichment.summaryConfidence : null;
    row.trendKeyEntities = enrichment?.keyEntities ?? null;
    row.trendEnrichment = enrichment
      ? {
          rawLabel: enrichment.rawLabel,
          canonicalName: enrichment.canonicalName,
          shortDescription: enrichment.shortDescription,
          contextParagraph: enrichment.contextParagraph,
          keyEntities: enrichment.keyEntities,
          trendCategory: enrichment.trendCategory,
          summaryConfidence: enrichment.summaryConfidence,
          modelName: enrichment.modelName,
          promptVersion: enrichment.promptVersion,
          generatedAt: enrichment.generatedAt,
          refreshedAt: enrichment.refreshedAt,
        }
      : null;
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
      leadingSignalLabel: displayLabel,
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
  const latestSourceAt = Number.isFinite(latestSourceTimestampMs)
    ? new Date(latestSourceTimestampMs).toISOString()
    : latestBucketAt;
  const sourceLagMinutes = ageMinutesFromIso(latestSourceAt, Date.now());

  const resolvedSelectedId = query.selectedId;
  const vm: TrendDashboardVM = {
    query: {
      ...query,
      mode: selectedMode,
      selectedId: resolvedSelectedId,
    },
    ingestionHealth: null,
    dataStatus: toSupabaseDataStatus({
      rowCount: parsedRows.length,
      source,
      latestSourceAt,
      freshnessProbe,
    }),
    blueskyOverview: buildBlueskyOverview(
      selectedLeaderboard,
      aggregatePoints,
      query.range,
      window.bucketMinutes,
      sourceLagMinutes,
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

  const selectedVm = applyTrendDashboardSelection(vm, resolvedSelectedId);
  return annotateSelectedFreshnessDiagnostics(
    attachSeriesWindows(selectedVm, query.range, window.bucketMinutes),
  );
}

function matchesStableScope(row: StableTopicDayTotalRow, scope: TrendDashboardQuery["scope"]) {
  if (scope !== "memes") {
    return true;
  }

  const haystack = `${row.topicLabel} ${row.topicKey}`.toLowerCase();
  return MEME_SCOPE_TERMS.some((term) => haystack.includes(term));
}

type SupabaseTrendDashboardStateOptions = {
  readProfile?: SupabaseTrendReadProfile;
};

async function getSupabaseTrendDashboardStateStable(
  query: TrendDashboardQuery,
  options: SupabaseTrendDashboardStateOptions = {},
): Promise<TrendDashboardVM> {
  const readProfile = options.readProfile ?? "summary";
  const window = buildWindowBuckets(query.range);
  const dayIsos = [...new Set([
    window.windowStart.toISOString().slice(0, 10),
    window.windowEnd.toISOString().slice(0, 10),
  ])];
  const freshnessProbe = await fetchSupabaseFreshnessProbe();
  const requestedStableTopicKey = normalizeRequestedStableTopicKey(query.selectedKey);
  const rollingTotalsResult = await fetchStableRollingTotals();
  const rawTotalsRows = rollingTotalsResult.rows;
  const totalsSource = rollingTotalsResult.source;

  const parsedTotalsRows = rawTotalsRows
    .map((row) => parseStableDayTotalRow(row))
    .filter((row): row is StableTopicDayTotalRow => Boolean(row))
    .filter((row) => matchesStableScope(row, query.scope));
  const aggregatedTotalsByStableKey = new Map<string, StableTopicDayTotalAggregateRow>();
  for (const row of parsedTotalsRows) {
    const resolvedStableTopicKey = resolveStableTopicIdentity(row.topicKey, row.topicLabel);
    const stableTopicKey =
      readProfile === "detail" &&
      requestedStableTopicKey &&
      row.topicKey === requestedStableTopicKey
        ? requestedStableTopicKey
        : resolvedStableTopicKey;
    if (!stableTopicKey) {
      continue;
    }

    const stableTopicLabel = normalizeTopic(row.topicLabel || stableTopicKey) || stableTopicKey;
    const existing = aggregatedTotalsByStableKey.get(stableTopicKey);
    if (!existing) {
      aggregatedTotalsByStableKey.set(stableTopicKey, {
        ...row,
        topicKey: stableTopicKey,
        topicLabel: stableTopicLabel,
        rawTopicKeys: [row.topicKey],
      });
      continue;
    }

    existing.totalMentions += row.totalMentions;
    existing.uniquePosts += row.uniquePosts;
    existing.uniqueAuthors += row.uniqueAuthors;
    existing.positiveCount += row.positiveCount;
    existing.neutralCount += row.neutralCount;
    existing.negativeCount += row.negativeCount;
    existing.platformCount = Math.max(existing.platformCount, row.platformCount);
    existing.firstSeenAt = minIsoTimestamp(existing.firstSeenAt, row.firstSeenAt);
    existing.lastSeenAt = maxIsoTimestamp(existing.lastSeenAt, row.lastSeenAt);
    if (!existing.rawTopicKeys.includes(row.topicKey)) {
      existing.rawTopicKeys.push(row.topicKey);
    }
  }

  const totalsRows = [...aggregatedTotalsByStableKey.values()]
    .sort((left, right) => {
      if (right.totalMentions !== left.totalMentions) {
        return right.totalMentions - left.totalMentions;
      }
      if (right.uniquePosts !== left.uniquePosts) {
        return right.uniquePosts - left.uniquePosts;
      }
      return left.topicKey.localeCompare(right.topicKey);
    })
    .slice(0, MAX_LEADERBOARD_ROWS);
  const enrichmentByTopicKey = await fetchTopicEnrichmentByTopicKeys(
    totalsRows.map((row) => row.topicKey),
    window.windowEnd.toISOString(),
  );

  if (totalsRows.length === 0) {
    const zero = createZeroTrendDashboardVM(query);
    return {
      ...zero,
      dataStatus: toSupabaseDataStatus({
        rowCount: rawTotalsRows.length,
        source: totalsSource,
        latestSourceAt: null,
        freshnessProbe,
      }),
    };
  }

  const selectedStableTopic = requestedStableTopicKey
    ? totalsRows.find((row) => row.topicKey === requestedStableTopicKey) ??
      (query.selectedId
        ? totalsRows.find((row) => buildTrendId(row.topicKey) === query.selectedId) ?? null
        : null)
    : query.selectedId
      ? totalsRows.find((row) => buildTrendId(row.topicKey) === query.selectedId) ?? null
      : null;
  const seededSeriesTopicLimit = query.selectedId
    ? readProfile === "detail"
      ? STABLE_DETAIL_SERIES_TOPIC_LIMIT
      : STABLE_SERIES_TOPIC_LIMIT
    : 0;
  const seriesSeedRows =
    seededSeriesTopicLimit > 0
      ? totalsRows.slice(0, seededSeriesTopicLimit)
      : [];
  const topicKeys = [...new Set([
    ...seriesSeedRows.flatMap((row) => row.rawTopicKeys),
    ...(selectedStableTopic?.rawTopicKeys ?? []),
  ])];
  const topicKeyToStableIdentity = new Map<string, string>();
  for (const row of totalsRows) {
    for (const rawTopicKey of row.rawTopicKeys) {
      topicKeyToStableIdentity.set(rawTopicKey, row.topicKey);
    }
  }
  const seriesFetchResults = topicKeys.length > 0
    ? await Promise.all(
      dayIsos.map((dayIso) => fetchStableDaySeries(
        dayIso,
        window.windowStart.toISOString(),
        window.windowEnd.toISOString(),
        topicKeys,
      )),
    )
    : [];
  const rawSeriesRows = seriesFetchResults.flatMap((result) => result.rows);
  const seriesSource = [...new Set(seriesFetchResults.map((result) => result.source))]
    .filter((source) => source.length > 0)
    .join("+");
  const allowedTopicKeys = new Set(topicKeys);
  const seriesRows = rawSeriesRows
    .map((row) => parseStableDaySeriesRow(row))
    .filter((row): row is StableTopicSeriesRow => Boolean(row))
    .filter((row) => allowedTopicKeys.has(row.topicKey));

  const bucketSet = new Set(window.buckets);
  const seriesByTopic = new Map<string, Map<string, number>>();
  let latestSourceTimestampMs = Number.NaN;

  for (const row of totalsRows) {
    const lastSeenMs = row.lastSeenAt ? Date.parse(row.lastSeenAt) : Number.NaN;
    if (Number.isFinite(lastSeenMs)) {
      latestSourceTimestampMs = Number.isFinite(latestSourceTimestampMs)
        ? Math.max(latestSourceTimestampMs, lastSeenMs)
        : lastSeenMs;
    }
  }

  for (const row of seriesRows) {
    const bucketKey = bucketKeyForIso(row.bucket5m, window.bucketMinutes);
    if (!bucketSet.has(bucketKey)) {
      continue;
    }

    const stableTopicKey =
      topicKeyToStableIdentity.get(row.topicKey) ??
      resolveStableTopicIdentity(row.topicKey, row.topicLabel);
    if (!stableTopicKey) {
      continue;
    }

    const topicSeries = seriesByTopic.get(stableTopicKey) ?? new Map<string, number>();
    topicSeries.set(bucketKey, (topicSeries.get(bucketKey) ?? 0) + row.interactions);
    seriesByTopic.set(stableTopicKey, topicSeries);

    const bucketMs = Date.parse(bucketKey);
    const updatedMs = row.updatedAt ? Date.parse(row.updatedAt) : Number.NaN;
    const sourceMs = Number.isFinite(bucketMs) ? bucketMs : updatedMs;
    if (Number.isFinite(sourceMs)) {
      latestSourceTimestampMs = Number.isFinite(latestSourceTimestampMs)
        ? Math.max(latestSourceTimestampMs, sourceMs)
        : sourceMs;
    }
  }

  const rankedBaseRows: RankedTrend[] = [];
  const allTrendTotalsByBucket = new Map<string, number>();
  const nowMs = Date.now();

  for (const topic of totalsRows) {
    const enrichment = enrichmentByTopicKey.get(topic.topicKey) ?? null;
    const displayLabel = normalizeTopic(enrichment?.canonicalName ?? topic.topicLabel) || topic.topicLabel;
    const topicSeries = seriesByTopic.get(topic.topicKey) ?? new Map<string, number>();
    const hasSeries = topicSeries.size > 0;
    const points: TimeSeriesPoint[] = window.buckets.map((bucketIso) => {
      const value = topicSeries.get(bucketIso) ?? 0;
      allTrendTotalsByBucket.set(bucketIso, (allTrendTotalsByBucket.get(bucketIso) ?? 0) + value);
      return {
        timestamp: bucketIso,
        value,
      };
    });

    const values = points.map((point) => point.value);
    const midpoint = Math.max(1, Math.floor(values.length / 2));
    const quarter = Math.max(1, Math.floor(values.length / 4));
    const previousHalf = hasSeries
      ? values.slice(0, midpoint).reduce((sum, value) => sum + value, 0)
      : 0;
    const recentHalf = hasSeries
      ? values.slice(midpoint).reduce((sum, value) => sum + value, 0)
      : 0;
    const previousQuarter = hasSeries
      ? values.slice(Math.max(0, values.length - quarter * 2), values.length - quarter)
        .reduce((sum, value) => sum + value, 0)
      : 0;
    const recentQuarter = hasSeries
      ? values.slice(values.length - quarter).reduce((sum, value) => sum + value, 0)
      : 0;
    const growthRate = hasSeries
      ? clamp(percentDelta(recentHalf, previousHalf), -100, 400)
      : 0;
    const acceleration = hasSeries
      ? clamp(percentDelta(recentQuarter, previousQuarter), -100, 400)
      : 0;

    const latestDataPoint = [...points].reverse().find((point) => point.value > 0);
    const latestDataMs = latestDataPoint
      ? Date.parse(latestDataPoint.timestamp)
      : Date.parse(topic.lastSeenAt ?? topic.firstSeenAt ?? "");
    const ageMinutes = Number.isFinite(latestDataMs)
      ? Math.max(0, Math.round((nowMs - latestDataMs) / 60_000))
      : 10_000;
    const freshnessState = getFreshnessState(ageMinutes);
    const freshnessScore = clamp(100 - ageMinutes * 1.6, 0, 100);

    const mentionCount = Math.max(0, topic.totalMentions);
    const postCount = Math.max(topic.uniquePosts, mentionCount > 0 ? 1 : 0);
    const uniqueAuthors = Math.max(0, topic.uniqueAuthors);
    const lowDataWarning = mentionCount < 8;
    const confidenceScore = clamp(
      20 +
        Math.log2(Math.max(1, mentionCount + postCount)) * 18 +
        Math.max(-10, Math.min(10, growthRate / 8)),
      0,
      100,
    );
    const velocityScore = hasSeries
      ? clamp(recentQuarter * (60 / window.bucketMinutes), 0, 1000)
      : clamp((mentionCount / Math.max(1, 24 * 60)) * 60, 0, 1000);
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
      Math.min(100, postCount * 2.2 + confidenceScore * 0.3),
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
      mentionCount * 0.15 +
        confidenceScore * 0.35 +
        Math.max(0, growthRate) * 0.12 +
        freshnessScore * 0.28,
      0,
      100,
    );

    const row = createZeroRankedTrend(query.scope, query.range);
    const id = buildTrendId(topic.topicKey);
    row.id = id;
    row.name = displayLabel;
    row.clusterId = id;
    row.clusterName = displayLabel;
    row.scope = query.scope;
    row.source = "bluesky";
    row.labelType = "entity_label";
    row.groupingSource = "fallback_singleton";
    row.leaderboardTier = lowDataWarning ? "secondary_singleton" : "primary_grouped";
    row.canonicalKeySummary = topic.topicKey;
    row.labelQualityScore = clamp(0.55 + (mentionCount > 20 ? 0.3 : 0.15), 0, 1);
    row.lowQualityLabel = lowDataWarning;
    row.aiAssisted = false;
    row.attentionInteractions = mentionCount;
    row.totalInteractions24h = mentionCount;
    row.qualityAdjustedScore = trendStrengthScore;
    row.attentionScore = clamp(
      mentionCount * 0.2 + trendStrengthScore * 0.35 + Math.max(0, growthRate) * 0.2,
      0,
      1000,
    );
    row.emergingScore = breakoutScore;
    row.breakoutScore = breakoutScore;
    row.velocityScore = velocityScore;
    row.noveltyScore = noveltyScore;
    row.confirmationScore = confirmationScore;
    row.rootsCount24h = postCount;
    row.uniqueAuthors24h = uniqueAuthors;
    row.positiveMentions24h = topic.positiveCount;
    row.neutralMentions24h = topic.neutralCount;
    row.negativeMentions24h = topic.negativeCount;
    row.sentimentBalance = sentimentBalance;
    row.singleAuthorShare = 0;
    row.firstSeenAt = topic.firstSeenAt;
    row.lastSeenAt = topic.lastSeenAt;
    row.isSingleton = postCount <= 1;
    row.trendCategory = enrichment?.trendCategory ?? null;
    row.trendDescription = enrichment?.shortDescription || null;
    row.trendContextParagraph = enrichment?.contextParagraph || null;
    row.trendRawLabel = enrichment?.rawLabel ?? topic.topicLabel;
    row.trendSummaryConfidence = enrichment ? enrichment.summaryConfidence : null;
    row.trendKeyEntities = enrichment?.keyEntities ?? null;
    row.trendEnrichment = enrichment
      ? {
          rawLabel: enrichment.rawLabel,
          canonicalName: enrichment.canonicalName,
          shortDescription: enrichment.shortDescription,
          contextParagraph: enrichment.contextParagraph,
          keyEntities: enrichment.keyEntities,
          trendCategory: enrichment.trendCategory,
          summaryConfidence: enrichment.summaryConfidence,
          modelName: enrichment.modelName,
          promptVersion: enrichment.promptVersion,
          generatedAt: enrichment.generatedAt,
          refreshedAt: enrichment.refreshedAt,
        }
      : null;
    row.contentType = null;
    row.spamLikelihood = 0;
    row.templateLikelihood = 0;
    row.contextualCoherence = undefined;
    row.lowInformation = lowDataWarning;
    row.templateSeries = false;
    row.confidenceScore = confidenceScore;
    row.freshnessScore = freshnessScore;
    row.freshnessState = freshnessState;
    row.sampleSize = mentionCount;
    row.supportingThreadCount = postCount;
    row.lowDataWarning = lowDataWarning;
    row.growthRate = growthRate;
    row.attentionAcceleration = acceleration;
    row.mentions = mentionCount;
    row.platforms = ["bluesky"];
    row.platformSpread = Math.max(1, topic.platformCount);
    row.confirmedPlatformSpread = Math.max(1, topic.platformCount);
    row.attentionHistory = points;
    row.platformBreakdown = [
      {
        platformId: "bluesky",
        interactions: mentionCount,
        sharePct: 100,
      },
    ];
    row.topPosts = [];
    row.lifecycleStage = getLifecycleStage(growthRate, freshnessState);
    row.originPlatform = "bluesky";
    row.platformMigrationPath = ["bluesky"];
    row.attentionDrivers = [
      {
        platformId: "bluesky",
        contributionPct: 100,
        deltaPct: 0,
      },
    ];
    row.hasSpike = acceleration >= 60;
    row.spikeMagnitude = row.hasSpike ? clamp(acceleration / 100, 0, 4) : 0;
    row.trendStrengthScore = trendStrengthScore;
    row.persistenceScore = clamp(confidenceScore * 0.6 + postCount * 0.9, 0, 100);
    row.isEarlyTrend = postCount <= 5 && growthRate >= 20;
    row.positionChange24h = 0;
    row.googleSearchInterest = null;
    row.blueskySummary = {
      attentionSharePct: 100,
      postCount,
      uniqueAuthorCount: uniqueAuthors,
      amplifierCount: 0,
      topAmplifierHandle: null,
      leadingSignalLabel: displayLabel,
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
  const latestSourceAt = Number.isFinite(latestSourceTimestampMs)
    ? new Date(latestSourceTimestampMs).toISOString()
    : latestBucketAt;
  const sourceLagMinutes = ageMinutesFromIso(latestSourceAt, Date.now());
  const sourceLabel = seriesSource ? `${totalsSource}+${seriesSource}` : totalsSource;

  const resolvedSelectedId =
    query.selectedId ??
    (selectedStableTopic ? buildTrendId(selectedStableTopic.topicKey) : undefined);
  const vm: TrendDashboardVM = {
    query: {
      ...query,
      mode: selectedMode,
      selectedId: resolvedSelectedId,
    },
    ingestionHealth: null,
    dataStatus: toSupabaseDataStatus({
      rowCount: totalsRows.length + seriesRows.length,
      source: sourceLabel,
      latestSourceAt,
      freshnessProbe,
    }),
    blueskyOverview: buildBlueskyOverview(
      selectedLeaderboard,
      aggregatePoints,
      query.range,
      window.bucketMinutes,
      sourceLagMinutes,
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

  const selectedVm = applyTrendDashboardSelection(vm, resolvedSelectedId);
  return annotateSelectedFreshnessDiagnostics(
    attachSeriesWindows(selectedVm, query.range, window.bucketMinutes),
  );
}

function shouldUseStableTrendReadModel() {
  return readBooleanEnv(process.env.USE_STABLE_TOPIC_READ_MODEL, true);
}

export async function getSupabaseTrendDashboardState(
  query: TrendDashboardQuery,
  options: SupabaseTrendDashboardStateOptions = {},
): Promise<TrendDashboardVM> {
  if (shouldUseStableTrendReadModel()) {
    try {
      return await getSupabaseTrendDashboardStateStable(query, options);
    } catch (error) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[supabase-trends] stable read model unavailable, falling back to legacy path", {
          error,
        });
      }
    }
  }

  return getSupabaseTrendDashboardStateLegacy(query);
}

export function shouldUseSupabaseTrendSource() {
  const explicit = process.env.USE_SUPABASE_TRENDS;
  if (typeof explicit === "string" && explicit.trim().length > 0) {
    return readBooleanEnv(explicit, false);
  }

  // Auto-enable DB-backed trend reads when credentials are available so
  // deployed environments don't fall back to empty runtime snapshots by default.
  return hasSupabaseServerCredentials() || hasDatabaseUrl();
}
