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

type StableTopicSeriesRow = {
  day: string;
  topicKey: string;
  topicLabel: string;
  bucket5m: string;
  interactions: number;
  cumulativeInteractions: number;
  updatedAt: string | null;
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
const STABLE_TOPIC_DAY_TOTALS_TABLE = "topic_day_totals";
const STABLE_TOPIC_DAY_TOTALS_VIEW = "v_topic_leaderboard_day";
const STABLE_TOPIC_DAY_SERIES_TABLE = "topic_day_series_5m";
const STABLE_TOPIC_DAY_SERIES_VIEW = "v_topic_series_day_5m";
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

async function fetchStableDayTotalsFromSupabaseSource(source: string, dayIso: string) {
  const client = getSupabaseServerClient();
  const rows: SupabaseTopicRow[] = [];

  for (let offset = 0; offset < MAX_QUERY_ROWS; offset += PAGE_SIZE) {
    const end = offset + PAGE_SIZE - 1;
    const { data, error } = await client
      .from(source)
      .select("*")
      .eq("day", dayIso)
      .order("total_mentions", { ascending: false })
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

async function fetchStableRollingTotalsFromSupabaseSource(source: string) {
  const client = getSupabaseServerClient();
  const rows: SupabaseTopicRow[] = [];

  for (let offset = 0; offset < MAX_QUERY_ROWS; offset += PAGE_SIZE) {
    const end = offset + PAGE_SIZE - 1;
    const { data, error } = await client
      .from(source)
      .select("*")
      .order("total_mentions", { ascending: false })
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

async function fetchStableDayTotalsFromPostgresSource(source: string, dayIso: string) {
  const pool = getServerPostgresPool();
  const query = `
    SELECT *
    FROM public.${source}
    WHERE day = $1::date
    ORDER BY total_mentions DESC
    LIMIT $2
  `;
  const result = await pool.query<Record<string, unknown>>(query, [
    dayIso,
    MAX_QUERY_ROWS,
  ]);
  return result.rows as SupabaseTopicRow[];
}

async function fetchStableRollingTotalsFromPostgresSource(source: string) {
  const pool = getServerPostgresPool();
  const query = `
    SELECT *
    FROM public.${source}
    ORDER BY total_mentions DESC, topic_key ASC
    LIMIT $1
  `;
  const result = await pool.query<Record<string, unknown>>(query, [MAX_QUERY_ROWS]);
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

async function fetchStableDayTotals(dayIso: string) {
  const sources = [STABLE_TOPIC_DAY_TOTALS_TABLE, STABLE_TOPIC_DAY_TOTALS_VIEW];
  if (hasSupabaseServerCredentials()) {
    let lastError: PostgrestError | null = null;
    for (const source of sources) {
      try {
        return {
          rows: await fetchStableDayTotalsFromSupabaseSource(source, dayIso),
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
          rows: await fetchStableDayTotalsFromPostgresSource(source, dayIso),
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
    "Stable topic day totals source not found. Create topic_day_totals or v_topic_leaderboard_day.",
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
      .select("*")
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
      rows.push(...data);
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
  const keyChunks = chunkArray(topicKeys, 500);

  for (const topicKeyChunk of keyChunks) {
    const query = `
      SELECT *
      FROM public.${source}
      WHERE day = $1::date
        AND bucket_5m >= $2::timestamptz
        AND bucket_5m <= $3::timestamptz
        AND topic_key = ANY($4::text[])
      ORDER BY bucket_5m ASC
      LIMIT $5
    `;
    const result = await pool.query<Record<string, unknown>>(query, [
      dayIso,
      windowStartIso,
      windowEndIso,
      topicKeyChunk,
      MAX_QUERY_ROWS,
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
  latestSourceAt: string | null;
}): DashboardDataStatus {
  const nowIso = new Date().toISOString();
  const latestBucketMs = params.latestSourceAt ? Date.parse(params.latestSourceAt) : Number.NaN;
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
    sourceSnapshotGeneratedAt: params.latestSourceAt,
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
        latestCreatedAt: params.latestSourceAt,
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

async function getSupabaseTrendDashboardStateLegacy(query: TrendDashboardQuery): Promise<TrendDashboardVM> {
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
        latestSourceAt: null,
      }),
    };
  }

  const bucketSet = new Set(window.buckets);
  const aggregateMap = new Map<string, TopicAggregate>();
  let latestSourceTimestampMs = Number.NaN;

  for (const row of parsedRows) {
    const updatedMs = row.updatedAt ? Date.parse(row.updatedAt) : Number.NaN;
    if (Number.isFinite(updatedMs)) {
      latestSourceTimestampMs = Number.isFinite(latestSourceTimestampMs)
        ? Math.max(latestSourceTimestampMs, updatedMs)
        : updatedMs;
    }

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
    const id = buildTrendId(topic.normalizedTopic);
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
  const latestSourceAt = Number.isFinite(latestSourceTimestampMs)
    ? new Date(latestSourceTimestampMs).toISOString()
    : latestBucketAt;

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
      latestSourceAt,
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

function matchesStableScope(row: StableTopicDayTotalRow, scope: TrendDashboardQuery["scope"]) {
  if (scope !== "memes") {
    return true;
  }

  const haystack = `${row.topicLabel} ${row.topicKey}`.toLowerCase();
  return MEME_SCOPE_TERMS.some((term) => haystack.includes(term));
}

async function getSupabaseTrendDashboardStateStable(query: TrendDashboardQuery): Promise<TrendDashboardVM> {
  const window = buildWindowBuckets(query.range);
  const dayIsos = [...new Set([
    window.windowStart.toISOString().slice(0, 10),
    window.windowEnd.toISOString().slice(0, 10),
  ])];
  const rollingTotalsResult = await fetchStableRollingTotals();
  const rawTotalsRows = rollingTotalsResult.rows;
  const totalsSource = rollingTotalsResult.source;

  const parsedTotalsRows = rawTotalsRows
    .map((row) => parseStableDayTotalRow(row))
    .filter((row): row is StableTopicDayTotalRow => Boolean(row))
    .filter((row) => matchesStableScope(row, query.scope));
  const totalsRows = [...parsedTotalsRows]
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

  if (totalsRows.length === 0) {
    const zero = createZeroTrendDashboardVM(query);
    return {
      ...zero,
      dataStatus: toSupabaseDataStatus({
        rowCount: rawTotalsRows.length,
        source: totalsSource,
        latestSourceAt: null,
      }),
    };
  }

  const topicKeys = totalsRows.map((row) => row.topicKey);
  const seriesFetchResults = await Promise.all(
    dayIsos.map((dayIso) => fetchStableDaySeries(
      dayIso,
      window.windowStart.toISOString(),
      window.windowEnd.toISOString(),
      topicKeys,
    )),
  );
  const rawSeriesRows = seriesFetchResults.flatMap((result) => result.rows);
  const seriesSource = [...new Set(seriesFetchResults.map((result) => result.source))].join("+");
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

    const topicSeries = seriesByTopic.get(row.topicKey) ?? new Map<string, number>();
    topicSeries.set(bucketKey, (topicSeries.get(bucketKey) ?? 0) + row.interactions);
    seriesByTopic.set(row.topicKey, topicSeries);

    const updatedMs = row.updatedAt ? Date.parse(row.updatedAt) : Number.NaN;
    const sourceMs = Number.isFinite(updatedMs) ? updatedMs : Date.parse(bucketKey);
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
    const topicSeries = seriesByTopic.get(topic.topicKey) ?? new Map<string, number>();
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
    row.name = topic.topicLabel;
    row.clusterId = id;
    row.clusterName = topic.topicLabel;
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
    row.singleAuthorShare = 0;
    row.firstSeenAt = topic.firstSeenAt;
    row.lastSeenAt = topic.lastSeenAt;
    row.isSingleton = postCount <= 1;
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
      leadingSignalLabel: topic.topicLabel,
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

  const vm: TrendDashboardVM = {
    query: {
      ...query,
      mode: selectedMode,
      selectedId: query.selectedId,
    },
    ingestionHealth: null,
    dataStatus: toSupabaseDataStatus({
      rowCount: totalsRows.length + seriesRows.length,
      source: `${totalsSource}+${seriesSource}`,
      latestSourceAt,
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

function shouldUseStableTrendReadModel() {
  return readBooleanEnv(process.env.USE_STABLE_TOPIC_READ_MODEL, true);
}

export async function getSupabaseTrendDashboardState(query: TrendDashboardQuery): Promise<TrendDashboardVM> {
  if (shouldUseStableTrendReadModel()) {
    try {
      return await getSupabaseTrendDashboardStateStable(query);
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
