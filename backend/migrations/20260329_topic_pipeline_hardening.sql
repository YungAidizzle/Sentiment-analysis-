BEGIN;

CREATE TABLE IF NOT EXISTS public.topic_alias_rules (
    alias_key TEXT PRIMARY KEY,
    canonical_key TEXT NOT NULL,
    canonical_label TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'entity',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_topic_alias_rules_canonical_key
    ON public.topic_alias_rules (canonical_key);

INSERT INTO public.topic_alias_rules (
    alias_key,
    canonical_key,
    canonical_label,
    entity_type,
    confidence,
    is_active,
    updated_at
) VALUES
    ('no kings', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('no king', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('nokings', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('no kingss', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('no kings s', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('no-kings', 'no kings', 'No Kings', 'movement', 0.99, true, now()),
    ('usa', 'usa', 'USA', 'country', 0.92, true, now()),
    ('america', 'usa', 'USA', 'country', 0.92, true, now()),
    ('american', 'usa', 'USA', 'country', 0.92, true, now()),
    ('americans', 'usa', 'USA', 'country', 0.92, true, now()),
    ('united states', 'usa', 'USA', 'country', 0.92, true, now()),
    ('unitedstates', 'usa', 'USA', 'country', 0.92, true, now()),
    ('iran', 'iran', 'Iran', 'country', 0.92, true, now()),
    ('iranian', 'iran', 'Iran', 'country', 0.92, true, now()),
    ('iranians', 'iran', 'Iran', 'country', 0.92, true, now()),
    ('bluesky', 'bluesky', 'Bluesky', 'platform', 0.96, true, now()),
    ('bsky', 'bluesky', 'Bluesky', 'platform', 0.96, true, now()),
    ('nyse', 'nyse', 'NYSE', 'market', 0.98, true, now()),
    ('new york stock exchange', 'nyse', 'NYSE', 'market', 0.98, true, now()),
    ('polymarket', 'polymarket', 'Polymarket', 'platform', 0.97, true, now()),
    ('poly market', 'polymarket', 'Polymarket', 'platform', 0.97, true, now()),
    ('kalshi', 'kalshi', 'Kalshi', 'platform', 0.97, true, now()),
    ('kal shi', 'kalshi', 'Kalshi', 'platform', 0.97, true, now())
ON CONFLICT (alias_key) DO UPDATE
SET canonical_key = EXCLUDED.canonical_key,
    canonical_label = EXCLUDED.canonical_label,
    entity_type = EXCLUDED.entity_type,
    confidence = EXCLUDED.confidence,
    is_active = true,
    updated_at = now();

ALTER TABLE public.post_topic_mentions
    ADD COLUMN IF NOT EXISTS topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE public.topic_buckets_1m_final
    ADD COLUMN IF NOT EXISTS avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE public.topic_day_totals
    ADD COLUMN IF NOT EXISTS avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS public.topic_read_model_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    last_finalize_before TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.topic_rolling_24h (
    topic_key TEXT PRIMARY KEY,
    topic_label TEXT NOT NULL,
    platform_count INTEGER NOT NULL DEFAULT 1,
    total_mentions INTEGER NOT NULL DEFAULT 0,
    unique_posts INTEGER NOT NULL DEFAULT 0,
    unique_authors INTEGER NOT NULL DEFAULT 0,
    positive_count INTEGER NOT NULL DEFAULT 0,
    neutral_count INTEGER NOT NULL DEFAULT 0,
    negative_count INTEGER NOT NULL DEFAULT 0,
    avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_topic_rolling_24h_mentions
    ON public.topic_rolling_24h (total_mentions DESC);

CREATE OR REPLACE VIEW public.v_topic_leaderboard_day AS
SELECT
    day,
    topic_key,
    topic_label,
    platform_count,
    total_mentions,
    unique_posts,
    unique_authors,
    positive_count,
    neutral_count,
    negative_count,
    avg_topic_confidence,
    first_seen_at,
    last_seen_at,
    updated_at
FROM public.topic_day_totals;

CREATE OR REPLACE VIEW public.v_topic_series_day_5m AS
SELECT
    day,
    bucket_5m,
    topic_key,
    topic_label,
    interactions,
    cumulative_interactions,
    updated_at
FROM public.topic_day_series_5m;

CREATE OR REPLACE VIEW public.v_topic_leaderboard_rolling_24h AS
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
    avg_topic_confidence,
    first_seen_at,
    last_seen_at,
    window_start,
    window_end,
    updated_at
FROM public.topic_rolling_24h;

DO $$
DECLARE
    fn RECORD;
BEGIN
    FOR fn IN
        SELECT
            p.proname AS function_name,
            pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p
        JOIN pg_namespace n
          ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname IN (
              'refresh_topic_buckets_1m_final',
              'refresh_topic_day_totals',
              'refresh_topic_day_series_5m',
              'refresh_topic_read_models'
          )
    LOOP
        EXECUTE format(
            'DROP FUNCTION IF EXISTS public.%I(%s) CASCADE',
            fn.function_name,
            fn.args
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.refresh_topic_day_totals(p_day date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows integer := 0;
BEGIN
    DELETE FROM public.topic_day_totals
    WHERE day = p_day;

    INSERT INTO public.topic_day_totals (
        day,
        topic_key,
        topic_label,
        platform_count,
        total_mentions,
        unique_posts,
        unique_authors,
        positive_count,
        neutral_count,
        negative_count,
        avg_topic_confidence,
        first_seen_at,
        last_seen_at,
        updated_at
    )
    SELECT
        p_day AS day,
        b.topic_key,
        COALESCE(t.canonical_label, INITCAP(b.topic_key)) AS topic_label,
        COUNT(DISTINCT b.platform)::int AS platform_count,
        SUM(b.mention_count)::int AS total_mentions,
        SUM(b.unique_posts)::int AS unique_posts,
        SUM(b.unique_authors)::int AS unique_authors,
        SUM(b.positive_count)::int AS positive_count,
        SUM(b.neutral_count)::int AS neutral_count,
        SUM(b.negative_count)::int AS negative_count,
        CASE
            WHEN SUM(b.mention_count) > 0
                THEN SUM(b.avg_topic_confidence * b.mention_count)::double precision
                     / SUM(b.mention_count)::double precision
            ELSE 0::double precision
        END AS avg_topic_confidence,
        MIN(b.bucket_minute) AS first_seen_at,
        MAX(b.bucket_minute) AS last_seen_at,
        now() AS updated_at
    FROM public.topic_buckets_1m_final b
    LEFT JOIN public.topics t
      ON t.topic_key = b.topic_key
    WHERE (b.bucket_minute AT TIME ZONE 'utc')::date = p_day
    GROUP BY b.topic_key, COALESCE(t.canonical_label, INITCAP(b.topic_key));

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN COALESCE(v_rows, 0);
END;
$$;

CREATE OR REPLACE FUNCTION public.refresh_topic_day_series_5m(p_day date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows integer := 0;
BEGIN
    DELETE FROM public.topic_day_series_5m
    WHERE day = p_day;

    WITH day_bounds AS (
        SELECT
            (p_day::text || ' 00:00:00+00')::timestamptz AS day_start,
            ((p_day + 1)::text || ' 00:00:00+00')::timestamptz AS day_end
    ),
    buckets AS (
        SELECT
            generate_series(day_start, day_end - interval '5 minute', interval '5 minute') AS bucket_5m
        FROM day_bounds
    ),
    topics_of_day AS (
        SELECT
            d.topic_key,
            d.topic_label
        FROM public.topic_day_totals d
        WHERE d.day = p_day
          AND d.total_mentions >= 2
        ORDER BY d.total_mentions DESC, d.topic_key ASC
        LIMIT 300
    ),
    aggregated AS (
        SELECT
            to_timestamp(floor(extract(epoch FROM b.bucket_minute) / 300) * 300)::timestamptz AS bucket_5m,
            b.topic_key,
            SUM(b.mention_count)::int AS interactions
        FROM public.topic_buckets_1m_final b
        JOIN day_bounds db
          ON b.bucket_minute >= db.day_start
         AND b.bucket_minute < db.day_end
        JOIN topics_of_day td
          ON td.topic_key = b.topic_key
        GROUP BY 1, 2
    ),
    filled AS (
        SELECT
            p_day AS day,
            bk.bucket_5m,
            td.topic_key,
            td.topic_label,
            COALESCE(ag.interactions, 0)::int AS interactions
        FROM topics_of_day td
        CROSS JOIN buckets bk
        LEFT JOIN aggregated ag
          ON ag.topic_key = td.topic_key
         AND ag.bucket_5m = bk.bucket_5m
    )
    INSERT INTO public.topic_day_series_5m (
        day,
        bucket_5m,
        topic_key,
        topic_label,
        interactions,
        cumulative_interactions,
        updated_at
    )
    SELECT
        day,
        bucket_5m,
        topic_key,
        topic_label,
        interactions,
        SUM(interactions) OVER (
            PARTITION BY topic_key
            ORDER BY bucket_5m
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::int AS cumulative_interactions,
        now() AS updated_at
    FROM filled;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN COALESCE(v_rows, 0);
END;
$$;

CREATE OR REPLACE FUNCTION public.refresh_topic_read_models(
    p_lag_minutes integer,
    p_recompute_hours integer
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    v_lag_minutes integer := GREATEST(1, COALESCE(p_lag_minutes, 3));
    v_recompute_hours integer := GREATEST(1, COALESCE(p_recompute_hours, 48));
    v_finalize_before timestamptz;
    v_hard_recompute_from timestamptz;
    v_recompute_from timestamptz;
    v_finalized_rows integer := 0;
    v_totals_today integer := 0;
    v_totals_yesterday integer := 0;
    v_series_today integer := 0;
    v_series_yesterday integer := 0;
    v_rolling_rows integer := 0;
    v_today date := (now() AT TIME ZONE 'utc')::date;
    v_yesterday date := ((now() AT TIME ZONE 'utc')::date - 1);
    v_window_end timestamptz;
    v_window_start timestamptz;
BEGIN
    v_finalize_before := date_trunc('minute', now() - make_interval(mins => v_lag_minutes));
    v_hard_recompute_from := date_trunc('minute', now() - make_interval(hours => v_recompute_hours));

    SELECT
        GREATEST(
            v_hard_recompute_from,
            COALESCE(last_finalize_before - interval '45 minute', v_hard_recompute_from)
        )
    INTO v_recompute_from
    FROM public.topic_read_model_state
    WHERE id = 1;

    IF v_recompute_from IS NULL THEN
        v_recompute_from := v_hard_recompute_from;
    END IF;

    IF v_recompute_from >= v_finalize_before THEN
        v_recompute_from := v_finalize_before - interval '1 minute';
    END IF;

    DELETE FROM public.topic_buckets_1m_final
    WHERE bucket_minute >= v_recompute_from
      AND bucket_minute < v_finalize_before;

    INSERT INTO public.topic_buckets_1m_final (
        bucket_minute,
        platform,
        topic_key,
        mention_count,
        unique_posts,
        unique_authors,
        positive_count,
        neutral_count,
        negative_count,
        avg_topic_confidence,
        finalized_at
    )
    SELECT
        date_trunc('minute', m.event_timestamp) AS bucket_minute,
        m.platform,
        m.topic_key,
        COUNT(*)::int AS mention_count,
        COUNT(DISTINCT m.raw_post_id)::int AS unique_posts,
        COUNT(DISTINCT NULLIF(m.author_id, ''))::int AS unique_authors,
        SUM(CASE WHEN m.sentiment_label = 'positive' THEN 1 ELSE 0 END)::int AS positive_count,
        SUM(CASE WHEN m.sentiment_label = 'neutral' THEN 1 ELSE 0 END)::int AS neutral_count,
        SUM(CASE WHEN m.sentiment_label = 'negative' THEN 1 ELSE 0 END)::int AS negative_count,
        AVG(COALESCE(m.topic_confidence, 0))::double precision AS avg_topic_confidence,
        now() AS finalized_at
    FROM public.post_topic_mentions m
    WHERE date_trunc('minute', m.event_timestamp) >= v_recompute_from
      AND date_trunc('minute', m.event_timestamp) < v_finalize_before
      AND COALESCE(m.topic_confidence, 0) >= 0.34
    GROUP BY 1, 2, 3
    ON CONFLICT (bucket_minute, platform, topic_key) DO UPDATE
    SET mention_count = EXCLUDED.mention_count,
        unique_posts = EXCLUDED.unique_posts,
        unique_authors = EXCLUDED.unique_authors,
        positive_count = EXCLUDED.positive_count,
        neutral_count = EXCLUDED.neutral_count,
        negative_count = EXCLUDED.negative_count,
        avg_topic_confidence = EXCLUDED.avg_topic_confidence,
        finalized_at = EXCLUDED.finalized_at;

    GET DIAGNOSTICS v_finalized_rows = ROW_COUNT;

    INSERT INTO public.topic_read_model_state (id, last_finalize_before, updated_at)
    VALUES (1, v_finalize_before, now())
    ON CONFLICT (id) DO UPDATE
    SET last_finalize_before = EXCLUDED.last_finalize_before,
        updated_at = now();

    v_totals_today := public.refresh_topic_day_totals(v_today);
    v_totals_yesterday := public.refresh_topic_day_totals(v_yesterday);
    v_series_today := public.refresh_topic_day_series_5m(v_today);
    v_series_yesterday := public.refresh_topic_day_series_5m(v_yesterday);

    v_window_end := v_finalize_before;
    v_window_start := v_window_end - interval '24 hour';

    DELETE FROM public.topic_rolling_24h;

    INSERT INTO public.topic_rolling_24h (
        topic_key,
        topic_label,
        platform_count,
        total_mentions,
        unique_posts,
        unique_authors,
        positive_count,
        neutral_count,
        negative_count,
        avg_topic_confidence,
        first_seen_at,
        last_seen_at,
        window_start,
        window_end,
        updated_at
    )
    SELECT
        m.topic_key,
        COALESCE(t.canonical_label, MAX(NULLIF(TRIM(m.topic_label), '')), INITCAP(m.topic_key)) AS topic_label,
        COUNT(DISTINCT m.platform)::int AS platform_count,
        COUNT(*)::int AS total_mentions,
        COUNT(DISTINCT m.raw_post_id)::int AS unique_posts,
        COUNT(DISTINCT NULLIF(m.author_id, ''))::int AS unique_authors,
        SUM(CASE WHEN m.sentiment_label = 'positive' THEN 1 ELSE 0 END)::int AS positive_count,
        SUM(CASE WHEN m.sentiment_label = 'neutral' THEN 1 ELSE 0 END)::int AS neutral_count,
        SUM(CASE WHEN m.sentiment_label = 'negative' THEN 1 ELSE 0 END)::int AS negative_count,
        AVG(COALESCE(m.topic_confidence, 0))::double precision AS avg_topic_confidence,
        MIN(m.event_timestamp) AS first_seen_at,
        MAX(m.event_timestamp) AS last_seen_at,
        v_window_start,
        v_window_end,
        now()
    FROM public.post_topic_mentions m
    LEFT JOIN public.topics t
      ON t.topic_key = m.topic_key
    WHERE m.event_timestamp >= v_window_start
      AND m.event_timestamp < v_window_end
      AND COALESCE(m.topic_confidence, 0) >= 0.34
    GROUP BY m.topic_key, t.canonical_label;

    GET DIAGNOSTICS v_rolling_rows = ROW_COUNT;

    RETURN jsonb_build_object(
        'finalized_bucket_rows', COALESCE(v_finalized_rows, 0),
        'day_totals_today_rows', COALESCE(v_totals_today, 0),
        'day_totals_yesterday_rows', COALESCE(v_totals_yesterday, 0),
        'day_series_today_rows', COALESCE(v_series_today, 0),
        'day_series_yesterday_rows', COALESCE(v_series_yesterday, 0),
        'rolling_24h_rows', COALESCE(v_rolling_rows, 0),
        'rolling_window_start', v_window_start,
        'rolling_window_end', v_window_end,
        'refreshed_at', now()
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.refresh_topic_buckets_1m_final(
    p_lag_minutes integer,
    p_recompute_hours integer
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_payload jsonb;
BEGIN
    v_payload := public.refresh_topic_read_models(
        COALESCE(p_lag_minutes, 3),
        COALESCE(p_recompute_hours, 48)
    );
    RETURN COALESCE((v_payload ->> 'finalized_bucket_rows')::integer, 0);
END;
$$;

COMMIT;
