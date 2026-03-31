-- Verify cross-table consistency after rebuild.
-- Replace topic keys in sample_topics as needed.

-- 1) Sanity checks for worker/run state and freshness.
SELECT source, started_at, ended_at, status, rows_inserted
FROM public.ingestion_runs
WHERE source = 'bluesky_firehose_worker'
ORDER BY started_at DESC
LIMIT 5;

SELECT
    (SELECT MAX(event_timestamp) FROM public.post_topic_mentions) AS latest_mention_event_at,
    (SELECT MAX(bucket_minute) FROM public.topic_buckets_1m_final) AS latest_bucket_final_at,
    (SELECT MAX(window_end) FROM public.topic_rolling_24h) AS latest_rolling_window_end_at,
    (SELECT MAX(bucket_5m) FROM public.topic_day_series_5m WHERE interactions > 0) AS latest_series_non_zero_bucket_at,
    (SELECT last_finalize_before FROM public.topic_read_model_state WHERE id = 1) AS last_finalize_before;

-- 2) 24h consistency: mentions vs rolling totals for sample topics.
WITH sample_topics AS (
    SELECT unnest(ARRAY[
        'epstein web image posts',
        'epstein web posts',
        'trump'
    ]::text[]) AS topic_key
),
window_bounds AS (
    SELECT
        COALESCE((SELECT MAX(window_end) FROM public.topic_rolling_24h), now()) AS window_end
),
base_mentions AS (
    SELECT
        m.topic_key,
        COUNT(*)::int AS mentions_24h,
        COUNT(DISTINCT m.raw_post_id)::int AS unique_posts_24h,
        COUNT(DISTINCT NULLIF(m.author_id, ''))::int AS unique_authors_24h
    FROM public.post_topic_mentions m
    CROSS JOIN window_bounds wb
    WHERE m.event_timestamp >= wb.window_end - interval '24 hour'
      AND m.event_timestamp < wb.window_end
      AND m.topic_key IN (SELECT topic_key FROM sample_topics)
    GROUP BY m.topic_key
)
SELECT
    st.topic_key,
    COALESCE(bm.mentions_24h, 0) AS post_topic_mentions_24h,
    COALESCE(bm.unique_posts_24h, 0) AS post_topic_mentions_unique_posts_24h,
    COALESCE(bm.unique_authors_24h, 0) AS post_topic_mentions_unique_authors_24h,
    COALESCE(r.total_mentions, 0) AS topic_rolling_24h_mentions,
    COALESCE(r.unique_posts, 0) AS topic_rolling_24h_unique_posts,
    COALESCE(r.unique_authors, 0) AS topic_rolling_24h_unique_authors,
    COALESCE(bm.mentions_24h, 0) - COALESCE(r.total_mentions, 0) AS mention_delta
FROM sample_topics st
LEFT JOIN base_mentions bm
  ON bm.topic_key = st.topic_key
LEFT JOIN public.topic_rolling_24h r
  ON r.topic_key = st.topic_key
ORDER BY st.topic_key;

-- 3) 24h consistency: finalized 1m buckets vs rolling totals for sample topics.
WITH sample_topics AS (
    SELECT unnest(ARRAY[
        'epstein web image posts',
        'epstein web posts',
        'trump'
    ]::text[]) AS topic_key
),
window_bounds AS (
    SELECT
        COALESCE((SELECT MAX(window_end) FROM public.topic_rolling_24h), now()) AS window_end
),
bucket_mentions AS (
    SELECT
        b.topic_key,
        SUM(b.mention_count)::int AS mentions_24h
    FROM public.topic_buckets_1m_final b
    CROSS JOIN window_bounds wb
    WHERE b.bucket_minute >= wb.window_end - interval '24 hour'
      AND b.bucket_minute < wb.window_end
      AND b.topic_key IN (SELECT topic_key FROM sample_topics)
    GROUP BY b.topic_key
)
SELECT
    st.topic_key,
    COALESCE(bk.mentions_24h, 0) AS topic_buckets_1m_final_mentions_24h,
    COALESCE(r.total_mentions, 0) AS topic_rolling_24h_mentions,
    COALESCE(bk.mentions_24h, 0) - COALESCE(r.total_mentions, 0) AS mention_delta
FROM sample_topics st
LEFT JOIN bucket_mentions bk
  ON bk.topic_key = st.topic_key
LEFT JOIN public.topic_rolling_24h r
  ON r.topic_key = st.topic_key
ORDER BY st.topic_key;

-- 4) Day consistency: day series interactions vs finalized 1m buckets.
WITH sample_topics AS (
    SELECT unnest(ARRAY[
        'epstein web image posts',
        'epstein web posts',
        'trump'
    ]::text[]) AS topic_key
),
series_sums AS (
    SELECT
        day,
        topic_key,
        SUM(interactions)::int AS day_series_interactions
    FROM public.topic_day_series_5m
    WHERE day IN ((now() AT TIME ZONE 'utc')::date, ((now() AT TIME ZONE 'utc')::date - 1))
      AND topic_key IN (SELECT topic_key FROM sample_topics)
    GROUP BY day, topic_key
),
bucket_sums AS (
    SELECT
        (bucket_minute AT TIME ZONE 'utc')::date AS day,
        topic_key,
        SUM(mention_count)::int AS bucket_mentions
    FROM public.topic_buckets_1m_final
    WHERE (bucket_minute AT TIME ZONE 'utc')::date IN ((now() AT TIME ZONE 'utc')::date, ((now() AT TIME ZONE 'utc')::date - 1))
      AND topic_key IN (SELECT topic_key FROM sample_topics)
    GROUP BY (bucket_minute AT TIME ZONE 'utc')::date, topic_key
)
SELECT
    COALESCE(s.day, b.day) AS day,
    COALESCE(s.topic_key, b.topic_key) AS topic_key,
    COALESCE(s.day_series_interactions, 0) AS topic_day_series_5m_interactions,
    COALESCE(b.bucket_mentions, 0) AS topic_buckets_1m_final_mentions,
    COALESCE(s.day_series_interactions, 0) - COALESCE(b.bucket_mentions, 0) AS interaction_delta
FROM series_sums s
FULL OUTER JOIN bucket_sums b
  ON b.day = s.day
 AND b.topic_key = s.topic_key
ORDER BY day DESC, topic_key ASC;

-- 5) Root extraction coverage: post_topics vs post_topic_mentions in 24h.
WITH sample_topics AS (
    SELECT unnest(ARRAY[
        'epstein web image posts',
        'epstein web posts',
        'trump'
    ]::text[]) AS topic_key
),
window_bounds AS (
    SELECT
        COALESCE((SELECT MAX(window_end) FROM public.topic_rolling_24h), now()) AS window_end
),
post_topics_24h AS (
    SELECT
        LOWER(BTRIM(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(pt.normalized_topic, ''), '[^a-z0-9$#\\s-]+', ' ', 'g'), '\\s+', ' ', 'g'))) AS topic_key,
        COUNT(*)::int AS post_topics_rows
    FROM public.post_topics pt
    CROSS JOIN window_bounds wb
    WHERE COALESCE(pt.source_created_at, pt.created_at, now()) >= wb.window_end - interval '24 hour'
      AND COALESCE(pt.source_created_at, pt.created_at, now()) < wb.window_end
    GROUP BY 1
),
mentions_24h AS (
    SELECT
        topic_key,
        COUNT(*)::int AS post_topic_mentions_rows
    FROM public.post_topic_mentions m
    CROSS JOIN window_bounds wb
    WHERE m.event_timestamp >= wb.window_end - interval '24 hour'
      AND m.event_timestamp < wb.window_end
    GROUP BY topic_key
)
SELECT
    st.topic_key,
    COALESCE(pt.post_topics_rows, 0) AS post_topics_rows_24h,
    COALESCE(pm.post_topic_mentions_rows, 0) AS post_topic_mentions_rows_24h,
    COALESCE(pt.post_topics_rows, 0) - COALESCE(pm.post_topic_mentions_rows, 0) AS extraction_delta
FROM sample_topics st
LEFT JOIN post_topics_24h pt
  ON pt.topic_key = st.topic_key
LEFT JOIN mentions_24h pm
  ON pm.topic_key = st.topic_key
ORDER BY st.topic_key;
