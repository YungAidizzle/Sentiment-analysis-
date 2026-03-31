-- Reset derived topic sorting/read-model data without schema changes.
-- Run this only while the worker is stopped.

BEGIN;

-- Clear legacy/derived aggregation output.
TRUNCATE TABLE public.topic_buckets_1m;

-- Clear stable read-model output and mention fact cache.
TRUNCATE TABLE
    public.topic_rolling_24h,
    public.topic_day_series_5m,
    public.topic_day_totals,
    public.topic_buckets_1m_final,
    public.post_topic_mentions
RESTART IDENTITY;

-- Force fresh read-model finalize bounds on rebuild.
DELETE FROM public.topic_read_model_state
WHERE id = 1;

-- Ensure old in-flight rows are not left open.
UPDATE public.ingestion_runs
SET ended_at = now(),
    status = CASE
        WHEN COALESCE(NULLIF(TRIM(status), ''), 'running') = 'running' THEN 'abandoned'
        ELSE status
    END
WHERE source = 'bluesky_firehose_worker'
  AND ended_at IS NULL;

COMMIT;
