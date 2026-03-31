BEGIN;

CREATE TABLE IF NOT EXISTS public.topic_ai_enrichments (
    id BIGSERIAL PRIMARY KEY,
    topic_key TEXT NOT NULL,
    as_of_window_end TIMESTAMPTZ NOT NULL,
    raw_label TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    short_description TEXT NOT NULL,
    context_paragraph TEXT NOT NULL,
    key_entities JSONB NOT NULL DEFAULT '[]'::jsonb,
    trend_category TEXT,
    summary_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    supporting_post_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    supporting_sample JSONB NOT NULL DEFAULT '[]'::jsonb,
    representative_post_count INTEGER NOT NULL DEFAULT 0,
    model_name TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT topic_ai_enrichments_summary_confidence_range
        CHECK (summary_confidence >= 0 AND summary_confidence <= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_ai_enrichments_topic_window
    ON public.topic_ai_enrichments (topic_key, as_of_window_end);

CREATE INDEX IF NOT EXISTS idx_topic_ai_enrichments_topic_generated
    ON public.topic_ai_enrichments (topic_key, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_ai_enrichments_refreshed
    ON public.topic_ai_enrichments (refreshed_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_ai_enrichments_window_end
    ON public.topic_ai_enrichments (as_of_window_end DESC);

COMMIT;
