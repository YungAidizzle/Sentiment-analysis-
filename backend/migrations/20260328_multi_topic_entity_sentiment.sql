BEGIN;

-- raw_posts uniqueness guard
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_posts_platform_source_post_id
    ON public.raw_posts (platform, source_post_id);

-- processed_posts shape updates
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS raw_post_id BIGINT;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS topic_entities TEXT[];
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_label TEXT;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_positive_score INTEGER DEFAULT 0;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_negative_score INTEGER DEFAULT 0;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_neutral_score INTEGER DEFAULT 0;
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ;

ALTER TABLE public.processed_posts ALTER COLUMN processed_at SET DEFAULT now();
ALTER TABLE public.processed_posts ALTER COLUMN topic_entities SET DEFAULT '{}'::text[];
ALTER TABLE public.processed_posts ALTER COLUMN sentiment_label SET DEFAULT 'neutral';
ALTER TABLE public.processed_posts ALTER COLUMN sentiment_positive_score SET DEFAULT 0;
ALTER TABLE public.processed_posts ALTER COLUMN sentiment_negative_score SET DEFAULT 0;
ALTER TABLE public.processed_posts ALTER COLUMN sentiment_neutral_score SET DEFAULT 0;

UPDATE public.processed_posts
SET topic_entities = COALESCE(topic_entities, '{}'::text[]),
    sentiment_label = COALESCE(sentiment_label, 'neutral'),
    sentiment_positive_score = COALESCE(sentiment_positive_score, 0),
    sentiment_negative_score = COALESCE(sentiment_negative_score, 0),
    sentiment_neutral_score = COALESCE(sentiment_neutral_score, 0),
    processed_at = COALESCE(processed_at, now()),
    bucket_minute = COALESCE(bucket_minute, date_trunc('minute', COALESCE(created_at, now())))
WHERE topic_entities IS NULL
   OR sentiment_label IS NULL
   OR sentiment_positive_score IS NULL
   OR sentiment_negative_score IS NULL
   OR sentiment_neutral_score IS NULL
   OR processed_at IS NULL
   OR bucket_minute IS NULL;

-- processed_posts constraints
CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_posts_raw_post_id
    ON public.processed_posts (raw_post_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'processed_posts_raw_post_id_fkey'
          AND conrelid = 'public.processed_posts'::regclass
    ) THEN
        ALTER TABLE public.processed_posts
            ADD CONSTRAINT processed_posts_raw_post_id_fkey
            FOREIGN KEY (raw_post_id)
            REFERENCES public.raw_posts(id)
            ON DELETE CASCADE;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_processed_posts_bucket_minute
    ON public.processed_posts (bucket_minute);

CREATE INDEX IF NOT EXISTS idx_processed_posts_platform_bucket_minute
    ON public.processed_posts (platform, bucket_minute);

CREATE INDEX IF NOT EXISTS idx_processed_posts_sentiment_label
    ON public.processed_posts (sentiment_label);

-- normalized topic mention table
CREATE TABLE IF NOT EXISTS public.post_topics (
    id BIGSERIAL PRIMARY KEY,
    raw_post_id BIGINT NOT NULL,
    processed_post_id BIGINT,
    platform TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    topic_text TEXT NOT NULL,
    normalized_topic TEXT NOT NULL,
    topic_type TEXT NOT NULL,
    language TEXT,
    source_created_at TIMESTAMPTZ,
    bucket_minute TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS raw_post_id BIGINT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS processed_post_id BIGINT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS platform TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS source_post_id TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS topic_text TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS normalized_topic TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS topic_type TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS source_created_at TIMESTAMPTZ;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ;
ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'post_topics_raw_post_id_fkey'
          AND conrelid = 'public.post_topics'::regclass
    ) THEN
        ALTER TABLE public.post_topics
            ADD CONSTRAINT post_topics_raw_post_id_fkey
            FOREIGN KEY (raw_post_id)
            REFERENCES public.raw_posts(id)
            ON DELETE CASCADE;
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'post_topics_processed_post_id_fkey'
          AND conrelid = 'public.post_topics'::regclass
    ) THEN
        ALTER TABLE public.post_topics
            ADD CONSTRAINT post_topics_processed_post_id_fkey
            FOREIGN KEY (processed_post_id)
            REFERENCES public.processed_posts(id)
            ON DELETE CASCADE;
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_post_topics_raw_topic_type
    ON public.post_topics (raw_post_id, normalized_topic, topic_type);

CREATE INDEX IF NOT EXISTS idx_post_topics_bucket_minute
    ON public.post_topics (bucket_minute);

CREATE INDEX IF NOT EXISTS idx_post_topics_normalized_topic
    ON public.post_topics (normalized_topic);

CREATE INDEX IF NOT EXISTS idx_post_topics_platform_bucket_minute
    ON public.post_topics (platform, bucket_minute);

COMMIT;
