BEGIN;

ALTER TABLE public.raw_posts
    ADD COLUMN IF NOT EXISTS id BIGINT;

CREATE SEQUENCE IF NOT EXISTS public.raw_posts_id_seq;

ALTER TABLE public.raw_posts
    ALTER COLUMN id SET DEFAULT nextval('public.raw_posts_id_seq');

ALTER SEQUENCE public.raw_posts_id_seq
    OWNED BY public.raw_posts.id;

UPDATE public.raw_posts
SET id = nextval('public.raw_posts_id_seq')
WHERE id IS NULL;

ALTER TABLE public.raw_posts
    ALTER COLUMN id SET NOT NULL;

SELECT setval(
    'public.raw_posts_id_seq',
    GREATEST(COALESCE((SELECT MAX(id) FROM public.raw_posts), 0), 1),
    true
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_posts_id
    ON public.raw_posts (id);

CREATE UNIQUE INDEX IF NOT EXISTS raw_posts_platform_source_post_id_idx
    ON public.raw_posts (platform, source_post_id);

ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS raw_post_id BIGINT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS platform TEXT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS source_post_id TEXT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS clean_text TEXT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS topic_key_candidate TEXT;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS tokens TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS hashtags TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS mentions TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS urls TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS domains TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS tags TEXT[];
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS source_created_at TIMESTAMPTZ;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE public.processed_posts
    ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ;

ALTER TABLE public.processed_posts
    ALTER COLUMN processed_at SET DEFAULT now();

UPDATE public.processed_posts
SET processed_at = COALESCE(processed_at, now())
WHERE processed_at IS NULL;

UPDATE public.processed_posts
SET bucket_minute = date_trunc('minute', COALESCE(source_created_at, created_at, processed_at, now()))
WHERE bucket_minute IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_posts_raw_post_id
    ON public.processed_posts (raw_post_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_posts_platform_source_post_id
    ON public.processed_posts (platform, source_post_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.processed_posts'::regclass
          AND confrelid = 'public.raw_posts'::regclass
          AND contype = 'f'
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

CREATE INDEX IF NOT EXISTS idx_processed_posts_topic_key_candidate
    ON public.processed_posts (topic_key_candidate);

CREATE INDEX IF NOT EXISTS idx_processed_posts_tags_gin
    ON public.processed_posts USING GIN (tags);

COMMIT;
