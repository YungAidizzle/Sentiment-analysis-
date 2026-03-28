# Bluesky Persistent Worker

This worker runs independently from the frontend and continuously ingests Bluesky firehose events into Supabase Postgres.

## What It Does

- Reuses `backend.bluesky_firehose.sync_bluesky_firehose` for Jetstream ingestion and normalization.
- Runs one internal two-stage pipeline inside the same worker service:
  - `ingestRawPost(...)`: persist validated/normalized records into `raw_posts`.
  - `processRawPost(...)`: derive reusable text features/tags and persist into `processed_posts`.
- Writes normalized posts into `raw_posts` first, then writes derived rows into `processed_posts` with `raw_post_id`.
- Upserts author records into `authors`.
- Tracks worker lifecycle in `ingestion_runs` (one row per process lifecycle).
- Resumes from the last cursor stored in `ingestion_runs.notes`.

## Required Env Vars

- `DATABASE_URL` (Supabase Postgres connection string)

## Optional Worker Env Vars

- `BLUESKY_WORKER_SOURCE` (default: `bluesky_firehose_worker`)
- `BLUESKY_DB_BATCH_SIZE` (default: `200`)
- `BLUESKY_WORKER_LOOP_SLEEP_SECONDS` (default: `2`)
- `BLUESKY_WORKER_RETRY_SECONDS` (default: `5`)
- `BLUESKY_WORKER_PROGRESS_UPDATE_SECONDS` (default: `15`)
- `BLUESKY_WORKER_LOG_LEVEL` (default: `INFO`)

The existing firehose env vars from `backend/bluesky_config.py` also apply.

## Local Run

```bash
python -m pip install -r backend/requirements.txt
python -m backend.main
```

One-cycle run:

```bash
python -m backend.main --once
```

DB/schema verification only:

```bash
python -m backend.main --verify-db-only
```

Time-bucket aggregation runs:

```bash
python -m backend.main --aggregate-1m
python -m backend.main --aggregate-1h
```

## Railway Deploy

Use `backend/Dockerfile` for the worker service.

Suggested Railway service settings:

- Build context: repo root
- Dockerfile path: `backend/Dockerfile`
- Start command override: not required (Dockerfile `CMD` already starts worker)
- Required env var: `DATABASE_URL`

The worker is frontend-independent and can run continuously as a dedicated background service.

## Processed Layer Shape

`processed_posts` is now a generic derived layer designed for future niche filters without changing ingestion:

- `raw_post_id` (FK to `raw_posts.id`)
- core identifiers: `platform`, `source_post_id`
- text/features: `clean_text`, `language`, `tokens`, `hashtags`, `mentions`, `urls`, `domains`
- lightweight routing fields: `topic_key_candidate`, `tags`
- timing fields: `source_created_at`, `processed_at`, `bucket_minute`
