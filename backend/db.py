from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from backend.topic_rules import (
    TOPIC_ACRONYM_ALLOWLIST,
    TOPIC_GENERIC_WEAK_TOKENS,
    TOPIC_NOISE_TOKENS,
    TOPIC_NUMBER_WORD_TOKENS,
    TOPIC_URL_DEBRIS_TOKENS,
    seed_topic_alias_rows,
)


def _chunked(rows: Sequence[dict[str, Any]], size: int) -> Iterator[Sequence[dict[str, Any]]]:
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


def _normalize_text_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        candidates = [values]
    elif isinstance(values, (list, tuple, set)):
        candidates = list(values)
    else:
        candidates = [values]

    normalized: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _worker_lock_key(source: str) -> int:
    digest = hashlib.sha1(str(source or "worker").encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & 0x7FFFFFFFFFFFFFFF


class PostgresStore:
    def __init__(
        self,
        *,
        database_url: str,
        batch_size: int,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database_url = database_url
        self._batch_size = max(1, int(batch_size))
        self._logger = logger or logging.getLogger("backend.db")
        self._conn: psycopg.Connection[Any] | None = None
        self._column_types: dict[tuple[str, str], tuple[str, str]] = {}
        self._schema_verified = False

    def connect(self) -> None:
        if self._conn is not None and not self._conn.closed:
            return
        self._conn = psycopg.connect(self._database_url)
        self._conn.autocommit = False
        self._ensure_core_tables()
        self._load_column_metadata()
        self._verify_required_schema()

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None

    def verify_connection(self) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

        self._run_with_retry("verify_connection", operation)

    def disable_legacy_topic_bucket_refresh_jobs(self) -> int:
        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('cron.job')")
                cron_job_relation = cursor.fetchone()
                if not cron_job_relation or cron_job_relation[0] is None:
                    return 0

                cursor.execute(
                    """
                    SELECT jobid
                    FROM cron.job
                    WHERE active = TRUE
                      AND (
                          jobname = 'topic_buckets_refresh_every_minute'
                          OR command ILIKE '%%run_topic_bucket_refresh_job%%'
                          OR command ILIKE '%%refresh_topic_buckets_1m%%'
                      )
                    ORDER BY jobid ASC
                    """
                )
                job_ids = [int(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
                for job_id in job_ids:
                    cursor.execute("SELECT cron.unschedule(%s)", (job_id,))
                return len(job_ids)

        return self._execute_write("disable_legacy_topic_bucket_refresh_jobs", operation)

    def fetch_resume_cursor(self, *, source: str) -> int | None:
        def operation(connection: psycopg.Connection[Any]) -> int | None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT notes
                    FROM public.ingestion_runs
                    WHERE source = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (source,),
                )
                row = cursor.fetchone()
            if not row:
                return None
            notes = self._decode_notes(row[0])
            candidate = notes.get("last_cursor_us", notes.get("cursor"))
            try:
                parsed = int(candidate)
            except (TypeError, ValueError):
                return None
            return parsed if parsed > 0 else None

        return self._run_with_retry("fetch_resume_cursor", operation)

    def create_ingestion_run(
        self,
        *,
        source: str,
        started_at: datetime,
        notes: dict[str, Any],
    ) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.ingestion_runs (
                        source,
                        started_at,
                        status,
                        rows_inserted,
                        notes
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        source,
                        started_at,
                        "running",
                        0,
                        self._adapt_notes(notes),
                    ),
                )

        self._execute_write("create_ingestion_run", operation)

    def update_ingestion_run(
        self,
        *,
        source: str,
        started_at: datetime,
        status: str,
        rows_inserted: int,
        notes: dict[str, Any],
        ended_at: datetime | None = None,
    ) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            with connection.cursor() as cursor:
                if ended_at is None:
                    cursor.execute(
                        """
                        UPDATE public.ingestion_runs
                        SET status = %s,
                            rows_inserted = %s,
                            notes = %s
                        WHERE source = %s
                          AND started_at = %s
                        """,
                        (
                            status,
                            max(0, int(rows_inserted)),
                            self._adapt_notes(notes),
                            source,
                            started_at,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE public.ingestion_runs
                        SET ended_at = %s,
                            status = %s,
                            rows_inserted = %s,
                            notes = %s
                        WHERE source = %s
                          AND started_at = %s
                        """,
                        (
                            ended_at,
                            status,
                            max(0, int(rows_inserted)),
                            self._adapt_notes(notes),
                            source,
                            started_at,
                        ),
                    )

                if cursor.rowcount <= 0:
                    raise RuntimeError(
                        "Could not update ingestion_runs row. Check table keys and started_at precision."
                    )

        self._execute_write("update_ingestion_run", operation)

    def acquire_worker_lease(
        self,
        *,
        source: str,
        stale_after_minutes: int = 30,
    ) -> dict[str, Any]:
        stale_after_minutes = max(5, int(stale_after_minutes))
        source_value = str(source or "").strip() or "bluesky_firehose_worker"
        lock_key = _worker_lock_key(source_value)

        def operation(connection: psycopg.Connection[Any]) -> dict[str, Any]:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
                acquired = bool((cursor.fetchone() or [False])[0])
                closed_stale_runs = 0
                if acquired:
                    cursor.execute(
                        """
                        WITH stale AS (
                            UPDATE public.ingestion_runs r
                            SET ended_at = now(),
                                status = CASE
                                    WHEN COALESCE(NULLIF(TRIM(r.status), ''), 'running') = 'running'
                                        THEN 'abandoned'
                                    ELSE r.status
                                END
                            WHERE r.source = %s
                              AND r.ended_at IS NULL
                              AND r.started_at < now() - make_interval(mins => %s)
                            RETURNING 1
                        )
                        SELECT COUNT(*)::bigint FROM stale
                        """,
                        (source_value, stale_after_minutes),
                    )
                    closed_stale_runs = int((cursor.fetchone() or [0])[0] or 0)
                return {
                    "acquired": acquired,
                    "lock_key": lock_key,
                    "closed_stale_runs": closed_stale_runs,
                }

        return self._execute_write("acquire_worker_lease", operation)

    def release_worker_lease(self, *, source: str) -> bool:
        source_value = str(source or "").strip() or "bluesky_firehose_worker"
        lock_key = _worker_lock_key(source_value)

        def operation(connection: psycopg.Connection[Any]) -> bool:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
                return bool((cursor.fetchone() or [False])[0])

        return self._execute_write("release_worker_lease", operation)

    def upsert_raw_posts(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = [self._prepare_raw_post_row(row) for row in rows]
        payload = [row for row in payload if row.get("post_id")]
        if not payload:
            return 0

        def operation(connection: psycopg.Connection[Any]) -> int:
            inserted_total = 0
            with connection.cursor() as cursor:
                for chunk in _chunked(payload, self._batch_size):
                    cursor.executemany(
                        """
                        INSERT INTO public.raw_posts (
                            platform,
                            source_post_id,
                            source_uri,
                            source_cid,
                            author_did,
                            post_id,
                            author_id,
                            author_handle,
                            root_post_id,
                            reply_parent_id,
                            created_at,
                            inserted_at,
                            ingested_at,
                            raw_text,
                            text_content,
                            language,
                            urls,
                            hashtags,
                            like_count,
                            repost_count,
                            reply_count,
                            reply_to_uri,
                            repost_of_uri,
                            processed,
                            metrics_json,
                            raw_json
                        ) VALUES (
                            %(platform)s,
                            %(source_post_id)s,
                            %(source_uri)s,
                            %(source_cid)s,
                            %(author_did)s,
                            %(post_id)s,
                            %(author_id)s,
                            %(author_handle)s,
                            %(root_post_id)s,
                            %(reply_parent_id)s,
                            %(created_at)s,
                            %(inserted_at)s,
                            %(ingested_at)s,
                            %(raw_text)s,
                            %(text_content)s,
                            %(language)s,
                            %(urls)s,
                            %(hashtags)s,
                            %(like_count)s,
                            %(repost_count)s,
                            %(reply_count)s,
                            %(reply_to_uri)s,
                            %(repost_of_uri)s,
                            %(processed)s,
                            %(metrics_json)s,
                            %(raw_json)s
                        )
                        ON CONFLICT (platform, source_post_id) DO UPDATE
                        SET source_uri = COALESCE(EXCLUDED.source_uri, public.raw_posts.source_uri),
                            source_cid = COALESCE(EXCLUDED.source_cid, public.raw_posts.source_cid),
                            author_did = COALESCE(EXCLUDED.author_did, public.raw_posts.author_did),
                            author_id = COALESCE(EXCLUDED.author_id, public.raw_posts.author_id),
                            author_handle = COALESCE(EXCLUDED.author_handle, public.raw_posts.author_handle),
                            root_post_id = COALESCE(EXCLUDED.root_post_id, public.raw_posts.root_post_id),
                            reply_parent_id = COALESCE(EXCLUDED.reply_parent_id, public.raw_posts.reply_parent_id),
                            created_at = COALESCE(EXCLUDED.created_at, public.raw_posts.created_at),
                            ingested_at = COALESCE(EXCLUDED.ingested_at, public.raw_posts.ingested_at),
                            inserted_at = COALESCE(EXCLUDED.inserted_at, public.raw_posts.inserted_at),
                            raw_text = COALESCE(EXCLUDED.raw_text, public.raw_posts.raw_text),
                            text_content = COALESCE(EXCLUDED.text_content, public.raw_posts.text_content),
                            language = COALESCE(EXCLUDED.language, public.raw_posts.language),
                            urls = COALESCE(EXCLUDED.urls, public.raw_posts.urls),
                            hashtags = COALESCE(EXCLUDED.hashtags, public.raw_posts.hashtags),
                            like_count = COALESCE(EXCLUDED.like_count, public.raw_posts.like_count),
                            repost_count = COALESCE(EXCLUDED.repost_count, public.raw_posts.repost_count),
                            reply_count = COALESCE(EXCLUDED.reply_count, public.raw_posts.reply_count),
                            reply_to_uri = COALESCE(EXCLUDED.reply_to_uri, public.raw_posts.reply_to_uri),
                            repost_of_uri = COALESCE(EXCLUDED.repost_of_uri, public.raw_posts.repost_of_uri),
                            processed = (public.raw_posts.processed OR COALESCE(EXCLUDED.processed, false)),
                            metrics_json = COALESCE(EXCLUDED.metrics_json, public.raw_posts.metrics_json),
                            raw_json = COALESCE(EXCLUDED.raw_json, public.raw_posts.raw_json),
                            post_id = COALESCE(EXCLUDED.post_id, public.raw_posts.post_id)
                        """,
                        list(chunk),
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        inserted_total += int(cursor.rowcount)
            return inserted_total

        return self._execute_write("upsert_raw_posts", operation)

    def ingest_raw_post(self, row: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._prepare_raw_post_row(row)
        if not payload.get("post_id"):
            return None

        def operation(connection: psycopg.Connection[Any]) -> dict[str, Any] | None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.raw_posts (
                        platform,
                        source_post_id,
                        source_uri,
                        source_cid,
                        author_did,
                        post_id,
                        author_id,
                        author_handle,
                        root_post_id,
                        reply_parent_id,
                        created_at,
                        inserted_at,
                        ingested_at,
                        raw_text,
                        text_content,
                        language,
                        urls,
                        hashtags,
                        like_count,
                        repost_count,
                        reply_count,
                        reply_to_uri,
                        repost_of_uri,
                        processed,
                        metrics_json,
                        raw_json
                    ) VALUES (
                        %(platform)s,
                        %(source_post_id)s,
                        %(source_uri)s,
                        %(source_cid)s,
                        %(author_did)s,
                        %(post_id)s,
                        %(author_id)s,
                        %(author_handle)s,
                        %(root_post_id)s,
                        %(reply_parent_id)s,
                        %(created_at)s,
                        %(inserted_at)s,
                        %(ingested_at)s,
                        %(raw_text)s,
                        %(text_content)s,
                        %(language)s,
                        %(urls)s,
                        %(hashtags)s,
                        %(like_count)s,
                        %(repost_count)s,
                        %(reply_count)s,
                        %(reply_to_uri)s,
                        %(repost_of_uri)s,
                        %(processed)s,
                        %(metrics_json)s,
                        %(raw_json)s
                    )
                    ON CONFLICT (platform, source_post_id) DO UPDATE
                    SET source_uri = COALESCE(EXCLUDED.source_uri, public.raw_posts.source_uri),
                        source_cid = COALESCE(EXCLUDED.source_cid, public.raw_posts.source_cid),
                        author_did = COALESCE(EXCLUDED.author_did, public.raw_posts.author_did),
                        author_id = COALESCE(EXCLUDED.author_id, public.raw_posts.author_id),
                        author_handle = COALESCE(EXCLUDED.author_handle, public.raw_posts.author_handle),
                        root_post_id = COALESCE(EXCLUDED.root_post_id, public.raw_posts.root_post_id),
                        reply_parent_id = COALESCE(EXCLUDED.reply_parent_id, public.raw_posts.reply_parent_id),
                        created_at = COALESCE(EXCLUDED.created_at, public.raw_posts.created_at),
                        ingested_at = COALESCE(EXCLUDED.ingested_at, public.raw_posts.ingested_at),
                        inserted_at = COALESCE(EXCLUDED.inserted_at, public.raw_posts.inserted_at),
                        raw_text = COALESCE(EXCLUDED.raw_text, public.raw_posts.raw_text),
                        text_content = COALESCE(EXCLUDED.text_content, public.raw_posts.text_content),
                        language = COALESCE(EXCLUDED.language, public.raw_posts.language),
                        urls = COALESCE(EXCLUDED.urls, public.raw_posts.urls),
                        hashtags = COALESCE(EXCLUDED.hashtags, public.raw_posts.hashtags),
                        like_count = COALESCE(EXCLUDED.like_count, public.raw_posts.like_count),
                        repost_count = COALESCE(EXCLUDED.repost_count, public.raw_posts.repost_count),
                        reply_count = COALESCE(EXCLUDED.reply_count, public.raw_posts.reply_count),
                        reply_to_uri = COALESCE(EXCLUDED.reply_to_uri, public.raw_posts.reply_to_uri),
                        repost_of_uri = COALESCE(EXCLUDED.repost_of_uri, public.raw_posts.repost_of_uri),
                        processed = (public.raw_posts.processed OR COALESCE(EXCLUDED.processed, false)),
                        metrics_json = COALESCE(EXCLUDED.metrics_json, public.raw_posts.metrics_json),
                        raw_json = COALESCE(EXCLUDED.raw_json, public.raw_posts.raw_json),
                        post_id = COALESCE(EXCLUDED.post_id, public.raw_posts.post_id)
                    RETURNING
                        id,
                        platform,
                        source_post_id,
                        post_id,
                        author_id,
                        author_handle,
                        root_post_id,
                        reply_parent_id,
                        created_at,
                        inserted_at,
                        ingested_at,
                        raw_text,
                        text_content,
                        language,
                        urls,
                        hashtags,
                        reply_to_uri,
                        repost_of_uri,
                        metrics_json,
                        raw_json
                    """,
                    payload,
                )
                row_result = cursor.fetchone()
                if not row_result:
                    return None
                return self._decode_ingested_raw_post_row(row_result)

        return self._execute_write("ingest_raw_post", operation)

    def upsert_processed_post_record(self, row: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._prepare_processed_post_row(row)
        raw_post_id = payload.get("raw_post_id")
        if raw_post_id in {None, 0}:
            return None
        source_post_id = str(payload.get("source_post_id") or "").strip()
        if not source_post_id:
            return None

        def operation(connection: psycopg.Connection[Any]) -> dict[str, Any] | None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.processed_posts (
                        raw_post_id,
                        platform,
                        source_post_id,
                        post_id,
                        author_id,
                        source_created_at,
                        created_at,
                        processed_at,
                        bucket_minute,
                        clean_text,
                        normalized_text,
                        language,
                        has_media,
                        is_reply,
                        is_repost,
                        is_quote,
                        author_hash,
                        token_count,
                        fingerprint,
                        tokens,
                        hashtags,
                        cashtags,
                        mentions,
                        domains,
                        urls,
                        key_phrases,
                        topic_seeds,
                        topic_key_candidate,
                        tags,
                        spam_score,
                        quality_score,
                        topic_entities,
                        sentiment_label,
                        sentiment_positive_score,
                        sentiment_negative_score,
                        sentiment_neutral_score,
                        topic
                    ) VALUES (
                        %(raw_post_id)s,
                        %(platform)s,
                        %(source_post_id)s,
                        %(post_id)s,
                        %(author_id)s,
                        %(source_created_at)s,
                        %(created_at)s,
                        %(processed_at)s,
                        %(bucket_minute)s,
                        %(clean_text)s,
                        %(normalized_text)s,
                        %(language)s,
                        %(has_media)s,
                        %(is_reply)s,
                        %(is_repost)s,
                        %(is_quote)s,
                        %(author_hash)s,
                        %(token_count)s,
                        %(fingerprint)s,
                        %(tokens)s,
                        %(hashtags)s,
                        %(cashtags)s,
                        %(mentions)s,
                        %(domains)s,
                        %(urls)s,
                        %(key_phrases)s,
                        %(topic_seeds)s,
                        %(topic_key_candidate)s,
                        %(tags)s,
                        %(spam_score)s,
                        %(quality_score)s,
                        %(topic_entities)s,
                        %(sentiment_label)s,
                        %(sentiment_positive_score)s,
                        %(sentiment_negative_score)s,
                        %(sentiment_neutral_score)s,
                        %(topic)s
                    )
                    ON CONFLICT (raw_post_id) DO UPDATE
                    SET platform = EXCLUDED.platform,
                        source_post_id = EXCLUDED.source_post_id,
                        post_id = EXCLUDED.post_id,
                        author_id = EXCLUDED.author_id,
                        source_created_at = COALESCE(EXCLUDED.source_created_at, public.processed_posts.source_created_at),
                        created_at = COALESCE(EXCLUDED.created_at, public.processed_posts.created_at),
                        processed_at = COALESCE(EXCLUDED.processed_at, public.processed_posts.processed_at),
                        bucket_minute = COALESCE(EXCLUDED.bucket_minute, public.processed_posts.bucket_minute),
                        clean_text = COALESCE(EXCLUDED.clean_text, public.processed_posts.clean_text),
                        normalized_text = COALESCE(EXCLUDED.normalized_text, public.processed_posts.normalized_text),
                        language = COALESCE(EXCLUDED.language, public.processed_posts.language),
                        has_media = COALESCE(EXCLUDED.has_media, public.processed_posts.has_media),
                        is_reply = COALESCE(EXCLUDED.is_reply, public.processed_posts.is_reply),
                        is_repost = COALESCE(EXCLUDED.is_repost, public.processed_posts.is_repost),
                        is_quote = COALESCE(EXCLUDED.is_quote, public.processed_posts.is_quote),
                        author_hash = COALESCE(EXCLUDED.author_hash, public.processed_posts.author_hash),
                        token_count = COALESCE(EXCLUDED.token_count, public.processed_posts.token_count),
                        fingerprint = COALESCE(EXCLUDED.fingerprint, public.processed_posts.fingerprint),
                        tokens = COALESCE(EXCLUDED.tokens, public.processed_posts.tokens),
                        hashtags = COALESCE(EXCLUDED.hashtags, public.processed_posts.hashtags),
                        cashtags = COALESCE(EXCLUDED.cashtags, public.processed_posts.cashtags),
                        mentions = COALESCE(EXCLUDED.mentions, public.processed_posts.mentions),
                        domains = COALESCE(EXCLUDED.domains, public.processed_posts.domains),
                        urls = COALESCE(EXCLUDED.urls, public.processed_posts.urls),
                        key_phrases = COALESCE(EXCLUDED.key_phrases, public.processed_posts.key_phrases),
                        topic_seeds = COALESCE(EXCLUDED.topic_seeds, public.processed_posts.topic_seeds),
                        topic_key_candidate = COALESCE(EXCLUDED.topic_key_candidate, public.processed_posts.topic_key_candidate),
                        tags = COALESCE(EXCLUDED.tags, public.processed_posts.tags),
                        spam_score = COALESCE(EXCLUDED.spam_score, public.processed_posts.spam_score),
                        quality_score = COALESCE(EXCLUDED.quality_score, public.processed_posts.quality_score),
                        topic_entities = COALESCE(EXCLUDED.topic_entities, public.processed_posts.topic_entities),
                        sentiment_label = COALESCE(EXCLUDED.sentiment_label, public.processed_posts.sentiment_label),
                        sentiment_positive_score = COALESCE(EXCLUDED.sentiment_positive_score, public.processed_posts.sentiment_positive_score),
                        sentiment_negative_score = COALESCE(EXCLUDED.sentiment_negative_score, public.processed_posts.sentiment_negative_score),
                        sentiment_neutral_score = COALESCE(EXCLUDED.sentiment_neutral_score, public.processed_posts.sentiment_neutral_score),
                        topic = COALESCE(EXCLUDED.topic, public.processed_posts.topic)
                    RETURNING id, raw_post_id, processed_at
                    """,
                    payload,
                )
                processed_row = cursor.fetchone()
                if not processed_row:
                    return None
                cursor.execute(
                    """
                    UPDATE public.raw_posts
                    SET processed = TRUE
                    WHERE id = %s
                    """,
                    (raw_post_id,),
                )
                topic_entities_payload = payload.get("topic_entities")
                if isinstance(topic_entities_payload, (list, tuple)):
                    topic_entities_value = [
                        str(value).strip()
                        for value in topic_entities_payload
                        if str(value or "").strip()
                    ]
                else:
                    topic_entities_value = (
                        [str(topic_entities_payload).strip()]
                        if str(topic_entities_payload or "").strip()
                        else []
                    )
                return {
                    "id": int(processed_row[0]),
                    "raw_post_id": int(processed_row[1]),
                    "source_post_id": source_post_id,
                    "platform": str(payload.get("platform") or ""),
                    "processed_at": processed_row[2],
                    "topic_entities": topic_entities_value,
                    "language": payload.get("language"),
                    "source_created_at": payload.get("source_created_at"),
                    "bucket_minute": payload.get("bucket_minute"),
                }

        return self._execute_write("upsert_processed_post_record", operation)

    def upsert_processed_post(self, row: dict[str, Any]) -> int:
        processed = self.upsert_processed_post_record(row)
        return 1 if processed else 0

    def persist_post_topics(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = [self._prepare_post_topic_row(row) for row in rows]
        payload = [
            row
            for row in payload
            if row.get("raw_post_id")
            and row.get("source_post_id")
            and row.get("normalized_topic")
            and row.get("topic_type")
        ]
        if not payload:
            return 0

        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO public.post_topics (
                        raw_post_id,
                        processed_post_id,
                        platform,
                        source_post_id,
                        topic_text,
                        normalized_topic,
                        topic_type,
                        language,
                        source_created_at,
                        bucket_minute,
                        created_at
                    ) VALUES (
                        %(raw_post_id)s,
                        %(processed_post_id)s,
                        %(platform)s,
                        %(source_post_id)s,
                        %(topic_text)s,
                        %(normalized_topic)s,
                        %(topic_type)s,
                        %(language)s,
                        %(source_created_at)s,
                        %(bucket_minute)s,
                        %(created_at)s
                    )
                    ON CONFLICT (raw_post_id, normalized_topic, topic_type) DO UPDATE
                    SET processed_post_id = COALESCE(EXCLUDED.processed_post_id, public.post_topics.processed_post_id),
                        platform = COALESCE(EXCLUDED.platform, public.post_topics.platform),
                        source_post_id = COALESCE(EXCLUDED.source_post_id, public.post_topics.source_post_id),
                        topic_text = COALESCE(EXCLUDED.topic_text, public.post_topics.topic_text),
                        language = COALESCE(EXCLUDED.language, public.post_topics.language),
                        source_created_at = COALESCE(EXCLUDED.source_created_at, public.post_topics.source_created_at),
                        bucket_minute = COALESCE(EXCLUDED.bucket_minute, public.post_topics.bucket_minute)
                    """,
                    payload,
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("persist_post_topics", operation)

    def ensure_stable_topic_read_model_tables(self) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            alias_rows = seed_topic_alias_rows()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topics (
                        topic_key TEXT PRIMARY KEY,
                        canonical_label TEXT NOT NULL,
                        entity_type TEXT NOT NULL DEFAULT 'keyword',
                        aliases TEXT[] NOT NULL DEFAULT '{}'::text[],
                        is_active BOOLEAN NOT NULL DEFAULT true,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_alias_rules (
                        alias_key TEXT PRIMARY KEY,
                        canonical_key TEXT NOT NULL,
                        canonical_label TEXT NOT NULL,
                        entity_type TEXT NOT NULL DEFAULT 'entity',
                        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                        is_active BOOLEAN NOT NULL DEFAULT true,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_alias_rules_canonical_key
                    ON public.topic_alias_rules (canonical_key)
                    """
                )
                if alias_rows:
                    cursor.executemany(
                        """
                        INSERT INTO public.topic_alias_rules (
                            alias_key,
                            canonical_key,
                            canonical_label,
                            entity_type,
                            confidence,
                            is_active,
                            updated_at
                        ) VALUES (%s, %s, %s, %s, %s, true, now())
                        ON CONFLICT (alias_key) DO UPDATE
                        SET canonical_key = EXCLUDED.canonical_key,
                            canonical_label = EXCLUDED.canonical_label,
                            entity_type = EXCLUDED.entity_type,
                            confidence = EXCLUDED.confidence,
                            is_active = true,
                            updated_at = now()
                        """,
                        alias_rows,
                    )
                    cursor.executemany(
                        """
                        INSERT INTO public.topics (
                            topic_key,
                            canonical_label,
                            entity_type,
                            aliases,
                            is_active,
                            updated_at
                        ) VALUES (%s, %s, %s, ARRAY[%s]::text[], true, now())
                        ON CONFLICT (topic_key) DO UPDATE
                        SET canonical_label = EXCLUDED.canonical_label,
                            entity_type = EXCLUDED.entity_type,
                            aliases = (
                                SELECT ARRAY(
                                    SELECT DISTINCT a
                                    FROM unnest(public.topics.aliases || EXCLUDED.aliases) AS a
                                )
                            ),
                            is_active = true,
                            updated_at = now()
                        """,
                        [
                            (
                                canonical_key,
                                canonical_label,
                                entity_type,
                                alias_key,
                            )
                            for alias_key, canonical_key, canonical_label, entity_type, _confidence in alias_rows
                        ],
                    )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.post_topic_mentions (
                        mention_id BIGSERIAL PRIMARY KEY,
                        raw_post_id BIGINT NOT NULL,
                        processed_post_id BIGINT,
                        platform TEXT NOT NULL DEFAULT 'bluesky',
                        topic_key TEXT NOT NULL,
                        topic_label TEXT NOT NULL,
                        event_timestamp TIMESTAMPTZ NOT NULL,
                        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        author_id TEXT,
                        sentiment_label TEXT NOT NULL DEFAULT 'neutral',
                        quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                        topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        is_repost BOOLEAN NOT NULL DEFAULT false,
                        is_reply BOOLEAN NOT NULL DEFAULT false,
                        has_link BOOLEAN NOT NULL DEFAULT false
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.post_topic_mentions
                    ADD COLUMN IF NOT EXISTS topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_post_topic_mentions_raw_platform_topic
                    ON public.post_topic_mentions (raw_post_id, platform, topic_key)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_post_topic_mentions_event_ts
                    ON public.post_topic_mentions (event_timestamp DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_post_topic_mentions_topic_event
                    ON public.post_topic_mentions (topic_key, event_timestamp DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_buckets_1m_final (
                        bucket_minute TIMESTAMPTZ NOT NULL,
                        platform TEXT NOT NULL,
                        topic_key TEXT NOT NULL,
                        mention_count INTEGER NOT NULL,
                        unique_posts INTEGER NOT NULL,
                        unique_authors INTEGER NOT NULL,
                        positive_count INTEGER NOT NULL DEFAULT 0,
                        neutral_count INTEGER NOT NULL DEFAULT 0,
                        negative_count INTEGER NOT NULL DEFAULT 0,
                        avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        finalized_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (bucket_minute, platform, topic_key)
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.topic_buckets_1m_final
                    ADD COLUMN IF NOT EXISTS avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_buckets_1m_final_topic_bucket
                    ON public.topic_buckets_1m_final (topic_key, bucket_minute DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_buckets_1m_final_platform_bucket
                    ON public.topic_buckets_1m_final (platform, bucket_minute DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_day_totals (
                        day DATE NOT NULL,
                        topic_key TEXT NOT NULL,
                        topic_label TEXT NOT NULL,
                        platform_count INTEGER NOT NULL,
                        total_mentions INTEGER NOT NULL,
                        unique_posts INTEGER NOT NULL,
                        unique_authors INTEGER NOT NULL,
                        positive_count INTEGER NOT NULL DEFAULT 0,
                        neutral_count INTEGER NOT NULL DEFAULT 0,
                        negative_count INTEGER NOT NULL DEFAULT 0,
                        avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        first_seen_at TIMESTAMPTZ NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (day, topic_key)
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.topic_day_totals
                    ADD COLUMN IF NOT EXISTS avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_day_totals_day_mentions
                    ON public.topic_day_totals (day, total_mentions DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_read_model_state (
                        id SMALLINT PRIMARY KEY CHECK (id = 1),
                        last_finalize_before TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_rolling_24h (
                        topic_key TEXT PRIMARY KEY,
                        topic_label TEXT NOT NULL,
                        platform_count INTEGER NOT NULL,
                        total_mentions INTEGER NOT NULL,
                        unique_posts INTEGER NOT NULL,
                        unique_authors INTEGER NOT NULL,
                        positive_count INTEGER NOT NULL DEFAULT 0,
                        neutral_count INTEGER NOT NULL DEFAULT 0,
                        negative_count INTEGER NOT NULL DEFAULT 0,
                        avg_topic_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        first_seen_at TIMESTAMPTZ NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL,
                        window_start TIMESTAMPTZ NOT NULL,
                        window_end TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_rolling_24h_mentions
                    ON public.topic_rolling_24h (total_mentions DESC, topic_key)
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_day_series_5m (
                        day DATE NOT NULL,
                        bucket_5m TIMESTAMPTZ NOT NULL,
                        topic_key TEXT NOT NULL,
                        topic_label TEXT NOT NULL,
                        interactions INTEGER NOT NULL,
                        cumulative_interactions INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (day, bucket_5m, topic_key)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_day_series_5m_day_topic_bucket
                    ON public.topic_day_series_5m (day, topic_key, bucket_5m)
                    """
                )
                cursor.execute(
                    """
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
                    FROM public.topic_day_totals
                    """
                )
                cursor.execute(
                    """
                    CREATE OR REPLACE VIEW public.v_topic_series_day_5m AS
                    SELECT
                        day,
                        bucket_5m,
                        topic_key,
                        topic_label,
                        interactions,
                        cumulative_interactions,
                        updated_at
                    FROM public.topic_day_series_5m
                    """
                )
                cursor.execute(
                    """
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
                    FROM public.topic_rolling_24h
                    """
                )

        self._execute_write("ensure_stable_topic_read_model_tables", operation)

    def reset_stable_topic_read_models(self) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                        public.topic_rolling_24h,
                        public.topic_day_series_5m,
                        public.topic_day_totals,
                        public.topic_buckets_1m_final,
                        public.post_topic_mentions
                    RESTART IDENTITY
                    """
                )
                cursor.execute(
                    """
                    DELETE FROM public.topic_read_model_state WHERE id = 1
                    """
                )

        self._execute_write("reset_stable_topic_read_models", operation)

    def sync_post_topic_mentions_from_post_topics(self, *, lookback_hours: int = 72) -> int:
        lookback_hours = max(1, int(lookback_hours))
        weak_tokens = sorted(set(TOPIC_GENERIC_WEAK_TOKENS))
        noise_tokens = sorted(set(TOPIC_NOISE_TOKENS))
        number_tokens = sorted(set(TOPIC_NUMBER_WORD_TOKENS))
        url_debris_tokens = sorted(set(TOPIC_URL_DEBRIS_TOKENS))
        acronym_tokens = sorted(set(TOPIC_ACRONYM_ALLOWLIST))

        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        to_regclass('public.post_topic_mentions'),
                        to_regclass('public.post_topics'),
                        to_regclass('public.processed_posts'),
                        to_regclass('public.topic_alias_rules')
                    """
                )
                tables = cursor.fetchone() or (None, None, None, None)
                if not tables[0] or not tables[1] or not tables[3]:
                    return 0

                cursor.execute(
                    """
                    WITH sync_bounds AS (
                        SELECT
                            GREATEST(
                                now() - make_interval(hours => %s),
                                COALESCE(
                                    (
                                        SELECT MAX(event_timestamp) - interval '3 hour'
                                        FROM public.post_topic_mentions
                                    ),
                                    now() - make_interval(hours => %s)
                                )
                            ) AS sync_from
                    ),
                    weak_topic_tokens(token) AS (SELECT unnest(%s::text[])),
                    topic_noise_tokens(token) AS (SELECT unnest(%s::text[])),
                    number_word_tokens(token) AS (SELECT unnest(%s::text[])),
                    url_debris_tokens(token) AS (SELECT unnest(%s::text[])),
                    acronym_allowlist(token) AS (SELECT unnest(%s::text[])),
                    mention_candidates_raw AS (
                        SELECT
                            pt.raw_post_id,
                            pt.processed_post_id,
                            COALESCE(NULLIF(TRIM(pt.platform), ''), 'bluesky') AS platform,
                            normalized_topic_base AS normalized_topic,
                            COALESCE(NULLIF(TRIM(pt.topic_text), ''), INITCAP(normalized_topic_base)) AS topic_text,
                            LOWER(COALESCE(NULLIF(TRIM(pt.topic_text), ''), '')) AS topic_text_lc,
                            COALESCE(
                                pt.source_created_at,
                                pt.bucket_minute,
                                pp.source_created_at,
                                pp.created_at,
                                pp.processed_at,
                                now()
                            ) AS event_timestamp,
                            COALESCE(pp.processed_at, pt.created_at, now()) AS ingested_at,
                            NULLIF(TRIM(COALESCE(pp.author_id, '')), '') AS author_id,
                            CASE
                                WHEN COALESCE(pp.sentiment_label, '') IN ('positive', 'negative', 'neutral')
                                    THEN pp.sentiment_label
                                ELSE 'neutral'
                            END AS sentiment_label,
                            LEAST(1.0, GREATEST(0.0, COALESCE(pp.quality_score, 0)::double precision)) AS quality_score,
                            COALESCE(pp.is_repost, false) AS is_repost,
                            COALESCE(pp.is_reply, false) AS is_reply,
                            (COALESCE(array_length(pp.urls, 1), 0) > 0) AS has_link,
                            LOWER(COALESCE(pt.topic_type, 'entity')) AS topic_type
                        FROM (
                            SELECT
                                pt.*,
                                LOWER(
                                    BTRIM(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REPLACE(REPLACE(COALESCE(pt.normalized_topic, pt.topic_text, ''), 'â€™', ''''), '’', ''''),
                                                    '([a-z0-9])''([a-z0-9])',
                                                    '\\1\\2',
                                                    'gi'
                                                ),
                                                '[^a-z0-9$#\\s-]+',
                                                ' ',
                                                'g'
                                            ),
                                            '\\s+',
                                            ' ',
                                            'g'
                                        )
                                    )
                                ) AS normalized_topic_base
                            FROM public.post_topics pt
                            WHERE COALESCE(pt.source_created_at, pt.created_at, now())
                                >= (SELECT sync_from FROM sync_bounds)
                        ) pt
                        LEFT JOIN public.processed_posts pp
                          ON pp.id = pt.processed_post_id
                    ),
                    canonicalized AS (
                        SELECT
                            r.raw_post_id,
                            r.processed_post_id,
                            r.platform,
                            COALESCE(a.canonical_key, r.normalized_topic) AS topic_key,
                            COALESCE(
                                a.canonical_label,
                                CASE
                                    WHEN r.normalized_topic IN (SELECT token FROM acronym_allowlist)
                                        THEN UPPER(r.normalized_topic)
                                    ELSE COALESCE(NULLIF(TRIM(r.topic_text), ''), INITCAP(r.normalized_topic))
                                END
                            ) AS topic_label,
                            r.topic_text_lc,
                            r.event_timestamp,
                            r.ingested_at,
                            r.author_id,
                            r.sentiment_label,
                            r.quality_score,
                            COALESCE(a.confidence, 0.0)::double precision AS alias_confidence,
                            r.is_repost,
                            r.is_reply,
                            r.has_link,
                            r.topic_type
                        FROM mention_candidates_raw r
                        LEFT JOIN public.topic_alias_rules a
                          ON a.alias_key = r.normalized_topic
                         AND a.is_active = true
                    ),
                    scored AS (
                        SELECT
                            c.*,
                            tok.token_count,
                            tok.weak_count,
                            tok.noise_count,
                            tok.number_count,
                            tok.url_count,
                            tok.informative_count,
                            LEAST(
                                1.0,
                                GREATEST(
                                    0.0,
                                    (
                                        CASE c.topic_type
                                            WHEN 'cashtag' THEN 0.74
                                            WHEN 'hashtag' THEN 0.67
                                            WHEN 'entity' THEN 0.56
                                            ELSE 0.40
                                        END
                                        + (c.quality_score * 0.20)
                                        + (c.alias_confidence * 0.18)
                                        + CASE WHEN tok.token_count >= 2 THEN 0.12 ELSE 0 END
                                        + CASE
                                            WHEN c.topic_key IN (SELECT token FROM acronym_allowlist) THEN 0.12
                                            ELSE 0
                                          END
                                        - ((tok.weak_count::double precision / NULLIF(tok.token_count, 0)) * 0.48)
                                        - ((tok.noise_count::double precision / NULLIF(tok.token_count, 0)) * 0.70)
                                        - ((tok.url_count::double precision / NULLIF(tok.token_count, 0)) * 1.00)
                                        - CASE
                                            WHEN tok.token_count = 1
                                              AND c.topic_key NOT IN (SELECT token FROM acronym_allowlist)
                                              AND LENGTH(c.topic_key) < 4
                                                THEN 0.32
                                            ELSE 0
                                          END
                                    )
                                )
                            ) AS topic_confidence
                        FROM canonicalized c
                        CROSS JOIN LATERAL (
                            SELECT
                                COALESCE(ARRAY_LENGTH(string_to_array(c.topic_key, ' '), 1), 0)::int AS token_count,
                                COALESCE((
                                    SELECT COUNT(*)
                                    FROM unnest(string_to_array(c.topic_key, ' ')) AS t(token)
                                    WHERE t.token IN (SELECT token FROM weak_topic_tokens)
                                ), 0)::int AS weak_count,
                                COALESCE((
                                    SELECT COUNT(*)
                                    FROM unnest(string_to_array(c.topic_key, ' ')) AS t(token)
                                    WHERE t.token IN (SELECT token FROM topic_noise_tokens)
                                ), 0)::int AS noise_count,
                                COALESCE((
                                    SELECT COUNT(*)
                                    FROM unnest(string_to_array(c.topic_key, ' ')) AS t(token)
                                    WHERE t.token IN (SELECT token FROM number_word_tokens)
                                ), 0)::int AS number_count,
                                COALESCE((
                                    SELECT COUNT(*)
                                    FROM unnest(string_to_array(c.topic_key, ' ')) AS t(token)
                                    WHERE t.token IN (SELECT token FROM url_debris_tokens)
                                ), 0)::int AS url_count,
                                COALESCE((
                                    SELECT COUNT(*)
                                    FROM unnest(string_to_array(c.topic_key, ' ')) AS t(token)
                                    WHERE t.token <> ''
                                      AND t.token NOT IN (SELECT token FROM weak_topic_tokens)
                                      AND t.token NOT IN (SELECT token FROM topic_noise_tokens)
                                      AND t.token NOT IN (SELECT token FROM number_word_tokens)
                                      AND t.token NOT IN (SELECT token FROM url_debris_tokens)
                                      AND (
                                          LENGTH(t.token) >= 4
                                          OR t.token IN (SELECT token FROM acronym_allowlist)
                                      )
                                ), 0)::int AS informative_count
                        ) tok
                    ),
                    mention_candidates AS (
                        SELECT
                            *
                        FROM scored
                        WHERE topic_key <> ''
                          AND topic_key NOT IN ('all', 'general', 'digit')
                          AND NOT (topic_text_lc LIKE 'digit:%%in words:%%')
                          AND length(replace(topic_key, ' ', '')) >= 2
                          AND token_count BETWEEN 1 AND 5
                          AND topic_key !~ '^[0-9]+$'
                          AND topic_key !~* '(^| )(https?|www|com|co|t|ly|amp)( |$)'
                          AND topic_key !~* '(^| )([a-z0-9]+bot)( |$)'
                          AND topic_key !~* '(area|forecast|discussion).*(afd|airnow|aqi)'
                          AND topic_key !~* '(additional|details) here'
                          AND (
                              token_count > 1
                              OR (
                                  topic_key NOT IN (SELECT token FROM weak_topic_tokens)
                                  AND topic_key NOT IN (SELECT token FROM topic_noise_tokens)
                                  AND topic_key NOT IN (SELECT token FROM number_word_tokens)
                                  AND topic_key NOT IN (SELECT token FROM url_debris_tokens)
                                  AND (
                                      length(topic_key) >= 4
                                      OR topic_key IN (SELECT token FROM acronym_allowlist)
                                      OR topic_type IN ('cashtag', 'hashtag')
                                  )
                              )
                          )
                          AND number_count < token_count
                          AND informative_count > 0
                          AND url_count = 0
                          AND topic_confidence >= CASE
                              WHEN topic_type = 'cashtag' THEN 0.34
                              WHEN topic_type = 'hashtag' THEN 0.38
                              WHEN topic_type = 'entity' THEN 0.45
                              ELSE 0.62
                          END
                    ),
                    deduped AS (
                        SELECT DISTINCT ON (raw_post_id, platform, topic_key)
                            raw_post_id,
                            processed_post_id,
                            platform,
                            topic_key,
                            topic_label,
                            event_timestamp,
                            ingested_at,
                            author_id,
                            sentiment_label,
                            quality_score,
                            topic_confidence,
                            is_repost,
                            is_reply,
                            has_link
                        FROM mention_candidates
                        ORDER BY raw_post_id, platform, topic_key, topic_confidence DESC, quality_score DESC, event_timestamp DESC
                    )
                    INSERT INTO public.post_topic_mentions (
                        raw_post_id,
                        processed_post_id,
                        platform,
                        topic_key,
                        topic_label,
                        event_timestamp,
                        ingested_at,
                        author_id,
                        sentiment_label,
                        quality_score,
                        topic_confidence,
                        is_repost,
                        is_reply,
                        has_link
                    )
                    SELECT
                        raw_post_id,
                        processed_post_id,
                        platform,
                        topic_key,
                        topic_label,
                        event_timestamp,
                        ingested_at,
                        author_id,
                        sentiment_label,
                        quality_score,
                        topic_confidence,
                        is_repost,
                        is_reply,
                        has_link
                    FROM deduped
                    ON CONFLICT (raw_post_id, platform, topic_key) DO UPDATE
                    SET processed_post_id = COALESCE(EXCLUDED.processed_post_id, public.post_topic_mentions.processed_post_id),
                        topic_label = COALESCE(EXCLUDED.topic_label, public.post_topic_mentions.topic_label),
                        event_timestamp = GREATEST(public.post_topic_mentions.event_timestamp, EXCLUDED.event_timestamp),
                        ingested_at = GREATEST(public.post_topic_mentions.ingested_at, EXCLUDED.ingested_at),
                        author_id = COALESCE(EXCLUDED.author_id, public.post_topic_mentions.author_id),
                        sentiment_label = EXCLUDED.sentiment_label,
                        quality_score = EXCLUDED.quality_score,
                        topic_confidence = GREATEST(public.post_topic_mentions.topic_confidence, EXCLUDED.topic_confidence),
                        is_repost = EXCLUDED.is_repost,
                        is_reply = EXCLUDED.is_reply,
                        has_link = EXCLUDED.has_link
                    """
                    ,
                    (
                        lookback_hours,
                        lookback_hours,
                        weak_tokens,
                        noise_tokens,
                        number_tokens,
                        url_debris_tokens,
                        acronym_tokens,
                    ),
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("sync_post_topic_mentions_from_post_topics", operation)

    def refresh_stable_topic_read_models(
        self,
        *,
        lag_minutes: int = 3,
        recompute_hours: int = 48,
        series_max_topics: int = 300,
        series_min_mentions: int = 2,
    ) -> dict[str, Any]:
        lag_minutes = max(1, int(lag_minutes))
        recompute_hours = max(1, int(recompute_hours))
        series_max_topics = max(25, int(series_max_topics))
        series_min_mentions = max(1, int(series_min_mentions))

        def refresh_day_totals(cursor: Any, day_value: date) -> int:
            cursor.execute(
                """
                DELETE FROM public.topic_day_totals
                WHERE day = %s
                """,
                (day_value,),
            )
            cursor.execute(
                """
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
                    %s::date AS day,
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
                WHERE (b.bucket_minute AT TIME ZONE 'utc')::date = %s::date
                GROUP BY b.topic_key, COALESCE(t.canonical_label, INITCAP(b.topic_key))
                """,
                (day_value, day_value),
            )
            return int(cursor.rowcount or 0)

        def refresh_day_series(cursor: Any, day_value: date) -> int:
            cursor.execute(
                """
                DELETE FROM public.topic_day_series_5m
                WHERE day = %s
                """,
                (day_value,),
            )
            cursor.execute(
                """
                WITH day_bounds AS (
                    SELECT
                        (%s::date::text || ' 00:00:00+00')::timestamptz AS day_start,
                        ((%s::date + 1)::text || ' 00:00:00+00')::timestamptz AS day_end
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
                    WHERE d.day = %s::date
                      AND d.total_mentions >= %s
                    ORDER BY d.total_mentions DESC, d.topic_key ASC
                    LIMIT %s
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
                        %s::date AS day,
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
                FROM filled
                """,
                (
                    day_value,
                    day_value,
                    day_value,
                    series_min_mentions,
                    series_max_topics,
                    day_value,
                ),
            )
            return int(cursor.rowcount or 0)

        def refresh_rolling_24h(cursor: Any, *, window_start: datetime, window_end: datetime) -> int:
            cursor.execute("DELETE FROM public.topic_rolling_24h")
            cursor.execute(
                """
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
                    %s::timestamptz AS window_start,
                    %s::timestamptz AS window_end,
                    now() AS updated_at
                FROM public.post_topic_mentions m
                LEFT JOIN public.topics t
                  ON t.topic_key = m.topic_key
                WHERE m.event_timestamp >= %s::timestamptz
                  AND m.event_timestamp < %s::timestamptz
                  AND COALESCE(m.topic_confidence, 0) >= 0.34
                GROUP BY m.topic_key, t.canonical_label
                """,
                (window_start, window_end, window_start, window_end),
            )
            return int(cursor.rowcount or 0)

        def operation(connection: psycopg.Connection[Any]) -> dict[str, Any]:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        to_regclass('public.post_topic_mentions'),
                        to_regclass('public.topic_buckets_1m_final'),
                        to_regclass('public.topic_day_totals'),
                        to_regclass('public.topic_day_series_5m'),
                        to_regclass('public.topic_rolling_24h'),
                        to_regclass('public.topic_read_model_state')
                    """
                )
                tables = cursor.fetchone() or (None, None, None, None, None, None)
                if not all(tables):
                    return {
                        "skipped": True,
                        "reason": "missing_stable_read_model_tables",
                        "refreshed_at": datetime.now(timezone.utc).isoformat(),
                    }

                cursor.execute(
                    """
                    WITH computed AS (
                        SELECT
                            date_trunc('minute', now() - make_interval(mins => %s)) AS finalize_before,
                            date_trunc('minute', now() - make_interval(hours => %s)) AS hard_recompute_from,
                            (
                                SELECT last_finalize_before
                                FROM public.topic_read_model_state
                                WHERE id = 1
                            ) AS last_finalize_before
                    ),
                    bounded AS (
                        SELECT
                            finalize_before,
                            GREATEST(
                                hard_recompute_from,
                                COALESCE(last_finalize_before - interval '45 minute', hard_recompute_from)
                            ) AS recompute_from
                        FROM computed
                    )
                    SELECT finalize_before, recompute_from
                    FROM bounded
                    """,
                    (lag_minutes, recompute_hours),
                )
                bounds_row = cursor.fetchone() or (None, None)
                finalize_before = bounds_row[0]
                recompute_from = bounds_row[1]
                if not isinstance(finalize_before, datetime) or not isinstance(recompute_from, datetime):
                    return {
                        "skipped": True,
                        "reason": "missing_refresh_bounds",
                        "refreshed_at": datetime.now(timezone.utc).isoformat(),
                    }
                if recompute_from >= finalize_before:
                    recompute_from = finalize_before - timedelta(minutes=1)

                cursor.execute(
                    """
                    DELETE FROM public.topic_buckets_1m_final
                    WHERE bucket_minute >= %s::timestamptz
                      AND bucket_minute < %s::timestamptz
                    """,
                    (recompute_from, finalize_before),
                )
                cursor.execute(
                    """
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
                    WHERE date_trunc('minute', m.event_timestamp) >= %s::timestamptz
                      AND date_trunc('minute', m.event_timestamp) < %s::timestamptz
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
                        finalized_at = EXCLUDED.finalized_at
                    """,
                    (recompute_from, finalize_before),
                )
                finalized_rows = int(cursor.rowcount or 0)

                cursor.execute(
                    """
                    INSERT INTO public.topic_read_model_state (id, last_finalize_before, updated_at)
                    VALUES (1, %s::timestamptz, now())
                    ON CONFLICT (id) DO UPDATE
                    SET last_finalize_before = EXCLUDED.last_finalize_before,
                        updated_at = now()
                    """,
                    (finalize_before,),
                )

                today_utc = datetime.now(timezone.utc).date()
                yesterday_utc = today_utc - timedelta(days=1)
                totals_today = refresh_day_totals(cursor, today_utc)
                totals_yesterday = refresh_day_totals(cursor, yesterday_utc)
                series_today = refresh_day_series(cursor, today_utc)
                series_yesterday = refresh_day_series(cursor, yesterday_utc)
                rolling_window_end = finalize_before
                rolling_window_start = rolling_window_end - timedelta(hours=24)
                rolling_rows = refresh_rolling_24h(
                    cursor,
                    window_start=rolling_window_start,
                    window_end=rolling_window_end,
                )

                return {
                    "finalized_bucket_rows": finalized_rows,
                    "day_totals_today_rows": totals_today,
                    "day_totals_yesterday_rows": totals_yesterday,
                    "day_series_today_rows": series_today,
                    "day_series_yesterday_rows": series_yesterday,
                    "rolling_24h_rows": rolling_rows,
                    "rolling_window_start": rolling_window_start.isoformat(),
                    "rolling_window_end": rolling_window_end.isoformat(),
                    "refreshed_at": datetime.now(timezone.utc).isoformat(),
                }

        return self._execute_write("refresh_stable_topic_read_models", operation)

    def cleanup_garbage_post_topic_mentions(self, *, lookback_hours: int = 168) -> int:
        lookback_hours = max(1, int(lookback_hours))
        weak_tokens = sorted(set(TOPIC_GENERIC_WEAK_TOKENS))
        noise_tokens = sorted(set(TOPIC_NOISE_TOKENS))
        number_tokens = sorted(set(TOPIC_NUMBER_WORD_TOKENS))
        url_debris_tokens = sorted(set(TOPIC_URL_DEBRIS_TOKENS))
        acronym_tokens = sorted(set(TOPIC_ACRONYM_ALLOWLIST))

        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('public.post_topic_mentions')")
                relation = cursor.fetchone()
                if not relation or relation[0] is None:
                    return 0

                cursor.execute(
                    """
                    WITH weak_topic_tokens(token) AS (SELECT unnest(%s::text[])),
                    topic_noise_tokens(token) AS (SELECT unnest(%s::text[])),
                    number_word_tokens(token) AS (SELECT unnest(%s::text[])),
                    url_debris_tokens(token) AS (SELECT unnest(%s::text[])),
                    acronym_allowlist(token) AS (SELECT unnest(%s::text[])),
                    bad_mentions AS (
                        SELECT m.mention_id
                        FROM public.post_topic_mentions m
                        WHERE m.event_timestamp >= now() - make_interval(hours => %s)
                          AND (
                              COALESCE(TRIM(m.topic_key), '') = ''
                              OR LOWER(m.topic_key) IN ('all', 'general', 'digit')
                              OR COALESCE(m.topic_confidence, 0) < 0.20
                              OR LOWER(m.topic_key) ~* '(^| )([a-z0-9]+bot)( |$)'
                              OR LOWER(m.topic_key) ~* '(^| )(https?|www|com|co|t|ly|amp)( |$)'
                              OR LOWER(m.topic_key) ~* '(area|forecast|discussion).*(afd|airnow|aqi)'
                              OR LOWER(m.topic_key) ~* '(additional|details) here'
                              OR (
                                  array_length(string_to_array(LOWER(m.topic_key), ' '), 1) = 1
                                  AND (
                                      LOWER(m.topic_key) IN (SELECT token FROM weak_topic_tokens)
                                      OR LOWER(m.topic_key) IN (SELECT token FROM topic_noise_tokens)
                                      OR LOWER(m.topic_key) IN (SELECT token FROM number_word_tokens)
                                      OR LOWER(m.topic_key) IN (SELECT token FROM url_debris_tokens)
                                  )
                              )
                              OR (
                                  array_length(string_to_array(LOWER(m.topic_key), ' '), 1) >= 1
                                  AND (
                                      SELECT COUNT(*)
                                      FROM unnest(string_to_array(LOWER(m.topic_key), ' ')) AS t(token)
                                      WHERE t.token <> ''
                                        AND t.token IN (SELECT token FROM number_word_tokens)
                                  ) = array_length(string_to_array(LOWER(m.topic_key), ' '), 1)
                              )
                              OR (
                                  array_length(string_to_array(LOWER(m.topic_key), ' '), 1) > 0
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM unnest(string_to_array(LOWER(m.topic_key), ' ')) AS t(token)
                                      WHERE t.token <> ''
                                        AND t.token NOT IN (SELECT token FROM weak_topic_tokens)
                                        AND t.token NOT IN (SELECT token FROM topic_noise_tokens)
                                        AND t.token NOT IN (SELECT token FROM number_word_tokens)
                                        AND t.token NOT IN (SELECT token FROM url_debris_tokens)
                                        AND (
                                            length(t.token) >= 4
                                            OR t.token IN (SELECT token FROM acronym_allowlist)
                                        )
                                  )
                              )
                          )
                    )
                    DELETE FROM public.post_topic_mentions m
                    USING bad_mentions b
                    WHERE m.mention_id = b.mention_id
                    """,
                    (
                        weak_tokens,
                        noise_tokens,
                        number_tokens,
                        url_debris_tokens,
                        acronym_tokens,
                        lookback_hours,
                    ),
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("cleanup_garbage_post_topic_mentions", operation)

    def prune_raw_posts_older_than(self, *, hours: float) -> int:
        retention_hours = max(0.0, float(hours))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH deleted AS (
                        DELETE FROM public.raw_posts
                        WHERE COALESCE(ingested_at, inserted_at, created_at, now()) < %s
                        RETURNING id
                    )
                    SELECT COUNT(*)::BIGINT FROM deleted
                    """,
                    (cutoff,),
                )
                row = cursor.fetchone()
                return int((row or [0])[0] or 0)

        return self._execute_write("prune_raw_posts_older_than", operation)

    def upsert_authors(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = [self._prepare_author_row(row) for row in rows]
        payload = [row for row in payload if row.get("author_id")]
        if not payload:
            return 0

        def operation(connection: psycopg.Connection[Any]) -> int:
            affected_total = 0
            with connection.cursor() as cursor:
                for chunk in _chunked(payload, self._batch_size):
                    cursor.executemany(
                        """
                        INSERT INTO public.authors (
                            platform,
                            author_id,
                            author_handle,
                            display_name,
                            followers_count,
                            metadata_json,
                            first_seen_at,
                            last_seen_at
                        ) VALUES (
                            %(platform)s,
                            %(author_id)s,
                            %(author_handle)s,
                            %(display_name)s,
                            %(followers_count)s,
                            %(metadata_json)s,
                            %(first_seen_at)s,
                            %(last_seen_at)s
                        )
                        ON CONFLICT (platform, author_id) DO UPDATE
                        SET author_handle = COALESCE(EXCLUDED.author_handle, public.authors.author_handle),
                            display_name = COALESCE(EXCLUDED.display_name, public.authors.display_name),
                            followers_count = COALESCE(EXCLUDED.followers_count, public.authors.followers_count),
                            metadata_json = COALESCE(EXCLUDED.metadata_json, public.authors.metadata_json),
                            first_seen_at = CASE
                                WHEN public.authors.first_seen_at IS NULL THEN EXCLUDED.first_seen_at
                                WHEN EXCLUDED.first_seen_at IS NULL THEN public.authors.first_seen_at
                                ELSE LEAST(public.authors.first_seen_at, EXCLUDED.first_seen_at)
                            END,
                            last_seen_at = CASE
                                WHEN public.authors.last_seen_at IS NULL THEN EXCLUDED.last_seen_at
                                WHEN EXCLUDED.last_seen_at IS NULL THEN public.authors.last_seen_at
                                ELSE GREATEST(public.authors.last_seen_at, EXCLUDED.last_seen_at)
                            END
                        """,
                        list(chunk),
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        affected_total += int(cursor.rowcount)
            return affected_total

        return self._execute_write("upsert_authors", operation)

    def _ensure_core_tables(self) -> None:
        if self._conn is None:
            return

        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS public.raw_posts (
                    id BIGSERIAL PRIMARY KEY,
                    platform TEXT NOT NULL,
                    source_post_id TEXT,
                    source_uri TEXT,
                    source_cid TEXT,
                    author_did TEXT,
                    post_id TEXT NOT NULL,
                    author_id TEXT,
                    author_handle TEXT,
                    root_post_id TEXT,
                    reply_parent_id TEXT,
                    created_at TIMESTAMPTZ,
                    inserted_at TIMESTAMPTZ DEFAULT now(),
                    ingested_at TIMESTAMPTZ,
                    raw_text TEXT,
                    text_content TEXT,
                    language TEXT,
                    urls TEXT[],
                    hashtags TEXT[],
                    like_count INTEGER,
                    repost_count INTEGER,
                    reply_count INTEGER,
                    reply_to_uri TEXT,
                    repost_of_uri TEXT,
                    processed BOOLEAN NOT NULL DEFAULT false,
                    metrics_json JSONB,
                    raw_json JSONB
                )
                """
            )
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS id BIGINT")
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS public.raw_posts_id_seq")
            cursor.execute(
                """
                ALTER TABLE public.raw_posts
                ALTER COLUMN id SET DEFAULT nextval('public.raw_posts_id_seq')
                """
            )
            cursor.execute(
                """
                ALTER SEQUENCE public.raw_posts_id_seq
                OWNED BY public.raw_posts.id
                """
            )
            cursor.execute(
                """
                UPDATE public.raw_posts
                SET id = nextval('public.raw_posts_id_seq')
                WHERE id IS NULL
                """
            )
            cursor.execute(
                """
                SELECT setval(
                    'public.raw_posts_id_seq',
                    GREATEST(
                        COALESCE((SELECT MAX(id) FROM public.raw_posts), 0),
                        1
                    ),
                    true
                )
                """
            )
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS platform TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS source_post_id TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS source_uri TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS source_cid TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS author_did TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS post_id TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS author_id TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS author_handle TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS root_post_id TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS reply_parent_id TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS raw_text TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS text_content TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS language TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS urls TEXT[]")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS hashtags TEXT[]")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS like_count INTEGER")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS repost_count INTEGER")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS reply_count INTEGER")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS reply_to_uri TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS repost_of_uri TEXT")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS processed BOOLEAN")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS metrics_json JSONB")
            cursor.execute("ALTER TABLE public.raw_posts ADD COLUMN IF NOT EXISTS raw_json JSONB")
            cursor.execute("ALTER TABLE public.raw_posts ALTER COLUMN id SET NOT NULL")
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_posts_id
                ON public.raw_posts (id)
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS public.authors (
                    platform TEXT NOT NULL,
                    author_id TEXT NOT NULL,
                    author_handle TEXT,
                    display_name TEXT,
                    followers_count BIGINT,
                    metadata_json JSONB,
                    first_seen_at TIMESTAMPTZ,
                    last_seen_at TIMESTAMPTZ
                )
                """
            )
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS platform TEXT")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS author_id TEXT")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS author_handle TEXT")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS display_name TEXT")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS followers_count BIGINT")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS metadata_json JSONB")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.authors ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS public.ingestion_runs (
                    source TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    ended_at TIMESTAMPTZ,
                    status TEXT,
                    rows_inserted BIGINT NOT NULL DEFAULT 0,
                    notes JSONB
                )
                """
            )
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS source TEXT")
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ")
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS status TEXT")
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS rows_inserted BIGINT")
            cursor.execute("ALTER TABLE public.ingestion_runs ADD COLUMN IF NOT EXISTS notes JSONB")

            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_posts_platform_post_id
                ON public.raw_posts (platform, post_id)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS raw_posts_platform_source_post_id_idx
                ON public.raw_posts (platform, source_post_id)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_authors_platform_author_id
                ON public.authors (platform, author_id)
                """
            )
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_runs_source_started_at
                ON public.ingestion_runs (source, started_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_raw_posts_created_at
                ON public.raw_posts (created_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_raw_posts_ingested_at
                ON public.raw_posts (ingested_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_raw_posts_platform_created_at
                ON public.raw_posts (platform, created_at DESC)
                """
            )

        self._conn.commit()

    def ensure_metric_bucket_tables(self) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            lock_key = 9_148_221
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.metric_buckets_1m (
                        bucket_start TIMESTAMPTZ NOT NULL,
                        platform TEXT NOT NULL,
                        mention_count BIGINT NOT NULL,
                        unique_authors BIGINT NOT NULL,
                        PRIMARY KEY (bucket_start, platform)
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS bucket_start TIMESTAMPTZ
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS platform TEXT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS mention_count BIGINT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS raw_post_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS processed_post_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS unique_authors BIGINT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS reply_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS repost_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS link_post_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS media_post_count INTEGER
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS avg_quality_score NUMERIC
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1m
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.metric_buckets_1h (
                        bucket_start TIMESTAMPTZ NOT NULL,
                        platform TEXT NOT NULL,
                        mention_count BIGINT NOT NULL,
                        unique_authors BIGINT NOT NULL,
                        PRIMARY KEY (bucket_start, platform)
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1h
                    ADD COLUMN IF NOT EXISTS bucket_start TIMESTAMPTZ
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1h
                    ADD COLUMN IF NOT EXISTS platform TEXT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1h
                    ADD COLUMN IF NOT EXISTS mention_count BIGINT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE public.metric_buckets_1h
                    ADD COLUMN IF NOT EXISTS unique_authors BIGINT
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_buckets_1m_bucket_platform
                    ON public.metric_buckets_1m (bucket_start, platform)
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS metric_buckets_1m_pkey
                    ON public.metric_buckets_1m (bucket_minute, platform)
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_buckets_1h_bucket_platform
                    ON public.metric_buckets_1h (bucket_start, platform)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_metric_buckets_1m_platform_bucket
                    ON public.metric_buckets_1m (platform, bucket_start DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_metric_buckets_1h_platform_bucket
                    ON public.metric_buckets_1h (platform, bucket_start DESC)
                    """
                )

        self._execute_write("ensure_metric_bucket_tables", operation)

    def aggregate_metric_buckets_1m(self) -> int:
        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.metric_buckets_1m (
                        bucket_minute,
                        bucket_start,
                        platform,
                        raw_post_count,
                        processed_post_count,
                        mention_count,
                        unique_authors,
                        reply_count,
                        repost_count,
                        link_post_count,
                        media_post_count,
                        avg_quality_score,
                        created_at
                    )
                    SELECT
                        date_trunc('minute', created_at) AS bucket_minute,
                        date_trunc('minute', created_at) AS bucket_start,
                        platform,
                        COUNT(*)::INT AS raw_post_count,
                        COUNT(*)::INT AS processed_post_count,
                        COUNT(*)::BIGINT AS mention_count,
                        COUNT(DISTINCT author_id)::BIGINT AS unique_authors,
                        SUM(CASE WHEN reply_parent_id IS NOT NULL THEN 1 ELSE 0 END)::INT AS reply_count,
                        SUM(CASE WHEN repost_of_uri IS NOT NULL THEN 1 ELSE 0 END)::INT AS repost_count,
                        SUM(CASE WHEN COALESCE(array_length(urls, 1), 0) > 0 THEN 1 ELSE 0 END)::INT AS link_post_count,
                        0::INT AS media_post_count,
                        0::NUMERIC AS avg_quality_score,
                        now() AS created_at
                    FROM public.raw_posts
                    WHERE created_at IS NOT NULL
                      AND platform IS NOT NULL
                    GROUP BY 1, 2, 3
                    ON CONFLICT (bucket_minute, platform) DO UPDATE
                    SET bucket_start = EXCLUDED.bucket_start,
                        raw_post_count = EXCLUDED.raw_post_count,
                        processed_post_count = EXCLUDED.processed_post_count,
                        mention_count = EXCLUDED.mention_count,
                        unique_authors = EXCLUDED.unique_authors,
                        reply_count = EXCLUDED.reply_count,
                        repost_count = EXCLUDED.repost_count,
                        link_post_count = EXCLUDED.link_post_count,
                        media_post_count = EXCLUDED.media_post_count,
                        avg_quality_score = EXCLUDED.avg_quality_score,
                        created_at = EXCLUDED.created_at
                    """
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("aggregate_metric_buckets_1m", operation)

    def aggregate_metric_buckets_1h(self) -> int:
        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.metric_buckets_1h (
                        bucket_start,
                        platform,
                        mention_count,
                        unique_authors
                    )
                    SELECT
                        date_trunc('hour', created_at) AS bucket_start,
                        platform,
                        COUNT(*)::BIGINT AS mention_count,
                        COUNT(DISTINCT author_id)::BIGINT AS unique_authors
                    FROM public.raw_posts
                    WHERE created_at IS NOT NULL
                      AND platform IS NOT NULL
                    GROUP BY 1, 2
                    ON CONFLICT (bucket_start, platform) DO UPDATE
                    SET mention_count = EXCLUDED.mention_count,
                        unique_authors = EXCLUDED.unique_authors
                    """
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("aggregate_metric_buckets_1h", operation)

    def ensure_processed_topic_tables(self) -> None:
        def operation(connection: psycopg.Connection[Any]) -> None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.processed_posts (
                        id BIGSERIAL PRIMARY KEY,
                        raw_post_id BIGINT NOT NULL,
                        platform TEXT NOT NULL,
                        source_post_id TEXT NOT NULL,
                        source_created_at TIMESTAMPTZ,
                        post_id TEXT,
                        author_id TEXT,
                        created_at TIMESTAMPTZ NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        bucket_minute TIMESTAMPTZ NOT NULL,
                        clean_text TEXT,
                        normalized_text TEXT,
                        language TEXT,
                        quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                        topic_key_candidate TEXT,
                        tokens TEXT[] NOT NULL DEFAULT '{}'::text[],
                        mentions TEXT[] NOT NULL DEFAULT '{}'::text[],
                        tags TEXT[] NOT NULL DEFAULT '{}'::text[],
                        topic_entities TEXT[] NOT NULL DEFAULT '{}'::text[],
                        sentiment_label TEXT NOT NULL DEFAULT 'neutral',
                        sentiment_positive_score INTEGER NOT NULL DEFAULT 0,
                        sentiment_negative_score INTEGER NOT NULL DEFAULT 0,
                        sentiment_neutral_score INTEGER NOT NULL DEFAULT 0,
                        has_media BOOLEAN NOT NULL DEFAULT false,
                        is_reply BOOLEAN NOT NULL DEFAULT false,
                        is_repost BOOLEAN NOT NULL DEFAULT false,
                        is_quote BOOLEAN NOT NULL DEFAULT false,
                        author_hash TEXT,
                        token_count INTEGER NOT NULL DEFAULT 0,
                        fingerprint TEXT,
                        hashtags TEXT[] NOT NULL DEFAULT '{}'::text[],
                        cashtags TEXT[] NOT NULL DEFAULT '{}'::text[],
                        domains TEXT[] NOT NULL DEFAULT '{}'::text[],
                        urls TEXT[] NOT NULL DEFAULT '{}'::text[],
                        key_phrases TEXT[] NOT NULL DEFAULT '{}'::text[],
                        topic_seeds TEXT[] NOT NULL DEFAULT '{}'::text[],
                        spam_score NUMERIC NOT NULL DEFAULT 0,
                        topic TEXT
                    )
                    """
                )
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS raw_post_id BIGINT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS platform TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS source_post_id TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS source_created_at TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS post_id TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS author_id TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS clean_text TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS normalized_text TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS language TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS topic_key_candidate TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS tokens TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS mentions TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS tags TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS topic_entities TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_label TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_positive_score INTEGER")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_negative_score INTEGER")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS sentiment_neutral_score INTEGER")
                cursor.execute("ALTER TABLE public.processed_posts ALTER COLUMN topic_entities SET DEFAULT '{}'::text[]")
                cursor.execute("ALTER TABLE public.processed_posts ALTER COLUMN sentiment_label SET DEFAULT 'neutral'")
                cursor.execute("ALTER TABLE public.processed_posts ALTER COLUMN sentiment_positive_score SET DEFAULT 0")
                cursor.execute("ALTER TABLE public.processed_posts ALTER COLUMN sentiment_negative_score SET DEFAULT 0")
                cursor.execute("ALTER TABLE public.processed_posts ALTER COLUMN sentiment_neutral_score SET DEFAULT 0")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS has_media BOOLEAN")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS is_reply BOOLEAN")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS is_repost BOOLEAN")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS is_quote BOOLEAN")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS author_hash TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS token_count INTEGER")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS fingerprint TEXT")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS hashtags TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS cashtags TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS domains TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS urls TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS key_phrases TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS topic_seeds TEXT[]")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS spam_score NUMERIC")
                cursor.execute("ALTER TABLE public.processed_posts ADD COLUMN IF NOT EXISTS topic TEXT")
                cursor.execute(
                    """
                    UPDATE public.processed_posts
                    SET processed_at = now()
                    WHERE processed_at IS NULL
                    """
                )
                cursor.execute(
                    """
                    UPDATE public.processed_posts
                    SET bucket_minute = date_trunc(
                        'minute',
                        COALESCE(source_created_at, created_at, processed_at, now())
                    )
                    WHERE bucket_minute IS NULL
                    """
                )
                cursor.execute(
                    """
                    UPDATE public.processed_posts
                    SET sentiment_label = COALESCE(sentiment_label, 'neutral'),
                        sentiment_positive_score = COALESCE(sentiment_positive_score, 0),
                        sentiment_negative_score = COALESCE(sentiment_negative_score, 0),
                        sentiment_neutral_score = COALESCE(sentiment_neutral_score, 0),
                        topic_entities = COALESCE(topic_entities, '{}'::text[])
                    WHERE sentiment_label IS NULL
                       OR sentiment_positive_score IS NULL
                       OR sentiment_negative_score IS NULL
                       OR sentiment_neutral_score IS NULL
                       OR topic_entities IS NULL
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.topic_buckets_1m (
                        bucket_minute TIMESTAMPTZ NOT NULL,
                        platform TEXT NOT NULL,
                        topic_key TEXT NOT NULL,
                        mention_count INTEGER NOT NULL,
                        unique_authors INTEGER NOT NULL,
                        total_quality_score NUMERIC NOT NULL DEFAULT 0,
                        avg_quality_score NUMERIC NOT NULL DEFAULT 0,
                        repost_count INTEGER NOT NULL DEFAULT 0,
                        reply_count INTEGER NOT NULL DEFAULT 0,
                        link_post_count INTEGER NOT NULL DEFAULT 0,
                        sample_size INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        bucket_start TIMESTAMPTZ,
                        topic TEXT,
                        PRIMARY KEY (bucket_minute, platform, topic_key)
                    )
                    """
                )
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS platform TEXT")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS topic_key TEXT")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS mention_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS unique_authors INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS total_quality_score NUMERIC")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS avg_quality_score NUMERIC")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS repost_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS reply_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS link_post_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS sample_size INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS bucket_start TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS topic TEXT")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS normalized_topic TEXT")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS topic_display TEXT")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS unique_posts INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS positive_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS neutral_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS negative_count INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS sentiment_net INTEGER")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS sentiment_score DOUBLE PRECISION")
                cursor.execute("ALTER TABLE public.topic_buckets_1m ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")
                cursor.execute(
                    """
                    UPDATE public.topic_buckets_1m
                    SET normalized_topic = COALESCE(
                            NULLIF(TRIM(normalized_topic), ''),
                            LOWER(COALESCE(NULLIF(TRIM(topic_key), ''), 'all'))
                        ),
                        topic_display = COALESCE(
                            NULLIF(TRIM(topic_display), ''),
                            NULLIF(TRIM(topic), ''),
                            INITCAP(COALESCE(NULLIF(TRIM(topic_key), ''), 'all'))
                        ),
                        unique_posts = COALESCE(unique_posts, mention_count, 0),
                        positive_count = COALESCE(positive_count, 0),
                        neutral_count = COALESCE(neutral_count, mention_count, 0),
                        negative_count = COALESCE(negative_count, 0),
                        sentiment_net = COALESCE(sentiment_net, 0),
                        sentiment_score = COALESCE(sentiment_score, 0),
                        updated_at = COALESCE(updated_at, created_at, now())
                    WHERE normalized_topic IS NULL
                       OR topic_display IS NULL
                       OR unique_posts IS NULL
                       OR positive_count IS NULL
                       OR neutral_count IS NULL
                       OR negative_count IS NULL
                       OR sentiment_net IS NULL
                       OR sentiment_score IS NULL
                       OR updated_at IS NULL
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_topic_buckets_1m_bucket_platform_topic
                    ON public.topic_buckets_1m (bucket_minute, platform, topic_key)
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_posts_raw_post_id
                    ON public.processed_posts (raw_post_id)
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_processed_posts_platform_source_post_id
                    ON public.processed_posts (platform, source_post_id)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_created_at
                    ON public.processed_posts (created_at DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_bucket_minute
                    ON public.processed_posts (bucket_minute)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_platform_bucket_minute
                    ON public.processed_posts (platform, bucket_minute)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_topic_key_candidate
                    ON public.processed_posts (topic_key_candidate)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_topic
                    ON public.processed_posts (platform, source_post_id, created_at DESC)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_sentiment_label
                    ON public.processed_posts (sentiment_label)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_posts_tags_gin
                    ON public.processed_posts
                    USING GIN (tags)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_topic_buckets_1m_platform_topic_bucket
                    ON public.topic_buckets_1m (platform, topic_key, bucket_minute DESC)
                    """
                )
                cursor.execute(
                    """
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
                    )
                    """
                )
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS id BIGSERIAL")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS raw_post_id BIGINT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS processed_post_id BIGINT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS platform TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS source_post_id TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS topic_text TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS normalized_topic TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS topic_type TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS language TEXT")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS source_created_at TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS bucket_minute TIMESTAMPTZ")
                cursor.execute("ALTER TABLE public.post_topics ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ")
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_post_topics_raw_topic_type
                    ON public.post_topics (raw_post_id, normalized_topic, topic_type)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_post_topics_bucket_minute
                    ON public.post_topics (bucket_minute)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_post_topics_normalized_topic
                    ON public.post_topics (normalized_topic)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_post_topics_platform_bucket_minute
                    ON public.post_topics (platform, bucket_minute)
                    """
                )

            self._load_column_metadata()

        self._execute_write("ensure_processed_topic_tables", operation)

    def refresh_processed_posts_from_raw_posts(self) -> int:
        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE public.processed_posts")
                cursor.execute(
                    """
                    INSERT INTO public.processed_posts (
                        raw_post_id,
                        platform,
                        source_post_id,
                        post_id,
                        author_id,
                        source_created_at,
                        created_at,
                        processed_at,
                        bucket_minute,
                        clean_text,
                        normalized_text,
                        language,
                        quality_score,
                        topic_key_candidate,
                        tokens,
                        has_media,
                        is_reply,
                        is_repost,
                        is_quote,
                        author_hash,
                        token_count,
                        mentions,
                        tags,
                        topic_entities,
                        hashtags,
                        cashtags,
                        domains,
                        urls,
                        key_phrases,
                        topic_seeds,
                        sentiment_label,
                        sentiment_positive_score,
                        sentiment_negative_score,
                        sentiment_neutral_score,
                        spam_score,
                        topic
                    )
                    SELECT
                        rp.id,
                        rp.platform,
                        COALESCE(rp.source_post_id, rp.post_id),
                        rp.post_id,
                        rp.author_id,
                        rp.created_at,
                        rp.created_at,
                        COALESCE(rp.ingested_at, rp.inserted_at, now()),
                        date_trunc('minute', rp.created_at),
                        COALESCE(rp.raw_text, rp.text_content),
                        COALESCE(rp.raw_text, rp.text_content),
                        rp.language,
                        0::double precision,
                        'general'::text,
                        '{}'::text[],
                        false,
                        (rp.reply_parent_id IS NOT NULL),
                        (rp.repost_of_uri IS NOT NULL),
                        false,
                        NULL::text,
                        0,
                        '{}'::text[],
                        ARRAY['general']::text[],
                        ARRAY['general']::text[],
                        COALESCE(rp.hashtags, '{}'::text[]),
                        '{}'::text[],
                        '{}'::text[],
                        COALESCE(rp.urls, '{}'::text[]),
                        '{}'::text[],
                        '{}'::text[],
                        'neutral'::text,
                        0::int,
                        0::int,
                        0::int,
                        0::numeric,
                        'all'::text AS topic
                    FROM public.raw_posts rp
                    WHERE rp.platform IS NOT NULL
                      AND COALESCE(rp.source_post_id, rp.post_id) IS NOT NULL
                      AND rp.created_at IS NOT NULL
                    """
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("refresh_processed_posts_from_raw_posts", operation)

    def aggregate_topic_buckets_1m_from_processed_posts(
        self,
        *,
        lookback_hours: float = 30.0,
        retention_hours: float = 192.0,
    ) -> int:
        recompute_lookback_hours = max(1.0, float(lookback_hours))
        stale_retention_hours = max(recompute_lookback_hours, float(retention_hours))

        def operation(connection: psycopg.Connection[Any]) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH aggregation_bounds AS (
                        SELECT date_trunc('minute', now() - (%s * interval '1 hour')) AS recompute_from
                    )
                    DELETE FROM public.topic_buckets_1m
                    WHERE bucket_minute >= (SELECT recompute_from FROM aggregation_bounds)
                    """,
                    (recompute_lookback_hours,),
                )
                cursor.execute(
                    """
                    DELETE FROM public.topic_buckets_1m
                    WHERE bucket_minute < date_trunc('minute', now() - (%s * interval '1 hour'))
                    """,
                    (stale_retention_hours,),
                )
                cursor.execute(
                    """
                    WITH aggregation_bounds AS (
                        SELECT date_trunc('minute', now() - (%s * interval '1 hour')) AS recompute_from
                    ),
                    weak_topic_tokens(token) AS (
                        VALUES
                            ('a'),
                            ('about'),
                            ('after'),
                            ('all'),
                            ('also'),
                            ('am'),
                            ('an'),
                            ('and'),
                            ('any'),
                            ('are'),
                            ('as'),
                            ('at'),
                            ('be'),
                            ('because'),
                            ('been'),
                            ('before'),
                            ('being'),
                            ('but'),
                            ('by'),
                            ('can'),
                            ('could'),
                            ('did'),
                            ('do'),
                            ('does'),
                            ('dont'),
                            ('for'),
                            ('from'),
                            ('get'),
                            ('got'),
                            ('had'),
                            ('has'),
                            ('have'),
                            ('here'),
                            ('how'),
                            ('i'),
                            ('ill'),
                            ('im'),
                            ('in'),
                            ('into'),
                            ('is'),
                            ('it'),
                            ('its'),
                            ('ive'),
                            ('just'),
                            ('like'),
                            ('many'),
                            ('may'),
                            ('me'),
                            ('might'),
                            ('more'),
                            ('most'),
                            ('my'),
                            ('need'),
                            ('no'),
                            ('not'),
                            ('now'),
                            ('of'),
                            ('on'),
                            ('one'),
                            ('or'),
                            ('our'),
                            ('same'),
                            ('she'),
                            ('should'),
                            ('so'),
                            ('some'),
                            ('still'),
                            ('that'),
                            ('the'),
                            ('their'),
                            ('them'),
                            ('then'),
                            ('there'),
                            ('these'),
                            ('they'),
                            ('this'),
                            ('those'),
                            ('to'),
                            ('today'),
                            ('tomorrow'),
                            ('us'),
                            ('very'),
                            ('was'),
                            ('we'),
                            ('were'),
                            ('what'),
                            ('when'),
                            ('where'),
                            ('which'),
                            ('who'),
                            ('why'),
                            ('will'),
                            ('with'),
                            ('would'),
                            ('absolutely'),
                            ('actually'),
                            ('ad'),
                            ('adorable'),
                            ('again'),
                            ('all'),
                            ('always'),
                            ('amazing'),
                            ('another'),
                            ('anyone'),
                            ('appreciate'),
                            ('area'),
                            ('available'),
                            ('back'),
                            ('believe'),
                            ('best'),
                            ('better'),
                            ('big'),
                            ('bluesky'),
                            ('bot'),
                            ('bsky'),
                            ('character'),
                            ('come'),
                            ('coming'),
                            ('cool'),
                            ('cute'),
                            ('day'),
                            ('digit'),
                            ('doing'),
                            ('don'),
                            ('early'),
                            ('else'),
                            ('enjoy'),
                            ('even'),
                            ('ever'),
                            ('every'),
                            ('everyone'),
                            ('exactly'),
                            ('facebook'),
                            ('feel'),
                            ('feels'),
                            ('feed'),
                            ('finally'),
                            ('first'),
                            ('funny'),
                            ('game'),
                            ('games'),
                            ('gave'),
                            ('global'),
                            ('going'),
                            ('gonna'),
                            ('gorgeous'),
                            ('half'),
                            ('happy'),
                            ('hear'),
                            ('hehe'),
                            ('hello'),
                            ('hey'),
                            ('hi'),
                            ('his'),
                            ('hope'),
                            ('house'),
                            ('id'),
                            ('imagine'),
                            ('incredible'),
                            ('instead'),
                            ('internet'),
                            ('kind'),
                            ('know'),
                            ('last'),
                            ('lets'),
                            ('little'),
                            ('live'),
                            ('looks'),
                            ('loved'),
                            ('love'),
                            ('major'),
                            ('make'),
                            ('making'),
                            ('mean'),
                            ('mind'),
                            ('much'),
                            ('myself'),
                            ('needs'),
                            ('never'),
                            ('new'),
                            ('news'),
                            ('next'),
                            ('nice'),
                            ('night'),
                            ('nowplaying'),
                            ('oh'),
                            ('ok'),
                            ('once'),
                            ('only'),
                            ('original'),
                            ('over'),
                            ('part'),
                            ('photography'),
                            ('place'),
                            ('please'),
                            ('post'),
                            ('pretty'),
                            ('probably'),
                            ('profile'),
                            ('pulse'),
                            ('read'),
                            ('right'),
                            ('said'),
                            ('say'),
                            ('see'),
                            ('seems'),
                            ('share'),
                            ('shit'),
                            ('short'),
                            ('social'),
                            ('someone'),
                            ('something'),
                            ('sometimes'),
                            ('sorry'),
                            ('stop'),
                            ('story'),
                            ('such'),
                            ('super'),
                            ('sure'),
                            ('sunday'),
                            ('talk'),
                            ('talent'),
                            ('technology'),
                            ('thank'),
                            ('thanks'),
                            ('thats'),
                            ('think'),
                            ('through'),
                            ('time'),
                            ('true'),
                            ('trying'),
                            ('tv'),
                            ('video'),
                            ('vote'),
                            ('wait'),
                            ('watching'),
                            ('well'),
                            ('went'),
                            ('while'),
                            ('wish'),
                            ('work'),
                            ('yeah'),
                            ('yes'),
                            ('young'),
                            ('you'),
                            ('your'),
                            ('de'),
                            ('del'),
                            ('der'),
                            ('des'),
                            ('die'),
                            ('el'),
                            ('en'),
                            ('es'),
                            ('est'),
                            ('et'),
                            ('ich'),
                            ('la'),
                            ('le'),
                            ('mas'),
                            ('pero'),
                            ('por'),
                            ('si'),
                            ('una'),
                            ('und')
                    ),
                    topic_noise_tokens(token) AS (
                        VALUES
                            ('additional'),
                            ('advisory'),
                            ('afd'),
                            ('airnow'),
                            ('aqi'),
                            ('details'),
                            ('discussion'),
                            ('forecast'),
                            ('iembot'),
                            ('issued'),
                            ('prelim'),
                            ('statement'),
                            ('update')
                    ),
                    single_word_topic_allowlist(token) AS (
                        VALUES
                            ('america'),
                            ('biden'),
                            ('bitcoin'),
                            ('btc'),
                            ('china'),
                            ('crypto'),
                            ('ethereum'),
                            ('eth'),
                            ('eu'),
                            ('gaza'),
                            ('iran'),
                            ('israel'),
                            ('maga'),
                            ('palestine'),
                            ('russia'),
                            ('solana'),
                            ('trump'),
                            ('uk'),
                            ('ukraine'),
                            ('usa')
                    ),
                    single_word_topic_minimum(min_mentions) AS (
                        VALUES (18)
                    ),
                    topic_mentions_raw AS (
                        SELECT
                            date_trunc(
                                'minute',
                                COALESCE(
                                    pt.bucket_minute,
                                    pp.bucket_minute,
                                    pp.source_created_at,
                                    pp.created_at,
                                    pp.processed_at,
                                    now()
                                )
                            ) AS bucket_minute,
                            COALESCE(NULLIF(TRIM(pt.platform), ''), COALESCE(pp.platform, 'bluesky')) AS platform,
                            CASE
                                WHEN btrim(
                                    regexp_replace(
                                        regexp_replace(lower(COALESCE(pt.normalized_topic, '')), '[^a-z0-9$#\\s]+', ' ', 'g'),
                                        '\\s+',
                                        ' ',
                                        'g'
                                    )
                                ) IN ('nokings', 'no king', 'no kings', 'no kings s', 'no kingss')
                                    THEN 'no kings'
                                ELSE btrim(
                                    regexp_replace(
                                        regexp_replace(lower(COALESCE(pt.normalized_topic, '')), '[^a-z0-9$#\\s]+', ' ', 'g'),
                                        '\\s+',
                                        ' ',
                                        'g'
                                    )
                                )
                            END AS normalized_topic,
                            COALESCE(NULLIF(TRIM(pt.topic_text), ''), NULLIF(TRIM(pt.normalized_topic), '')) AS topic_text,
                            pt.raw_post_id,
                            COALESCE(pp.author_id, '') AS author_id,
                            COALESCE(pp.quality_score, 0)::double precision AS quality_score,
                            COALESCE(pp.is_repost, false) AS is_repost,
                            COALESCE(pp.is_reply, false) AS is_reply,
                            (COALESCE(array_length(pp.urls, 1), 0) > 0) AS has_link,
                            CASE
                                WHEN COALESCE(pp.sentiment_label, '') IN ('positive', 'negative', 'neutral')
                                    THEN pp.sentiment_label
                                ELSE 'neutral'
                            END AS sentiment_label,
                            CASE
                                WHEN pt.topic_type = 'cashtag' THEN 1
                                WHEN pt.topic_type = 'hashtag' THEN 2
                                WHEN pt.topic_type = 'entity' THEN 3
                                WHEN pt.topic_type = 'keyword' THEN 4
                                ELSE 9
                            END AS topic_priority
                        FROM public.post_topics pt
                        LEFT JOIN public.processed_posts pp
                            ON pp.id = pt.processed_post_id
                        WHERE COALESCE(pt.normalized_topic, '') <> ''
                          AND date_trunc(
                              'minute',
                              COALESCE(
                                  pt.bucket_minute,
                                  pp.bucket_minute,
                                  pp.source_created_at,
                                  pp.created_at,
                                  pp.processed_at,
                                  now()
                              )
                          ) >= (SELECT recompute_from FROM aggregation_bounds)
                    ),
                    topic_mentions AS (
                        SELECT DISTINCT ON (raw_post_id, platform, normalized_topic)
                            bucket_minute,
                            platform,
                            normalized_topic,
                            topic_text,
                            raw_post_id,
                            author_id,
                            quality_score,
                            is_repost,
                            is_reply,
                            has_link,
                            sentiment_label,
                            topic_priority,
                            false AS from_fallback
                        FROM topic_mentions_raw
                        WHERE normalized_topic <> ''
                          AND normalized_topic NOT IN ('all', 'general')
                          AND normalized_topic NOT IN (SELECT token FROM weak_topic_tokens)
                          AND length(replace(normalized_topic, ' ', '')) >= 2
                          AND array_length(string_to_array(normalized_topic, ' '), 1) <= 4
                          AND NOT EXISTS (
                              SELECT 1
                              FROM unnest(string_to_array(normalized_topic, ' ')) AS topic_token(token)
                              WHERE topic_token.token <> ''
                                AND topic_token.token IN (SELECT token FROM topic_noise_tokens)
                          )
                        ORDER BY raw_post_id, platform, normalized_topic, topic_priority ASC, quality_score DESC
                    ),
                    fallback_mentions AS (
                        SELECT
                            date_trunc(
                                'minute',
                                COALESCE(
                                    pp.bucket_minute,
                                    pp.source_created_at,
                                    pp.created_at,
                                    pp.processed_at,
                                    now()
                                )
                            ) AS bucket_minute,
                            COALESCE(pp.platform, 'bluesky') AS platform,
                            CASE
                                WHEN btrim(
                                    regexp_replace(
                                        regexp_replace(
                                            lower(COALESCE(pp.topic, pp.topic_key_candidate, '')),
                                            '[^a-z0-9$#\\s]+',
                                            ' ',
                                            'g'
                                        ),
                                        '\\s+',
                                        ' ',
                                        'g'
                                    )
                                ) IN ('nokings', 'no king', 'no kings', 'no kings s', 'no kingss')
                                    THEN 'no kings'
                                ELSE btrim(
                                    regexp_replace(
                                        regexp_replace(
                                            lower(COALESCE(pp.topic, pp.topic_key_candidate, '')),
                                            '[^a-z0-9$#\\s]+',
                                            ' ',
                                            'g'
                                        ),
                                        '\\s+',
                                        ' ',
                                        'g'
                                    )
                                )
                            END AS normalized_topic,
                            COALESCE(NULLIF(TRIM(pp.topic), ''), NULLIF(TRIM(pp.topic_key_candidate), '')) AS topic_text,
                            pp.raw_post_id,
                            COALESCE(pp.author_id, '') AS author_id,
                            COALESCE(pp.quality_score, 0)::double precision AS quality_score,
                            COALESCE(pp.is_repost, false) AS is_repost,
                            COALESCE(pp.is_reply, false) AS is_reply,
                            (COALESCE(array_length(pp.urls, 1), 0) > 0) AS has_link,
                            CASE
                                WHEN COALESCE(pp.sentiment_label, '') IN ('positive', 'negative', 'neutral')
                                    THEN pp.sentiment_label
                                ELSE 'neutral'
                            END AS sentiment_label,
                            9 AS topic_priority,
                            true AS from_fallback
                        FROM public.processed_posts pp
                        WHERE COALESCE(pp.topic, pp.topic_key_candidate, '') <> ''
                          AND date_trunc(
                              'minute',
                              COALESCE(
                                  pp.bucket_minute,
                                  pp.source_created_at,
                                  pp.created_at,
                                  pp.processed_at,
                                  now()
                              )
                          ) >= (SELECT recompute_from FROM aggregation_bounds)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM public.post_topics pt
                              WHERE pt.raw_post_id = pp.raw_post_id
                          )
                    ),
                    all_mentions AS (
                        SELECT * FROM topic_mentions
                        UNION ALL
                        SELECT * FROM fallback_mentions
                    ),
                    filtered_mentions AS (
                        SELECT *
                        FROM all_mentions
                        WHERE normalized_topic <> ''
                          AND normalized_topic NOT IN ('all', 'general')
                          AND normalized_topic NOT IN (SELECT token FROM weak_topic_tokens)
                          AND length(replace(normalized_topic, ' ', '')) >= 2
                          AND array_length(string_to_array(normalized_topic, ' '), 1) <= 4
                          AND NOT EXISTS (
                              SELECT 1
                              FROM unnest(string_to_array(normalized_topic, ' ')) AS topic_token(token)
                              WHERE topic_token.token <> ''
                                AND topic_token.token IN (SELECT token FROM topic_noise_tokens)
                          )
                          AND normalized_topic !~* '(^| )([a-z0-9]+bot)( |$)'
                          AND normalized_topic !~* '(area|forecast|discussion).*(afd|airnow|aqi)'
                          AND normalized_topic !~* '(additional|details) here'
                          AND (
                              from_fallback = false
                              OR normalized_topic IN (SELECT token FROM single_word_topic_allowlist)
                              OR (
                                  POSITION(' ' IN normalized_topic) > 0
                                  AND (
                                      SELECT COUNT(*)
                                      FROM unnest(string_to_array(normalized_topic, ' ')) AS topic_token(token)
                                      WHERE topic_token.token <> ''
                                        AND topic_token.token NOT IN (SELECT token FROM weak_topic_tokens)
                                        AND topic_token.token NOT IN (SELECT token FROM topic_noise_tokens)
                                        AND length(topic_token.token) >= 4
                                  ) >= 2
                              )
                          )
                          AND (
                              SELECT COUNT(*)
                              FROM unnest(string_to_array(normalized_topic, ' ')) AS topic_token(token)
                              WHERE topic_token.token <> ''
                                AND (
                                    topic_token.token IN (SELECT token FROM weak_topic_tokens)
                                    OR topic_token.token IN (SELECT token FROM topic_noise_tokens)
                                )
                          ) <= GREATEST(1, array_length(string_to_array(normalized_topic, ' '), 1) - 1)
                          AND EXISTS (
                              SELECT 1
                              FROM unnest(string_to_array(normalized_topic, ' ')) AS topic_token(token)
                              WHERE topic_token.token <> ''
                                AND topic_token.token NOT IN (SELECT token FROM weak_topic_tokens)
                                AND topic_token.token NOT IN (SELECT token FROM topic_noise_tokens)
                                AND (
                                    length(topic_token.token) >= 4
                                    OR topic_token.token IN (SELECT token FROM single_word_topic_allowlist)
                                )
                          )
                    ),
                    aggregated AS (
                        SELECT
                            bucket_minute,
                            platform,
                            normalized_topic,
                            COUNT(*)::INT AS mention_count,
                            COUNT(DISTINCT NULLIF(author_id, ''))::INT AS unique_authors,
                            COUNT(DISTINCT raw_post_id)::INT AS unique_posts,
                            COALESCE(SUM(quality_score), 0)::numeric AS total_quality_score,
                            COALESCE(AVG(quality_score), 0)::numeric AS avg_quality_score,
                            MIN(topic_priority)::INT AS best_topic_priority,
                            SUM(CASE WHEN is_repost THEN 1 ELSE 0 END)::INT AS repost_count,
                            SUM(CASE WHEN is_reply THEN 1 ELSE 0 END)::INT AS reply_count,
                            SUM(CASE WHEN has_link THEN 1 ELSE 0 END)::INT AS link_post_count,
                            SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END)::INT AS positive_count,
                            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END)::INT AS negative_count,
                            SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END)::INT AS neutral_count
                        FROM filtered_mentions
                        GROUP BY 1, 2, 3
                    ),
                    topic_totals AS (
                        SELECT
                            platform,
                            normalized_topic,
                            SUM(mention_count)::INT AS total_mentions
                        FROM aggregated
                        GROUP BY 1, 2
                    ),
                    filtered_aggregated AS (
                        SELECT aggregated.*
                        FROM aggregated
                        JOIN topic_totals
                          ON topic_totals.platform = aggregated.platform
                         AND topic_totals.normalized_topic = aggregated.normalized_topic
                        CROSS JOIN single_word_topic_minimum
                        WHERE
                            POSITION(' ' IN aggregated.normalized_topic) > 0
                            OR aggregated.best_topic_priority <= 3
                            OR (
                                aggregated.best_topic_priority <= 4
                                AND topic_totals.total_mentions >= 6
                                AND aggregated.avg_quality_score >= 0.40
                            )
                            OR aggregated.normalized_topic IN (
                                SELECT token FROM single_word_topic_allowlist
                            )
                            OR topic_totals.total_mentions >= single_word_topic_minimum.min_mentions
                    )
                    INSERT INTO public.topic_buckets_1m (
                        bucket_minute,
                        platform,
                        topic_key,
                        mention_count,
                        unique_authors,
                        total_quality_score,
                        avg_quality_score,
                        repost_count,
                        reply_count,
                        link_post_count,
                        sample_size,
                        created_at,
                        bucket_start,
                        topic,
                        normalized_topic,
                        topic_display,
                        unique_posts,
                        positive_count,
                        neutral_count,
                        negative_count,
                        sentiment_net,
                        sentiment_score,
                        updated_at
                    )
                    SELECT
                        bucket_minute,
                        platform,
                        normalized_topic AS topic_key,
                        mention_count,
                        unique_authors,
                        total_quality_score,
                        avg_quality_score,
                        repost_count,
                        reply_count,
                        link_post_count,
                        mention_count AS sample_size,
                        now() AS created_at,
                        bucket_minute AS bucket_start,
                        CASE
                            WHEN normalized_topic = 'no kings' THEN 'No Kings'
                            ELSE INITCAP(normalized_topic)
                        END AS topic,
                        normalized_topic,
                        CASE
                            WHEN normalized_topic = 'no kings' THEN 'No Kings'
                            ELSE INITCAP(normalized_topic)
                        END AS topic_display,
                        unique_posts,
                        positive_count,
                        neutral_count,
                        negative_count,
                        (positive_count - negative_count)::INT AS sentiment_net,
                        CASE
                            WHEN mention_count > 0 THEN
                                ROUND(((positive_count - negative_count)::numeric / mention_count)::numeric, 4)::double precision
                            ELSE 0::double precision
                        END AS sentiment_score,
                        now() AS updated_at
                    FROM filtered_aggregated
                    ON CONFLICT (bucket_minute, platform, topic_key) DO UPDATE
                    SET mention_count = EXCLUDED.mention_count,
                        unique_authors = EXCLUDED.unique_authors,
                        total_quality_score = EXCLUDED.total_quality_score,
                        avg_quality_score = EXCLUDED.avg_quality_score,
                        repost_count = EXCLUDED.repost_count,
                        reply_count = EXCLUDED.reply_count,
                        link_post_count = EXCLUDED.link_post_count,
                        sample_size = EXCLUDED.sample_size,
                        bucket_start = EXCLUDED.bucket_start,
                        topic = EXCLUDED.topic,
                        normalized_topic = EXCLUDED.normalized_topic,
                        topic_display = EXCLUDED.topic_display,
                        unique_posts = EXCLUDED.unique_posts,
                        positive_count = EXCLUDED.positive_count,
                        neutral_count = EXCLUDED.neutral_count,
                        negative_count = EXCLUDED.negative_count,
                        sentiment_net = EXCLUDED.sentiment_net,
                        sentiment_score = EXCLUDED.sentiment_score,
                        updated_at = EXCLUDED.updated_at
                    """
                    ,
                    (recompute_lookback_hours,),
                )
                return int(cursor.rowcount or 0)

        return self._execute_write("aggregate_topic_buckets_1m_from_processed_posts", operation)

    def _prepare_raw_post_row(self, row: dict[str, Any]) -> dict[str, Any]:
        ingested_at = row.get("ingested_at")
        if ingested_at is None:
            ingested_at = datetime.now(timezone.utc)
        created_at = row.get("created_at") or ingested_at
        raw_json_payload = row.get("raw_json") or {}
        if not isinstance(raw_json_payload, dict):
            raw_json_payload = {}
        source_post_id = str(row.get("source_post_id") or row.get("post_id") or "").strip()
        metrics_payload = dict(row.get("metrics_json") or {})
        return {
            "platform": str(row.get("platform") or "bluesky"),
            "source_post_id": source_post_id,
            "source_uri": str(row.get("source_uri") or source_post_id).strip() or None,
            "source_cid": str(row.get("source_cid") or raw_json_payload.get("cid") or "").strip() or None,
            "author_did": str(
                row.get("author_did")
                or row.get("author_id")
                or raw_json_payload.get("authorDid")
                or ""
            ).strip()
            or None,
            "post_id": str(row.get("post_id") or "").strip(),
            "author_id": str(row.get("author_id") or "").strip() or None,
            "author_handle": str(row.get("author_handle") or "").strip() or None,
            "root_post_id": str(row.get("root_post_id") or "").strip() or None,
            "reply_parent_id": str(row.get("reply_parent_id") or "").strip() or None,
            "created_at": created_at,
            "inserted_at": ingested_at,
            "ingested_at": ingested_at,
            "raw_text": str(row.get("raw_text") or row.get("text_content") or "").strip() or None,
            "text_content": str(row.get("text_content") or "").strip() or None,
            "language": str(row.get("language") or "").strip() or None,
            "urls": self._adapt_collection("raw_posts", "urls", row.get("urls")),
            "hashtags": self._adapt_collection("raw_posts", "hashtags", row.get("hashtags")),
            "like_count": int(
                row.get("like_count")
                or metrics_payload.get("likeCount")
                or 0
            ),
            "repost_count": int(
                row.get("repost_count")
                or metrics_payload.get("repostCount")
                or 0
            ),
            "reply_count": int(
                row.get("reply_count")
                or metrics_payload.get("replyCount")
                or 0
            ),
            "reply_to_uri": str(row.get("reply_to_uri") or row.get("reply_parent_id") or "").strip() or None,
            "repost_of_uri": str(
                row.get("repost_of_uri")
                or row.get("root_post_id")
                or raw_json_payload.get("quotedUri")
                or ""
            ).strip()
            or None,
            "processed": bool(row.get("processed", False)),
            "metrics_json": self._adapt_json("raw_posts", "metrics_json", metrics_payload),
            "raw_json": self._adapt_json("raw_posts", "raw_json", raw_json_payload),
        }

    @staticmethod
    def _decode_ingested_raw_post_row(row: Sequence[Any]) -> dict[str, Any]:
        return {
            "id": row[0],
            "platform": row[1],
            "source_post_id": row[2],
            "post_id": row[3],
            "author_id": row[4],
            "author_handle": row[5],
            "root_post_id": row[6],
            "reply_parent_id": row[7],
            "created_at": row[8],
            "inserted_at": row[9],
            "ingested_at": row[10],
            "raw_text": row[11],
            "text_content": row[12],
            "language": row[13],
            "urls": row[14],
            "hashtags": row[15],
            "reply_to_uri": row[16],
            "repost_of_uri": row[17],
            "metrics_json": row[18] or {},
            "raw_json": row[19] or {},
        }

    def _prepare_processed_post_row(self, row: dict[str, Any]) -> dict[str, Any]:
        processed_at = row.get("processed_at") or datetime.now(timezone.utc)
        source_created_at = row.get("source_created_at") or row.get("created_at")
        if source_created_at is None:
            source_created_at = processed_at
        created_at = row.get("created_at") or source_created_at
        if created_at is None:
            created_at = processed_at
        bucket_minute = row.get("bucket_minute")
        if bucket_minute is None and created_at is not None:
            try:
                bucket_minute = created_at.replace(second=0, microsecond=0)
            except Exception:
                bucket_minute = processed_at.replace(second=0, microsecond=0)
        if bucket_minute is None:
            bucket_minute = processed_at.replace(second=0, microsecond=0)

        quality_score = row.get("quality_score")
        try:
            quality_score_value = float(quality_score if quality_score is not None else 0.0)
        except (TypeError, ValueError):
            quality_score_value = 0.0

        spam_score = row.get("spam_score")
        try:
            spam_score_value = float(spam_score if spam_score is not None else 0.0)
        except (TypeError, ValueError):
            spam_score_value = 0.0

        def _safe_int(value: Any, default: int = 0) -> int:
            try:
                return int(value if value is not None else default)
            except (TypeError, ValueError):
                return default

        topic_key_candidate = str(row.get("topic_key_candidate") or row.get("topic") or "general").strip()
        if not topic_key_candidate:
            topic_key_candidate = "general"

        return {
            "raw_post_id": row.get("raw_post_id"),
            "platform": str(row.get("platform") or "bluesky").strip() or "bluesky",
            "source_post_id": str(row.get("source_post_id") or row.get("post_id") or "").strip(),
            "post_id": str(row.get("post_id") or "").strip() or None,
            "author_id": str(row.get("author_id") or "").strip() or None,
            "source_created_at": source_created_at,
            "created_at": created_at,
            "processed_at": processed_at,
            "bucket_minute": bucket_minute,
            "clean_text": str(row.get("clean_text") or "").strip() or None,
            "normalized_text": str(row.get("normalized_text") or row.get("clean_text") or "").strip() or None,
            "language": str(row.get("language") or "").strip() or None,
            "quality_score": quality_score_value,
            "topic_key_candidate": topic_key_candidate,
            "tokens": self._adapt_collection("processed_posts", "tokens", row.get("tokens")),
            "hashtags": self._adapt_collection("processed_posts", "hashtags", row.get("hashtags")),
            "mentions": self._adapt_collection("processed_posts", "mentions", row.get("mentions")),
            "urls": self._adapt_collection("processed_posts", "urls", row.get("urls")),
            "domains": self._adapt_collection("processed_posts", "domains", row.get("domains")),
            "tags": self._adapt_collection("processed_posts", "tags", row.get("tags")),
            "topic_entities": self._adapt_collection("processed_posts", "topic_entities", row.get("topic_entities")),
            "has_media": bool(row.get("has_media", False)),
            "is_reply": bool(row.get("is_reply", False)),
            "is_repost": bool(row.get("is_repost", False)),
            "is_quote": bool(row.get("is_quote", False)),
            "author_hash": str(row.get("author_hash") or "").strip() or None,
            "token_count": int(row.get("token_count", 0) or 0),
            "fingerprint": str(row.get("fingerprint") or "").strip() or None,
            "cashtags": self._adapt_collection("processed_posts", "cashtags", row.get("cashtags")),
            "key_phrases": self._adapt_collection("processed_posts", "key_phrases", row.get("key_phrases")),
            "topic_seeds": self._adapt_collection("processed_posts", "topic_seeds", row.get("topic_seeds")),
            "sentiment_label": str(row.get("sentiment_label") or "neutral").strip() or "neutral",
            "sentiment_positive_score": _safe_int(row.get("sentiment_positive_score"), 0),
            "sentiment_negative_score": _safe_int(row.get("sentiment_negative_score"), 0),
            "sentiment_neutral_score": _safe_int(row.get("sentiment_neutral_score"), 0),
            "spam_score": spam_score_value,
            "topic": str(row.get("topic") or topic_key_candidate).strip() or topic_key_candidate,
        }

    def _prepare_post_topic_row(self, row: dict[str, Any]) -> dict[str, Any]:
        created_at = row.get("created_at") or datetime.now(timezone.utc)
        return {
            "raw_post_id": row.get("raw_post_id"),
            "processed_post_id": row.get("processed_post_id"),
            "platform": str(row.get("platform") or "bluesky").strip() or "bluesky",
            "source_post_id": str(row.get("source_post_id") or "").strip(),
            "topic_text": str(row.get("topic_text") or "").strip(),
            "normalized_topic": str(row.get("normalized_topic") or "").strip(),
            "topic_type": str(row.get("topic_type") or "entity").strip() or "entity",
            "language": str(row.get("language") or "").strip() or None,
            "source_created_at": row.get("source_created_at"),
            "bucket_minute": row.get("bucket_minute") or created_at,
            "created_at": created_at,
        }

    def _prepare_author_row(self, row: dict[str, Any]) -> dict[str, Any]:
        observed_at = datetime.now(timezone.utc)
        first_seen = row.get("first_seen_at") or observed_at
        last_seen = row.get("last_seen_at") or observed_at
        return {
            "platform": str(row.get("platform") or "bluesky"),
            "author_id": str(row.get("author_id") or "").strip(),
            "author_handle": str(row.get("author_handle") or "").strip() or None,
            "display_name": str(row.get("display_name") or "").strip() or None,
            "followers_count": row.get("followers_count"),
            "metadata_json": self._adapt_json(
                "authors",
                "metadata_json",
                row.get("metadata_json") or {},
            ),
            "first_seen_at": first_seen,
            "last_seen_at": last_seen,
        }

    def _run_with_retry(
        self,
        label: str,
        operation: Callable[[psycopg.Connection[Any]], Any],
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, 3):
            try:
                self.connect()
                if self._conn is None:
                    raise RuntimeError("Database connection is not available")
                return operation(self._conn)
            except psycopg.OperationalError as error:
                last_error = error
                self._logger.warning(
                    "db_operation_retry label=%s attempt=%s error=%s",
                    label,
                    attempt,
                    error,
                )
                self.close()
                if attempt < 2:
                    time.sleep(min(2.0, 0.5 * attempt))
            except psycopg.InterfaceError as error:
                last_error = error
                self._logger.warning(
                    "db_connection_reset label=%s attempt=%s error=%s",
                    label,
                    attempt,
                    error,
                )
                self.close()
                if attempt < 2:
                    time.sleep(min(2.0, 0.5 * attempt))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Database operation failed: {label}")

    def _execute_write(
        self,
        label: str,
        operation: Callable[[psycopg.Connection[Any]], Any],
    ) -> Any:
        def wrapped(connection: psycopg.Connection[Any]) -> Any:
            try:
                result = operation(connection)
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

        return self._run_with_retry(label, wrapped)

    def _load_column_metadata(self) -> None:
        if self._conn is None:
            return
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name, column_name, data_type, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ANY(%s)
                """,
                (
                    [
                        "raw_posts",
                        "authors",
                        "ingestion_runs",
                        "processed_posts",
                        "post_topics",
                    ],
                ),
            )
            rows = cursor.fetchall()
        self._column_types = {
            (str(table_name), str(column_name)): (str(data_type), str(udt_name))
            for table_name, column_name, data_type, udt_name in rows
        }

    def _verify_required_schema(self) -> None:
        if self._schema_verified:
            return

        required_columns = {
            "raw_posts": {
                "id",
                "platform",
                "source_post_id",
                "post_id",
                "author_id",
                "author_handle",
                "root_post_id",
                "reply_parent_id",
                "created_at",
                "ingested_at",
                "text_content",
                "language",
                "urls",
                "hashtags",
                "metrics_json",
                "raw_json",
            },
            "authors": {
                "platform",
                "author_id",
                "author_handle",
                "display_name",
                "followers_count",
                "metadata_json",
                "first_seen_at",
                "last_seen_at",
            },
            "ingestion_runs": {
                "source",
                "started_at",
                "ended_at",
                "status",
                "rows_inserted",
                "notes",
            },
        }

        available_by_table: dict[str, set[str]] = {}
        for (table_name, column_name), _metadata in self._column_types.items():
            available_by_table.setdefault(table_name, set()).add(column_name)

        missing: dict[str, list[str]] = {}
        for table_name, columns in required_columns.items():
            missing_columns = sorted(columns - available_by_table.get(table_name, set()))
            if missing_columns:
                missing[table_name] = missing_columns

        if missing:
            raise RuntimeError(f"Database schema does not match required worker columns: {missing}")

        self._schema_verified = True

    def _column_metadata(self, table: str, column: str) -> tuple[str, str]:
        return self._column_types.get((table, column), ("", ""))

    def _expects_json(self, table: str, column: str) -> bool:
        data_type, _udt_name = self._column_metadata(table, column)
        return data_type in {"json", "jsonb"}

    def _expects_array(self, table: str, column: str) -> bool:
        _data_type, udt_name = self._column_metadata(table, column)
        return udt_name.startswith("_")

    def _adapt_json(self, table: str, column: str, payload: Any) -> Any:
        if payload is None:
            return None
        if self._expects_json(table, column):
            return Jsonb(payload)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)

    def _adapt_collection(self, table: str, column: str, values: Any) -> Any:
        normalized = _normalize_text_list(values)
        if self._expects_json(table, column):
            return Jsonb(normalized)
        if self._expects_array(table, column):
            return normalized
        return ", ".join(normalized)

    def _adapt_notes(self, payload: dict[str, Any]) -> Any:
        if self._expects_json("ingestion_runs", "notes"):
            return Jsonb(payload)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)

    @staticmethod
    def _decode_notes(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return {"message": text}
            if isinstance(parsed, dict):
                return parsed
            return {"value": parsed}
        return {"value": str(value)}
