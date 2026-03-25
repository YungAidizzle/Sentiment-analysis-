from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


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

    def fetch_resume_cursor(self, *, source: str) -> int | None:
        def operation(connection: psycopg.Connection[Any]) -> int | None:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT notes
                    FROM ingestion_runs
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
                    INSERT INTO ingestion_runs (
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
                        UPDATE ingestion_runs
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
                        UPDATE ingestion_runs
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
                        INSERT INTO raw_posts (
                            platform,
                            post_id,
                            author_id,
                            author_handle,
                            root_post_id,
                            reply_parent_id,
                            created_at,
                            ingested_at,
                            text_content,
                            language,
                            urls,
                            hashtags,
                            metrics_json,
                            raw_json
                        ) VALUES (
                            %(platform)s,
                            %(post_id)s,
                            %(author_id)s,
                            %(author_handle)s,
                            %(root_post_id)s,
                            %(reply_parent_id)s,
                            %(created_at)s,
                            %(ingested_at)s,
                            %(text_content)s,
                            %(language)s,
                            %(urls)s,
                            %(hashtags)s,
                            %(metrics_json)s,
                            %(raw_json)s
                        )
                        ON CONFLICT (platform, post_id) DO NOTHING
                        """,
                        list(chunk),
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        inserted_total += int(cursor.rowcount)
            return inserted_total

        return self._execute_write("upsert_raw_posts", operation)

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
                        INSERT INTO authors (
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
                        SET author_handle = COALESCE(EXCLUDED.author_handle, authors.author_handle),
                            display_name = COALESCE(EXCLUDED.display_name, authors.display_name),
                            followers_count = COALESCE(EXCLUDED.followers_count, authors.followers_count),
                            metadata_json = COALESCE(EXCLUDED.metadata_json, authors.metadata_json),
                            first_seen_at = CASE
                                WHEN authors.first_seen_at IS NULL THEN EXCLUDED.first_seen_at
                                WHEN EXCLUDED.first_seen_at IS NULL THEN authors.first_seen_at
                                ELSE LEAST(authors.first_seen_at, EXCLUDED.first_seen_at)
                            END,
                            last_seen_at = CASE
                                WHEN authors.last_seen_at IS NULL THEN EXCLUDED.last_seen_at
                                WHEN EXCLUDED.last_seen_at IS NULL THEN authors.last_seen_at
                                ELSE GREATEST(authors.last_seen_at, EXCLUDED.last_seen_at)
                            END
                        """,
                        list(chunk),
                    )
                    if cursor.rowcount and cursor.rowcount > 0:
                        affected_total += int(cursor.rowcount)
            return affected_total

        return self._execute_write("upsert_authors", operation)

    def _prepare_raw_post_row(self, row: dict[str, Any]) -> dict[str, Any]:
        ingested_at = row.get("ingested_at")
        if ingested_at is None:
            ingested_at = datetime.now(timezone.utc)
        created_at = row.get("created_at") or ingested_at
        return {
            "platform": str(row.get("platform") or "bluesky"),
            "post_id": str(row.get("post_id") or "").strip(),
            "author_id": str(row.get("author_id") or "").strip() or None,
            "author_handle": str(row.get("author_handle") or "").strip() or None,
            "root_post_id": str(row.get("root_post_id") or "").strip() or None,
            "reply_parent_id": str(row.get("reply_parent_id") or "").strip() or None,
            "created_at": created_at,
            "ingested_at": ingested_at,
            "text_content": str(row.get("text_content") or "").strip() or None,
            "language": str(row.get("language") or "").strip() or None,
            "urls": self._adapt_collection("raw_posts", "urls", row.get("urls")),
            "hashtags": self._adapt_collection("raw_posts", "hashtags", row.get("hashtags")),
            "metrics_json": self._adapt_json("raw_posts", "metrics_json", row.get("metrics_json") or {}),
            "raw_json": self._adapt_json("raw_posts", "raw_json", row.get("raw_json") or {}),
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
                WHERE table_schema = current_schema()
                  AND table_name = ANY(%s)
                """,
                (
                    [
                        "raw_posts",
                        "authors",
                        "ingestion_runs",
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
                "platform",
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
