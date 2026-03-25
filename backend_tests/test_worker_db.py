import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from backend.db import PostgresStore


SCHEMA_ROWS = [
    ("raw_posts", "platform", "text", "text"),
    ("raw_posts", "post_id", "text", "text"),
    ("raw_posts", "author_id", "text", "text"),
    ("raw_posts", "author_handle", "text", "text"),
    ("raw_posts", "root_post_id", "text", "text"),
    ("raw_posts", "reply_parent_id", "text", "text"),
    ("raw_posts", "created_at", "timestamp with time zone", "timestamptz"),
    ("raw_posts", "ingested_at", "timestamp with time zone", "timestamptz"),
    ("raw_posts", "text_content", "text", "text"),
    ("raw_posts", "language", "text", "text"),
    ("raw_posts", "urls", "ARRAY", "_text"),
    ("raw_posts", "hashtags", "ARRAY", "_text"),
    ("raw_posts", "metrics_json", "jsonb", "jsonb"),
    ("raw_posts", "raw_json", "jsonb", "jsonb"),
    ("authors", "platform", "text", "text"),
    ("authors", "author_id", "text", "text"),
    ("authors", "author_handle", "text", "text"),
    ("authors", "display_name", "text", "text"),
    ("authors", "followers_count", "bigint", "int8"),
    ("authors", "metadata_json", "jsonb", "jsonb"),
    ("authors", "first_seen_at", "timestamp with time zone", "timestamptz"),
    ("authors", "last_seen_at", "timestamp with time zone", "timestamptz"),
    ("ingestion_runs", "source", "text", "text"),
    ("ingestion_runs", "started_at", "timestamp with time zone", "timestamptz"),
    ("ingestion_runs", "ended_at", "timestamp with time zone", "timestamptz"),
    ("ingestion_runs", "status", "text", "text"),
    ("ingestion_runs", "rows_inserted", "bigint", "int8"),
    ("ingestion_runs", "notes", "text", "text"),
]


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0
        self._fetchall = []
        self._fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.connection.execute_calls.append((query, params))
        normalized = " ".join(str(query).split()).lower()
        if "from information_schema.columns" in normalized:
            self._fetchall = list(SCHEMA_ROWS)
            self._fetchone = None
            self.rowcount = len(self._fetchall)
            return
        if "select 1" in normalized:
            self._fetchone = (1,)
            self._fetchall = []
            self.rowcount = 1
            return
        if "select notes" in normalized and "from public.ingestion_runs" in normalized:
            self._fetchone = (json.dumps({"last_cursor_us": 987654321}),)
            self._fetchall = []
            self.rowcount = 1
            return
        self.rowcount = 1
        self._fetchall = []
        self._fetchone = None

    def executemany(self, query, params_seq):
        payload = list(params_seq)
        self.connection.executemany_calls.append((query, payload))
        self.rowcount = len(payload)
        self._fetchall = []
        self._fetchone = None

    def fetchall(self):
        return list(self._fetchall)

    def fetchone(self):
        return self._fetchone


class FakeConnection:
    def __init__(self):
        self.closed = False
        self.autocommit = False
        self.execute_calls = []
        self.executemany_calls = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class WorkerDbTests(unittest.TestCase):
    @patch("backend.db.psycopg.connect")
    def test_verify_connection_executes_select_one(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        store.verify_connection()

        self.assertTrue(
            any("SELECT 1" in str(query) for query, _params in fake_connection.execute_calls)
        )

    @patch("backend.db.psycopg.connect")
    def test_fetch_resume_cursor_reads_notes_json(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        cursor = store.fetch_resume_cursor(source="bluesky_firehose_worker")

        self.assertEqual(cursor, 987654321)

    @patch("backend.db.psycopg.connect")
    def test_upsert_raw_posts_uses_conflict_protection(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        inserted = store.upsert_raw_posts(
            [
                {
                    "platform": "bluesky",
                    "post_id": "at://did:plc:abc/app.bsky.feed.post/xyz",
                    "author_id": "did:plc:abc",
                    "author_handle": "test.bsky.social",
                    "root_post_id": None,
                    "reply_parent_id": None,
                    "created_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                    "ingested_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                    "text_content": "test",
                    "language": "en",
                    "urls": ["https://example.com"],
                    "hashtags": ["ai"],
                    "metrics_json": {"likeCount": 1},
                    "raw_json": {"id": "at://did:plc:abc/app.bsky.feed.post/xyz"},
                }
            ]
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(len(fake_connection.executemany_calls), 1)
        query, _payload = fake_connection.executemany_calls[0]
        self.assertIn("INSERT INTO public.raw_posts", query)
        self.assertIn("ON CONFLICT (platform, source_post_id) DO UPDATE", query)

    @patch("backend.db.psycopg.connect")
    def test_upsert_authors_uses_conflict_upsert(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        affected = store.upsert_authors(
            [
                {
                    "platform": "bluesky",
                    "author_id": "did:plc:abc",
                    "author_handle": "test.bsky.social",
                    "display_name": "Test User",
                    "followers_count": 10,
                    "metadata_json": {"did": "did:plc:abc"},
                    "first_seen_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                    "last_seen_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                }
            ]
        )

        self.assertEqual(affected, 1)
        self.assertEqual(len(fake_connection.executemany_calls), 1)
        query, _payload = fake_connection.executemany_calls[0]
        self.assertIn("INSERT INTO public.authors", query)
        self.assertIn("ON CONFLICT (platform, author_id) DO UPDATE", query)

    @patch("backend.db.psycopg.connect")
    def test_ensure_metric_bucket_tables_creates_required_tables(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        store.ensure_metric_bucket_tables()

        queries = [str(query) for query, _params in fake_connection.execute_calls]
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS public.metric_buckets_1m" in query for query in queries))
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS public.metric_buckets_1h" in query for query in queries))

    @patch("backend.db.psycopg.connect")
    def test_aggregate_metric_buckets_1m_uses_upsert(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        affected = store.aggregate_metric_buckets_1m()

        self.assertGreaterEqual(affected, 1)
        queries = [str(query) for query, _params in fake_connection.execute_calls]
        aggregate_query = next(query for query in queries if "INSERT INTO public.metric_buckets_1m" in query)
        self.assertIn("date_trunc('minute', created_at)", aggregate_query)
        self.assertIn("FROM public.raw_posts", aggregate_query)
        self.assertIn("ON CONFLICT (bucket_minute, platform) DO UPDATE", aggregate_query)

    @patch("backend.db.psycopg.connect")
    def test_aggregate_metric_buckets_1h_uses_upsert(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        affected = store.aggregate_metric_buckets_1h()

        self.assertGreaterEqual(affected, 1)
        queries = [str(query) for query, _params in fake_connection.execute_calls]
        aggregate_query = next(query for query in queries if "INSERT INTO public.metric_buckets_1h" in query)
        self.assertIn("date_trunc('hour', created_at)", aggregate_query)
        self.assertIn("FROM public.raw_posts", aggregate_query)
        self.assertIn("ON CONFLICT (bucket_start, platform) DO UPDATE", aggregate_query)

    @patch("backend.db.psycopg.connect")
    def test_refresh_processed_posts_reads_public_raw_posts(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        affected = store.refresh_processed_posts_from_raw_posts()

        self.assertGreaterEqual(affected, 1)
        queries = [str(query) for query, _params in fake_connection.execute_calls]
        refresh_query = next(query for query in queries if "INSERT INTO public.processed_posts" in query)
        self.assertIn("FROM public.raw_posts", refresh_query)
        self.assertTrue(any("TRUNCATE TABLE public.processed_posts" in query for query in queries))

    @patch("backend.db.psycopg.connect")
    def test_aggregate_topic_buckets_1m_reads_processed_posts(self, connect_mock):
        fake_connection = FakeConnection()
        connect_mock.return_value = fake_connection
        store = PostgresStore(
            database_url="postgresql://example",
            batch_size=50,
        )

        affected = store.aggregate_topic_buckets_1m_from_processed_posts()

        self.assertGreaterEqual(affected, 1)
        queries = [str(query) for query, _params in fake_connection.execute_calls]
        aggregate_query = next(query for query in queries if "INSERT INTO public.topic_buckets_1m" in query)
        self.assertIn("FROM public.processed_posts", aggregate_query)
        self.assertTrue(any("TRUNCATE TABLE public.topic_buckets_1m" in query for query in queries))


if __name__ == "__main__":
    unittest.main()
