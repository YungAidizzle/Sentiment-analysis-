import unittest
from datetime import datetime, timezone

from backend.collectors.bluesky_worker import (
    normalize_authors_for_authors_table,
    normalize_posts_for_raw_table,
)


class BlueskyWorkerNormalizationTests(unittest.TestCase):
    def test_normalize_posts_for_raw_table_maps_required_fields(self):
        ingested_at = datetime(2026, 3, 25, 10, 0, tzinfo=timezone.utc)
        posts = [
            {
                "id": "at://did:plc:abc/app.bsky.feed.post/123",
                "authorDid": "did:plc:abc",
                "authorHandle": "alice.bsky.social",
                "rootUri": None,
                "parentUri": None,
                "createdUtc": int(datetime(2026, 3, 25, 9, 30, tzinfo=timezone.utc).timestamp()),
                "summary": "AI launch https://example.com #AI #ai",
                "langs": ["en", "es"],
                "score": 7,
                "likeCount": 5,
                "replyCount": 2,
                "repostCount": 1,
                "quoteCount": 0,
                "bookmarkCount": 0,
                "interactionCounts": {"posts": 1},
                "priorityScore": 8.5,
            }
        ]

        rows = normalize_posts_for_raw_table(posts, ingested_at=ingested_at)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["platform"], "bluesky")
        self.assertEqual(row["post_id"], posts[0]["id"])
        self.assertEqual(row["author_id"], "did:plc:abc")
        self.assertEqual(row["author_handle"], "alice.bsky.social")
        self.assertEqual(row["language"], "en")
        self.assertEqual(row["urls"], ["https://example.com"])
        self.assertEqual(row["hashtags"], ["ai"])
        self.assertEqual(row["metrics_json"]["likeCount"], 5)
        self.assertEqual(row["raw_json"]["id"], posts[0]["id"])

    def test_normalize_authors_for_authors_table_merges_profile_and_post_rows(self):
        observed_at = datetime(2026, 3, 25, 10, 0, tzinfo=timezone.utc)
        profiles = [
            {
                "did": "did:plc:abc",
                "handle": "alice.bsky.social",
                "displayName": "Alice",
                "followersCount": 100,
                "firstObservedAt": "2026-03-25T08:00:00Z",
                "lastObservedAt": "2026-03-25T09:00:00Z",
            }
        ]
        posts = [
            {
                "id": "at://did:plc:abc/app.bsky.feed.post/123",
                "authorDid": "did:plc:abc",
                "authorHandle": "alice.bsky.social",
                "authorDisplayName": "Alice Updated",
                "fetchedAt": "2026-03-25T09:30:00Z",
                "lastObservedAt": "2026-03-25T09:35:00Z",
            }
        ]

        rows = normalize_authors_for_authors_table(
            profiles=profiles,
            posts=posts,
            observed_at=observed_at,
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["platform"], "bluesky")
        self.assertEqual(row["author_id"], "did:plc:abc")
        self.assertEqual(row["author_handle"], "alice.bsky.social")
        self.assertEqual(row["display_name"], "Alice Updated")
        self.assertEqual(row["followers_count"], 100)
        self.assertEqual(row["first_seen_at"].isoformat(), "2026-03-25T08:00:00+00:00")
        self.assertEqual(row["last_seen_at"].isoformat(), "2026-03-25T09:35:00+00:00")


if __name__ == "__main__":
    unittest.main()
