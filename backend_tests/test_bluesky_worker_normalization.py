import unittest
from datetime import datetime, timezone

from backend.collectors.bluesky_worker import (
    analyzeSentiment,
    extractFeatures,
    extractTopicEntities,
    normalizeIncomingEvent,
    normalize_authors_for_authors_table,
    normalize_posts_for_raw_table,
    processRawPost,
)


class BlueskyWorkerNormalizationTests(unittest.TestCase):
    def test_normalize_incoming_event_maps_source_post_fields(self):
        ingested_at = datetime(2026, 3, 25, 10, 0, tzinfo=timezone.utc)
        event = {
            "id": "at://did:plc:abc/app.bsky.feed.post/123",
            "uri": "at://did:plc:abc/app.bsky.feed.post/123",
            "cid": "cid-123",
            "authorDid": "did:plc:abc",
            "authorHandle": "alice.bsky.social",
            "createdUtc": int(datetime(2026, 3, 25, 9, 30, tzinfo=timezone.utc).timestamp()),
            "summary": "Shipping update https://example.com #Dropshipping",
            "langs": ["en"],
        }

        row = normalizeIncomingEvent(event, ingested_at=ingested_at)

        self.assertIsNotNone(row)
        if row is None:
            return
        self.assertEqual(row["source_post_id"], event["uri"])
        self.assertEqual(row["source_cid"], "cid-123")
        self.assertEqual(row["platform"], "bluesky")
        self.assertEqual(row["hashtags"], ["dropshipping"])

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

    def test_normalize_incoming_event_clamps_future_created_at_to_indexed_at(self):
        ingested_at = datetime(2026, 3, 25, 10, 0, tzinfo=timezone.utc)
        indexed_at = datetime(2026, 3, 25, 9, 59, tzinfo=timezone.utc)
        event = {
            "id": "at://did:plc:abc/app.bsky.feed.post/124",
            "uri": "at://did:plc:abc/app.bsky.feed.post/124",
            "authorDid": "did:plc:abc",
            "createdUtc": int(datetime(2026, 3, 26, 9, 30, tzinfo=timezone.utc).timestamp()),
            "indexedAt": indexed_at.isoformat(),
            "summary": "future created timestamp should be clamped",
        }

        row = normalizeIncomingEvent(event, ingested_at=ingested_at)

        self.assertIsNotNone(row)
        if row is None:
            return
        self.assertEqual(row["created_at"], indexed_at)

    def test_extract_features_and_process_raw_post_adds_generic_fields(self):
        raw_row = {
            "id": 99,
            "platform": "bluesky",
            "source_post_id": "at://did:plc:abc/app.bsky.feed.post/999",
            "post_id": "at://did:plc:abc/app.bsky.feed.post/999",
            "author_id": "did:plc:abc",
            "created_at": datetime(2026, 3, 25, 9, 30, tzinfo=timezone.utc),
            "text_content": "New $DOGE memecoin drop + dropshipping product idea https://example.com #Crypto @alice",
            "language": "en",
            "metrics_json": {"likeCount": 10, "replyCount": 3, "repostCount": 2, "quoteCount": 1},
            "raw_json": {"quotedUri": "at://did:plc:def/app.bsky.feed.post/1"},
        }

        features = extractFeatures(raw_row)
        processed = processRawPost(raw_row, processed_at=datetime(2026, 3, 25, 10, 0, tzinfo=timezone.utc))

        self.assertIn("tokens", features)
        self.assertIn("tags", features)
        self.assertIn("memecoin", features["tags"])
        self.assertIn("dropshipping", features["tags"])
        self.assertEqual(processed["raw_post_id"], 99)
        self.assertEqual(processed["topic_key_candidate"], processed["topic"])
        self.assertEqual(processed["mentions"], ["alice"])
        self.assertEqual(processed["domains"], ["example.com"])
        self.assertIn("sentiment_label", processed)
        self.assertIsInstance(processed["topic_entities"], list)

    def test_extract_topic_entities_prefers_entity_like_terms(self):
        raw_row = {
            "text_content": (
                "NYSE parent finalizes investment in Polymarket. "
                "ICE has invested, and rival Kalshi recently raised capital."
            )
        }

        topics = extractTopicEntities(raw_row)
        normalized_topics = [row["normalized_topic"] for row in topics]

        self.assertIn("NYSE", normalized_topics)
        self.assertIn("Polymarket", normalized_topics)
        self.assertIn("ICE", normalized_topics)
        self.assertIn("Kalshi", normalized_topics)
        self.assertNotIn("parent", [topic.lower() for topic in normalized_topics])

    def test_extract_topic_entities_filters_weak_fragments_and_canonicalizes_aliases(self):
        raw_row = {
            "text_content": (
                "I'm glad you asked. We should talk later. "
                "Crowd heading to #NoKings today."
            )
        }

        topics = extractTopicEntities(raw_row)
        normalized_topics = [row["normalized_topic"] for row in topics]
        normalized_lower = [topic.lower() for topic in normalized_topics]

        self.assertIn("No Kings", normalized_topics)
        self.assertNotIn("i'm", normalized_lower)
        self.assertNotIn("you", normalized_lower)
        self.assertNotIn("we", normalized_lower)

    def test_analyze_sentiment_applies_weighted_lexicon(self):
        sentiment = analyzeSentiment(
            clean_text="I love this but hate the bad execution, still ok overall.",
            language="en",
        )

        self.assertEqual(sentiment["sentiment_positive_score"], 3)
        self.assertEqual(sentiment["sentiment_negative_score"], 4)
        self.assertEqual(sentiment["sentiment_neutral_score"], 1)
        self.assertEqual(sentiment["sentiment_label"], "negative")

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
