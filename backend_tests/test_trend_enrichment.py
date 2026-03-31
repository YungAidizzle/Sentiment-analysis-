import unittest
from datetime import datetime, timedelta, timezone

from backend.trend_enrichment import (
    _should_refresh_enrichment,
    build_enrichment_input_hash,
    select_representative_posts,
)


class TrendEnrichmentTests(unittest.TestCase):
    def test_select_representative_posts_dedupes_near_identical_rows(self):
        now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        rows = [
            {
                "raw_post_id": 1,
                "source_post_id": "p1",
                "text_content": "Polymarket launches new sports contracts and users discuss liquidity shifts.",
                "fingerprint": "fp_a",
                "quality_score": 0.92,
                "like_count": 45,
                "repost_count": 10,
                "reply_count": 8,
                "quote_count": 3,
                "event_timestamp": now.isoformat(),
            },
            {
                "raw_post_id": 2,
                "source_post_id": "p2",
                "text_content": "Polymarket launches new sports contracts and users discuss liquidity shifts.",
                "fingerprint": "fp_a",
                "quality_score": 0.91,
                "like_count": 30,
                "repost_count": 7,
                "reply_count": 5,
                "quote_count": 1,
                "event_timestamp": (now - timedelta(minutes=5)).isoformat(),
            },
            {
                "raw_post_id": 3,
                "source_post_id": "p3",
                "text_content": "Kalshi and ICE strategy is being compared against Polymarket expansion plans.",
                "fingerprint": "fp_b",
                "quality_score": 0.88,
                "like_count": 25,
                "repost_count": 6,
                "reply_count": 4,
                "quote_count": 2,
                "event_timestamp": (now - timedelta(minutes=12)).isoformat(),
            },
            {
                "raw_post_id": 4,
                "source_post_id": "p4",
                "text_content": "Short",
                "fingerprint": "fp_c",
                "quality_score": 0.2,
                "like_count": 1,
                "repost_count": 0,
                "reply_count": 0,
                "quote_count": 0,
                "event_timestamp": (now - timedelta(minutes=1)).isoformat(),
            },
        ]

        selected, diagnostics = select_representative_posts(
            rows,
            limit=10,
            max_post_chars=220,
            min_text_chars=25,
            min_word_count=5,
        )

        selected_ids = {row.get("source_post_id") for row in selected}
        self.assertIn("p1", selected_ids)
        self.assertIn("p3", selected_ids)
        self.assertNotIn("p2", selected_ids)
        self.assertLessEqual(len(selected), 10)
        self.assertGreaterEqual(diagnostics.get("candidate_count", 0), 3)

    def test_input_hash_changes_when_sample_changes(self):
        topic = {
            "topic_key": "prediction market",
            "topic_label": "Prediction Market",
            "window_end": datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc).isoformat(),
            "total_mentions": 120,
            "unique_posts": 40,
            "unique_authors": 28,
        }
        base_posts = [
            {
                "candidate_id": "p1",
                "source_post_id": "p1",
                "truncated_text": "Polymarket and Kalshi competition discussion.",
                "engagement_score": 20.0,
                "event_dt": datetime(2026, 3, 31, 11, 55, tzinfo=timezone.utc),
                "quality_score": 0.8,
            }
        ]
        changed_posts = [
            {
                **base_posts[0],
                "truncated_text": "Polymarket and ICE infrastructure expansion discussion.",
            }
        ]

        hash_a = build_enrichment_input_hash(
            topic_row=topic,
            representative_posts=base_posts,
        )
        hash_b = build_enrichment_input_hash(
            topic_row=topic,
            representative_posts=changed_posts,
        )
        self.assertNotEqual(hash_a, hash_b)

    def test_should_refresh_skips_when_hash_model_prompt_match_and_recent(self):
        now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        should_refresh, reason = _should_refresh_enrichment(
            existing_state={
                "input_hash": "abc",
                "prompt_version": "v1",
                "model_name": "gpt-5.4-mini",
                "refreshed_at": (now - timedelta(hours=1)).isoformat(),
                "expires_at": (now + timedelta(hours=3)).isoformat(),
            },
            input_hash="abc",
            model_name="gpt-5.4-mini",
            prompt_version="v1",
            stale_after_hours=6,
            now=now,
        )
        self.assertFalse(should_refresh)
        self.assertEqual(reason, "unchanged")


if __name__ == "__main__":
    unittest.main()
