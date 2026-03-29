from __future__ import annotations

import argparse
import json
import logging

from backend.config import WorkerConfig
from backend.db import PostgresStore
from backend.logging_setup import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild stable topic read models from existing post topics.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate stable mention/read-model tables before resyncing.",
    )
    parser.add_argument(
        "--fact-lookback-hours",
        type=int,
        default=168,
        help="Lookback window used to sync mention facts from post_topics.",
    )
    parser.add_argument(
        "--cleanup-lookback-hours",
        type=int,
        default=168,
        help="Lookback window used to cleanup low-quality mention facts.",
    )
    parser.add_argument(
        "--lag-minutes",
        type=int,
        default=3,
        help="Finalization lag (minutes) for stable bucket read-model refresh.",
    )
    parser.add_argument(
        "--recompute-hours",
        type=int,
        default=72,
        help="Hard recompute horizon for stable bucket refresh.",
    )
    parser.add_argument(
        "--series-max-topics",
        type=int,
        default=300,
        help="Max topics retained in day series output.",
    )
    parser.add_argument(
        "--series-min-mentions",
        type=int,
        default=2,
        help="Minimum day mentions required for day series inclusion.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = WorkerConfig.from_env()
    setup_logging(config.log_level)
    logger = logging.getLogger("backend.rebuild_topic_models")

    store = PostgresStore(
        database_url=config.database_url,
        batch_size=config.batch_size,
        logger=logger,
    )

    try:
        store.verify_connection()
        store.ensure_processed_topic_tables()
        store.ensure_stable_topic_read_model_tables()
        if args.reset:
            store.reset_stable_topic_read_models()

        synced_rows = store.sync_post_topic_mentions_from_post_topics(
            lookback_hours=max(1, int(args.fact_lookback_hours)),
        )
        cleaned_rows = store.cleanup_garbage_post_topic_mentions(
            lookback_hours=max(1, int(args.cleanup_lookback_hours)),
        )
        refresh_payload = store.refresh_stable_topic_read_models(
            lag_minutes=max(1, int(args.lag_minutes)),
            recompute_hours=max(1, int(args.recompute_hours)),
            series_max_topics=max(25, int(args.series_max_topics)),
            series_min_mentions=max(1, int(args.series_min_mentions)),
        )
    finally:
        store.close()

    print(
        json.dumps(
            {
                "reset": bool(args.reset),
                "synced_rows": int(synced_rows),
                "cleaned_rows": int(cleaned_rows),
                "refresh": refresh_payload,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
