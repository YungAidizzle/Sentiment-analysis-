from __future__ import annotations

import os

from backend.env import load_repo_env

load_repo_env()

FULL_FIREHOSE_COLLECTION_TOKENS = {"*", "all", "full", "full_firehose"}


def _parse_bool_env(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off"}

BLUESKY_API_BASE_URL = os.getenv("BLUESKY_API_BASE_URL", "https://api.bsky.app/xrpc").rstrip("/")
BLUESKY_ENABLED = _parse_bool_env("BLUESKY_ENABLED")
BLUESKY_FIREHOSE_ENABLED = _parse_bool_env("BLUESKY_FIREHOSE_ENABLED")
BLUESKY_FIREHOSE_ENDPOINT = os.getenv(
    "BLUESKY_FIREHOSE_ENDPOINT",
    "wss://jetstream2.us-west.bsky.network/subscribe",
).strip()
BLUESKY_RAW_EVENT_STORE_PATH = os.getenv(
    "BLUESKY_RAW_EVENT_STORE_PATH",
    "data/reddit_engine/bluesky_jetstream_raw.sqlite3",
).strip()
BLUESKY_RAW_PERSIST_ENABLED = _parse_bool_env("BLUESKY_RAW_PERSIST_ENABLED")

DEFAULT_BLUESKY_DISCOVERY_QUERIES = [
    "ai agents",
    "openai",
    "bitcoin",
    "ethereum",
    "solana",
    "memecoin",
    "tariffs",
    "inflation",
    "election",
    "geopolitics",
    "cybersecurity",
    "gaming",
    "startup",
    "health",
    "climate",
]


def _parse_text_csv_env(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)
    values = [part.strip() for part in raw.split(",")]
    return [value for value in values if value]


def _parse_firehose_collections_env(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)

    if raw.strip().lower() in FULL_FIREHOSE_COLLECTION_TOKENS:
        return []

    values = [part.strip() for part in raw.split(",")]
    return [value for value in values if value]


DEFAULT_BLUESKY_FIREHOSE_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.actor.profile",
    "app.bsky.graph.follow",
]


try:
    BLUESKY_REFRESH_MINUTES = max(15, min(240, int(os.getenv("BLUESKY_REFRESH_MINUTES", "60") or 60)))
except ValueError:
    BLUESKY_REFRESH_MINUTES = 60

try:
    BLUESKY_FIREHOSE_BOOTSTRAP_HOURS = max(
        1,
        min(24 * 7, int(os.getenv("BLUESKY_FIREHOSE_BOOTSTRAP_HOURS", "1") or 1)),
    )
except ValueError:
    BLUESKY_FIREHOSE_BOOTSTRAP_HOURS = 1

try:
    bootstrap_minutes_default = "15"
    if os.getenv("BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES") is None and os.getenv(
        "BLUESKY_FIREHOSE_BOOTSTRAP_HOURS"
    ):
        bootstrap_minutes_default = str(BLUESKY_FIREHOSE_BOOTSTRAP_HOURS * 60)
    BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES = max(
        5,
        min(
            24 * 7 * 60,
            int(
                os.getenv(
                    "BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES",
                    bootstrap_minutes_default,
                )
                or bootstrap_minutes_default
            ),
        ),
    )
except ValueError:
    BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES = 15

BLUESKY_FIREHOSE_BOOTSTRAP_MODE = (
    os.getenv("BLUESKY_FIREHOSE_BOOTSTRAP_MODE", "rewind").strip().lower()
)
if BLUESKY_FIREHOSE_BOOTSTRAP_MODE not in {"rewind", "head"}:
    BLUESKY_FIREHOSE_BOOTSTRAP_MODE = "rewind"

try:
    BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES = max(
        0,
        min(
            24 * 14 * 60,
            int(
                os.getenv(
                    "BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES",
                    str(BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES),
                )
                or BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES
            ),
        ),
    )
except ValueError:
    BLUESKY_FIREHOSE_STALE_CURSOR_MAX_AGE_MINUTES = BLUESKY_FIREHOSE_BOOTSTRAP_MINUTES

try:
    BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN = max(
        500,
        min(250_000, int(os.getenv("BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN", "40000") or 40000)),
    )
except ValueError:
    BLUESKY_FIREHOSE_MAX_EVENTS_PER_RUN = 40_000

try:
    BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN = max(
        5,
        min(240, int(os.getenv("BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN", "35") or 35)),
    )
except ValueError:
    BLUESKY_FIREHOSE_MAX_SECONDS_PER_RUN = 35

try:
    BLUESKY_RAW_PERSIST_BATCH_SIZE = max(
        1,
        min(2_000, int(os.getenv("BLUESKY_RAW_PERSIST_BATCH_SIZE", "250") or 250)),
    )
except ValueError:
    BLUESKY_RAW_PERSIST_BATCH_SIZE = 250

try:
    BLUESKY_FIREHOSE_IDLE_TIMEOUT_SECONDS = max(
        1,
        min(30, int(os.getenv("BLUESKY_FIREHOSE_IDLE_TIMEOUT_SECONDS", "4") or 4)),
    )
except ValueError:
    BLUESKY_FIREHOSE_IDLE_TIMEOUT_SECONDS = 4

try:
    BLUESKY_FIREHOSE_RECONNECT_MAX_ATTEMPTS = max(
        0,
        min(24, int(os.getenv("BLUESKY_FIREHOSE_RECONNECT_MAX_ATTEMPTS", "6") or 6)),
    )
except ValueError:
    BLUESKY_FIREHOSE_RECONNECT_MAX_ATTEMPTS = 6

try:
    BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS = max(
        0.25,
        min(30.0, float(os.getenv("BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS", "1.0") or 1.0)),
    )
except ValueError:
    BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS = 1.0

try:
    BLUESKY_FIREHOSE_RECONNECT_MAX_DELAY_SECONDS = max(
        BLUESKY_FIREHOSE_RECONNECT_BASE_DELAY_SECONDS,
        min(
            60.0,
            float(
                os.getenv("BLUESKY_FIREHOSE_RECONNECT_MAX_DELAY_SECONDS", "12.0") or 12.0
            ),
        ),
    )
except ValueError:
    BLUESKY_FIREHOSE_RECONNECT_MAX_DELAY_SECONDS = 12.0

try:
    BLUESKY_FIREHOSE_CURSOR_REWIND_SECONDS = max(
        0,
        min(120, int(os.getenv("BLUESKY_FIREHOSE_CURSOR_REWIND_SECONDS", "5") or 5)),
    )
except ValueError:
    BLUESKY_FIREHOSE_CURSOR_REWIND_SECONDS = 5

try:
    BLUESKY_FIREHOSE_RETENTION_HOURS = max(
        6,
        min(24 * 14, int(os.getenv("BLUESKY_FIREHOSE_RETENTION_HOURS", "168") or 168)),
    )
except ValueError:
    BLUESKY_FIREHOSE_RETENTION_HOURS = 168

try:
    BLUESKY_SEARCH_RESULTS_PER_QUERY = max(
        1,
        min(50, int(os.getenv("BLUESKY_SEARCH_RESULTS_PER_QUERY", "20") or 20)),
    )
except ValueError:
    BLUESKY_SEARCH_RESULTS_PER_QUERY = 20

try:
    BLUESKY_MAX_SEARCH_QUERIES_PER_RUN = max(
        0,
        min(24, int(os.getenv("BLUESKY_MAX_SEARCH_QUERIES_PER_RUN", "8") or 8)),
    )
except ValueError:
    BLUESKY_MAX_SEARCH_QUERIES_PER_RUN = 8

try:
    BLUESKY_MAX_BREAKOUT_QUERIES_PER_RUN = max(
        0,
        min(24, int(os.getenv("BLUESKY_MAX_BREAKOUT_QUERIES_PER_RUN", "8") or 8)),
    )
except ValueError:
    BLUESKY_MAX_BREAKOUT_QUERIES_PER_RUN = 8

try:
    BLUESKY_MAX_TRACKED_POSTS_PER_RUN = max(
        10,
        min(400, int(os.getenv("BLUESKY_MAX_TRACKED_POSTS_PER_RUN", "120") or 120)),
    )
except ValueError:
    BLUESKY_MAX_TRACKED_POSTS_PER_RUN = 120

try:
    BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN = max(
        0,
        min(60, int(os.getenv("BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN", "12") or 12)),
    )
except ValueError:
    BLUESKY_MAX_AUTHOR_EXPANSIONS_PER_RUN = 12

try:
    BLUESKY_MAX_AUTHOR_FEED_POSTS = max(
        1,
        min(50, int(os.getenv("BLUESKY_MAX_AUTHOR_FEED_POSTS", "8") or 8)),
    )
except ValueError:
    BLUESKY_MAX_AUTHOR_FEED_POSTS = 8

try:
    BLUESKY_MAX_THREAD_POSTS_PER_RUN = max(
        1,
        min(80, int(os.getenv("BLUESKY_MAX_THREAD_POSTS_PER_RUN", "24") or 24)),
    )
except ValueError:
    BLUESKY_MAX_THREAD_POSTS_PER_RUN = 24

try:
    BLUESKY_MAX_QUOTES_PER_POST = max(
        0,
        min(20, int(os.getenv("BLUESKY_MAX_QUOTES_PER_POST", "6") or 6)),
    )
except ValueError:
    BLUESKY_MAX_QUOTES_PER_POST = 6

try:
    BLUESKY_MAX_REPOSTED_BY_PER_POST = max(
        0,
        min(20, int(os.getenv("BLUESKY_MAX_REPOSTED_BY_PER_POST", "12") or 12)),
    )
except ValueError:
    BLUESKY_MAX_REPOSTED_BY_PER_POST = 12

try:
    BLUESKY_MAX_SNAPSHOTS_PER_POST = max(
        2,
        min(500, int(os.getenv("BLUESKY_MAX_SNAPSHOTS_PER_POST", "96") or 96)),
    )
except ValueError:
    BLUESKY_MAX_SNAPSHOTS_PER_POST = 96

try:
    BLUESKY_DISCOVERY_MAX_AGE_HOURS = max(
        12,
        min(24 * 14, int(os.getenv("BLUESKY_DISCOVERY_MAX_AGE_HOURS", "168") or 168)),
    )
except ValueError:
    BLUESKY_DISCOVERY_MAX_AGE_HOURS = 168

try:
    BLUESKY_QUOTA_BUDGET_PER_RUN = max(
        20,
        min(5_000, int(os.getenv("BLUESKY_QUOTA_BUDGET_PER_RUN", "450") or 450)),
    )
except ValueError:
    BLUESKY_QUOTA_BUDGET_PER_RUN = 450

BLUESKY_DISCOVERY_QUERIES = _parse_text_csv_env(
    "BLUESKY_DISCOVERY_QUERIES",
    DEFAULT_BLUESKY_DISCOVERY_QUERIES,
)
BLUESKY_FIREHOSE_COLLECTIONS = _parse_firehose_collections_env(
    "BLUESKY_FIREHOSE_COLLECTIONS",
    DEFAULT_BLUESKY_FIREHOSE_COLLECTIONS,
)
