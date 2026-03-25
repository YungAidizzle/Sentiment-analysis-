"""
DEV ONLY: environment-backed YouTube fetch settings.
Secrets must come from local environment variables only.
"""

from __future__ import annotations

import os

from backend.env import load_repo_env
from backend.youtube_curated_channels import YOUTUBE_CURATED_CHANNELS

load_repo_env()

YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"

DEFAULT_YOUTUBE_DISCOVERY_QUERIES = [
    "ai agents",
    "openai",
    "nvidia earnings",
    "bitcoin",
    "ethereum",
    "solana memecoin",
    "fed rates",
    "inflation",
    "tariffs",
    "shipping crisis",
    "middle east",
    "ukraine",
    "election",
    "tiktok",
    "gaming update",
]


def _parse_csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    values = [part.strip().upper() for part in raw.split(",")]
    return [value for value in values if value]


def _parse_text_csv_env(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, ",".join(default))
    values = [part.strip() for part in raw.split(",")]
    return [value for value in values if value]


YOUTUBE_REGIONS = _parse_csv_env(
    "YOUTUBE_REGIONS",
    os.getenv("YOUTUBE_REGION", "US,GB,CA,AU"),
)
YOUTUBE_REGION = YOUTUBE_REGIONS[0] if YOUTUBE_REGIONS else "US"

try:
    YOUTUBE_MAX_RESULTS = max(1, min(25, int(os.getenv("YOUTUBE_MAX_RESULTS", "20") or 20)))
except ValueError:
    YOUTUBE_MAX_RESULTS = 20

try:
    YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL = max(
        1,
        min(
            6,
            int(
                os.getenv(
                    "YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL",
                    str(min(6, YOUTUBE_MAX_RESULTS)),
                )
                or min(6, YOUTUBE_MAX_RESULTS)
            ),
        ),
    )
except ValueError:
    YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL = 3

try:
    YOUTUBE_CHANNEL_LOOKBACK_HOURS = max(
        12,
        min(168, int(os.getenv("YOUTUBE_CHANNEL_LOOKBACK_HOURS", "72") or 72)),
    )
except ValueError:
    YOUTUBE_CHANNEL_LOOKBACK_HOURS = 72

try:
    YOUTUBE_CHANNEL_MIN_SUBSCRIBERS = max(
        0,
        int(os.getenv("YOUTUBE_CHANNEL_MIN_SUBSCRIBERS", "250000") or 250000),
    )
except ValueError:
    YOUTUBE_CHANNEL_MIN_SUBSCRIBERS = 250000

try:
    YOUTUBE_CURATED_CHANNEL_LIMIT = max(
        1,
        min(
            len(YOUTUBE_CURATED_CHANNELS),
            int(
                os.getenv(
                    "YOUTUBE_CURATED_CHANNEL_LIMIT",
                    str(len(YOUTUBE_CURATED_CHANNELS)),
                )
                or len(YOUTUBE_CURATED_CHANNELS)
            ),
        ),
    )
except ValueError:
    YOUTUBE_CURATED_CHANNEL_LIMIT = len(YOUTUBE_CURATED_CHANNELS)

try:
    YOUTUBE_REFRESH_MINUTES = max(
        15,
        min(240, int(os.getenv("YOUTUBE_REFRESH_MINUTES", "60") or 60)),
    )
except ValueError:
    YOUTUBE_REFRESH_MINUTES = 60

try:
    YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN = max(
        0,
        min(12, int(os.getenv("YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN", "4") or 4)),
    )
except ValueError:
    YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN = 4

try:
    YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN = max(
        0,
        min(12, int(os.getenv("YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN", "4") or 4)),
    )
except ValueError:
    YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN = 4

try:
    YOUTUBE_SEARCH_RESULTS_PER_QUERY = max(
        1,
        min(25, int(os.getenv("YOUTUBE_SEARCH_RESULTS_PER_QUERY", "8") or 8)),
    )
except ValueError:
    YOUTUBE_SEARCH_RESULTS_PER_QUERY = 8

try:
    YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN = max(
        10,
        min(300, int(os.getenv("YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN", "120") or 120)),
    )
except ValueError:
    YOUTUBE_MAX_TRACKED_VIDEOS_PER_RUN = 120

try:
    YOUTUBE_MAX_COMMENT_VIDEOS_PER_RUN = max(
        1,
        min(50, int(os.getenv("YOUTUBE_MAX_COMMENT_VIDEOS_PER_RUN", "18") or 18)),
    )
except ValueError:
    YOUTUBE_MAX_COMMENT_VIDEOS_PER_RUN = 18

try:
    YOUTUBE_COMMENT_THREADS_PER_VIDEO = max(
        1,
        min(100, int(os.getenv("YOUTUBE_COMMENT_THREADS_PER_VIDEO", "30") or 30)),
    )
except ValueError:
    YOUTUBE_COMMENT_THREADS_PER_VIDEO = 30

try:
    YOUTUBE_COMMENT_REPLIES_PER_THREAD = max(
        0,
        min(20, int(os.getenv("YOUTUBE_COMMENT_REPLIES_PER_THREAD", "6") or 6)),
    )
except ValueError:
    YOUTUBE_COMMENT_REPLIES_PER_THREAD = 6

try:
    YOUTUBE_MAX_RELATED_SEEDS_PER_RUN = max(
        0,
        min(20, int(os.getenv("YOUTUBE_MAX_RELATED_SEEDS_PER_RUN", "6") or 6)),
    )
except ValueError:
    YOUTUBE_MAX_RELATED_SEEDS_PER_RUN = 6

try:
    YOUTUBE_MAX_RELATED_RESULTS_PER_SEED = max(
        0,
        min(10, int(os.getenv("YOUTUBE_MAX_RELATED_RESULTS_PER_SEED", "4") or 4)),
    )
except ValueError:
    YOUTUBE_MAX_RELATED_RESULTS_PER_SEED = 4

try:
    YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN = max(
        0,
        min(40, int(os.getenv("YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN", "12") or 12)),
    )
except ValueError:
    YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN = 12

try:
    YOUTUBE_TRACKED_VIDEO_LOOKBACK_HOURS = max(
        24,
        min(24 * 14, int(os.getenv("YOUTUBE_TRACKED_VIDEO_LOOKBACK_HOURS", "168") or 168)),
    )
except ValueError:
    YOUTUBE_TRACKED_VIDEO_LOOKBACK_HOURS = 168

try:
    YOUTUBE_MAX_SNAPSHOTS_PER_VIDEO = max(
        2,
        min(400, int(os.getenv("YOUTUBE_MAX_SNAPSHOTS_PER_VIDEO", "96") or 96)),
    )
except ValueError:
    YOUTUBE_MAX_SNAPSHOTS_PER_VIDEO = 96

try:
    YOUTUBE_QUOTA_BUDGET_PER_RUN = max(
        50,
        min(10_000, int(os.getenv("YOUTUBE_QUOTA_BUDGET_PER_RUN", "900") or 900)),
    )
except ValueError:
    YOUTUBE_QUOTA_BUDGET_PER_RUN = 900

try:
    YOUTUBE_DISCOVERY_MAX_AGE_HOURS = max(
        12,
        min(24 * 14, int(os.getenv("YOUTUBE_DISCOVERY_MAX_AGE_HOURS", "168") or 168)),
    )
except ValueError:
    YOUTUBE_DISCOVERY_MAX_AGE_HOURS = 168

YOUTUBE_DISCOVERY_QUERIES = _parse_text_csv_env(
    "YOUTUBE_DISCOVERY_QUERIES",
    DEFAULT_YOUTUBE_DISCOVERY_QUERIES,
)


def get_youtube_api_key() -> str:
    return (
        os.getenv("YOUTUBE_API_KEY", "").strip()
        or os.getenv("GOOGLE_API_KEY", "").strip()
    )


def get_curated_youtube_channels() -> list[dict[str, str]]:
    return YOUTUBE_CURATED_CHANNELS[:YOUTUBE_CURATED_CHANNEL_LIMIT]


def get_youtube_discovery_queries() -> list[str]:
    return YOUTUBE_DISCOVERY_QUERIES[:]


def youtube_enabled() -> bool:
    api_key = get_youtube_api_key()
    return bool(api_key and not api_key.startswith("REPLACE_WITH_"))
