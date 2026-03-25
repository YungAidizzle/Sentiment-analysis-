from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

RedditCatalogEntry = Dict[str, Any]

DATA_ROOT = Path(__file__).resolve().parents[1] / "data"
DISCOVERED_SUBREDDITS_PATH = DATA_ROOT / "reddit_engine" / "discovered_subreddits.json"

TIER_PRIORITY = {
    "tier1": 100,
    "tier2": 70,
    "tier3": 45,
}

ACTIVITY_PRIORITY = {
    "massive": 26,
    "high": 18,
    "medium": 10,
    "niche": 4,
}

AUDIENCE_PRIORITY = {
    "mainstream": 16,
    "broad": 10,
    "specialist": 6,
    "niche": 0,
}

TIER_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "tier1": {
        "liveCadenceWeight": 3,
        "liveLimit": 100,
        "liveListings": ["new"],
        "liveMaxPages": 6,
        "liveMaxPostsPerRun": 600,
        "commentThreshold": 18,
        "maxTrackedPosts": 20,
        "maxCommentsPerPost": 40,
        "knownPostsRefreshLimit": 12,
        "activePostLookbackHours": 48,
        "backfillMaxPosts": 2200,
        "backfillMaxPages": 32,
        "backfillMaxCommentThreads": 20,
    },
    "tier2": {
        "liveCadenceWeight": 2,
        "liveLimit": 100,
        "liveListings": ["new"],
        "liveMaxPages": 4,
        "liveMaxPostsPerRun": 320,
        "commentThreshold": 12,
        "maxTrackedPosts": 14,
        "maxCommentsPerPost": 28,
        "knownPostsRefreshLimit": 8,
        "activePostLookbackHours": 36,
        "backfillMaxPosts": 1400,
        "backfillMaxPages": 20,
        "backfillMaxCommentThreads": 12,
    },
    "tier3": {
        "liveCadenceWeight": 1,
        "liveLimit": 100,
        "liveListings": ["new"],
        "liveMaxPages": 3,
        "liveMaxPostsPerRun": 180,
        "commentThreshold": 8,
        "maxTrackedPosts": 8,
        "maxCommentsPerPost": 18,
        "knownPostsRefreshLimit": 5,
        "activePostLookbackHours": 24,
        "backfillMaxPosts": 700,
        "backfillMaxPages": 10,
        "backfillMaxCommentThreads": 6,
    },
}

LIVE_BUCKET_SIZE_BY_TIER = {
    "tier1": 8,
    "tier2": 10,
    "tier3": 12,
}

BACKFILL_BUCKET_SIZE_BY_TIER = {
    "tier1": 6,
    "tier2": 8,
    "tier3": 10,
}

LIVE_BUCKET_TIER_PATTERN = ["tier1", "tier2", "tier1", "tier3"]
BACKFILL_BUCKET_TIER_PATTERN = ["tier1", "tier2", "tier3"]

CATEGORY_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "world_news_breaking": {
        "label": "World News / Breaking",
        "defaultTier": "tier1",
        "defaultActivityTier": "massive",
        "defaultAudienceScale": "mainstream",
        "defaultTags": ["news", "world", "breaking"],
        "subreddits": [
            "worldnews",
            "news",
            "inthenews",
            "geopolitics",
            "foreignpolicy",
            "credibledefense",
            "anime_titties",
            {"name": "europe", "tier": "tier2"},
            {"name": "unitedkingdom", "tier": "tier2"},
            {"name": "canada", "tier": "tier2"},
            {"name": "australia", "tier": "tier2"},
            {"name": "india", "tier": "tier2"},
            {"name": "japan", "tier": "tier2"},
            {"name": "china", "tier": "tier2"},
            {"name": "ukpolitics", "tier": "tier2"},
        ],
    },
    "politics_geopolitics": {
        "label": "Politics / Public Affairs",
        "defaultTier": "tier1",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "mainstream",
        "defaultTags": ["politics", "policy", "elections"],
        "subreddits": [
            "politics",
            "PoliticalDiscussion",
            "NeutralPolitics",
            "ModeratePolitics",
            "Conservative",
            "Liberal",
            "Republican",
            "democrats",
            "neoliberal",
            "Libertarian",
            {"name": "PoliticalHumor", "tier": "tier2", "spamProne": True},
            {"name": "Ask_Politics", "tier": "tier2"},
        ],
    },
    "finance_markets": {
        "label": "Finance / Markets",
        "defaultTier": "tier1",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "mainstream",
        "defaultTags": ["finance", "markets", "stocks"],
        "subreddits": [
            "stocks",
            "investing",
            "wallstreetbets",
            "SecurityAnalysis",
            "options",
            "StockMarket",
            "Daytrading",
            "ValueInvesting",
            "Bogleheads",
            "dividends",
            "economics",
            "finance",
            "Forex",
            "algotrading",
            {"name": "personalfinance", "tier": "tier2", "audienceScale": "broad"},
        ],
    },
    "crypto": {
        "label": "Crypto",
        "defaultTier": "tier1",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["crypto", "tokens", "markets"],
        "subreddits": [
            "CryptoCurrency",
            "CryptoMarkets",
            "Bitcoin",
            "BitcoinMarkets",
            "ethereum",
            "ethtrader",
            "solana",
            "dogecoin",
            "defi",
            {"name": "SatoshiStreetBets", "tier": "tier2", "spamProne": True},
            {"name": "CryptoMoonShots", "tier": "tier3", "spamProne": True},
            {"name": "nft", "tier": "tier3", "activityTier": "medium"},
            {"name": "Buttcoin", "tier": "tier3", "audienceScale": "specialist"},
        ],
    },
    "business_startups": {
        "label": "Business / Startups",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["business", "startups", "work"],
        "subreddits": [
            "business",
            "startups",
            "Entrepreneur",
            "smallbusiness",
            "marketing",
            "ecommerce",
            "jobs",
            "workplace",
            "accounting",
            "sales",
            "consulting",
            "productmanagement",
            {"name": "careerguidance", "tier": "tier3"},
        ],
    },
    "ai_ml": {
        "label": "AI / ML",
        "defaultTier": "tier1",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["ai", "ml", "models"],
        "subreddits": [
            "OpenAI",
            "ChatGPT",
            "singularity",
            "artificial",
            "MachineLearning",
            "LocalLLaMA",
            "StableDiffusion",
            "deeplearning",
            "reinforcementlearning",
            "PromptEngineering",
            "computervision",
            {"name": "MistralAI", "tier": "tier3", "activityTier": "medium"},
            {"name": "ClaudeAI", "tier": "tier3", "activityTier": "medium"},
        ],
    },
    "software_developer": {
        "label": "Software / Developer",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["software", "developer", "programming"],
        "subreddits": [
            "technology",
            "programming",
            "webdev",
            "javascript",
            "typescript",
            "python",
            "golang",
            "rust",
            "linux",
            "sysadmin",
            "devops",
            "cybersecurity",
            "netsec",
            "SaaS",
            {"name": "learnprogramming", "tier": "tier3", "spamProne": True},
        ],
    },
    "gaming": {
        "label": "Gaming",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["gaming", "games", "releases"],
        "subreddits": [
            "gaming",
            "Games",
            "pcgaming",
            "xbox",
            "playstation",
            "nintendo",
            "Steam",
            "leagueoflegends",
            "DotA2",
            "GlobalOffensive",
            "VALORANT",
            "Overwatch",
            "FortNiteBR",
            "apexlegends",
            "helldivers2",
        ],
    },
    "internet_culture_memes": {
        "label": "Internet Culture / Memes",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["internet", "memes", "culture"],
        "subreddits": [
            {"name": "memes", "spamProne": True},
            {"name": "dankmemes", "spamProne": True},
            {"name": "MemeEconomy", "spamProne": True},
            {"name": "comedyheaven", "tier": "tier3", "spamProne": True},
            {"name": "ComedyCemetery", "tier": "tier3", "spamProne": True},
            "OutOfTheLoop",
            {"name": "shitposting", "tier": "tier3", "spamProne": True},
            {"name": "196", "tier": "tier3", "spamProne": True},
            {"name": "TikTokCringe", "tier": "tier3", "spamProne": True},
            {"name": "wholesomememes", "tier": "tier3", "spamProne": True},
            {"name": "AdviceAnimals", "tier": "tier3", "spamProne": True},
            {"name": "meirl", "tier": "tier3", "spamProne": True},
            {"name": "AskReddit", "tier": "tier2", "spamProne": True, "audienceScale": "mainstream"},
            {"name": "NoStupidQuestions", "tier": "tier2", "spamProne": True},
            {"name": "TooAfraidToAsk", "tier": "tier2", "spamProne": True},
        ],
    },
    "entertainment_celebrities": {
        "label": "Entertainment / Celebrities",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["entertainment", "celebrities", "culture"],
        "subreddits": [
            "popculturechat",
            "Fauxmoi",
            "television",
            "movies",
            "boxoffice",
            "Music",
            "hiphopheads",
            "kpop",
            "books",
            "documentaries",
            {"name": "marvelstudios", "tier": "tier3"},
            {"name": "fantasy", "tier": "tier3"},
        ],
    },
    "sports": {
        "label": "Sports",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["sports", "teams", "leagues"],
        "subreddits": [
            "nba",
            "nfl",
            "baseball",
            "hockey",
            "soccer",
            "formula1",
            "MMA",
            "CFB",
            "CollegeBasketball",
            "tennis",
            {"name": "golf", "tier": "tier3"},
        ],
    },
    "science_health": {
        "label": "Science / Health",
        "defaultTier": "tier2",
        "defaultActivityTier": "high",
        "defaultAudienceScale": "broad",
        "defaultTags": ["science", "health", "research"],
        "subreddits": [
            "science",
            "askscience",
            "medicine",
            "Health",
            "biotech",
            "nutrition",
            "fitness",
            "Longevity",
            "space",
            "environment",
            {"name": "bodybuilding", "tier": "tier3"},
            {"name": "running", "tier": "tier3"},
        ],
    },
    "consumer_products_brands": {
        "label": "Consumer Products / Brands",
        "defaultTier": "tier2",
        "defaultActivityTier": "medium",
        "defaultAudienceScale": "broad",
        "defaultTags": ["products", "brands", "consumer"],
        "subreddits": [
            "apple",
            "Android",
            "GooglePixel",
            "Samsung",
            "gadgets",
            "teslamotors",
            "electricvehicles",
            "buildapc",
            "buildapcsales",
            "headphones",
            "audiophile",
            {"name": "cars", "tier": "tier3"},
        ],
    },
    "lifestyle_fashion_fitness": {
        "label": "Lifestyle / Fashion / Fitness",
        "defaultTier": "tier3",
        "defaultActivityTier": "medium",
        "defaultAudienceScale": "broad",
        "defaultTags": ["lifestyle", "fashion", "fitness"],
        "subreddits": [
            "malefashionadvice",
            "femalefashionadvice",
            "SkincareAddiction",
            "Cooking",
            "food",
            "travel",
            "HomeImprovement",
            "Frugal",
            "BuyItForLife",
            {"name": "streetwear", "tier": "tier3"},
        ],
    },
    "regional_hubs": {
        "label": "Regional / City Hubs",
        "defaultTier": "tier3",
        "defaultActivityTier": "medium",
        "defaultAudienceScale": "niche",
        "defaultTags": ["regional", "city", "local"],
        "subreddits": [
            "nyc",
            "losangeles",
            "bayarea",
            "chicago",
            "seattle",
            "boston",
            "london",
            "toronto",
            "sydney",
            "melbourne",
        ],
    },
    "niche_enthusiasts": {
        "label": "Niche Enthusiasts",
        "defaultTier": "tier3",
        "defaultActivityTier": "medium",
        "defaultAudienceScale": "niche",
        "defaultTags": ["enthusiast", "niche", "communities"],
        "subreddits": [
            "mechanicalkeyboards",
            "boardgames",
            "3Dprinting",
            "homeassistant",
            "photography",
            "woodworking",
            "watches",
            "Sneakers",
            "EDC",
            "virtualreality",
        ],
    },
}


def _unique_strings(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def _normalize_live_listings(listings: Iterable[str] | None, *, spam_prone: bool) -> List[str]:
    selected = [str(listing).strip().lower() for listing in listings or [] if str(listing).strip()]
    if not selected:
        selected = ["new", "hot"] if spam_prone else ["new", "hot", "rising"]
    if "new" not in selected:
        selected.insert(0, "new")
    return _unique_strings(selected)


def _build_fetch_profile(
    *,
    tier: str,
    activity_tier: str,
    audience_scale: str,
    spam_prone: bool,
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    defaults = {**TIER_DEFAULTS[tier], **overrides}
    live_limit = int(defaults.get("liveLimit", 100) or 100)
    live_max_pages = int(defaults.get("liveMaxPages", 1) or 1)
    live_max_posts_per_run = int(defaults.get("liveMaxPostsPerRun", 0) or 0)
    comment_threshold = int(defaults.get("commentThreshold", 0) or 0)
    max_comments = int(defaults.get("maxCommentsPerPost", 0) or 0)
    max_tracked = int(defaults.get("maxTrackedPosts", 0) or 0)
    known_refresh = int(defaults.get("knownPostsRefreshLimit", 0) or 0)
    backfill_posts = int(defaults.get("backfillMaxPosts", 0) or 0)
    backfill_pages = int(defaults.get("backfillMaxPages", 0) or 0)
    backfill_threads = int(defaults.get("backfillMaxCommentThreads", 0) or 0)

    if audience_scale == "mainstream":
        max_tracked += 4
        live_max_pages += 1
        live_max_posts_per_run += 120
        backfill_posts += 400
        known_refresh += 2

    if activity_tier == "massive":
        max_tracked += 2
        live_max_pages += 1
        live_max_posts_per_run += 160
        backfill_posts += 200
    elif activity_tier == "niche":
        live_max_pages = max(2, live_max_pages - 1)
        live_max_posts_per_run = max(120, live_max_posts_per_run - 80)
        backfill_posts = max(400, backfill_posts - 200)
        known_refresh = max(3, known_refresh - 1)

    if spam_prone:
        comment_threshold += 8
        max_comments = min(max_comments, 20)
        known_refresh = max(2, known_refresh // 2)
        live_max_posts_per_run = max(80, live_max_posts_per_run - 80)

    return {
        "liveCadenceWeight": int(defaults.get("liveCadenceWeight", 1) or 1),
        "liveLimit": max(25, min(100, live_limit)),
        "liveListings": _normalize_live_listings(
            defaults.get("liveListings"),
            spam_prone=spam_prone,
        ),
        "liveMaxPages": max(1, live_max_pages),
        "liveMaxPostsPerRun": max(50, live_max_posts_per_run),
        "commentThreshold": comment_threshold,
        "maxTrackedPosts": max_tracked,
        "maxCommentsPerPost": max_comments,
        "knownPostsRefreshLimit": known_refresh,
        "activePostLookbackHours": int(defaults.get("activePostLookbackHours", 24) or 24),
        "backfillMaxPosts": backfill_posts,
        "backfillMaxPages": backfill_pages,
        "backfillMaxCommentThreads": backfill_threads,
    }


def _entry_from_definition(category: str, category_definition: Dict[str, Any], raw_entry: Any) -> RedditCatalogEntry:
    if isinstance(raw_entry, str):
        raw_entry = {"name": raw_entry}

    name = str(raw_entry.get("name", "") or "").strip()
    if not name:
        raise ValueError(f"subreddit entry in category {category} is missing a name")

    tier = str(raw_entry.get("tier") or category_definition["defaultTier"])
    activity_tier = str(raw_entry.get("activityTier") or category_definition["defaultActivityTier"])
    audience_scale = str(raw_entry.get("audienceScale") or category_definition["defaultAudienceScale"])
    spam_prone = bool(raw_entry.get("spamProne", False))
    tags = _unique_strings(
        [
            *category_definition.get("defaultTags", []),
            *raw_entry.get("tags", []),
        ]
    )
    fetch_profile = _build_fetch_profile(
        tier=tier,
        activity_tier=activity_tier,
        audience_scale=audience_scale,
        spam_prone=spam_prone,
        overrides=raw_entry.get("fetchProfile", {}),
    )
    priority_score = (
        TIER_PRIORITY.get(tier, 0)
        + ACTIVITY_PRIORITY.get(activity_tier, 0)
        + AUDIENCE_PRIORITY.get(audience_scale, 0)
        + (6 if not spam_prone else -8)
    )

    return {
        "subreddit": name,
        "key": name.lower(),
        "category": category,
        "categoryLabel": category_definition["label"],
        "tier": tier,
        "activityTier": activity_tier,
        "audienceScale": audience_scale,
        "broadCommunity": audience_scale in {"mainstream", "broad"},
        "spamProne": spam_prone,
        "tags": tags,
        "priorityScore": priority_score,
        "discoverySource": raw_entry.get("discoverySource", "seed"),
        "fetchProfile": fetch_profile,
    }


def _load_discovered_entries() -> List[RedditCatalogEntry]:
    try:
        payload = json.loads(DISCOVERED_SUBREDDITS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(payload, list):
        return []

    entries: List[RedditCatalogEntry] = []
    for raw_entry in payload:
        if not isinstance(raw_entry, dict):
            continue

        name = str(raw_entry.get("name", "") or "").strip()
        category = str(raw_entry.get("category", "") or "").strip()
        if not name or category not in CATEGORY_DEFINITIONS:
            continue

        if bool(raw_entry.get("excluded")):
            continue

        if bool(raw_entry.get("nsfw")) or bool(raw_entry.get("quarantined")) or bool(raw_entry.get("private")):
            continue

        subscribers = int(raw_entry.get("subscribers", 0) or 0)
        posts_per_day = float(raw_entry.get("postsPerDay", 0) or 0)
        if subscribers < 15_000 and posts_per_day < 5:
            continue

        if bool(raw_entry.get("spamProne")) and subscribers < 100_000 and posts_per_day < 12:
            continue

        normalized = _entry_from_definition(
            category,
            CATEGORY_DEFINITIONS[category],
            {
                **raw_entry,
                "discoverySource": raw_entry.get("discoverySource", "discovered"),
            },
        )
        entries.append(normalized)
    return entries


def build_subreddit_catalog() -> List[RedditCatalogEntry]:
    entries: List[RedditCatalogEntry] = []
    seen: set[str] = set()

    for category, category_definition in CATEGORY_DEFINITIONS.items():
        for raw_entry in category_definition["subreddits"]:
            entry = _entry_from_definition(category, category_definition, raw_entry)
            if entry["key"] in seen:
                continue
            seen.add(entry["key"])
            entries.append(entry)

    for entry in _load_discovered_entries():
        if entry["key"] in seen:
            continue
        seen.add(entry["key"])
        entries.append(entry)

    return sorted(
        entries,
        key=lambda item: (
            -int(item.get("priorityScore", 0) or 0),
            str(item.get("categoryLabel", "")),
            str(item.get("subreddit", "")),
        ),
    )


def build_subreddit_buckets() -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    for category, category_definition in CATEGORY_DEFINITIONS.items():
        names: List[str] = []
        seen: set[str] = set()
        for raw_entry in category_definition["subreddits"]:
            name = str(raw_entry if isinstance(raw_entry, str) else raw_entry.get("name", "")).strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            names.append(name)
        buckets[category] = names
    return buckets


SUBREDDIT_CATALOG = build_subreddit_catalog()
SUBREDDIT_LOOKUP = {
    str(entry["key"]): entry
    for entry in SUBREDDIT_CATALOG
}
SUBREDDIT_BUCKETS = build_subreddit_buckets()
DEFAULT_SUBREDDITS = [entry["subreddit"] for entry in SUBREDDIT_CATALOG]
