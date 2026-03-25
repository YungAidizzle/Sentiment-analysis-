from __future__ import annotations

from backend.youtube_config import (
    YOUTUBE_CHANNEL_LOOKBACK_HOURS,
    YOUTUBE_CHANNEL_MIN_SUBSCRIBERS,
    YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL,
    YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN,
    YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN,
    YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN,
    YOUTUBE_MAX_RELATED_RESULTS_PER_SEED,
    YOUTUBE_MAX_RELATED_SEEDS_PER_RUN,
    YOUTUBE_SEARCH_RESULTS_PER_QUERY,
    get_curated_youtube_channels,
    get_youtube_discovery_queries,
)

PUBLIC_SOURCE_ITEM_LIMIT = 40

PUBLIC_RSS_FEEDS = [
    {
        "id": "bbc-world",
        "label": "BBC World",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
    },
    {
        "id": "npr-top",
        "label": "NPR Top",
        "url": "https://feeds.npr.org/1001/rss.xml",
    },
    {
        "id": "techcrunch",
        "label": "TechCrunch",
        "url": "https://techcrunch.com/feed/",
    },
    {
        "id": "verge",
        "label": "The Verge",
        "url": "https://www.theverge.com/rss/index.xml",
    },
    {
        "id": "ars",
        "label": "Ars Technica",
        "url": "https://feeds.arstechnica.com/arstechnica/index",
    },
]

PUBLIC_JSON_FEEDS = [
    {
        "id": "hackernews-top",
        "label": "Hacker News Top",
        "kind": "hackernews",
        "url": "https://hacker-news.firebaseio.com/v0/topstories.json",
        "itemUrlTemplate": "https://hacker-news.firebaseio.com/v0/item/{id}.json",
    },
    {
        "id": "hackernews-best",
        "label": "Hacker News Best",
        "kind": "hackernews",
        "url": "https://hacker-news.firebaseio.com/v0/beststories.json",
        "itemUrlTemplate": "https://hacker-news.firebaseio.com/v0/item/{id}.json",
    },
    {
        "id": "hackernews-new",
        "label": "Hacker News New",
        "kind": "hackernews",
        "url": "https://hacker-news.firebaseio.com/v0/newstories.json",
        "itemUrlTemplate": "https://hacker-news.firebaseio.com/v0/item/{id}.json",
    },
    {
        "id": "lobsters-newest",
        "label": "Lobsters Newest",
        "kind": "lobsters",
        "url": "https://lobste.rs/newest.json",
    },
    {
        "id": "lobsters-hottest",
        "label": "Lobsters Hottest",
        "kind": "lobsters",
        "url": "https://lobste.rs/hottest.json",
    },
]

PUBLIC_GOOGLE_TRENDS_FEEDS = [
    {
        "id": "google-trends-us",
        "label": "Google Trends US",
        "kind": "googletrends",
        "url": "https://trends.google.com/trending/rss?geo=US",
        "geo": "US",
    },
    {
        "id": "google-trends-au",
        "label": "Google Trends AU",
        "kind": "googletrends",
        "url": "https://trends.google.com/trending/rss?geo=AU",
        "geo": "AU",
    },
    {
        "id": "google-trends-gb",
        "label": "Google Trends GB",
        "kind": "googletrends",
        "url": "https://trends.google.com/trending/rss?geo=GB",
        "geo": "GB",
    },
    {
        "id": "google-trends-ca",
        "label": "Google Trends CA",
        "kind": "googletrends",
        "url": "https://trends.google.com/trending/rss?geo=CA",
        "geo": "CA",
    },
]

PUBLIC_YOUTUBE_FEEDS = [
    {
        "id": "youtube-curated-news",
        "label": "YouTube Curated News",
        "kind": "youtube",
        "lane": "curated",
        "channels": get_curated_youtube_channels(),
        "recentUploadsPerChannel": YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL,
        "lookbackHours": YOUTUBE_CHANNEL_LOOKBACK_HOURS,
        "minSubscriberCount": YOUTUBE_CHANNEL_MIN_SUBSCRIBERS,
    },
    {
        "id": "youtube-keyword-discovery",
        "label": "YouTube Keyword Discovery",
        "kind": "youtube",
        "lane": "keyword",
        "queries": get_youtube_discovery_queries(),
        "maxQueriesPerRun": YOUTUBE_MAX_KEYWORD_QUERIES_PER_RUN,
        "maxResults": YOUTUBE_SEARCH_RESULTS_PER_QUERY,
        "lookbackHours": YOUTUBE_CHANNEL_LOOKBACK_HOURS,
    },
    {
        "id": "youtube-breakout-discovery",
        "label": "YouTube Breakout Discovery",
        "kind": "youtube",
        "lane": "breakout",
        "maxQueriesPerRun": YOUTUBE_MAX_BREAKOUT_QUERIES_PER_RUN,
        "maxResults": YOUTUBE_SEARCH_RESULTS_PER_QUERY,
        "lookbackHours": YOUTUBE_CHANNEL_LOOKBACK_HOURS,
    },
    {
        "id": "youtube-related-discovery",
        "label": "YouTube Related Discovery",
        "kind": "youtube",
        "lane": "related",
        "maxSeedVideos": YOUTUBE_MAX_RELATED_SEEDS_PER_RUN,
        "maxResultsPerSeed": YOUTUBE_MAX_RELATED_RESULTS_PER_SEED,
        "lookbackHours": YOUTUBE_CHANNEL_LOOKBACK_HOURS,
    },
    {
        "id": "youtube-channel-expansion",
        "label": "YouTube Channel Expansion",
        "kind": "youtube",
        "lane": "channel-expansion",
        "recentUploadsPerChannel": YOUTUBE_CHANNEL_UPLOADS_PER_CHANNEL,
        "channelLimit": YOUTUBE_MAX_CHANNEL_EXPANSIONS_PER_RUN,
        "lookbackHours": YOUTUBE_CHANNEL_LOOKBACK_HOURS,
        "minSubscriberCount": YOUTUBE_CHANNEL_MIN_SUBSCRIBERS,
    },
]
