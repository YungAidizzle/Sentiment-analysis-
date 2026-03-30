from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

from backend.bluesky_firehose import sync_bluesky_firehose
from backend.topic_rules import (
    CANONICAL_TOPIC_RULES,
    TOPIC_ACRONYM_ALLOWLIST,
    TOPIC_ENTITY_ALLOWLIST,
    TOPIC_GENERIC_WEAK_TOKENS,
    TOPIC_URL_DEBRIS_TOKENS,
    build_topic_alias_lookup,
    canonicalize_topic_key,
    normalize_topic_key_text,
    score_topic_candidate,
)

PLATFORM = "bluesky"
URL_PATTERN = re.compile(r"https?://[^\s<>\"]+")
HASHTAG_PATTERN = re.compile(r"(?:^|\s)#([A-Za-z0-9_]{1,100})")
MENTION_PATTERN = re.compile(r"(?:^|\s)@([A-Za-z0-9_.-]{1,100})")
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_'-]{1,63}")
CASHTAG_PATTERN = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,15})")
WHITESPACE_PATTERN = re.compile(r"\s+")
PHRASE_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9&'-]*")

TOPIC_CONNECTOR_WORDS = {
    "&",
    "at",
    "for",
    "in",
    "of",
    "on",
    "the",
    "to",
}

TOPIC_BLOCKLIST = {
    "a",
    "an",
    "and",
    "bad",
    "bro",
    "crazy",
    "good",
    "got",
    "i",
    "like",
    "look",
    "need",
    "people",
    "really",
    "rival",
    "that",
    "thing",
    "this",
    "today",
    "want",
    "wild",
}

STOPWORD_TOKENS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "he",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "that",
    "the",
    "to",
    "was",
    "were",
    "will",
    "with",
}

TOPIC_ACRONYM_EXCEPTIONS = {
    "api",
    "btc",
    "djt",
    "eth",
    "eu",
    "fbi",
    "gop",
    "nfl",
    "nba",
    "nsa",
    "nft",
    "sec",
    "spy",
    "uk",
    "un",
    "usa",
    "xrp",
}
TOPIC_ACRONYM_EXCEPTIONS = TOPIC_ACRONYM_EXCEPTIONS.union(TOPIC_ACRONYM_ALLOWLIST)

TOPIC_NOISE_TOKENS = {
    "additional",
    "advisory",
    "afd",
    "airnow",
    "aqi",
    "details",
    "discussion",
    "forecast",
    "iembot",
    "issued",
    "prelim",
    "statement",
    "update",
}
TOPIC_NOISE_TOKENS = TOPIC_NOISE_TOKENS.union(TOPIC_URL_DEBRIS_TOKENS)

TOPIC_GARBAGE_PHRASE_PATTERNS = (
    re.compile(r"\b(?:area|forecast|discussion)\b.*\b(?:afd|airnow|aqi)\b", re.IGNORECASE),
    re.compile(r"\b(?:additional|details)\s+here\b", re.IGNORECASE),
    re.compile(r"\b[a-z0-9]+bot\b", re.IGNORECASE),
)

TOPIC_NUMBER_WORD_TOKENS = {
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
    "hundred",
    "thousand",
    "million",
    "billion",
    "trillion",
    "first",
    "second",
    "third",
    "fourth",
    "fifth",
    "sixth",
    "seventh",
    "eighth",
    "ninth",
    "tenth",
}

WEAK_TOPIC_TOKENS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "both",
    "but",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "dont",
    "for",
    "from",
    "get",
    "got",
    "had",
    "has",
    "have",
    "here",
    "how",
    "i",
    "if",
    "ill",
    "im",
    "in",
    "into",
    "is",
    "it",
    "its",
    "ive",
    "just",
    "like",
    "many",
    "may",
    "me",
    "might",
    "more",
    "most",
    "my",
    "need",
    "no",
    "not",
    "now",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "same",
    "she",
    "should",
    "so",
    "some",
    "still",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "to",
    "today",
    "tomorrow",
    "us",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "with",
    "would",
    # Conversational and platform-generic labels that should not rank as narratives.
    "absolutely",
    "actually",
    "ad",
    "adorable",
    "again",
    "all",
    "always",
    "amazing",
    "another",
    "anyone",
    "appreciate",
    "area",
    "available",
    "back",
    "believe",
    "best",
    "better",
    "big",
    "bluesky",
    "bot",
    "bsky",
    "character",
    "come",
    "coming",
    "cool",
    "cute",
    "day",
    "de",
    "del",
    "der",
    "des",
    "die",
    "digit",
    "doing",
    "don",
    "early",
    "el",
    "en",
    "es",
    "est",
    "et",
    "else",
    "enjoy",
    "even",
    "ever",
    "every",
    "everyone",
    "exactly",
    "facebook",
    "feel",
    "feels",
    "feed",
    "finally",
    "first",
    "funny",
    "game",
    "games",
    "gave",
    "global",
    "going",
    "gonna",
    "gorgeous",
    "half",
    "happy",
    "hear",
    "hehe",
    "hello",
    "hey",
    "hi",
    "his",
    "hope",
    "house",
    "ich",
    "id",
    "imagine",
    "incredible",
    "instead",
    "internet",
    "kind",
    "know",
    "la",
    "last",
    "le",
    "lets",
    "little",
    "live",
    "looks",
    "loved",
    "love",
    "major",
    "mas",
    "make",
    "making",
    "mean",
    "mind",
    "much",
    "myself",
    "needs",
    "never",
    "new",
    "news",
    "next",
    "nice",
    "night",
    "nowplaying",
    "oh",
    "ok",
    "once",
    "only",
    "original",
    "over",
    "part",
    "pero",
    "por",
    "photography",
    "place",
    "please",
    "post",
    "pretty",
    "probably",
    "profile",
    "pulse",
    "read",
    "right",
    "said",
    "say",
    "see",
    "seems",
    "si",
    "share",
    "shit",
    "short",
    "social",
    "someone",
    "something",
    "sometimes",
    "sorry",
    "stop",
    "story",
    "such",
    "super",
    "sure",
    "sunday",
    "talk",
    "talent",
    "technology",
    "thank",
    "thanks",
    "thats",
    "think",
    "through",
    "time",
    "true",
    "trying",
    "tv",
    "una",
    "und",
    "video",
    "vote",
    "wait",
    "watching",
    "well",
    "went",
    "while",
    "wish",
    "work",
    "yeah",
    "yes",
    "young",
    "you",
    "your",
}
WEAK_TOPIC_TOKENS = WEAK_TOPIC_TOKENS.union(TOPIC_GENERIC_WEAK_TOKENS)

TOPIC_CANONICAL_ALIASES = {
    alias_key: rule.canonical_label
    for alias_key, rule in build_topic_alias_lookup(CANONICAL_TOPIC_RULES).items()
}

CRYPTO_TERMS = {
    "airdrop",
    "altcoin",
    "bitcoin",
    "blockchain",
    "coin",
    "crypto",
    "dex",
    "defi",
    "eth",
    "ethereum",
    "nft",
    "onchain",
    "sol",
    "solana",
    "token",
    "wallet",
    "web3",
}

MEMECOIN_TERMS = {
    "ape",
    "degen",
    "doge",
    "memecoin",
    "moon",
    "pepe",
    "pump",
    "rug",
    "shib",
}

ECOMMERCE_TERMS = {
    "alibaba",
    "aliexpress",
    "checkout",
    "ecommerce",
    "fulfillment",
    "inventory",
    "shop",
    "shopify",
    "sku",
    "supplier",
    "wholesale",
}

DROPSHIPPING_TERMS = {
    "dropship",
    "dropshipping",
    "drop-shipping",
}

PRODUCT_TERMS = {
    "catalog",
    "launch",
    "listing",
    "merch",
    "product",
    "retail",
    "store",
    "trend",
}

POSITIVE_SENTIMENT_LEXICON = {
    "awesome": 2,
    "bullish": 2,
    "excellent": 2,
    "good": 1,
    "great": 2,
    "improve": 1,
    "improved": 1,
    "love": 3,
    "positive": 1,
    "strong": 1,
    "upside": 1,
    "win": 2,
}

NEGATIVE_SENTIMENT_LEXICON = {
    "awful": 2,
    "bad": 1,
    "bearish": 2,
    "crash": 2,
    "dump": 2,
    "fail": 2,
    "hate": 3,
    "loss": 2,
    "negative": 1,
    "risk": 1,
    "scam": 2,
    "weak": 1,
}

NEUTRAL_SENTIMENT_LEXICON = {
    "fine": 1,
    "ok": 1,
    "okay": 1,
}


def _to_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

    candidate = str(value or "").strip()
    if not candidate:
        return None
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _dedupe_text(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        output.append(candidate)
    return output


def _normalize_whitespace(text: str) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return ""
    return WHITESPACE_PATTERN.sub(" ", candidate)


def _extract_urls(text: str) -> List[str]:
    if not text:
        return []
    urls: List[str] = []
    for match in URL_PATTERN.findall(str(text)):
        url = match.strip().rstrip(".,)")
        if not url:
            continue
        urls.append(url)
    return _dedupe_text(urls)


def _extract_hashtags(text: str) -> List[str]:
    if not text:
        return []
    tags: List[str] = []
    for match in HASHTAG_PATTERN.findall(text):
        hashtag = str(match or "").strip().lower()
        if not hashtag:
            continue
        tags.append(hashtag)
    return _dedupe_text(tags)


def _extract_mentions(text: str) -> List[str]:
    if not text:
        return []
    mentions: List[str] = []
    for match in MENTION_PATTERN.findall(text):
        mention = str(match or "").strip().lower()
        if not mention:
            continue
        mentions.append(mention)
    return _dedupe_text(mentions)


def _extract_cashtags(text: str) -> List[str]:
    if not text:
        return []
    cashtags: List[str] = []
    for match in CASHTAG_PATTERN.findall(text):
        token = str(match or "").strip().lower()
        if not token:
            continue
        cashtags.append(token)
    return _dedupe_text(cashtags)


def _extract_tokens(text: str) -> List[str]:
    if not text:
        return []
    tokens: List[str] = []
    for match in TOKEN_PATTERN.findall(text.lower()):
        token = str(match or "").strip("._-")
        if len(token) < 2:
            continue
        if token.isdigit():
            continue
        if token in STOPWORD_TOKENS:
            continue
        if token in TOPIC_URL_DEBRIS_TOKENS:
            continue
        tokens.append(token)
    return _dedupe_text(tokens)


def _extract_domains(urls: Iterable[str]) -> List[str]:
    domains: List[str] = []
    for url in urls:
        candidate = str(url or "").strip()
        if not candidate:
            continue
        try:
            parsed = urlparse(candidate)
        except Exception:
            continue
        host = str(parsed.netloc or "").lower().strip()
        if not host:
            continue
        host = host.removeprefix("www.")
        if not host:
            continue
        domains.append(host)
    return _dedupe_text(domains)


def _normalize_topic_text(value: str) -> str:
    candidate = _normalize_whitespace(str(value or ""))
    candidate = re.sub(r"[`\u2019\u2018\u00b4\u02bc\u02b9]", "'", candidate)
    candidate = re.sub(r"(?<=\w)[?\uFFFD](?=\w)", "", candidate)
    candidate = re.sub(r"(?<=\w)'(?=\w)", "", candidate)
    candidate = candidate.replace("'", "")
    candidate = candidate.strip("`~!%^*()[]{}<>:;\",.?/\\|")
    candidate = re.sub(r"\s+", " ", candidate).strip()
    return candidate


def _topic_tokens(value: str) -> list[str]:
    normalized = normalize_topic_key_text(str(value or ""))
    return [token for token in re.findall(r"[a-z0-9$#]+", normalized) if token]


def _is_acronym_like_token(value: str) -> bool:
    candidate = str(value or "").strip()
    if not candidate:
        return False
    if not candidate.isupper():
        return False
    if not (2 <= len(candidate) <= 10):
        return False
    return any(character.isalpha() for character in candidate)


def _is_garbage_topic_phrase(value: str, *, topic_type: str) -> bool:
    normalized_value = _normalize_whitespace(str(value or "").lower())
    tokens = _topic_tokens(normalized_value)
    if not tokens:
        return True

    if len(tokens) > 5:
        return True
    if any(pattern.search(normalized_value) for pattern in TOPIC_GARBAGE_PHRASE_PATTERNS):
        return True

    noise_count = sum(1 for token in tokens if token in TOPIC_NOISE_TOKENS)
    weak_count = sum(1 for token in tokens if token in WEAK_TOPIC_TOKENS)
    number_word_count = sum(1 for token in tokens if token in TOPIC_NUMBER_WORD_TOKENS)
    url_debris_count = sum(1 for token in tokens if token in TOPIC_URL_DEBRIS_TOKENS)
    informative_count = sum(
        1
        for token in tokens
        if token not in WEAK_TOPIC_TOKENS
        and token not in TOPIC_NOISE_TOKENS
        and token not in TOPIC_NUMBER_WORD_TOKENS
        and token not in TOPIC_URL_DEBRIS_TOKENS
        and (
            len(token) >= 4
            or token in TOPIC_ACRONYM_EXCEPTIONS
            or token in TOPIC_ENTITY_ALLOWLIST
        )
    )

    if len(tokens) == 1 and tokens[0] in TOPIC_NOISE_TOKENS:
        return True
    if len(tokens) == 1 and tokens[0] in TOPIC_NUMBER_WORD_TOKENS:
        return True
    if number_word_count == len(tokens):
        return True
    if topic_type in {"entity", "keyword"} and len(tokens) >= 2 and number_word_count >= (len(tokens) - 1):
        return True
    if url_debris_count > 0:
        return True
    if noise_count >= max(2, len(tokens) - 1):
        return True
    if len(tokens) >= 4 and noise_count >= 2:
        return True
    if informative_count == 0 and not (len(tokens) == 1 and _is_acronym_like_token(str(value or "").strip())):
        return True
    if topic_type == "keyword" and len(tokens) > 2:
        return True
    if topic_type in {"entity", "keyword"} and weak_count >= max(1, len(tokens) - 1):
        return True
    return False


def _is_high_signal_keyword_token(value: str) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return False
    if token in TOPIC_URL_DEBRIS_TOKENS:
        return False
    if token in STOPWORD_TOKENS or token in TOPIC_BLOCKLIST:
        return False
    if token in WEAK_TOPIC_TOKENS or token in TOPIC_NOISE_TOKENS:
        return False
    if token in TOPIC_NUMBER_WORD_TOKENS:
        return False
    if token in TOPIC_ENTITY_ALLOWLIST:
        return True
    if token in TOPIC_ACRONYM_EXCEPTIONS:
        return True
    if len(token) < 5:
        return False
    if token.isdigit():
        return False
    confidence = score_topic_candidate(topic_key=token, topic_type="keyword")
    return confidence >= 0.62


def _is_weak_topic_phrase(value: str, *, topic_type: str) -> bool:
    tokens = _topic_tokens(value)
    if not tokens:
        return True

    candidate = str(value or "").strip()
    candidate_lower = candidate.lower()
    if _is_acronym_like_token(candidate):
        if candidate_lower in WEAK_TOPIC_TOKENS or candidate_lower in TOPIC_NOISE_TOKENS:
            return True
        return False
    if _is_garbage_topic_phrase(value, topic_type=topic_type):
        return True

    if len(tokens) == 1:
        token = tokens[0]
        if token in WEAK_TOPIC_TOKENS or token in TOPIC_NOISE_TOKENS:
            return True
        if token in TOPIC_URL_DEBRIS_TOKENS:
            return True
        if token in TOPIC_NUMBER_WORD_TOKENS:
            return True
        if len(token) < 3 and token not in TOPIC_ACRONYM_EXCEPTIONS:
            return True
        if (
            topic_type == "keyword"
            and len(token) < 4
            and token not in TOPIC_ACRONYM_EXCEPTIONS
        ):
            return True

    weak_count = sum(
        1
        for token in tokens
        if token in WEAK_TOPIC_TOKENS or token in TOPIC_NOISE_TOKENS
    )
    informative_count = sum(
        1
        for token in tokens
        if token not in WEAK_TOPIC_TOKENS
        and token not in TOPIC_NOISE_TOKENS
        and (
            len(token) >= 4
            or token in TOPIC_ACRONYM_EXCEPTIONS
            or token in TOPIC_ENTITY_ALLOWLIST
        )
    )

    if weak_count == len(tokens):
        return True
    if informative_count == 0:
        return True
    if topic_type == "keyword" and weak_count >= max(1, len(tokens) - 1):
        return True
    if len(tokens) >= 4 and weak_count >= 2:
        return True

    return False


def _normalize_topic_value(value: str, *, topic_type: str) -> str:
    candidate = _normalize_topic_text(value)
    if not candidate:
        return ""

    if topic_type == "cashtag":
        return candidate.removeprefix("$").upper()
    if topic_type == "hashtag":
        hashtag_topic = normalize_topic_key_text(candidate.removeprefix("#"))
        alias = TOPIC_CANONICAL_ALIASES.get(hashtag_topic)
        if alias:
            return alias
        if hashtag_topic in TOPIC_ACRONYM_EXCEPTIONS:
            return hashtag_topic.upper()
        return hashtag_topic

    words = candidate.split(" ")
    normalized_words: list[str] = []
    for word in words:
        token = str(word or "").strip()
        if not token:
            continue
        token = re.sub(r"(?<=\w)'(?=\w)", "", token.replace("â€™", "'"))
        token = token.replace("'", "")
        cleaned = token[:-2] if token.lower().endswith("'s") and len(token) > 3 else token
        if cleaned.lower() in TOPIC_CONNECTOR_WORDS:
            normalized_words.append(cleaned.lower())
            continue
        if cleaned.isupper() and 2 <= len(cleaned) <= 10:
            normalized_words.append(cleaned)
            continue
        if any(character.isdigit() for character in cleaned):
            normalized_words.append(cleaned.upper() if cleaned.isupper() else cleaned)
            continue
        if "-" in cleaned:
            normalized_words.append(
                "-".join(
                    part[:1].upper() + part[1:].lower()
                    for part in cleaned.split("-")
                    if part
                )
            )
            continue
        normalized_words.append(cleaned[:1].upper() + cleaned[1:].lower())

    normalized = " ".join(normalized_words).strip()
    normalized_lookup = normalize_topic_key_text(normalized)
    alias = TOPIC_CANONICAL_ALIASES.get(normalized_lookup)
    if alias:
        return alias
    canonical_key, canonical_label, _ = canonicalize_topic_key(normalized_lookup)
    if canonical_key and canonical_key != normalized_lookup and canonical_label:
        return canonical_label
    if normalized_lookup in TOPIC_ACRONYM_EXCEPTIONS:
        return normalized_lookup.upper()
    return normalized


def _is_valid_topic_candidate(value: str, *, topic_type: str) -> bool:
    candidate = _normalize_topic_text(value)
    if not candidate:
        return False

    normalized = _normalize_topic_value(candidate, topic_type=topic_type)
    normalized_lower = normalize_topic_key_text(normalized)
    tokens = _topic_tokens(normalized)

    if not normalized:
        return False
    if not tokens:
        return False
    if len(normalized) < 2:
        return False
    if len(tokens) > 5:
        return False
    if normalized_lower.isdigit():
        return False
    if normalized_lower in TOPIC_BLOCKLIST:
        return False
    if normalized_lower in TOPIC_URL_DEBRIS_TOKENS:
        return False
    if len(tokens) == 1 and tokens[0] in TOPIC_NUMBER_WORD_TOKENS:
        return False
    if len(tokens) == 1 and tokens[0] in TOPIC_NOISE_TOKENS:
        return False
    if len(tokens) == 1 and tokens[0] in TOPIC_URL_DEBRIS_TOKENS:
        return False
    if _is_garbage_topic_phrase(normalized, topic_type=topic_type):
        return False
    if topic_type == "keyword" and len(normalized_lower) < 4:
        return False
    if topic_type == "keyword" and normalized_lower in STOPWORD_TOKENS:
        return False
    if _is_weak_topic_phrase(normalized, topic_type=topic_type):
        return False
    if any(token in TOPIC_URL_DEBRIS_TOKENS for token in tokens):
        return False
    if topic_type in {"entity", "keyword"} and all(token in WEAK_TOPIC_TOKENS for token in tokens):
        return False

    _canonical_key, _canonical_label, alias_confidence = canonicalize_topic_key(normalized_lower)
    confidence = score_topic_candidate(
        topic_key=normalized,
        topic_type=topic_type,
        alias_confidence=alias_confidence,
    )
    minimum_confidence = {
        "cashtag": 0.34,
        "hashtag": 0.38,
        "entity": 0.45,
        "keyword": 0.62,
    }.get(topic_type, 0.50)
    if len(tokens) == 1 and tokens[0] in TOPIC_ENTITY_ALLOWLIST:
        minimum_confidence = min(minimum_confidence, 0.35)
    return confidence >= minimum_confidence


def _is_proper_like_token(token: str) -> bool:
    value = str(token or "").strip()
    if not value:
        return False
    lower = normalize_topic_key_text(value)
    if not lower:
        return False
    if lower in STOPWORD_TOKENS or lower in TOPIC_BLOCKLIST:
        return False
    if lower in TOPIC_NOISE_TOKENS or lower in TOPIC_URL_DEBRIS_TOKENS:
        return False
    if lower in WEAK_TOPIC_TOKENS and lower not in TOPIC_ENTITY_ALLOWLIST:
        return False
    if len(lower) < 3 and lower not in TOPIC_ACRONYM_EXCEPTIONS:
        return False
    if value.isupper() and len(value) >= 2:
        return True
    if len(lower) < 4 and lower not in TOPIC_ACRONYM_EXCEPTIONS and lower not in TOPIC_ENTITY_ALLOWLIST:
        return False
    if value[:1].isupper() and any(character.islower() for character in value[1:]):
        return True
    if any(character.isupper() for character in value[1:]) and any(character.islower() for character in value):
        return True
    return False


def _is_sentence_leading_token(text: str, *, token_start: int) -> bool:
    if token_start <= 0:
        return True
    prefix = str(text[:token_start]).rstrip()
    if not prefix:
        return True
    return prefix[-1] in ".!?;:\n"


def _infer_topic_key_candidate(
    *,
    hashtags: List[str],
    tokens: List[str],
    tags: List[str],
) -> str:
    if hashtags:
        for hashtag in hashtags:
            if _is_valid_topic_candidate(f"#{hashtag}", topic_type="hashtag"):
                return _normalize_topic_value(f"#{hashtag}", topic_type="hashtag")
    prioritized = [
        token
        for token in tokens
        if _is_high_signal_keyword_token(token)
        and _is_valid_topic_candidate(token, topic_type="keyword")
    ]
    if prioritized:
        return _normalize_topic_value(prioritized[0], topic_type="keyword")
    return "general"


def _estimate_quality_score(
    *,
    clean_text: str,
    tokens: List[str],
    urls: List[str],
    metrics_json: Dict[str, Any],
) -> float:
    text_density = min(1.0, len(tokens) / 20.0)
    has_text = 0.25 if clean_text else 0.0
    has_link = 0.05 if urls else 0.0

    likes = int(metrics_json.get("likeCount", 0) or 0)
    replies = int(metrics_json.get("replyCount", 0) or 0)
    reposts = int(metrics_json.get("repostCount", 0) or 0)
    quotes = int(metrics_json.get("quoteCount", 0) or 0)
    engagement = max(0, likes + (2 * replies) + (2 * reposts) + (2 * quotes))
    engagement_signal = min(0.35, math.log1p(engagement) / 12.0)

    quality = has_text + (text_density * 0.35) + engagement_signal + has_link
    return round(min(1.0, max(0.0, quality)), 4)


def _parse_language(post: Dict[str, Any]) -> str | None:
    langs = post.get("langs")
    if isinstance(langs, list):
        for value in langs:
            normalized = str(value or "").strip()
            if normalized:
                return normalized
    language = str(post.get("language") or "").strip()
    return language or None


def _parse_followers_count(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def run_firehose_window(
    *,
    cursor_us: int | None,
    max_seconds: int | None = None,
    max_events: int | None = None,
    progress_callback: Any | None = None,
) -> Dict[str, Any]:
    existing_state: Dict[str, Any] = {}
    if isinstance(cursor_us, int) and cursor_us > 0:
        existing_state["cursor"] = cursor_us
    return sync_bluesky_firehose(
        existing_posts=[],
        existing_snapshots=[],
        existing_profiles=[],
        existing_interactions=[],
        existing_state=existing_state,
        max_seconds=max_seconds,
        max_events=max_events,
        progress_callback=progress_callback,
    )


def normalizeIncomingEvent(
    event: Dict[str, Any],
    *,
    ingested_at: datetime,
) -> Dict[str, Any] | None:
    post_id = str(event.get("id") or event.get("uri") or "").strip()
    if not post_id:
        return None

    text_content = str(event.get("summary") or event.get("title") or "").strip()
    indexed_at = _to_utc_datetime(event.get("indexedAt"))
    created_at = _to_utc_datetime(event.get("createdUtc"))
    if created_at is None:
        created_at = indexed_at
    if created_at is None:
        created_at = ingested_at

    max_future_allowed = ingested_at + timedelta(minutes=5)
    if created_at > max_future_allowed:
        created_at = indexed_at if indexed_at and indexed_at <= max_future_allowed else ingested_at

    urls = _extract_urls(text_content)
    hashtags = _extract_hashtags(text_content)

    metrics_json = {
        "score": int(event.get("score", 0) or 0),
        "likeCount": int(event.get("likeCount", 0) or 0),
        "replyCount": int(event.get("replyCount", 0) or 0),
        "repostCount": int(event.get("repostCount", 0) or 0),
        "quoteCount": int(event.get("quoteCount", 0) or 0),
        "bookmarkCount": int(event.get("bookmarkCount", 0) or 0),
        "interactionCounts": dict(event.get("interactionCounts") or {}),
        "priorityScore": float(event.get("priorityScore", 0.0) or 0.0),
    }

    source_post_id = str(event.get("uri") or post_id).strip() or post_id

    return {
        "platform": PLATFORM,
        "source_post_id": source_post_id,
        "source_uri": source_post_id,
        "source_cid": str(event.get("cid") or "").strip() or None,
        "author_did": str(event.get("authorDid") or "").strip() or None,
        "post_id": post_id,
        "author_id": str(event.get("authorDid") or event.get("authorHandle") or "").strip() or None,
        "author_handle": str(event.get("authorHandle") or "").strip() or None,
        "root_post_id": str(event.get("rootUri") or "").strip() or None,
        "reply_parent_id": str(event.get("parentUri") or "").strip() or None,
        "created_at": created_at,
        "ingested_at": ingested_at,
        "text_content": text_content or None,
        "language": _parse_language(event),
        "urls": urls,
        "hashtags": hashtags,
        "metrics_json": metrics_json,
        "raw_json": dict(event),
    }


def normalize_incoming_event(
    event: Dict[str, Any],
    *,
    ingested_at: datetime,
) -> Dict[str, Any] | None:
    return normalizeIncomingEvent(event, ingested_at=ingested_at)


def normalize_posts_for_raw_table(
    posts: Iterable[Dict[str, Any]],
    *,
    ingested_at: datetime,
) -> List[Dict[str, Any]]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    for post in posts:
        normalized = normalizeIncomingEvent(post, ingested_at=ingested_at)
        if not normalized:
            continue
        rows_by_id[str(normalized.get("post_id") or normalized.get("source_post_id"))] = normalized

    return list(rows_by_id.values())


def extractTopicEntities(raw_post: Dict[str, Any]) -> List[Dict[str, str]]:
    text_content = _normalize_whitespace(
        str(raw_post.get("text_content") or raw_post.get("raw_text") or "")
    )
    if not text_content:
        return []

    candidates: list[dict[str, str]] = []
    seen_normalized: set[str] = set()
    source_quality = float(raw_post.get("quality_score") or 0.0)

    def add_candidate(topic_text: str, topic_type: str) -> None:
        normalized_topic = _normalize_topic_value(topic_text, topic_type=topic_type)
        normalized_key = normalize_topic_key_text(normalized_topic)
        if not _is_valid_topic_candidate(normalized_topic, topic_type=topic_type):
            return
        _canonical_key, _canonical_label, alias_confidence = canonicalize_topic_key(normalized_key)
        confidence = score_topic_candidate(
            topic_key=normalized_topic,
            topic_type=topic_type,
            source_quality=source_quality,
            alias_confidence=alias_confidence,
        )
        minimum_confidence = {
            "cashtag": 0.34,
            "hashtag": 0.40,
            "entity": 0.48,
            "keyword": 0.62,
        }.get(topic_type, 0.50)
        if normalized_key in TOPIC_ENTITY_ALLOWLIST:
            minimum_confidence = min(minimum_confidence, 0.34)
        elif alias_confidence > 0:
            minimum_confidence = min(minimum_confidence, 0.38)
        if confidence < minimum_confidence:
            return
        if normalized_key in seen_normalized:
            return
        seen_normalized.add(normalized_key)
        candidates.append(
            {
                "topic_text": _normalize_topic_text(topic_text),
                "normalized_topic": normalized_topic,
                "topic_type": topic_type,
            }
        )

    # 1) Cashtags
    for match in CASHTAG_PATTERN.finditer(text_content):
        token = str(match.group(1) or "").strip()
        if not token:
            continue
        add_candidate(f"${token}", "cashtag")

    # 2) Hashtags
    for match in HASHTAG_PATTERN.finditer(text_content):
        token = str(match.group(1) or "").strip()
        if not token:
            continue
        add_candidate(f"#{token}", "hashtag")

    # 3 + 4) Proper nouns/acronyms and grouped adjacent name phrases
    phrase_matches = list(PHRASE_TOKEN_PATTERN.finditer(text_content))
    phrase_tokens = [match.group(0) for match in phrase_matches]
    phrase_token_frequency: dict[str, int] = {}
    for token in phrase_tokens:
        lowered = normalize_topic_key_text(token)
        if not lowered:
            continue
        phrase_token_frequency[lowered] = phrase_token_frequency.get(lowered, 0) + 1
    index = 0
    while index < len(phrase_tokens):
        token = phrase_tokens[index]
        token_lower = normalize_topic_key_text(token)
        if not _is_proper_like_token(token):
            index += 1
            continue
        if token_lower in TOPIC_NOISE_TOKENS or token_lower in TOPIC_URL_DEBRIS_TOKENS:
            index += 1
            continue
        if (
            _is_sentence_leading_token(text_content, token_start=phrase_matches[index].start())
            and token[:1].isupper()
            and token[1:].islower()
            and phrase_token_frequency.get(token_lower, 0) <= 1
            and token_lower not in TOPIC_ENTITY_ALLOWLIST
            and token_lower not in TOPIC_ACRONYM_EXCEPTIONS
        ):
            index += 1
            continue

        grouped_tokens = [token]
        cursor = index + 1
        while cursor < len(phrase_tokens) and len(grouped_tokens) < 5:
            next_token = phrase_tokens[cursor]
            next_lower = normalize_topic_key_text(next_token)
            between_text = text_content[
                phrase_matches[cursor - 1].end() : phrase_matches[cursor].start()
            ]
            if any(marker in between_text for marker in (".", "!", "?", ";", ":", "\n")):
                break
            if next_lower in TOPIC_NOISE_TOKENS or next_lower in TOPIC_URL_DEBRIS_TOKENS:
                break
            if (
                next_lower in TOPIC_CONNECTOR_WORDS
                and (cursor + 1) < len(phrase_tokens)
                and _is_proper_like_token(phrase_tokens[cursor + 1])
            ):
                grouped_tokens.append(next_token.lower())
                grouped_tokens.append(phrase_tokens[cursor + 1])
                cursor += 2
                continue
            break

        add_candidate(" ".join(grouped_tokens), "entity")
        index = max(index + 1, cursor)

    # 5) Keyword fallback if no better topic candidates were found.
    if not candidates:
        for token in _extract_tokens(text_content):
            if not _is_high_signal_keyword_token(token):
                continue
            add_candidate(token, "keyword")
            if len(candidates) >= 5:
                break

    return candidates[:15]


def extract_topic_entities(raw_post: Dict[str, Any]) -> List[Dict[str, str]]:
    return extractTopicEntities(raw_post)


def analyzeSentiment(*, clean_text: str, language: str | None) -> Dict[str, Any]:
    english_tokens = re.findall(r"[a-z']+", str(clean_text or "").lower())
    positive_score = 0
    negative_score = 0
    neutral_score = 0

    for token in english_tokens:
        positive_score += int(POSITIVE_SENTIMENT_LEXICON.get(token, 0))
        negative_score += int(NEGATIVE_SENTIMENT_LEXICON.get(token, 0))
        neutral_score += int(NEUTRAL_SENTIMENT_LEXICON.get(token, 0))

    sentiment_label = "neutral"
    if positive_score > negative_score:
        sentiment_label = "positive"
    elif negative_score > positive_score:
        sentiment_label = "negative"

    if positive_score == 0 and negative_score == 0:
        sentiment_label = "neutral"

    return {
        "sentiment_label": sentiment_label,
        "sentiment_positive_score": positive_score,
        "sentiment_negative_score": negative_score,
        "sentiment_neutral_score": neutral_score,
        "sentiment_language": language,
    }


def analyze_sentiment(*, clean_text: str, language: str | None) -> Dict[str, Any]:
    return analyzeSentiment(clean_text=clean_text, language=language)


def deriveTags(
    *,
    clean_text: str,
    tokens: List[str],
    hashtags: List[str],
    cashtags: List[str],
    domains: List[str],
) -> List[str]:
    tags: List[str] = []
    text_lc = clean_text.lower()
    token_set = set(tokens)

    def add_tag(tag: str) -> None:
        if tag and tag not in tags:
            tags.append(tag)

    has_crypto_signal = bool(token_set.intersection(CRYPTO_TERMS) or cashtags)
    has_memecoin_signal = bool(token_set.intersection(MEMECOIN_TERMS))
    if any(fragment in text_lc for fragment in ("meme coin", "meme-coin", "memecoin")):
        has_memecoin_signal = True
        has_crypto_signal = True

    has_ecommerce_signal = bool(token_set.intersection(ECOMMERCE_TERMS))
    if "e-commerce" in text_lc:
        has_ecommerce_signal = True

    has_dropshipping_signal = bool(token_set.intersection(DROPSHIPPING_TERMS))
    if "drop shipping" in text_lc:
        has_dropshipping_signal = True
        has_ecommerce_signal = True

    has_product_signal = bool(token_set.intersection(PRODUCT_TERMS))

    if has_crypto_signal:
        add_tag("crypto")
    if has_memecoin_signal:
        add_tag("memecoin")
    if has_ecommerce_signal:
        add_tag("ecommerce")
    if has_dropshipping_signal:
        add_tag("dropshipping")
    if has_product_signal:
        add_tag("product")
    if domains:
        add_tag("link")
    if hashtags:
        add_tag("hashtagged")
    if not tags:
        add_tag("general")

    return tags


def derive_tags(
    *,
    clean_text: str,
    tokens: List[str],
    hashtags: List[str],
    cashtags: List[str],
    domains: List[str],
) -> List[str]:
    return deriveTags(
        clean_text=clean_text,
        tokens=tokens,
        hashtags=hashtags,
        cashtags=cashtags,
        domains=domains,
    )


def extractFeatures(raw_post: Dict[str, Any]) -> Dict[str, Any]:
    text_content = _normalize_whitespace(
        str(raw_post.get("text_content") or raw_post.get("raw_text") or "")
    )
    language = str(raw_post.get("language") or "").strip().lower() or None

    urls = _extract_urls(text_content)
    if not urls:
        urls = _dedupe_text(raw_post.get("urls") or [])

    hashtags = _extract_hashtags(text_content)
    if not hashtags:
        hashtags = _dedupe_text([str(item).lower() for item in (raw_post.get("hashtags") or [])])

    mentions = _extract_mentions(text_content)
    cashtags = _extract_cashtags(text_content)
    tokens = _extract_tokens(text_content)
    domains = _extract_domains(urls)
    topic_entities = extractTopicEntities(
        {
            **raw_post,
            "text_content": text_content,
            "hashtags": hashtags,
        }
    )
    normalized_topics = [str(row.get("normalized_topic") or "").strip() for row in topic_entities]
    normalized_topics = [value for value in _dedupe_text(normalized_topics) if value]

    tags = deriveTags(
        clean_text=text_content,
        tokens=tokens,
        hashtags=hashtags,
        cashtags=cashtags,
        domains=domains,
    )
    if any(row.get("topic_type") == "cashtag" for row in topic_entities):
        tags = _dedupe_text([*tags, "ticker"])
    if any(row.get("topic_type") == "entity" for row in topic_entities):
        tags = _dedupe_text([*tags, "entity"])

    topic_key_candidate = (
        normalized_topics[0]
        if normalized_topics
        else _infer_topic_key_candidate(
            hashtags=hashtags,
            tokens=tokens,
            tags=tags,
        )
    )

    metrics_json = dict(raw_post.get("metrics_json") or {})
    quality_score = _estimate_quality_score(
        clean_text=text_content,
        tokens=tokens,
        urls=urls,
        metrics_json=metrics_json,
    )
    key_phrases = tokens[:3]
    topic_seeds = [topic_key_candidate] if topic_key_candidate else []
    sentiment = analyzeSentiment(clean_text=text_content, language=language)

    return {
        "clean_text": text_content or None,
        "normalized_text": text_content or None,
        "language": language,
        "token_count": len(tokens),
        "tokens": tokens,
        "hashtags": hashtags,
        "cashtags": cashtags,
        "mentions": mentions,
        "urls": urls,
        "domains": domains,
        "tags": tags,
        "topic_key_candidate": topic_key_candidate,
        "key_phrases": key_phrases,
        "topic_seeds": topic_seeds,
        "quality_score": quality_score,
        "spam_score": 0.0,
        "topic_entities": normalized_topics,
        "topic_records": topic_entities,
        **sentiment,
    }


def extract_features(raw_post: Dict[str, Any]) -> Dict[str, Any]:
    return extractFeatures(raw_post)


def processRawPost(
    raw_post: Dict[str, Any],
    *,
    processed_at: datetime | None = None,
) -> Dict[str, Any]:
    processed_time = processed_at or datetime.now(timezone.utc)
    source_created_at = _to_utc_datetime(raw_post.get("created_at")) or processed_time
    bucket_minute = source_created_at.replace(second=0, microsecond=0)
    features = extractFeatures(raw_post)
    raw_json = dict(raw_post.get("raw_json") or {})

    author_id = str(raw_post.get("author_id") or "").strip() or None
    author_hash = (
        hashlib.sha1(author_id.encode("utf-8")).hexdigest()[:16]
        if author_id
        else None
    )
    fingerprint_source = str(features.get("normalized_text") or "")
    fingerprint = (
        hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest()[:24]
        if fingerprint_source
        else None
    )

    topic_key_candidate = str(features.get("topic_key_candidate") or "general")
    topic_entities = [str(value or "").strip() for value in (features.get("topic_entities") or [])]
    topic_entities = [value for value in _dedupe_text(topic_entities) if value]

    return {
        "raw_post_id": raw_post.get("id"),
        "platform": str(raw_post.get("platform") or PLATFORM),
        "source_post_id": str(raw_post.get("source_post_id") or raw_post.get("post_id") or "").strip(),
        "post_id": str(raw_post.get("post_id") or "").strip() or None,
        "author_id": author_id,
        "source_created_at": source_created_at,
        "created_at": source_created_at,
        "processed_at": processed_time,
        "bucket_minute": bucket_minute,
        "clean_text": features.get("clean_text"),
        "normalized_text": features.get("normalized_text"),
        "language": features.get("language"),
        "has_media": bool(raw_json.get("media") or raw_json.get("embed") or raw_json.get("image")),
        "is_reply": bool(raw_post.get("reply_parent_id")),
        "is_repost": bool(raw_post.get("repost_of_uri")),
        "is_quote": bool(raw_json.get("quotedUri") or raw_json.get("quoted_uri")),
        "author_hash": author_hash,
        "token_count": int(features.get("token_count") or 0),
        "fingerprint": fingerprint,
        "tokens": list(features.get("tokens") or []),
        "hashtags": list(features.get("hashtags") or []),
        "cashtags": list(features.get("cashtags") or []),
        "mentions": list(features.get("mentions") or []),
        "domains": list(features.get("domains") or []),
        "urls": list(features.get("urls") or []),
        "key_phrases": list(features.get("key_phrases") or []),
        "topic_seeds": list(features.get("topic_seeds") or []),
        "topic_entities": topic_entities,
        "topic_key_candidate": topic_key_candidate,
        "tags": list(features.get("tags") or []),
        "spam_score": float(features.get("spam_score") or 0.0),
        "quality_score": float(features.get("quality_score") or 0.0),
        "sentiment_label": str(features.get("sentiment_label") or "neutral"),
        "sentiment_positive_score": int(features.get("sentiment_positive_score") or 0),
        "sentiment_negative_score": int(features.get("sentiment_negative_score") or 0),
        "sentiment_neutral_score": int(features.get("sentiment_neutral_score") or 0),
        "topic": topic_key_candidate,
        "topic_records": list(features.get("topic_records") or []),
    }


def process_raw_post(
    raw_post: Dict[str, Any],
    *,
    processed_at: datetime | None = None,
) -> Dict[str, Any]:
    return processRawPost(raw_post, processed_at=processed_at)


def _merge_author_rows(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    first_seen_existing = _to_utc_datetime(existing.get("first_seen_at"))
    first_seen_incoming = _to_utc_datetime(incoming.get("first_seen_at"))
    last_seen_existing = _to_utc_datetime(existing.get("last_seen_at"))
    last_seen_incoming = _to_utc_datetime(incoming.get("last_seen_at"))

    first_seen = first_seen_incoming or first_seen_existing
    if first_seen_existing and first_seen_incoming:
        first_seen = min(first_seen_existing, first_seen_incoming)

    last_seen = last_seen_incoming or last_seen_existing
    if last_seen_existing and last_seen_incoming:
        last_seen = max(last_seen_existing, last_seen_incoming)

    metadata = dict(existing.get("metadata_json") or {})
    metadata.update(dict(incoming.get("metadata_json") or {}))

    return {
        **existing,
        "author_handle": incoming.get("author_handle") or existing.get("author_handle"),
        "display_name": incoming.get("display_name") or existing.get("display_name"),
        "followers_count": (
            incoming.get("followers_count")
            if incoming.get("followers_count") is not None
            else existing.get("followers_count")
        ),
        "metadata_json": metadata,
        "first_seen_at": first_seen,
        "last_seen_at": last_seen,
    }


def normalize_authors_for_authors_table(
    *,
    profiles: Iterable[Dict[str, Any]],
    posts: Iterable[Dict[str, Any]],
    observed_at: datetime,
) -> List[Dict[str, Any]]:
    rows_by_id: Dict[str, Dict[str, Any]] = {}

    for profile in profiles:
        author_id = str(profile.get("did") or profile.get("handle") or "").strip()
        if not author_id:
            continue
        first_seen = _to_utc_datetime(profile.get("firstObservedAt")) or _to_utc_datetime(
            profile.get("fetchedAt")
        )
        last_seen = _to_utc_datetime(profile.get("lastObservedAt")) or _to_utc_datetime(
            profile.get("fetchedAt")
        )
        candidate = {
            "platform": PLATFORM,
            "author_id": author_id,
            "author_handle": str(profile.get("handle") or "").strip() or None,
            "display_name": str(profile.get("displayName") or "").strip() or None,
            "followers_count": _parse_followers_count(profile.get("followersCount")),
            "metadata_json": dict(profile),
            "first_seen_at": first_seen or observed_at,
            "last_seen_at": last_seen or observed_at,
        }
        existing = rows_by_id.get(author_id)
        rows_by_id[author_id] = _merge_author_rows(existing, candidate) if existing else candidate

    for post in posts:
        author_id = str(post.get("authorDid") or post.get("authorHandle") or "").strip()
        if not author_id:
            continue
        first_seen = _to_utc_datetime(post.get("firstSeenAt")) or _to_utc_datetime(
            post.get("firstObservedAt")
        )
        last_seen = _to_utc_datetime(post.get("lastObservedAt")) or _to_utc_datetime(
            post.get("fetchedAt")
        )
        candidate = {
            "platform": PLATFORM,
            "author_id": author_id,
            "author_handle": str(post.get("authorHandle") or "").strip() or None,
            "display_name": str(post.get("authorDisplayName") or "").strip() or None,
            "followers_count": _parse_followers_count(post.get("authorFollowersCount")),
            "metadata_json": {
                "did": post.get("authorDid"),
                "handle": post.get("authorHandle"),
                "displayName": post.get("authorDisplayName"),
                "avatar": post.get("authorAvatar"),
                "description": post.get("authorDescription"),
                "lastObservedPostUri": post.get("id"),
            },
            "first_seen_at": first_seen or observed_at,
            "last_seen_at": last_seen or observed_at,
        }
        existing = rows_by_id.get(author_id)
        rows_by_id[author_id] = _merge_author_rows(existing, candidate) if existing else candidate

    return list(rows_by_id.values())

