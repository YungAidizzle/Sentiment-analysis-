from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

APOSTROPHE_VARIANTS_PATTERN = re.compile(r"[`\u2019\u2018\u00b4\u02bc\u02b9]")
INTRA_WORD_APOSTROPHE_PATTERN = re.compile(r"(?<=\w)'(?=\w)")
NON_TOPIC_CHARS_PATTERN = re.compile(r"[^a-z0-9$#\s-]+")
WHITESPACE_PATTERN = re.compile(r"\s+")
TOKEN_PATTERN = re.compile(r"[a-z0-9$#]+")

TOPIC_URL_DEBRIS_TOKENS = {
    "bit",
    "com",
    "http",
    "https",
    "ly",
    "t",
    "tinyurl",
    "url",
    "www",
    "amp",
    "co",
}

TOPIC_NOISE_TOKENS = {
    "additional",
    "advisory",
    "afd",
    "airnow",
    "aqi",
    "details",
    "digit",
    "discussion",
    "forecast",
    "iembot",
    "issued",
    "prelim",
    "statement",
    "update",
}

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
}

TOPIC_GENERIC_WEAK_TOKENS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "am",
    "an",
    "and",
    "another",
    "any",
    "anyone",
    "anything",
    "are",
    "as",
    "at",
    "back",
    "beautiful",
    "because",
    "been",
    "before",
    "being",
    "best",
    "better",
    "both",
    "but",
    "by",
    "can",
    "cant",
    "cest",
    "come",
    "could",
    "couldnt",
    "day",
    "de",
    "did",
    "didnt",
    "do",
    "does",
    "doesnt",
    "dont",
    "else",
    "ever",
    "every",
    "everyone",
    "first",
    "for",
    "from",
    "get",
    "give",
    "go",
    "good",
    "got",
    "great",
    "had",
    "has",
    "have",
    "havent",
    "here",
    "how",
    "i",
    "ich",
    "if",
    "ill",
    "im",
    "in",
    "into",
    "is",
    "isnt",
    "it",
    "its",
    "ive",
    "just",
    "keep",
    "kind",
    "last",
    "let",
    "like",
    "link",
    "little",
    "look",
    "make",
    "many",
    "maybe",
    "me",
    "mind",
    "more",
    "morning",
    "most",
    "much",
    "must",
    "my",
    "need",
    "new",
    "next",
    "no",
    "not",
    "now",
    "of",
    "oh",
    "ok",
    "okay",
    "on",
    "once",
    "one",
    "only",
    "or",
    "our",
    "out",
    "over",
    "part",
    "people",
    "please",
    "point",
    "por",
    "post",
    "pretty",
    "que",
    "read",
    "right",
    "same",
    "say",
    "see",
    "so",
    "some",
    "someone",
    "something",
    "still",
    "such",
    "take",
    "talk",
    "that",
    "thats",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "time",
    "to",
    "today",
    "tomorrow",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "true",
    "und",
    "us",
    "very",
    "wait",
    "was",
    "wasnt",
    "watch",
    "we",
    "well",
    "went",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "why",
    "will",
    "with",
    "would",
    "wouldnt",
    "yeah",
    "yes",
    "you",
    "your",
}

TOPIC_ACRONYM_ALLOWLIST = {
    "api",
    "btc",
    "djt",
    "eth",
    "eu",
    "fbi",
    "gop",
    "nba",
    "nfl",
    "nft",
    "nsa",
    "nyse",
    "sec",
    "spy",
    "uk",
    "un",
    "usa",
    "xrp",
}

TOPIC_ENTITY_ALLOWLIST = {
    "america",
    "bluesky",
    "bsky",
    "china",
    "djt",
    "ethereum",
    "google",
    "ice",
    "iran",
    "israel",
    "kalshi",
    "no kings",
    "nyse",
    "openai",
    "palestine",
    "polymarket",
    "republican",
    "republicans",
    "russia",
    "trump",
    "ukraine",
    "usa",
}


@dataclass(frozen=True)
class CanonicalTopicRule:
    canonical_key: str
    canonical_label: str
    aliases: tuple[str, ...]
    entity_type: str = "entity"
    confidence: float = 0.95


CANONICAL_TOPIC_RULES: tuple[CanonicalTopicRule, ...] = (
    CanonicalTopicRule(
        canonical_key="no kings",
        canonical_label="No Kings",
        aliases=(
            "nokings",
            "no king",
            "no kings",
            "no kings's",
            "no kingss",
            "no-kings",
            "#nokings",
            "#no-kings",
            "#no_kings",
        ),
        entity_type="movement",
        confidence=0.99,
    ),
    CanonicalTopicRule(
        canonical_key="usa",
        canonical_label="USA",
        aliases=(
            "usa",
            "america",
            "american",
            "americans",
            "united states",
            "united-states",
            "unitedstates",
            "u s a",
            "u.s.a",
        ),
        entity_type="country",
        confidence=0.92,
    ),
    CanonicalTopicRule(
        canonical_key="iran",
        canonical_label="Iran",
        aliases=(
            "iran",
            "iranian",
            "iranians",
        ),
        entity_type="country",
        confidence=0.92,
    ),
    CanonicalTopicRule(
        canonical_key="bluesky",
        canonical_label="Bluesky",
        aliases=(
            "bluesky",
            "bsky",
            "#bluesky",
            "#bsky",
        ),
        entity_type="platform",
        confidence=0.96,
    ),
    CanonicalTopicRule(
        canonical_key="nyse",
        canonical_label="NYSE",
        aliases=(
            "nyse",
            "new york stock exchange",
            "new-york-stock-exchange",
        ),
        entity_type="market",
        confidence=0.98,
    ),
    CanonicalTopicRule(
        canonical_key="polymarket",
        canonical_label="Polymarket",
        aliases=(
            "polymarket",
            "poly market",
            "poly-market",
        ),
        entity_type="platform",
        confidence=0.97,
    ),
    CanonicalTopicRule(
        canonical_key="kalshi",
        canonical_label="Kalshi",
        aliases=(
            "kalshi",
            "kal shi",
            "kal-shi",
        ),
        entity_type="platform",
        confidence=0.97,
    ),
)


def normalize_topic_key_text(value: str) -> str:
    candidate = str(value or "").lower()
    candidate = candidate.replace("ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢", "'")
    candidate = candidate.replace("Ã¢â‚¬â„¢", "'")
    candidate = candidate.replace("â€™", "'")
    candidate = APOSTROPHE_VARIANTS_PATTERN.sub("'", candidate)
    candidate = re.sub(r"(?<=\w)[?\uFFFD](?=\w)", "", candidate)
    candidate = INTRA_WORD_APOSTROPHE_PATTERN.sub("", candidate)
    candidate = NON_TOPIC_CHARS_PATTERN.sub(" ", candidate)
    candidate = WHITESPACE_PATTERN.sub(" ", candidate).strip(" -")
    return candidate


def topic_tokens(value: str) -> list[str]:
    normalized = normalize_topic_key_text(value)
    if not normalized:
        return []
    return [token for token in TOKEN_PATTERN.findall(normalized) if token]


def build_topic_alias_lookup(
    rules: Iterable[CanonicalTopicRule] = CANONICAL_TOPIC_RULES,
) -> dict[str, CanonicalTopicRule]:
    lookup: dict[str, CanonicalTopicRule] = {}
    for rule in rules:
        canonical_key = normalize_topic_key_text(rule.canonical_key)
        if not canonical_key:
            continue
        lookup[canonical_key] = rule
        for alias in rule.aliases:
            alias_key = normalize_topic_key_text(alias)
            if alias_key:
                lookup[alias_key] = rule
    return lookup


def canonicalize_topic_key(
    value: str,
    *,
    rules_lookup: dict[str, CanonicalTopicRule] | None = None,
) -> tuple[str, str, float]:
    lookup = rules_lookup or build_topic_alias_lookup()
    normalized = normalize_topic_key_text(value)
    if not normalized:
        return ("", "", 0.0)
    matched = lookup.get(normalized)
    if matched is None:
        return (normalized, normalized, 0.0)
    return (
        normalize_topic_key_text(matched.canonical_key),
        matched.canonical_label,
        float(matched.confidence),
    )


def canonical_label_for_topic_key(
    topic_key: str,
    *,
    rules_lookup: dict[str, CanonicalTopicRule] | None = None,
) -> str:
    canonical_key, canonical_label, _ = canonicalize_topic_key(topic_key, rules_lookup=rules_lookup)
    if not canonical_key:
        return ""
    if canonical_label:
        return canonical_label
    return canonical_key


def score_topic_candidate(
    *,
    topic_key: str,
    topic_type: str,
    source_quality: float = 0.0,
    alias_confidence: float = 0.0,
) -> float:
    tokens = topic_tokens(topic_key)
    if not tokens:
        return 0.0
    if len(tokens) > 5:
        return 0.0
    if all(token in TOPIC_NUMBER_WORD_TOKENS for token in tokens):
        return 0.0

    token_count = len(tokens)
    weak_count = sum(1 for token in tokens if token in TOPIC_GENERIC_WEAK_TOKENS)
    noise_count = sum(1 for token in tokens if token in TOPIC_NOISE_TOKENS)
    url_count = sum(1 for token in tokens if token in TOPIC_URL_DEBRIS_TOKENS)
    informative_count = sum(
        1
        for token in tokens
        if token not in TOPIC_GENERIC_WEAK_TOKENS
        and token not in TOPIC_NOISE_TOKENS
        and token not in TOPIC_NUMBER_WORD_TOKENS
        and token not in TOPIC_URL_DEBRIS_TOKENS
        and (len(token) >= 4 or token in TOPIC_ACRONYM_ALLOWLIST)
    )
    has_entity_signal = any(
        token in TOPIC_ENTITY_ALLOWLIST or token in TOPIC_ACRONYM_ALLOWLIST
        for token in tokens
    )

    score = {
        "cashtag": 0.74,
        "hashtag": 0.67,
        "entity": 0.56,
        "keyword": 0.40,
    }.get(str(topic_type or "").lower(), 0.40)
    score += min(max(float(source_quality or 0.0), 0.0), 1.0) * 0.20
    score += min(max(float(alias_confidence or 0.0), 0.0), 1.0) * 0.18
    score += 0.12 if token_count >= 2 else 0.0
    score += 0.14 if has_entity_signal else 0.0
    score += 0.08 if informative_count >= 2 else 0.0
    score -= (weak_count / max(1, token_count)) * 0.48
    score -= (noise_count / max(1, token_count)) * 0.70
    score -= (url_count / max(1, token_count)) * 1.00

    if token_count == 1:
        token = tokens[0]
        if token in TOPIC_GENERIC_WEAK_TOKENS:
            score -= 0.40
        if token in TOPIC_URL_DEBRIS_TOKENS:
            score -= 0.60
        if len(token) < 4 and token not in TOPIC_ACRONYM_ALLOWLIST:
            score -= 0.32

    return max(0.0, min(1.0, score))


def seed_topic_alias_rows() -> list[tuple[str, str, str, str, float]]:
    rows: list[tuple[str, str, str, str, float]] = []
    for rule in CANONICAL_TOPIC_RULES:
        canonical_key = normalize_topic_key_text(rule.canonical_key)
        if not canonical_key:
            continue
        aliases = {canonical_key}
        aliases.update(normalize_topic_key_text(alias) for alias in rule.aliases)
        for alias_key in sorted(alias for alias in aliases if alias):
            rows.append(
                (
                    alias_key,
                    canonical_key,
                    rule.canonical_label,
                    rule.entity_type,
                    float(rule.confidence),
                )
            )
    return rows
