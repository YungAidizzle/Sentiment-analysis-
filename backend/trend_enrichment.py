from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib import error as urllib_error
from urllib import request as urllib_request

from backend.logging_setup import log_event

OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

OPENAI_SYSTEM_PROMPT = (
    "You are a conservative trend naming analyst for social discussion clusters. "
    "Use only the supplied evidence. Do not invent events, claims, people, timelines, "
    "or causal explanations not directly supported by the sample posts and stats. "
    "If evidence is mixed or incoherent, return a broad generic label and lower confidence."
)

OPENAI_JSON_SCHEMA = {
    "name": "trend_enrichment",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "canonical_name": {
                "type": "string",
                "description": "Broad, user-facing trend name grounded in the sample evidence.",
            },
            "short_description": {
                "type": "string",
                "description": "One concise sentence summarizing what the trend is about.",
            },
            "context_paragraph": {
                "type": "string",
                "description": "A short paragraph explaining what people are discussing and why.",
            },
            "key_entities": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional entities/topics central to the discussion.",
            },
            "trend_category": {
                "type": ["string", "null"],
                "description": "Optional category label such as policy, markets, culture, sports, or mixed discussion cluster.",
            },
            "summary_confidence": {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
                "description": "Confidence in coherence and naming quality from 0.0 to 1.0.",
            },
        },
        "required": [
            "canonical_name",
            "short_description",
            "context_paragraph",
            "key_entities",
            "trend_category",
            "summary_confidence",
        ],
    },
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
    "have",
    "i",
    "if",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "there",
    "this",
    "to",
    "was",
    "we",
    "with",
    "you",
}

TOKEN_PATTERN = re.compile(r"[a-z0-9$#][a-z0-9$#'_-]{1,63}")
URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class TrendEnrichmentRuntimeConfig:
    enabled: bool
    openai_api_key: str
    model_name: str
    prompt_version: str
    interval_seconds: float
    max_topics: int
    representative_posts: int
    candidate_post_limit: int
    stale_after_hours: float
    request_timeout_seconds: float
    max_retries: int
    retry_backoff_seconds: float
    max_post_chars: int
    enforce_min_text_chars: int
    enforce_min_word_count: int


def _parse_bool_env(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    value = str(os.getenv(name, "")).strip()
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def _parse_float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    value = str(os.getenv(name, "")).strip()
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def build_trend_enrichment_runtime_config_from_env() -> TrendEnrichmentRuntimeConfig:
    return TrendEnrichmentRuntimeConfig(
        enabled=_parse_bool_env("BLUESKY_TREND_ENRICHMENT_ENABLED", True),
        openai_api_key=str(os.getenv("OPENAI_API_KEY", "")).strip(),
        model_name=str(os.getenv("BLUESKY_TREND_ENRICHMENT_MODEL", "gpt-5.4-mini")).strip() or "gpt-5.4-mini",
        prompt_version=str(os.getenv("BLUESKY_TREND_ENRICHMENT_PROMPT_VERSION", "v1")).strip() or "v1",
        interval_seconds=_parse_float_env("BLUESKY_TREND_ENRICHMENT_INTERVAL_SECONDS", 300.0, 5.0, 3600.0),
        max_topics=_parse_int_env("BLUESKY_TREND_ENRICHMENT_MAX_TOPICS", 250, 1, 500),
        representative_posts=_parse_int_env("BLUESKY_TREND_ENRICHMENT_POSTS_PER_TOPIC", 10, 3, 20),
        candidate_post_limit=_parse_int_env("BLUESKY_TREND_ENRICHMENT_CANDIDATE_POST_LIMIT", 120, 20, 400),
        stale_after_hours=_parse_float_env("BLUESKY_TREND_ENRICHMENT_STALE_HOURS", 6.0, 0.5, 336.0),
        request_timeout_seconds=_parse_float_env("BLUESKY_TREND_ENRICHMENT_TIMEOUT_SECONDS", 30.0, 5.0, 120.0),
        max_retries=_parse_int_env("BLUESKY_TREND_ENRICHMENT_MAX_RETRIES", 3, 1, 6),
        retry_backoff_seconds=_parse_float_env("BLUESKY_TREND_ENRICHMENT_RETRY_BACKOFF_SECONDS", 1.2, 0.1, 10.0),
        max_post_chars=_parse_int_env("BLUESKY_TREND_ENRICHMENT_MAX_POST_CHARS", 260, 120, 600),
        enforce_min_text_chars=_parse_int_env("BLUESKY_TREND_ENRICHMENT_MIN_TEXT_CHARS", 28, 0, 200),
        enforce_min_word_count=_parse_int_env("BLUESKY_TREND_ENRICHMENT_MIN_WORDS", 5, 0, 40),
    )


def _to_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    candidate = str(value or "").strip()
    if not candidate:
        return None
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = URL_PATTERN.sub(" ", text)
    text = WHITESPACE_PATTERN.sub(" ", text)
    return text.strip()


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1].rstrip() + "..."


def _tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for match in TOKEN_PATTERN.findall(text.lower()):
        token = str(match or "").strip("._-")
        if not token:
            continue
        if token in STOPWORD_TOKENS:
            continue
        if token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _engagement_score(row: dict[str, Any]) -> float:
    return (
        _safe_int(row.get("like_count")) * 1.0
        + _safe_int(row.get("repost_count")) * 3.0
        + _safe_int(row.get("reply_count")) * 2.5
        + _safe_int(row.get("quote_count")) * 3.5
        + max(0.0, _safe_float(row.get("quality_score")) * 12.0)
    )


def _row_id(row: dict[str, Any]) -> str:
    source_post_id = str(row.get("source_post_id") or "").strip()
    if source_post_id:
        return source_post_id
    raw_post_id = row.get("raw_post_id")
    if raw_post_id is not None:
        return f"raw:{raw_post_id}"
    processed_post_id = row.get("processed_post_id")
    if processed_post_id is not None:
        return f"processed:{processed_post_id}"
    return hashlib.sha1(str(row).encode("utf-8")).hexdigest()[:16]

def _prepare_candidates(
    rows: Iterable[dict[str, Any]],
    *,
    max_post_chars: int,
    min_text_chars: int,
    min_word_count: int,
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for row in rows:
        text = _normalize_text(row.get("text_content"))
        if not text:
            continue
        words = text.split()
        token_list = _tokenize(text)
        prepared.append(
            {
                **row,
                "candidate_id": _row_id(row),
                "normalized_text": text,
                "truncated_text": _truncate_text(text, max_post_chars),
                "token_list": token_list,
                "token_set": set(token_list),
                "word_count": len(words),
                "char_count": len(text),
                "engagement_score": _engagement_score(row),
                "event_dt": _to_utc_datetime(row.get("event_timestamp")),
                "is_short": len(text) < min_text_chars or len(words) < min_word_count,
            }
        )
    return prepared


def _dedupe_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []

    ranked = sorted(
        rows,
        key=lambda row: (
            row["engagement_score"],
            _safe_float(row.get("quality_score")),
            (row.get("event_dt") or datetime.fromtimestamp(0, tz=timezone.utc)).timestamp(),
        ),
        reverse=True,
    )

    exact_seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in ranked:
        fingerprint = str(row.get("fingerprint") or "").strip()
        dedupe_key = fingerprint or row.get("normalized_text") or ""
        if not dedupe_key:
            continue
        if dedupe_key in exact_seen:
            continue
        exact_seen.add(dedupe_key)
        deduped.append(row)

    selected: list[dict[str, Any]] = []
    for row in deduped:
        token_set = row.get("token_set") or set()
        near_duplicate = any(
            _jaccard_similarity(token_set, existing.get("token_set") or set()) >= 0.92
            for existing in selected
        )
        if near_duplicate:
            continue
        selected.append(row)

    return selected


def _score_candidate_centrality(rows: list[dict[str, Any]]) -> None:
    token_frequency: dict[str, int] = {}
    for row in rows:
        for token in row.get("token_set") or set():
            token_frequency[token] = token_frequency.get(token, 0) + 1

    for row in rows:
        token_set = row.get("token_set") or set()
        if not token_set:
            row["centrality_score"] = 0.0
            continue
        total = sum(token_frequency.get(token, 0) for token in token_set)
        row["centrality_score"] = total / max(1, len(token_set))


def _coherence_score(rows: list[dict[str, Any]]) -> float:
    if len(rows) <= 1:
        return 0.5
    pairs = 0
    similarity_sum = 0.0
    for left_index in range(len(rows)):
        left_tokens = rows[left_index].get("token_set") or set()
        for right_index in range(left_index + 1, len(rows)):
            right_tokens = rows[right_index].get("token_set") or set()
            similarity_sum += _jaccard_similarity(left_tokens, right_tokens)
            pairs += 1
            if pairs >= 45:
                break
        if pairs >= 45:
            break
    if pairs <= 0:
        return 0.0
    return max(0.0, min(1.0, round(similarity_sum / pairs, 4)))


def select_representative_posts(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int,
    max_post_chars: int,
    min_text_chars: int,
    min_word_count: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prepared = _prepare_candidates(
        rows,
        max_post_chars=max_post_chars,
        min_text_chars=min_text_chars,
        min_word_count=min_word_count,
    )
    deduped = _dedupe_candidates(prepared)
    if not deduped:
        return [], {
            "candidate_count": 0,
            "deduped_count": 0,
            "coherence_score": 0.0,
            "short_post_ratio": 1.0,
        }

    _score_candidate_centrality(deduped)

    strict_rows = [row for row in deduped if not row.get("is_short")]
    pool = strict_rows if strict_rows else deduped

    latest_ts = max(
        (row.get("event_dt") or datetime.fromtimestamp(0, tz=timezone.utc)).timestamp()
        for row in pool
    )
    earliest_ts = min(
        (row.get("event_dt") or datetime.fromtimestamp(0, tz=timezone.utc)).timestamp()
        for row in pool
    )
    span = max(1.0, latest_ts - earliest_ts)
    for row in pool:
        event_dt = row.get("event_dt") or datetime.fromtimestamp(0, tz=timezone.utc)
        recency_norm = ((event_dt.timestamp() - earliest_ts) / span) if span > 0 else 1.0
        row["recency_score"] = recency_norm
        row["hybrid_score"] = (
            row.get("engagement_score", 0.0) * 0.45
            + row.get("centrality_score", 0.0) * 8.0
            + recency_norm * 8.0
            + _safe_float(row.get("quality_score")) * 5.0
        )

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    def extend_candidates(candidates: list[dict[str, Any]], max_count: int) -> None:
        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            if not candidate_id or candidate_id in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(candidate_id)
            if len(selected) >= max_count:
                return

    engagement_quota = min(limit, 4)
    recency_quota = min(limit, 3)
    centrality_quota = min(limit, 3)

    extend_candidates(
        sorted(pool, key=lambda row: row.get("engagement_score", 0.0), reverse=True),
        engagement_quota,
    )
    extend_candidates(
        sorted(pool, key=lambda row: row.get("event_dt") or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True),
        max(len(selected), engagement_quota) + recency_quota,
    )
    extend_candidates(
        sorted(pool, key=lambda row: row.get("centrality_score", 0.0), reverse=True),
        max(len(selected), engagement_quota + recency_quota) + centrality_quota,
    )
    extend_candidates(
        sorted(pool, key=lambda row: row.get("hybrid_score", 0.0), reverse=True),
        limit,
    )

    final_rows = selected[:limit]
    coherence = _coherence_score(final_rows)
    short_ratio = (
        sum(1 for row in final_rows if row.get("is_short")) / max(1, len(final_rows))
    )

    return final_rows, {
        "candidate_count": len(prepared),
        "deduped_count": len(deduped),
        "pool_count": len(pool),
        "strict_pool_count": len(strict_rows),
        "coherence_score": coherence,
        "short_post_ratio": round(short_ratio, 4),
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def build_enrichment_input_hash(
    *,
    topic_row: dict[str, Any],
    representative_posts: list[dict[str, Any]],
) -> str:
    # Hash only evidence/context so unchanged trend content does not trigger
    # unnecessary re-enrichment when rolling windows advance.
    payload = {
        "topic_key": str(topic_row.get("topic_key") or "").strip(),
        "raw_label": str(topic_row.get("topic_label") or topic_row.get("topic_key") or "").strip(),
        "stats": {
            "total_mentions": _safe_int(topic_row.get("total_mentions")),
            "unique_posts": _safe_int(topic_row.get("unique_posts")),
            "unique_authors": _safe_int(topic_row.get("unique_authors")),
            "platform_count": _safe_int(topic_row.get("platform_count")),
            "positive_count": _safe_int(topic_row.get("positive_count")),
            "neutral_count": _safe_int(topic_row.get("neutral_count")),
            "negative_count": _safe_int(topic_row.get("negative_count")),
        },
        "posts": [
            {
                "id": str(post.get("candidate_id") or ""),
                "source_post_id": str(post.get("source_post_id") or ""),
                "text": str(post.get("truncated_text") or ""),
                "engagement_score": round(_safe_float(post.get("engagement_score")), 3),
                "event_timestamp": str(post.get("event_dt") or ""),
                "quality_score": round(_safe_float(post.get("quality_score")), 4),
            }
            for post in representative_posts
        ],
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _summarize_prompt_input(
    *,
    topic_row: dict[str, Any],
    representative_posts: list[dict[str, Any]],
    coherence_score: float,
) -> dict[str, Any]:
    total_mentions = _safe_int(topic_row.get("total_mentions"))
    unique_posts = _safe_int(topic_row.get("unique_posts"))
    unique_authors = _safe_int(topic_row.get("unique_authors"))
    positive = _safe_int(topic_row.get("positive_count"))
    neutral = _safe_int(topic_row.get("neutral_count"))
    negative = _safe_int(topic_row.get("negative_count"))
    sentiment_total = max(1, positive + neutral + negative)
    sentiment_balance = round((positive - negative) / sentiment_total, 4)

    posts_payload = []
    for post in representative_posts:
        event_dt = post.get("event_dt")
        posts_payload.append(
            {
                "id": str(post.get("candidate_id") or ""),
                "source_post_id": str(post.get("source_post_id") or ""),
                "platform": str(post.get("platform") or "bluesky"),
                "event_timestamp": event_dt.isoformat() if isinstance(event_dt, datetime) else None,
                "engagement": {
                    "score": round(_safe_float(post.get("engagement_score")), 3),
                    "likes": _safe_int(post.get("like_count")),
                    "reposts": _safe_int(post.get("repost_count")),
                    "replies": _safe_int(post.get("reply_count")),
                    "quotes": _safe_int(post.get("quote_count")),
                },
                "quality_score": round(_safe_float(post.get("quality_score")), 4),
                "text": str(post.get("truncated_text") or ""),
            }
        )

    return {
        "trend": {
            "topic_key": str(topic_row.get("topic_key") or "").strip(),
            "raw_label": str(topic_row.get("topic_label") or topic_row.get("topic_key") or "").strip(),
            "window_end": str(topic_row.get("window_end") or ""),
            "stats": {
                "total_mentions": total_mentions,
                "unique_posts": unique_posts,
                "unique_authors": unique_authors,
                "platform_count": _safe_int(topic_row.get("platform_count")),
                "positive_count": positive,
                "neutral_count": neutral,
                "negative_count": negative,
                "sentiment_balance": sentiment_balance,
            },
            "coherence_hint": round(coherence_score, 4),
        },
        "posts": posts_payload,
        "instructions": {
            "naming": "Use a broad, user-facing trend name grounded in evidence.",
            "style": "Concise, neutral, professional. Avoid hype language.",
            "guardrail": (
                "If posts are mixed, return a cautious broad label and lower confidence."
                " Prefer 'mixed discussion cluster' over fabricated specificity."
            ),
        },
    }

def _extract_json_object(text: str) -> dict[str, Any]:
    candidate = str(text or "").strip()
    if not candidate:
        raise ValueError("OpenAI response was empty")
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        candidate = candidate.replace("json\n", "", 1).strip()
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(candidate[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI payload was not a JSON object")
    return parsed


def _validate_openai_enrichment_payload(payload: dict[str, Any]) -> dict[str, Any]:
    canonical_name = str(payload.get("canonical_name") or "").strip()
    short_description = str(payload.get("short_description") or "").strip()
    context_paragraph = str(payload.get("context_paragraph") or "").strip()
    trend_category_value = payload.get("trend_category")
    trend_category = str(trend_category_value or "").strip() or None

    entities_value = payload.get("key_entities")
    entities_raw = entities_value if isinstance(entities_value, list) else []
    key_entities: list[str] = []
    seen_entities: set[str] = set()
    for value in entities_raw:
        entity = str(value or "").strip()
        if not entity:
            continue
        entity_key = entity.lower()
        if entity_key in seen_entities:
            continue
        seen_entities.add(entity_key)
        key_entities.append(entity[:80])
        if len(key_entities) >= 12:
            break

    confidence = _safe_float(payload.get("summary_confidence"))
    confidence = max(0.0, min(1.0, confidence))

    if not canonical_name:
        raise ValueError("canonical_name missing from OpenAI payload")
    if not short_description:
        raise ValueError("short_description missing from OpenAI payload")
    if not context_paragraph:
        raise ValueError("context_paragraph missing from OpenAI payload")

    return {
        "canonical_name": canonical_name[:120],
        "short_description": short_description[:280],
        "context_paragraph": context_paragraph[:900],
        "key_entities": key_entities,
        "trend_category": trend_category[:80] if trend_category else None,
        "summary_confidence": confidence,
    }


def _extract_chat_completion_content(response_payload: dict[str, Any]) -> str:
    choices = response_payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenAI response missing choices")
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first, dict) else {}
    if not isinstance(message, dict):
        raise ValueError("OpenAI response missing message")
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                text_value = str(item.get("text") or "").strip()
                if text_value:
                    text_chunks.append(text_value)
        if text_chunks:
            return "\n".join(text_chunks)
    raise ValueError("OpenAI message content was empty")


def _post_openai_chat_completions(
    *,
    api_key: str,
    timeout_seconds: float,
    request_body: dict[str, Any],
) -> dict[str, Any]:
    payload_bytes = _json_dumps(request_body).encode("utf-8")
    request = urllib_request.Request(
        OPENAI_CHAT_COMPLETIONS_URL,
        data=payload_bytes,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI response was not a JSON object")
    return parsed


def _call_openai_for_enrichment(
    *,
    api_key: str,
    model_name: str,
    prompt_version: str,
    prompt_input: dict[str, Any],
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> tuple[dict[str, Any], dict[str, int]]:
    messages = [
        {"role": "system", "content": OPENAI_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                "Generate enrichment for this detected trend.\n"
                "Rules:\n"
                "- Use only provided evidence.\n"
                "- Prefer broad product-grade naming, not literal token cleanup.\n"
                "- Avoid hype and certainty inflation.\n"
                "- If evidence is mixed, use cautious language and lower confidence.\n"
                f"- Prompt version: {prompt_version}\n\n"
                f"Input JSON:\n{_json_dumps(prompt_input)}"
            ),
        },
    ]
    request_body = {
        "model": model_name,
        "temperature": 0.2,
        "messages": messages,
        "response_format": {
            "type": "json_schema",
            "json_schema": OPENAI_JSON_SCHEMA,
        },
    }

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            payload = _post_openai_chat_completions(
                api_key=api_key,
                timeout_seconds=timeout_seconds,
                request_body=request_body,
            )
            content = _extract_chat_completion_content(payload)
            parsed = _extract_json_object(content)
            normalized = _validate_openai_enrichment_payload(parsed)
            usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
            usage_summary = {
                "prompt_tokens": _safe_int(usage.get("prompt_tokens")),
                "completion_tokens": _safe_int(usage.get("completion_tokens")),
                "total_tokens": _safe_int(usage.get("total_tokens")),
            }
            return normalized, usage_summary
        except urllib_error.HTTPError as error:
            status = int(getattr(error, "code", 0) or 0)
            should_retry = status in {408, 409, 429, 500, 502, 503, 504}
            details = error.read().decode("utf-8", errors="replace")
            last_error = RuntimeError(f"openai_http_error status={status} body={details[:400]}")
            if not should_retry or attempt >= max_retries:
                break
        except (urllib_error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as error:
            last_error = error
            if attempt >= max_retries:
                break

        sleep_seconds = retry_backoff_seconds * (2 ** max(0, attempt - 1))
        time.sleep(max(0.0, sleep_seconds))

    if last_error is None:
        raise RuntimeError("OpenAI enrichment failed with unknown error")
    raise RuntimeError(f"OpenAI enrichment failed after retries: {last_error}")


def _build_fallback_enrichment(
    *,
    raw_label: str,
    representative_posts: list[dict[str, Any]],
    coherence_score: float,
) -> dict[str, Any]:
    category = "mixed discussion cluster" if coherence_score < 0.1 else "general discussion"
    sample_phrase = (
        "Signals are still mixed across sampled posts."
        if coherence_score < 0.1
        else "Signals are directionally related but still broad."
    )
    return {
        "canonical_name": raw_label,
        "short_description": f"{raw_label} discussion trend detected from recent social posts.",
        "context_paragraph": (
            f"{sample_phrase} This label is a safe fallback because AI enrichment was unavailable for this cycle."
        ),
        "key_entities": [],
        "trend_category": category,
        "summary_confidence": 0.18 if representative_posts else 0.05,
    }


def _should_refresh_enrichment(
    *,
    existing_state: dict[str, Any] | None,
    input_hash: str,
    model_name: str,
    prompt_version: str,
    stale_after_hours: float,
    now: datetime,
) -> tuple[bool, str]:
    if not existing_state:
        return True, "missing"

    existing_hash = str(existing_state.get("input_hash") or "").strip()
    if not existing_hash:
        return True, "missing_hash"
    if existing_hash != input_hash:
        return True, "input_hash_changed"

    existing_prompt_version = str(existing_state.get("prompt_version") or "").strip()
    if existing_prompt_version != prompt_version:
        return True, "prompt_version_changed"

    existing_model = str(existing_state.get("model_name") or "").strip()
    if existing_model != model_name:
        return True, "model_changed"

    expires_at = _to_utc_datetime(existing_state.get("expires_at"))
    if expires_at and expires_at <= now:
        return True, "expired"

    refreshed_at = _to_utc_datetime(existing_state.get("refreshed_at")) or _to_utc_datetime(
        existing_state.get("generated_at")
    )
    if refreshed_at is None:
        return True, "missing_timestamp"
    if refreshed_at + timedelta(hours=max(0.5, stale_after_hours)) <= now:
        return True, "stale"

    return False, "unchanged"

def run_trend_enrichment_cycle(
    *,
    store: Any,
    logger: Any,
    config: TrendEnrichmentRuntimeConfig,
    reason: str,
) -> dict[str, Any]:
    started_at_monotonic = time.monotonic()
    now = datetime.now(timezone.utc)
    if not config.enabled:
        return {"skipped": True, "reason": "disabled"}
    if not config.openai_api_key:
        return {"skipped": True, "reason": "missing_openai_api_key"}

    topics = store.fetch_top_topics_for_enrichment(limit=config.max_topics)
    if not topics:
        return {"skipped": True, "reason": "no_topics"}

    topic_keys = [
        str(row.get("topic_key") or "").strip()
        for row in topics
        if str(row.get("topic_key") or "").strip()
    ]
    existing_by_topic = store.fetch_latest_topic_enrichment_state(topic_keys=topic_keys)

    enriched_count = 0
    fallback_count = 0
    skipped_unchanged = 0
    skipped_no_posts = 0
    failed_count = 0
    attempted_count = 0
    usage_prompt_tokens = 0
    usage_completion_tokens = 0
    usage_total_tokens = 0
    processed_topics = 0
    reasons: dict[str, int] = {}

    for topic in topics:
        topic_key = str(topic.get("topic_key") or "").strip()
        if not topic_key:
            continue
        processed_topics += 1
        raw_label = str(topic.get("topic_label") or topic_key).strip() or topic_key
        window_start = _to_utc_datetime(topic.get("window_start"))
        window_end = _to_utc_datetime(topic.get("window_end")) or now
        candidates = store.fetch_topic_post_candidates_for_enrichment(
            topic_key=topic_key,
            limit=config.candidate_post_limit,
            window_start=window_start,
            window_end=window_end,
        )
        representative_posts, sample_diagnostics = select_representative_posts(
            candidates,
            limit=config.representative_posts,
            max_post_chars=config.max_post_chars,
            min_text_chars=config.enforce_min_text_chars,
            min_word_count=config.enforce_min_word_count,
        )
        if not representative_posts:
            skipped_no_posts += 1
            reasons["no_representative_posts"] = reasons.get("no_representative_posts", 0) + 1
            continue

        input_hash = build_enrichment_input_hash(
            topic_row=topic,
            representative_posts=representative_posts,
        )
        existing_state = existing_by_topic.get(topic_key)
        should_refresh, refresh_reason = _should_refresh_enrichment(
            existing_state=existing_state,
            input_hash=input_hash,
            model_name=config.model_name,
            prompt_version=config.prompt_version,
            stale_after_hours=config.stale_after_hours,
            now=now,
        )
        reasons[refresh_reason] = reasons.get(refresh_reason, 0) + 1
        if not should_refresh:
            skipped_unchanged += 1
            continue

        attempted_count += 1
        coherence_score = _safe_float(sample_diagnostics.get("coherence_score"))
        prompt_input = _summarize_prompt_input(
            topic_row=topic,
            representative_posts=representative_posts,
            coherence_score=coherence_score,
        )

        used_fallback = False
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        try:
            enrichment_payload, usage = _call_openai_for_enrichment(
                api_key=config.openai_api_key,
                model_name=config.model_name,
                prompt_version=config.prompt_version,
                prompt_input=prompt_input,
                timeout_seconds=config.request_timeout_seconds,
                max_retries=config.max_retries,
                retry_backoff_seconds=config.retry_backoff_seconds,
            )
            if coherence_score < 0.09:
                enrichment_payload["summary_confidence"] = min(
                    _safe_float(enrichment_payload.get("summary_confidence")),
                    0.45,
                )
                if not enrichment_payload.get("trend_category"):
                    enrichment_payload["trend_category"] = "mixed discussion cluster"
        except Exception as error:
            failed_count += 1
            log_event(
                logger,
                40,
                "trend_enrichment_topic_failed",
                reason=reason,
                topic_key=topic_key,
                raw_label=raw_label,
                error=str(error),
                refresh_reason=refresh_reason,
            )
            enrichment_payload = _build_fallback_enrichment(
                raw_label=raw_label,
                representative_posts=representative_posts,
                coherence_score=coherence_score,
            )
            used_fallback = True

        usage_prompt_tokens += _safe_int(usage.get("prompt_tokens"))
        usage_completion_tokens += _safe_int(usage.get("completion_tokens"))
        usage_total_tokens += _safe_int(usage.get("total_tokens"))

        supporting_post_ids = [
            str(row.get("source_post_id") or row.get("candidate_id") or "").strip()
            for row in representative_posts
            if str(row.get("source_post_id") or row.get("candidate_id") or "").strip()
        ]
        supporting_sample = [
            {
                "id": str(row.get("candidate_id") or ""),
                "source_post_id": str(row.get("source_post_id") or ""),
                "event_timestamp": (
                    row.get("event_dt").isoformat()
                    if isinstance(row.get("event_dt"), datetime)
                    else None
                ),
                "engagement_score": round(_safe_float(row.get("engagement_score")), 3),
                "text": str(row.get("truncated_text") or ""),
            }
            for row in representative_posts
        ]
        now_value = datetime.now(timezone.utc)
        expires_at = now_value + timedelta(hours=max(0.5, config.stale_after_hours))

        persisted = store.upsert_topic_ai_enrichment(
            {
                "topic_key": topic_key,
                "as_of_window_end": window_end,
                "raw_label": raw_label,
                "canonical_name": enrichment_payload.get("canonical_name") or raw_label,
                "short_description": enrichment_payload.get("short_description") or "",
                "context_paragraph": enrichment_payload.get("context_paragraph") or "",
                "key_entities": enrichment_payload.get("key_entities") or [],
                "trend_category": enrichment_payload.get("trend_category"),
                "summary_confidence": _safe_float(enrichment_payload.get("summary_confidence")),
                "supporting_post_ids": supporting_post_ids,
                "supporting_sample": supporting_sample,
                "representative_post_count": len(representative_posts),
                "model_name": config.model_name,
                "prompt_version": config.prompt_version,
                "input_hash": input_hash,
                "generated_at": now_value,
                "refreshed_at": now_value,
                "expires_at": expires_at,
                "metadata_json": {
                    "refresh_reason": refresh_reason,
                    "reason": reason,
                    "sample_diagnostics": sample_diagnostics,
                    "token_usage": usage,
                    "used_fallback": used_fallback,
                },
            }
        )

        if persisted:
            if used_fallback:
                fallback_count += 1
            else:
                enriched_count += 1

    duration_ms = round((time.monotonic() - started_at_monotonic) * 1000, 1)
    summary = {
        "processed_topics": processed_topics,
        "attempted_topics": attempted_count,
        "enriched_topics": enriched_count,
        "fallback_topics": fallback_count,
        "failed_topics": failed_count,
        "skipped_unchanged": skipped_unchanged,
        "skipped_no_posts": skipped_no_posts,
        "usage_prompt_tokens": usage_prompt_tokens,
        "usage_completion_tokens": usage_completion_tokens,
        "usage_total_tokens": usage_total_tokens,
        "duration_ms": duration_ms,
        "reason_counts": reasons,
        "model_name": config.model_name,
        "prompt_version": config.prompt_version,
        "top_topics_limit": config.max_topics,
    }
    return summary
