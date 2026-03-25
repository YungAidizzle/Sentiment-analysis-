from __future__ import annotations

from typing import Any, Dict


def normalize_submission(raw: Dict[str, Any], fetched_at: str) -> Dict[str, Any]:
    permalink = raw.get("permalink") or ""

    return {
        "id": str(raw.get("id", "")),
        "source": "reddit",
        "subreddit": str(raw.get("subreddit", "")),
        "title": str(raw.get("title", "")),
        "selftext": str(raw.get("selftext", "")),
        "author": str(raw.get("author", "[deleted]")),
        "permalink": permalink,
        "createdUtc": int(float(raw.get("created_utc", 0) or 0)),
        "score": int(raw.get("score", 0) or 0),
        "numComments": int(raw.get("num_comments", 0) or 0),
        "url": str(raw.get("url", "")),
        "fetchedAt": fetched_at,
    }


def normalize_comment(raw: Dict[str, Any], fetched_at: str) -> Dict[str, Any]:
    permalink = raw.get("permalink") or ""

    return {
        "id": str(raw.get("id", "")),
        "source": "reddit",
        "postId": str(raw.get("post_id", "")),
        "parentId": str(raw.get("parent_id", "")),
        "subreddit": str(raw.get("subreddit", "")),
        "author": str(raw.get("author", "[deleted]")),
        "body": str(raw.get("body", "")),
        "permalink": permalink,
        "createdUtc": int(float(raw.get("created_utc", 0) or 0)),
        "score": int(raw.get("score", 0) or 0),
        "fetchedAt": fetched_at,
    }
