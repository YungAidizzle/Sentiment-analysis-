"""
Local env-file loader for Python maintenance scripts.
Loads `.env.local` / `.env` from the repo root without requiring python-dotenv.
"""

from __future__ import annotations

import os
from pathlib import Path

_ENV_LOADED = False


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]

    return key, value


def load_repo_env() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    repo_root = Path(__file__).resolve().parent.parent
    for filename in (".env.local", ".env"):
        path = repo_root / filename
        if not path.exists():
            continue

        for line in path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(line)
            if not parsed:
                continue

            key, value = parsed
            os.environ.setdefault(key, value)

    _ENV_LOADED = True
