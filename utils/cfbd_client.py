"""CollegeFootballData.com client (Bearer key from env / .env)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

_REPO = Path(__file__).resolve().parents[1]
_CFBD_BASE = "https://api.collegefootballdata.com"

_KEY_NAMES = ("CFBD_API_KEY", "CFBD_BEARER", "COLLEGEFOOTBALLDATA_API_KEY")


def load_dotenv_keys() -> None:
    env_path = _REPO / ".env"
    if not env_path.is_file():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, _, v = s.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except OSError:
        return


def resolve_cfbd_key() -> str:
    load_dotenv_keys()
    for name in _KEY_NAMES:
        val = str(os.environ.get(name) or "").strip()
        if val:
            return val
    return ""


def infer_default_year(now: datetime | None = None) -> int:
    now = now or datetime.now(tz=timezone.utc)
    return now.year if now.month >= 8 else now.year - 1


def cfbd_get(path: str, params: dict[str, Any] | None, api_key: str, *, timeout: float = 60.0) -> Any:
    url = path if path.startswith("http") else f"{_CFBD_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params or {}, headers=headers, timeout=timeout)
    if r.status_code == 401:
        raise SystemExit("[CFBD] key rejected (401). Check CFBD_API_KEY.")
    r.raise_for_status()
    return r.json()
