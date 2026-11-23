"""Shared FanDuel / DraftKings / Underdog ladder schema + history helpers.

Reference tape only — never feed into ticket EV / Goblin-70 gates.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

LADDER_COLUMNS = [
    "fetched_at",
    "slate_date",
    "book",
    "sport",
    "player",
    "prop",
    "line",
    "side",
    "american_odds",
    "payout_multiplier",
    "kind",
    "source_event_id",
]

PP_SAME_LINE_COLUMNS = [
    "fetched_at",
    "slate_date",
    "book",
    "sport",
    "player",
    "prop",
    "line",
    "pp_pick",
    "book_kind",
    "american_odds",
    "payout_multiplier",
    "book_implied_p",
    "pp_need_p",
    "ud_need_p",
    "book_below_pp_need",
]

PP_NEED = {
    "Standard": (1.0 / 6.0) ** (1.0 / 3.0),
    "Goblin": (1.0 / 1.65) ** (1.0 / 3.0),
}
UD_BASE = 6.0 ** (1.0 / 3.0)

HISTORY_DIR_NAME = "book_line_history"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def american_to_prob(a) -> float:
    try:
        t = str(a).replace("\u2212", "-").replace("+", "").strip()
        if not t or t.lower() in {"nan", "none"}:
            return float("nan")
        v = float(t)
    except (TypeError, ValueError):
        return float("nan")
    if v < 0:
        return -v / (-v + 100.0)
    return 100.0 / (v + 100.0)


def ud_need_from_mult(mult) -> float:
    try:
        m = float(mult)
    except (TypeError, ValueError):
        return float("nan")
    if m <= 0:
        return float("nan")
    return 1.0 / (UD_BASE * m)


def empty_ladder() -> pd.DataFrame:
    return pd.DataFrame(columns=LADDER_COLUMNS)


def normalize_ladder(df: pd.DataFrame, *, book: str, slate_date: str, fetched_at: str = "") -> pd.DataFrame:
    """Coerce a book CSV into the shared ladder schema."""
    if df is None or df.empty:
        out = empty_ladder()
        return out
    raw = df.copy()
    rename = {
        "prop_type": "prop",
        "odds": "american_odds",
        "fd_american_odds": "american_odds",
        "dk_american_odds": "american_odds",
        "board_sport": "sport",
        "ud_sport_id": "sport",
        "fd_event_id": "source_event_id",
        "dk_event_id": "source_event_id",
        "pp_game_id": "source_event_id",
        "mult": "payout_multiplier",
    }
    raw = raw.rename(columns={k: v for k, v in rename.items() if k in raw.columns and v not in raw.columns})
    if "side" not in raw.columns:
        raw["side"] = "OVER"
    if "kind" not in raw.columns:
        raw["kind"] = "main"
    if "payout_multiplier" not in raw.columns:
        raw["payout_multiplier"] = pd.NA
    if "american_odds" not in raw.columns:
        raw["american_odds"] = pd.NA
    if "source_event_id" not in raw.columns:
        raw["source_event_id"] = ""
    raw["book"] = book
    raw["slate_date"] = slate_date
    raw["fetched_at"] = fetched_at or utc_now_iso()
    raw["line"] = pd.to_numeric(raw.get("line"), errors="coerce")
    raw["side"] = raw["side"].fillna("OVER").astype(str).str.upper()
    raw["kind"] = raw["kind"].fillna("main").astype(str).str.lower()
    raw["sport"] = raw["sport"].astype(str).str.upper()
    # Keep odds as cleaned strings so "+127" / unicode minus survive parquet.
    raw["american_odds"] = (
        raw["american_odds"]
        .map(lambda x: "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).replace("\u2212", "-").strip())
    )
    raw["payout_multiplier"] = pd.to_numeric(raw["payout_multiplier"], errors="coerce")
    for c in LADDER_COLUMNS:
        if c not in raw.columns:
            raw[c] = pd.NA
    out = raw[LADDER_COLUMNS].dropna(subset=["line", "player", "prop"])
    return out.reset_index(drop=True)


def history_dir(repo_root: Path) -> Path:
    d = Path(repo_root) / "data" / HISTORY_DIR_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _month_path(repo_root: Path, prefix: str, slate_date: str) -> Path:
    ym = str(slate_date)[:7]
    return history_dir(repo_root) / f"{prefix}_{ym}.parquet"


def append_history(repo_root: Path, prefix: str, slate_date: str, df: pd.DataFrame) -> Path:
    """Append rows to monthly parquet; hour-level dedupe on identity cols."""
    path = _month_path(repo_root, prefix, slate_date)
    if df is None or df.empty:
        return path
    new = df.copy()
    if "fetched_at" in new.columns:
        new["_hour"] = pd.to_datetime(new["fetched_at"], utc=True, errors="coerce").dt.floor("h")
    else:
        new["_hour"] = pd.NaT
    key_cols = [c for c in ("book", "sport", "player", "prop", "line", "side", "kind", "_hour") if c in new.columns]
    if path.exists():
        old = pd.read_parquet(path)
        if "_hour" not in old.columns and "fetched_at" in old.columns:
            old["_hour"] = pd.to_datetime(old["fetched_at"], utc=True, errors="coerce").dt.floor("h")
        merged = pd.concat([old, new], ignore_index=True)
    else:
        merged = new
    if key_cols:
        merged = merged.drop_duplicates(subset=key_cols, keep="last")
    merged = merged.drop(columns=["_hour"], errors="ignore")
    if "american_odds" in merged.columns:
        merged["american_odds"] = merged["american_odds"].astype(str).replace({"nan": "", "None": ""})
    if "payout_multiplier" in merged.columns:
        merged["payout_multiplier"] = pd.to_numeric(merged["payout_multiplier"], errors="coerce")
    if "book_implied_p" in merged.columns:
        merged["book_implied_p"] = pd.to_numeric(merged["book_implied_p"], errors="coerce")
    if "line" in merged.columns:
        merged["line"] = pd.to_numeric(merged["line"], errors="coerce")
    merged.to_parquet(path, index=False)
    return path


def load_history(repo_root: Path, prefix: str = "rungs", months: Optional[Iterable[str]] = None) -> pd.DataFrame:
    d = history_dir(repo_root)
    files = sorted(d.glob(f"{prefix}_*.parquet"))
    if months:
        want = {str(m)[:7] for m in months}
        files = [f for f in files if f.stem.split("_")[-1] in want]
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def dk_frame_to_ladder(dk: pd.DataFrame) -> pd.DataFrame:
    """DraftKings step1-shaped CSV → ladder rows (Over + N+ only)."""
    if dk is None or dk.empty:
        return pd.DataFrame(columns=["sport", "player", "prop", "line", "american_odds", "kind", "source_event_id", "side"])
    raw = dk.copy()
    if "player" in raw.columns:
        raw = raw[raw["player"].astype(str).str.len() >= 5]
    if "dk_subcategory" in raw.columns:
        raw = raw[
            ~raw["dk_subcategory"].astype(str).str.contains(
                r"1Q|1H|Combined|Either|Each|Drive|H2H|Longest", case=False, na=False
            )
        ]
    lab = raw.get("dk_selection_label", pd.Series(dtype=str)).astype(str)
    is_over = lab.str.lower().eq("over")
    is_plus = lab.str.match(r"^\d+\+$")
    raw = raw[is_over | is_plus].copy()
    if raw.empty:
        return pd.DataFrame(columns=["sport", "player", "prop", "line", "american_odds", "kind", "source_event_id", "side"])
    raw["kind"] = ["main" if o else "alt" for o in is_over.loc[raw.index]]
    sport = raw["board_sport"] if "board_sport" in raw.columns else raw.get("sport", "NFL")
    prop = raw["prop_type"] if "prop_type" in raw.columns else raw.get("prop", "")
    odds = raw["dk_american_odds"] if "dk_american_odds" in raw.columns else raw.get("american_odds", pd.NA)
    eid = raw["dk_event_id"] if "dk_event_id" in raw.columns else ""
    return pd.DataFrame(
        {
            "sport": sport,
            "player": raw["player"],
            "prop": prop,
            "line": raw["line"],
            "american_odds": odds,
            "kind": raw["kind"],
            "source_event_id": eid,
            "side": "OVER",
            "payout_multiplier": pd.NA,
        }
    )
