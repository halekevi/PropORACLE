#!/usr/bin/env python3
"""Pre-submit slip check. Paste legs, get PASS/FAIL. Observe only.

  py -3.14 -m ml.slip_check
  py -3.14 -m ml.slip_check --file legs.txt
  py -3.14 -m ml.slip_check --legs "Player|Prop|OVER|9.5|Goblin|NYG"
  py -3.14 -m ml.slip_check --place --quote 1.65 --source live_cdp --legs "..."

A share-to-endpoint or short site form is later; text paste is the interface now.

Rules (all must pass):
  1. 3 legs max
  2. Each leg a different team (Rule 7 same-team / same-player hard blocks)
  3. Every leg on the cleaned L5=5 board OR gift-scan clean
     (all_pass and not published_after_start). Flagged-only and
     published_after_start gifts are not submittable. A Demon
     observe-only list is not enough.
  4. No unsupported / mapping-fail / fantasy / period / Demon UNDER
  5. Real N-correct scrape for the exact mix >= 1.4x
     (live_cdp / n_correct_live only; never 1st-place; never guess Demon)

Does not import the frozen NFL tracks. Check-only logs placed: false.
--place with a real scrape (source + N-correct + captured_at now) writes
placed: true on the same jsonl. Nothing writes /tickets.
NFL corr-stack traps print as warns (mixed-direction QB/WR, RB carries
Over + own QB attempts Over). Warns never set FAIL or drop a ticket.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
from utils.l5_push_aware import is_clean_l5_five  # noqa: E402
from utils.n_correct_payout import (  # noqa: E402
    goblin_delta_sig,
    lookup_live_cdp_scrape,
)
from utils.nfl_unsupported_props import is_football_period_split_prop  # noqa: E402
from utils.prop_norm import canon_prop, prop_key_for_player  # noqa: E402
from utils.scraped_quote import (  # noqa: E402
    SCRAPED_PAYOUT_SOURCES,
    is_real_scraped_quote,
)
from utils.nfl_corr_stack import nfl_corr_stack_warnings  # noqa: E402
from utils.slip_correlation import (  # noqa: E402
    has_duplicate_player,
    player_key,
    team_key,
)
from utils.stat_attach_invalid import (  # noqa: E402
    analysis_invalid_reason,
    is_analysis_invalid_prop,
)

from ml.gift_scan import (  # noqa: E402
    OUT_DIR as GIFT_DIR,
    goblin_line_delta,
    _f,
    _fold,
    _pick,
    _side,
)

ET = ZoneInfo("America/New_York")
MIN_QUOTE = 1.4
MAX_LEGS = 3
LOG = GIFT_DIR / "slip_check.jsonl"
IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"})
SCREENSHOT_MSG = "not implemented — paste the text legs"
PERIOD_LEAGUES = ("1H", "1Q", "2H", "4Q")
PLACE_FAIL = (
    "place: missing real scrape (live_cdp / n_correct_live + N-correct) - fail closed"
)
_PLACE_REJECT_SOURCES = frozenset(
    {
        "",
        "mix_floor",
        "guessed",
        "guess",
        "estimated",
        "estimate",
        "1st_place",
        "1st-place",
        "1st place",
        "first_place",
        "firstplace",
        "first place",
        "goblin_3",
        "median",
        "p25",
        "injected",
    }
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slate_date(raw: object = "") -> str:
    text = str(raw or "").strip()[:10]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return datetime.now(ET).date().isoformat()


def _rung(raw: object) -> str:
    return _pick(raw)


def _line(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    text = str(raw).strip()
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not m:
        return _f(raw)
    return _f(m.group(0))


def _prop_of(row: dict[str, Any]) -> str:
    return str(row.get("prop") or row.get("prop_type") or row.get("Prop") or "").strip()


def _sport_of(row: dict[str, Any]) -> str:
    return str(row.get("sport") or row.get("Sport") or row.get("league") or "").strip()


def _norm_leg(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["player"] = str(out.get("player") or out.get("player_name") or out.get("Player") or "").strip()
    out["prop"] = _prop_of(out)
    out["prop_type"] = out["prop"]
    out["side"] = _side(out) or "OVER"
    out["direction"] = out["side"]
    out["rung"] = _rung(out.get("rung") or out.get("pick_type") or out.get("Pick"))
    out["pick_type"] = out["rung"] or out.get("pick_type") or ""
    line = _line(out.get("line") or out.get("Line") or out.get("Line_num"))
    if line is not None:
        out["line"] = line
    team = team_key(out)
    if team:
        out["team"] = team
    sport = _sport_of(out)
    if sport:
        out["sport"] = sport
    key = prop_key_for_player(out["prop"], sport, out["player"])
    if key:
        out["prop_canon"] = key
    return out


def parse_leg_arg(raw: str) -> dict[str, Any]:
    """player|prop|side|line|rung|team  (pipe, tab, or comma)."""
    text = str(raw or "").strip()
    sep = "|" if "|" in text else ("\t" if "\t" in text else ",")
    parts = [p.strip() for p in text.split(sep)]
    while len(parts) < 6:
        parts.append("")
    player, prop, side, line, rung, team = parts[:6]
    if not side and not line:
        return parse_free_line(text)
    return _norm_leg(
        {
            "player": player,
            "prop": prop,
            "side": side or "OVER",
            "line": line,
            "rung": rung,
            "pick_type": rung,
            "team": team,
        }
    )


_LINE_RE = re.compile(
    r"(?P<side>over|under|more|less|\bo\b|\bu\b)\s*"
    r"(?P<line>[-+]?\d+(?:\.\d+)?)",
    re.I,
)
_RUNG_RE = re.compile(r"\b(goblin|demon|standard|std|gob|dem)\b", re.I)
_TEAM_RE = re.compile(r"\b([A-Z]{2,4})\b")


def parse_free_line(text: str) -> dict[str, Any]:
    """One-line paste: 'Cam Skattebo Rush Attempts Over 9.5 Goblin NYG'."""
    raw = re.sub(r"^\s*\d+[.)]\s*", "", str(text or "").strip())
    raw = raw.replace("—", " ").replace("–", " ").replace("  ", " ")
    side, line, rung, team = "", None, "", ""
    m = _LINE_RE.search(raw)
    work = raw
    if m:
        side = m.group("side")
        line = m.group("line")
        work = (raw[: m.start()] + " " + raw[m.end() :]).strip()
    rm = _RUNG_RE.search(work)
    if rm:
        rung = rm.group(1)
        work = (work[: rm.start()] + " " + work[rm.end() :]).strip()
    tm = list(_TEAM_RE.finditer(work))
    if tm:
        cand = tm[-1].group(1)
        if cand.upper() not in {"OVER", "UNDER", "MORE", "LESS"}:
            team = cand
            work = (work[: tm[-1].start()] + " " + work[tm[-1].end() :]).strip()
    bits = [b for b in re.split(r"\s+", work) if b]
    player, prop = "", ""
    if len(bits) >= 3:
        player = " ".join(bits[:2])
        prop = " ".join(bits[2:])
    elif len(bits) == 2:
        player, prop = bits[0], bits[1]
    elif bits:
        player = bits[0]
    return _norm_leg(
        {
            "player": player,
            "prop": prop,
            "side": side or "OVER",
            "line": line,
            "rung": rung,
            "pick_type": rung,
            "team": team,
        }
    )


def parse_legs(text: str) -> list[dict[str, Any]]:
    """Parse pasted text: JSON list, pipe rows, or PrizePicks-ish blocks."""
    blob = str(text or "").strip()
    if not blob:
        return []
    if blob[0] in {"[", "{"}:
        raw = json.loads(blob)
        rows = raw if isinstance(raw, list) else list(raw.get("legs") or [])
        return [_norm_leg(r) for r in rows if isinstance(r, dict)]
    lines = [ln.strip() for ln in blob.replace("\r\n", "\n").split("\n")]
    lines = [ln for ln in lines if ln and not ln.startswith("#")]
    if not lines:
        return []
    if any(sep in lines[0] for sep in ("|", "\t")) or (
        lines[0].count(",") >= 2 and _LINE_RE.search(lines[0])
    ):
        return [parse_leg_arg(ln) for ln in lines]
    if all(_LINE_RE.search(ln) and len(ln.split()) >= 3 for ln in lines):
        return [parse_free_line(ln) for ln in lines]
    return _parse_blocks(lines)


def _parse_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Stacked paste: player / prop / Over 9.5 / Goblin / TEAM."""
    legs: list[dict[str, Any]] = []
    cur: dict[str, Any] = {}

    def flush() -> None:
        nonlocal cur
        if cur.get("player") or cur.get("prop"):
            legs.append(_norm_leg(cur))
        cur = {}

    for ln in lines:
        if _LINE_RE.search(ln):
            m = _LINE_RE.search(ln)
            assert m is not None
            cur["side"] = m.group("side")
            cur["line"] = m.group("line")
            rest = (ln[: m.start()] + " " + ln[m.end() :]).strip()
            if rest and not cur.get("prop"):
                cur["prop"] = rest
            rm = _RUNG_RE.search(ln)
            if rm:
                cur["rung"] = rm.group(1)
            continue
        rm = _RUNG_RE.fullmatch(ln.strip())
        if rm:
            cur["rung"] = rm.group(1)
            continue
        if re.fullmatch(r"[A-Z]{2,4}", ln.strip()):
            cur["team"] = ln.strip()
            flush()
            continue
        if cur.get("player") and not cur.get("prop"):
            cur["prop"] = ln
            continue
        if cur.get("player") and cur.get("prop") and cur.get("line") is not None:
            flush()
        if not cur.get("player"):
            cur["player"] = ln
        else:
            cur["prop"] = ln
    flush()
    return [x for x in legs if x.get("player")]


def screenshot_message(path: object) -> str:
    dest = Path(str(path or ""))
    if dest.suffix.lower() in IMAGE_SUFFIXES:
        return SCREENSHOT_MSG
    return ""


def unsupported_reason(leg: dict[str, Any]) -> str:
    sport = _sport_of(leg)
    prop = _prop_of(leg)
    rung = _rung(leg.get("rung") or leg.get("pick_type"))
    side = _side(leg)
    if rung == "demon" and side == "UNDER":
        return "Demon UNDER is invalid"
    if is_fantasy_prop_label(prop):
        return "fantasy is unsupported"
    if is_football_period_split_prop(prop):
        return "period / first-N prop has no own stat"
    league = str(leg.get("league") or sport).strip().upper()
    if league.endswith(PERIOD_LEAGUES):
        return "period slate is unsupported"
    why = analysis_invalid_reason(sport, prop)
    if why:
        return why
    if is_analysis_invalid_prop(sport, prop):
        return "mapping_fail"
    return ""


def _dir_l5_five(row: dict[str, Any]) -> bool:
    side = _side(row)
    hits = row.get("l5_over") if side != "UNDER" else row.get("l5_under")
    if hits is None:
        hits = row.get("L5_over") if side != "UNDER" else row.get("L5_under")
    if hits is None:
        hits = row.get("last5_over") if side != "UNDER" else row.get("last5_under")
    return is_clean_l5_five(
        hits,
        row.get("l5_decided") or row.get("L5_decided"),
        row.get("l5_push") or row.get("L5_push"),
    )


def is_cleaned_l5eq5_row(row: dict[str, Any]) -> bool:
    """Best-props cleaned board: not Demon, ticketable, directional L5=5.

    The ranker xlsx / ``_clears_list_gate`` already dropped observe-only
    Demon lists, fantasy, and sport-keep fails. This only re-checks the
    L5=5 tighten plus the hard product bans.
    """
    rec = _norm_leg(row)
    if _rung(rec.get("rung") or rec.get("pick_type")) == "demon":
        return False
    if rec.get("ticketable") is False or rec.get("Ticketable") is False:
        return False
    if is_fantasy_prop_label(_prop_of(rec)):
        return False
    if unsupported_reason(rec):
        return False
    if rec.get("cleaned") or rec.get("on_cleaned_list"):
        return True
    return _dir_l5_five(rec)


def is_clean_gift(row: dict[str, Any]) -> bool:
    if not row.get("all_pass"):
        return False
    if row.get("published_after_start"):
        return False
    return True


def load_clean_gifts(*, repo: Path | None = None, date: str = "") -> list[dict[str, Any]]:
    root = repo or _REPO
    day = slate_date(date)
    for dest in (
        root / "data" / "reports" / "gift_scan" / f"gift_scan_{day}.json",
        root / "data" / "reports" / "gift_scan" / "gift_scan_latest.json",
        GIFT_DIR / f"gift_scan_{day}.json",
        GIFT_DIR / "gift_scan_latest.json",
    ):
        if not dest.is_file():
            continue
        try:
            payload = json.loads(dest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        rows = payload.get("gifts") or []
        return [_norm_leg(g) | dict(g) for g in rows if isinstance(g, dict) and is_clean_gift(g)]
    return []


def _xlsx_to_leg(row: dict[str, Any]) -> dict[str, Any]:
    rec = {
        "player": row.get("Player") or row.get("player"),
        "prop": row.get("Prop") or row.get("prop"),
        "side": row.get("Side") or row.get("side"),
        "line": row.get("Line_num") or row.get("line") or row.get("Line"),
        "pick_type": row.get("Pick") or row.get("pick_type"),
        "rung": row.get("Pick") or row.get("rung"),
        "team": row.get("Team") or row.get("team"),
        "sport": row.get("Sport") or row.get("sport"),
        "l5_over": row.get("L5_over") or row.get("l5_over"),
        "l5_under": row.get("L5_under") or row.get("l5_under"),
        "l5_decided": row.get("L5_decided") or row.get("l5_decided"),
        "l5_push": row.get("L5_push") or row.get("l5_push"),
        "ticketable": row.get("Ticketable", row.get("ticketable", True)),
    }
    return _norm_leg(rec)


def load_cleaned_xlsx(*, repo: Path | None = None, date: str = "") -> list[dict[str, Any]]:
    root = repo or _REPO
    day = slate_date(date)
    dest = root / "outputs" / day / f"best_props_{day}.xlsx"
    if not dest.is_file():
        return []
    try:
        import pandas as pd
    except ImportError:
        return []
    out: list[dict[str, Any]] = []
    try:
        sheets = pd.read_excel(dest, sheet_name=None)
    except Exception:
        return []
    for name, df in (sheets or {}).items():
        if df is None or getattr(df, "empty", True):
            continue
        if str(name).lower().startswith("fantasy"):
            continue
        for rec in df.to_dict(orient="records"):
            if not isinstance(rec, dict):
                continue
            if str(rec.get("note") or rec.get("Note") or "").strip():
                if not rec.get("Player") and not rec.get("player"):
                    continue
            leg = _xlsx_to_leg(rec)
            if is_cleaned_l5eq5_row({**rec, **leg}):
                out.append(leg)
    return out


def load_cleaned_from_boards(*, repo: Path | None = None, date: str = "") -> list[dict[str, Any]]:
    """Ranker list gate + directional L5=5. Slow; used when the xlsx is missing."""
    root = repo or _REPO
    day = slate_date(date)
    try:
        import rank_best_props_today as R
    except Exception:
        return []
    loaders = (
        ("NBA", lambda: R.load_nba(root, day)),
        ("WNBA", lambda: R.load_sport(root, day, "WNBA", "wnba", "step8_wnba_direction.csv")),
        ("NHL", lambda: R.load_nhl(root, day)),
        ("NFL", lambda: R.load_nfl(root, day)),
        ("CFB", lambda: R.load_cfb(root, day)),
        ("MLB", lambda: R.load_mlb(root, day)),
        ("Soccer", lambda: R.load_sport(root, day, "SOCCER", "soccer", "step8_soccer_direction.csv")),
        ("Tennis", lambda: R.load_sport(root, day, "TENNIS", "tennis", "step8_tennis_direction.csv")),
    )
    out: list[dict[str, Any]] = []
    for sport, loader in loaders:
        try:
            df = loader()
        except Exception:
            continue
        if df is None or getattr(df, "empty", True):
            continue
        try:
            recs = R.recs(df)
        except Exception:
            continue
        so, su, gob = R.bucket(recs, sport)
        for rec in so + su + gob:
            if _dir_l5_five(rec):
                out.append(_norm_leg(rec))
    return out


def load_allowed_legs(
    *,
    repo: Path | None = None,
    date: str = "",
    listed: list[dict[str, Any]] | None = None,
    gifts: list[dict[str, Any]] | None = None,
    load_boards: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (cleaned L5=5 rows, clean gifts). Injected lists win."""
    if listed is not None or gifts is not None:
        clean_listed = [_norm_leg(x) for x in (listed or [])]
        clean_gifts = [_norm_leg(x) | dict(x) for x in (gifts or []) if is_clean_gift(x)]
        return clean_listed, clean_gifts
    root = repo or _REPO
    day = slate_date(date)
    clean_gifts = load_clean_gifts(repo=root, date=day)
    clean_listed = load_cleaned_xlsx(repo=root, date=day)
    if not clean_listed and load_boards:
        clean_listed = load_cleaned_from_boards(repo=root, date=day)
    return clean_listed, clean_gifts


def _same_line(a: object, b: object) -> bool:
    x, y = _f(a), _f(b)
    if x is None or y is None:
        return x is None and y is None
    return abs(x - y) < 0.011


def _match_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    sport = _sport_of(row)
    prop = canon_prop(sport, _prop_of(row)) or _fold(_prop_of(row))
    return (_fold(row.get("player")), prop, _side(row) or "OVER", _rung(row.get("rung") or row.get("pick_type")))


def find_allowed(
    leg: dict[str, Any],
    cleaned: list[dict[str, Any]],
    gifts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    want = _match_key(leg)
    line = _line(leg.get("line"))
    for src, pool in (("cleaned_l5eq5", cleaned), ("gift_scan_clean", gifts)):
        for row in pool:
            got = _match_key(row)
            if want[:3] != got[:3]:
                continue
            if want[3] and got[3] and want[3] != got[3]:
                continue
            if line is not None and row.get("line") is not None and not _same_line(line, row.get("line")):
                continue
            hit = dict(row)
            hit["allowed_source"] = src
            return hit
    return None


def enrich_from_allowed(leg: dict[str, Any], hit: dict[str, Any]) -> dict[str, Any]:
    out = dict(leg)
    for key in ("team", "sport", "standard_line", "goblin_delta", "opp_team", "opponent"):
        if not out.get(key) and hit.get(key) not in (None, ""):
            out[key] = hit[key]
    if not out.get("rung") and hit.get("rung"):
        out["rung"] = hit["rung"]
        out["pick_type"] = hit["rung"]
    out["allowed_source"] = hit.get("allowed_source")
    return out


def distinct_teams_ok(legs: list[dict[str, Any]]) -> tuple[bool, str]:
    if has_duplicate_player(legs):
        names = [player_key(x) for x in legs if player_key(x)]
        return False, f"same player twice ({', '.join(sorted(set(names)))})"
    teams: list[str] = []
    missing = 0
    for leg in legs:
        tk = team_key(leg)
        if not tk:
            missing += 1
        else:
            teams.append(tk)
    if missing:
        return False, f"{missing} leg(s) missing team - cannot prove distinct teams"
    uniq = sorted(set(teams))
    if len(uniq) != len(legs):
        return False, f"need {len(legs)} different teams, got {len(uniq)} ({', '.join(uniq)})"
    return True, ""


def lookup_mix_quote(
    legs: list[dict[str, Any]],
    *,
    date: str = "",
    repo: Path | None = None,
    product: str = "Power",
) -> tuple[float | None, dict[str, Any]]:
    """Exact-mix live_cdp / n_correct_live multiple. Fail-closed. Never guess Demon."""
    n = len(legs)
    n_d = sum(1 for x in legs if _rung(x.get("rung") or x.get("pick_type")) == "demon")
    n_g = sum(1 for x in legs if _rung(x.get("rung") or x.get("pick_type")) == "goblin")
    n_s = sum(1 for x in legs if _rung(x.get("rung") or x.get("pick_type")) == "standard")
    if n_d:
        return None, {
            "reason": "missing_real_scrape",
            "detail": "never guess Demon - no live_cdp / n_correct_live scrape for this mix",
        }
    for leg in legs:
        if _rung(leg.get("rung") or leg.get("pick_type")) != "goblin":
            continue
        if _f(leg.get("goblin_delta")) is None and _f(leg.get("standard_line")) is None:
            d = goblin_line_delta(leg)
            if d is not None:
                leg["goblin_delta"] = d
    delta_sig = goblin_delta_sig(legs)
    if not delta_sig and n_g:
        parts: list[float] = []
        for leg in legs:
            if _rung(leg.get("rung") or leg.get("pick_type")) != "goblin":
                continue
            d = _f(leg.get("goblin_delta"))
            if d is None:
                d = goblin_line_delta(leg)
            if d is not None:
                parts.append(d)
        if parts:
            delta_sig = "+".join(f"{v:g}" for v in sorted(parts))
    if n_g and not delta_sig:
        return None, {
            "reason": "missing_real_scrape",
            "detail": "Goblin mix has no GΔ - cannot look up the rate-card cell",
        }
    if n_s == n and n > 0:
        # All-Standard still needs a real scrape (lookup requires a Δ sig for Goblins).
        # Try the empty-sig path only if a caller stored a 0G scrape under a dummy sig.
        found = lookup_live_cdp_scrape(date, n_s, 0, n, product, "0", repo=repo)
    else:
        found = lookup_live_cdp_scrape(date, n_s, n_g, n, product, delta_sig, repo=repo)
    if not found:
        return None, {
            "reason": "missing_real_scrape",
            "detail": f"no live_cdp / n_correct_live cell for {n_s}S+{n_g}G {product} d={delta_sig or '-'}",
        }
    if not is_real_scraped_quote(found.get("payout_source"), found.get("captured_at")):
        return None, {
            "reason": "missing_real_scrape",
            "detail": "quote is not live_cdp / n_correct_live with captured_at",
        }
    table = found.get("n_correct") or {}
    px = _f(table.get(n) or table.get(str(n)))
    if px is None or px <= 0:
        return None, {"reason": "missing_real_scrape", "detail": "N-correct table has no all-hit multiple"}
    return float(px), {
        "payout_x": float(px),
        "payout_source": found.get("payout_source"),
        "captured_at": found.get("captured_at"),
        "note": found.get("note"),
        "n_s": n_s,
        "n_g": n_g,
        "n_d": n_d,
        "delta_sig": delta_sig,
    }


def _coerce_injected_quote(quote: object) -> tuple[float | None, dict[str, Any]]:
    if quote is None:
        return None, {}
    if isinstance(quote, (int, float)) and math.isfinite(float(quote)):
        return float(quote), {
            "payout_x": float(quote),
            "payout_source": "n_correct_live",
            "captured_at": "injected",
            "note": "caller-provided N-correct",
        }
    if isinstance(quote, dict):
        src = quote.get("payout_source")
        cap = quote.get("captured_at")
        if not is_real_scraped_quote(src, cap):
            return None, {"reason": "missing_real_scrape", "detail": "injected quote is not live_cdp / n_correct_live"}
        px = _f(
            quote.get("payout_x")
            or quote.get("quote")
            or quote.get("sweep_payout_x")
            or (quote.get("n_correct") or {}).get(quote.get("n_legs"))
        )
        if px is None:
            return None, {"reason": "missing_real_scrape", "detail": "injected quote has no N-correct multiple"}
        return float(px), {**quote, "payout_x": float(px)}
    return None, {"reason": "missing_real_scrape", "detail": "quote unreadable"}


def _rejected_place_source(source: object) -> bool:
    src = str(source or "").strip().lower()
    if src in _PLACE_REJECT_SOURCES:
        return True
    if "1st" in src or "first_place" in src or "first place" in src:
        return True
    if "guess" in src:
        return True
    return False


def place_source_of(
    *,
    source: str = "",
    quote: object = None,
    quote_meta: dict[str, Any] | None = None,
) -> str:
    """Explicit --source, else quote-dict source, else lookup source. Not a bare float."""
    raw = str(source or "").strip()
    if raw:
        return raw
    if isinstance(quote, dict):
        got = str(quote.get("payout_source") or "").strip()
        if got:
            return got
    if quote is None:
        got = str((quote_meta or {}).get("payout_source") or "").strip()
        if got:
            return got
    return ""


def is_placeable_quote(
    source: object,
    payout_x: object,
    *,
    captured_at: object = "",
    explicit_source: bool = False,
) -> bool:
    """Real N-correct scrape only. mix_floor / guessed / 1st-place fail closed."""
    px = _f(payout_x)
    if px is None or px <= 0:
        return False
    src = str(source or "").strip().lower()
    if _rejected_place_source(src):
        return False
    if src not in SCRAPED_PAYOUT_SOURCES:
        return False
    cap = str(captured_at or "").strip().lower()
    if cap in {"injected", "guessed", "guess"} and not explicit_source:
        return False
    return True


def check_slip(
    legs: list[dict[str, Any]] | None,
    *,
    date: str = "",
    repo: Path | None = None,
    listed: list[dict[str, Any]] | None = None,
    gifts: list[dict[str, Any]] | None = None,
    quote: object = None,
    product: str = "Power",
    write: bool = False,
    load_boards: bool = False,
    place: bool = False,
    source: str = "",
) -> dict[str, Any]:
    """Return PASS/FAIL plus one reason line per failed rule.

    placed is false unless place=True and the quote is a real scrape.
    warns are printed and logged; they never change verdict.
    """
    day = slate_date(date)
    root = repo or _REPO
    rows = [_norm_leg(x) for x in (legs or []) if isinstance(x, dict)]
    cleaned, clean_gifts = load_allowed_legs(
        repo=root, date=day, listed=listed, gifts=gifts, load_boards=load_boards
    )
    fails: list[str] = []
    warns: list[str] = []
    matched: list[dict[str, Any]] = []

    if len(rows) > MAX_LEGS:
        fails.append(f"3 legs max: {len(rows)}-leg slip")

    for i, leg in enumerate(rows):
        hit = find_allowed(leg, cleaned, clean_gifts)
        if hit:
            rows[i] = enrich_from_allowed(leg, hit)
            matched.append(rows[i])
        else:
            matched.append(leg)

    ok_teams, team_why = distinct_teams_ok(rows)
    if not ok_teams:
        fails.append(f"three different teams: {team_why}")

    missing_listed: list[str] = []
    for leg in rows:
        if not find_allowed(leg, cleaned, clean_gifts):
            label = f"{leg.get('player') or '?'} {_prop_of(leg) or '?'}".strip()
            missing_listed.append(label)
    if missing_listed:
        fails.append(
            "cleaned / gift-scan: not on cleaned L5=5 board or gift-scan clean - "
            + "; ".join(missing_listed)
        )

    bad_props: list[str] = []
    for leg in rows:
        why = unsupported_reason(leg)
        if why:
            bad_props.append(f"{leg.get('player') or '?'} {_prop_of(leg) or '?'}: {why}")
    if bad_props:
        fails.append("unsupported prop: " + "; ".join(bad_props))

    quote_meta: dict[str, Any] = {}
    px: float | None = None
    place_quote = quote
    if place and isinstance(quote, dict):
        src_in = str(source or quote.get("payout_source") or "").strip()
        if src_in and not quote.get("captured_at"):
            place_quote = {**quote, "payout_source": src_in, "captured_at": _now_iso()}
        elif source and not quote.get("payout_source"):
            place_quote = {**quote, "payout_source": str(source).strip()}
    if place_quote is not None:
        px, quote_meta = _coerce_injected_quote(place_quote)
        if px is None:
            fails.append(
                "quote: missing real scrape (live_cdp / n_correct_live) - "
                + str(quote_meta.get("detail") or "fail closed")
            )
        elif px < MIN_QUOTE:
            fails.append(f"quote: {px:g}x is below the {MIN_QUOTE:g}x floor")
    else:
        px, quote_meta = lookup_mix_quote(rows, date=day, repo=root, product=product)
        if px is None:
            fails.append(
                "quote: missing real scrape (live_cdp / n_correct_live) - "
                + str(quote_meta.get("detail") or "fail closed")
            )
        elif px < MIN_QUOTE:
            fails.append(f"quote: {px:g}x is below the {MIN_QUOTE:g}x floor")

    placed = False
    if place:
        src = place_source_of(source=source, quote=quote, quote_meta=quote_meta)
        explicit = bool(str(source or "").strip()) or isinstance(quote, dict)
        if is_placeable_quote(src, px, captured_at=quote_meta.get("captured_at"), explicit_source=explicit):
            placed = True
            quote_meta = {
                **quote_meta,
                "payout_source": src,
                "captured_at": _now_iso(),
                "payout_x": float(px) if px is not None else None,
            }
        else:
            fails.append(PLACE_FAIL)

    warns.extend(nfl_corr_stack_warnings(rows))
    # Injury soft flags (Q/GTD/Doubtful) and stale snapshots — warn only.
    # OUT is already excluded from cleaned / gift boards; still warn if pasted.
    try:
        from utils.injury_gate import (
            injury_captured_at,
            injury_snapshot_stale,
            is_injury_out,
            is_injury_soft_flag,
            normalize_inj_status,
        )

        for leg in rows:
            status = normalize_inj_status(leg)
            label = f"{leg.get('player') or '?'} {_prop_of(leg) or '?'}".strip()
            if is_injury_out(leg):
                warns.append(f"injury OUT: {label} — should not be ticketed")
            elif is_injury_soft_flag(leg):
                warns.append(f"injury {status}: {label}")
            cap = injury_captured_at(leg)
            if cap and injury_snapshot_stale(cap):
                warns.append(f"injury snapshot stale (>6h): {label}")
    except Exception:
        pass
    verdict = "PASS" if not fails else "FAIL"
    rec = {
        "ts": _now_iso(),
        "date": day,
        "kind": "slip_check",
        "placed": placed,
        "verdict": verdict,
        "fails": fails,
        "warns": warns,
        "n_legs": len(rows),
        "quote": px,
        "quote_meta": quote_meta,
        "product": product,
        "legs": rows,
        "n_cleaned": len(cleaned),
        "n_gifts": len(clean_gifts),
    }
    if write:
        _append_log(rec, repo=root)
    return rec


def _append_log(rec: dict[str, Any], *, repo: Path | None = None) -> None:
    root = repo or _REPO
    dest = root / "data" / "reports" / "gift_scan" / "slip_check.jsonl"
    dest.parent.mkdir(parents=True, exist_ok=True)
    slim = {
        "ts": rec.get("ts"),
        "date": rec.get("date"),
        "kind": "slip_check",
        "placed": bool(rec.get("placed")),
        "verdict": rec.get("verdict"),
        "fails": rec.get("fails"),
        "warns": rec.get("warns") or [],
        "n_legs": rec.get("n_legs"),
        "quote": rec.get("quote"),
        "quote_source": (rec.get("quote_meta") or {}).get("payout_source"),
        "captured_at": (rec.get("quote_meta") or {}).get("captured_at"),
        "legs": [
            {
                "player": x.get("player"),
                "prop": _prop_of(x),
                "side": _side(x),
                "line": x.get("line"),
                "rung": _rung(x.get("rung") or x.get("pick_type")),
                "team": team_key(x),
                "sport": _sport_of(x),
                "allowed_source": x.get("allowed_source"),
            }
            for x in rec.get("legs") or []
        ],
    }
    with dest.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(slim, ensure_ascii=False) + "\n")


def format_result(rec: dict[str, Any]) -> str:
    lines = [str(rec.get("verdict") or "FAIL")]
    for why in rec.get("fails") or []:
        lines.append(why)
    for why in rec.get("warns") or []:
        lines.append(why)
    if rec.get("placed"):
        src = (rec.get("quote_meta") or {}).get("payout_source") or ""
        px = rec.get("quote")
        extra = f"placed {px:g}x {src}".strip() if px is not None else "placed"
        lines.append(extra)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="Slate date YYYY-MM-DD (default: ET today)")
    ap.add_argument("--file", default="", help="Text file of pasted legs (not a screenshot)")
    ap.add_argument(
        "--legs",
        action="append",
        default=[],
        help="player|prop|side|line|rung|team (repeatable)",
    )
    ap.add_argument("--screenshot", default="", help="Image path — OCR is not implemented")
    ap.add_argument("--product", default="Power", help="Power or Flex (N-correct lookup)")
    ap.add_argument("--quote", default="", help="Scraped N-correct multiple for this exact mix")
    ap.add_argument(
        "--source",
        default="",
        help="Quote source for --place (live_cdp or n_correct_live)",
    )
    ap.add_argument(
        "--place",
        action="store_true",
        help="Log placed: true (requires a real scrape; never from a check-only run)",
    )
    ap.add_argument("--json", action="store_true", help="Print the full result dict")
    ap.add_argument("--no-log", action="store_true", help="Do not append slip_check.jsonl")
    ap.add_argument(
        "--no-boards",
        action="store_true",
        help="Skip rebuilding the cleaned L5=5 board from step8 (xlsx / gifts only)",
    )
    args = ap.parse_args(argv)

    if args.screenshot:
        print(SCREENSHOT_MSG)
        return 2
    if args.file and screenshot_message(args.file):
        print(SCREENSHOT_MSG)
        return 2

    rows: list[dict[str, Any]] = []
    if args.file:
        dest = Path(args.file)
        if not dest.is_file():
            print(f"FAIL\nfile not found: {dest}", file=sys.stderr)
            return 2
        rows.extend(parse_legs(dest.read_text(encoding="utf-8")))
    if not sys.stdin.isatty() and not args.file and not args.legs:
        rows.extend(parse_legs(sys.stdin.read()))
    for item in args.legs:
        rows.append(parse_leg_arg(item))
    if not rows:
        print("FAIL\nno legs — paste text or pass --legs / --file", file=sys.stderr)
        return 2

    injected: object | None = None
    src = str(args.source or "").strip()
    if str(args.quote or "").strip():
        if args.place:
            injected = {
                "payout_x": _f(args.quote),
                "payout_source": src,
                "captured_at": _now_iso() if src else "",
                "note": "CLI --quote (user-scraped N-correct)",
            }
        else:
            injected = {
                "payout_x": _f(args.quote),
                "payout_source": src or "n_correct_live",
                "captured_at": _now_iso(),
                "note": "CLI --quote (user-scraped N-correct)",
            }
    rec = check_slip(
        rows,
        date=args.date,
        product=args.product,
        quote=injected,
        write=not args.no_log,
        load_boards=not args.no_boards,
        place=bool(args.place),
        source=src,
    )
    if args.json:
        print(json.dumps({k: v for k, v in rec.items() if k != "quote_meta"} | {"quote_meta": rec.get("quote_meta")}, indent=2))
    else:
        print(format_result(rec))
    return 0 if rec.get("verdict") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
