#!/usr/bin/env python3
"""Gift scan after every fetch (Steps 3–5). Observe only.

  py -3.14 -m ml.gift_scan
  py -3.14 -m ml.gift_scan --date 2026-10-25

Does not import or edit the frozen NFL tracks. Alerts fire only when
every adverse-selection check passes. Failed gifts are still logged.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from ml.floor_persistence import OUT_DIR as FLOOR_DIR, lookup_p_clear  # noqa: E402
from ml.volume_floor import is_volume_prop  # noqa: E402
from utils.n_correct_payout import lookup_live_cdp_scrape  # noqa: E402
from utils.nba_proj_edge_snapshot import row_still_pre_tip  # noqa: E402
from utils.nba_slate_minutes import (  # noqa: E402
    _norm_name,
    _norm_team,
    _parse_tip,
    classify_injury_status,
    load_pre_tip_lookup,
)
from utils.prop_norm import canon_prop  # noqa: E402
from utils.scraped_quote import is_real_scraped_quote  # noqa: E402
from utils.stat_attach_guard import row_qualifies_for_list  # noqa: E402
from utils.stat_attach_invalid import is_analysis_invalid_prop  # noqa: E402
from utils.volume_floor_observe import (  # noqa: E402
    face_verified_step1,
    l5_current_season_n,
    l5_floor,
    role_tag,
)

PREREG = _REPO / "ml" / "prereg" / "gift_scan_2026-10-25.json"
OUT_DIR = _REPO / "data" / "reports" / "gift_scan"
LOG = OUT_DIR / "gift_scan.jsonl"
SLIP_LOG = OUT_DIR / "gift_slips.jsonl"
LINE_SNAP = OUT_DIR / "line_snapshot.json"
EDGE = 0.05
GOBLIN_POWER3_QUOTE = 1.65
GOBLIN_BE_SMALL_DELTA = round(GOBLIN_POWER3_QUOTE ** (-1.0 / 3), 4)  # 0.846 at GΔ≈1
STD_BE = {"2": 0.577, "3": 0.550}
GOBLIN_MIN_K = 1
MIN_QUOTE = 1.4
GOBLIN_QUOTE_FLOOR = 1.01
BAD_STATUS = frozenset({"Q", "QUESTIONABLE", "D", "DOUBTFUL", "OUT", "O"})
FOLDERS = {
    "NBA": "nba",
    "WNBA": "wnba",
    "NHL": "nhl",
    "NFL": "nfl",
    "CFB": "cfb",
    "MLB": "mlb",
    "Soccer": "soccer",
    "Tennis": "tennis",
}


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _pick(raw: object) -> str:
    k = str(raw or "").strip().lower()
    if "gob" in k:
        return "goblin"
    if "dem" in k:
        return "demon"
    if k in {"standard", "std"} or "standard" in k:
        return "standard"
    return ""


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"U", "UNDER", "LESS"}:
        return "UNDER"
    if raw in {"O", "OVER", "MORE"}:
        return "OVER"
    return raw


def load_prereg(*, path: Path | None = None) -> dict[str, Any]:
    return json.loads((path or PREREG).read_text(encoding="utf-8"))


def assert_prereg(*, path: Path | None = None) -> dict[str, Any]:
    spec = load_prereg(path=path)
    assert spec["track"] == "gift_scan"
    assert spec["prereg_date"] == "2026-10-25"
    assert spec["alert_only"] == "all_checks_pass"
    assert spec["log_failed_checks"] is True
    assert spec["placed_default"] is False
    return spec


def floor_k(floor: float, line: float) -> int | None:
    gap = floor - line
    if gap < 0:
        return None
    if gap < 1:
        return 0
    if gap < 2:
        return 1
    return 2


def _twin_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("sport") or "").strip().upper(),
        _fold(row.get("player")),
        canon_prop(row.get("sport"), row.get("prop") or row.get("prop_type")),
    )


def standard_twins(board: list[dict[str, Any]]) -> dict[tuple[str, str, str], float]:
    """Highest Standard face per player-prop, for Goblin GΔ."""
    out: dict[tuple[str, str, str], float] = {}
    for row in board:
        if _pick(row.get("pick_type")) != "standard":
            continue
        line = _f(row.get("line"))
        if line is None:
            continue
        key = _twin_key(row)
        prev = out.get(key)
        if prev is None or line > prev:
            out[key] = line
    return out


def goblin_line_delta(
    row: dict[str, Any],
    *,
    twins: dict[tuple[str, str, str], float] | None = None,
) -> float | None:
    """Standard twin minus Goblin line (GΔ). Existing delta fields win."""
    for key in (
        "goblin_delta",
        "line_distance",
        "line_discount_vs_standard",
        "goblin_line_delta",
    ):
        num = _f(row.get(key))
        if num is not None and num >= 0.25:
            return num
    std = _f(row.get("standard_line") or row.get("Standard Line") or row.get("std_line"))
    if std is None and twins:
        std = twins.get(_twin_key(row))
    line = _f(row.get("line"))
    if std is None or line is None:
        return None
    delta = std - line
    return delta if delta >= 0.25 else None


def _fmt_delta(delta: float) -> str:
    return f"{delta:g}"


def _delta_sig_parts(sig: object) -> list[float]:
    text = str(sig or "").strip()
    if not text or text in {"—", "-", "unknown"}:
        return []
    out: list[float] = []
    for part in text.replace("|", "+").replace(",", "+").split("+"):
        num = _f(part)
        if num is not None:
            out.append(num)
    return out


def _goblin_discount_per_unit(*, repo: Path | None = None) -> float | None:
    """Fitted GΔ discount from the scraped rate card. None if the card is empty."""
    root = repo or _REPO
    for rel in (
        Path("data") / "reports" / "payout_rate_card.json",
        Path("data") / "payout_rate_cards.json",
    ):
        dest = root / rel
        if not dest.is_file():
            continue
        try:
            raw = json.loads(dest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        fitted = raw.get("goblin_discount_per_unit")
        if fitted is None:
            fitted = (raw.get("fitted_file") or {}).get("goblin_discount_per_unit")
        if isinstance(fitted, dict):
            vals = [_f(v) for v in fitted.values()]
            vals = [v for v in vals if v is not None and v > 0]
            if vals:
                return sum(vals) / len(vals)
        num = _f(fitted)
        if num is not None and num > 0:
            return num
    return None


def _real_goblin_power3_quotes(delta: float, *, repo: Path | None = None) -> list[float]:
    """live_cdp / n_correct_live 0S+3G Power multiples that cover this GΔ."""
    root = repo or _REPO
    quotes: list[float] = []
    sig = "+".join(_fmt_delta(delta) for _ in range(3))
    found = lookup_live_cdp_scrape("", 0, 3, 3, "Power", sig, repo=root)
    if found and is_real_scraped_quote(found.get("payout_source"), found.get("captured_at")):
        px = _f((found.get("n_correct") or {}).get(3))
        if px is not None and px > 1:
            quotes.append(px)
    for rel in (
        Path("ui_runner") / "data" / "payout_ladder_live_cdp.json",
        Path("data") / "reports" / "sg_delta_payout_rate_card_latest.json",
        Path("data") / "reports" / "sg_delta_payout_rate_card_2026-10-25.json",
    ):
        dest = root / rel
        if not dest.is_file():
            continue
        try:
            payload = json.loads(dest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        rows = payload.get("rows") or payload.get("cells") or payload.get("cards") or []
        for rec in rows:
            if not isinstance(rec, dict):
                continue
            src = rec.get("payout_source") or rec.get("source")
            cap = rec.get("captured_at") or rec.get("last_captured_at") or rec.get("source_date")
            if not is_real_scraped_quote(src, cap):
                # sg_delta stamps live_cdp without captured_at — require the source family.
                if str(src or "").strip().lower() != "live_cdp":
                    continue
            try:
                n_legs = int(rec.get("n_legs") or 0)
                n_g = int(rec.get("n_g") or rec.get("n_goblin") or 0)
                n_s = int(rec.get("n_s") or rec.get("n_standard") or 0)
            except (TypeError, ValueError):
                continue
            if n_legs != 3 or n_g != 3 or n_s:
                continue
            parts = _delta_sig_parts(rec.get("goblin_delta_sig") or rec.get("goblin_deltas"))
            if not parts:
                continue
            # Same-delta 3-leg only. A deeper cell would overstate BE on a shallow GΔ.
            if not all(abs(p - delta) < 0.26 for p in parts):
                continue
            px = _f(
                rec.get("power_payout_x")
                or rec.get("power_min_x")
                or rec.get("power_x")
                or rec.get("min_x")
                or rec.get("sweep_payout_x")
            )
            if px is not None and px > 1:
                quotes.append(px)
    return quotes


def scraped_goblin_power_x(delta: float | None, *, repo: Path | None = None) -> float | None:
    """3-leg all-Goblin Power multiple for this GΔ. Deep lines pay less than 1.65x."""
    if delta is None or delta < 0:
        return None
    quotes = list(_real_goblin_power3_quotes(delta, repo=repo))
    dpu = _goblin_discount_per_unit(repo=repo)
    if dpu is not None:
        discounted = GOBLIN_POWER3_QUOTE * max(0.30, 1.0 - dpu * max(0.0, float(delta) - 1.0))
        if discounted > 1.0:
            quotes.append(discounted)
    if not quotes:
        return GOBLIN_POWER3_QUOTE if float(delta) <= 1.0 + 1e-9 else None
    px = min(quotes)
    if px <= 1.0:
        return None
    return max(GOBLIN_QUOTE_FLOOR, float(px))


def published_after_start(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
) -> bool:
    """True when fetch/publish is at or after this game's tip (NBA freeze lock).

    Same comparison as ``row_still_pre_tip`` / slate-minutes ``as_of < tip``.
    Unknown tip cannot prove a pre-tip board, so it is treated as after start.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    fetch = _parse_tip(
        row.get("fetched_at")
        or row.get("line_asof")
        or row.get("as_of")
        or row.get("slate_as_of")
    )
    obs = now
    if fetch is not None and fetch > obs:
        obs = fetch
    probe = {
        "tip_utc": row.get("tip_utc") or row.get("start_time") or row.get("game_time"),
        "game_window": row.get("game_window"),
        "slate_pre_tip": row.get("slate_pre_tip"),
    }
    return not row_still_pre_tip(probe, obs)


def rung_breakeven(
    rung: str,
    *,
    demon_quote_x: float | None = None,
    n_legs: int = 2,
    goblin_delta: float | None = None,
    goblin_quote_x: float | None = None,
    repo: Path | None = None,
) -> float | None:
    if rung == "goblin":
        px = goblin_quote_x
        if px is None:
            px = scraped_goblin_power_x(goblin_delta, repo=repo)
        if px is None or px <= 1:
            return None
        return round(float(px) ** (-1.0 / 3), 4)
    if rung == "standard":
        return STD_BE["3"] if n_legs >= 3 else STD_BE["2"]
    if rung == "demon":
        if demon_quote_x is None or demon_quote_x <= 1:
            return None
        return round(demon_quote_x ** (-1.0 / max(n_legs, 2)), 4)
    return None


def scraped_demon_power_x(*, path: Path | None = None) -> float | None:
    """Lowest live_cdp / n_correct_live all-Demon Power multiple, or None."""
    dest = path or (_REPO / "data" / "payout_rate_cards.json")
    if not dest.is_file():
        return None
    try:
        raw = json.loads(dest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    best: float | None = None
    for row in raw.get("rows") or raw.get("cards") or []:
        if not isinstance(row, dict):
            continue
        src = row.get("payout_source") or row.get("source")
        cap = row.get("captured_at")
        if not is_real_scraped_quote(src, cap):
            continue
        n_d = int(row.get("n_demon") or row.get("n_demons") or 0)
        n_s = int(row.get("n_standard") or row.get("n_s") or 0)
        n_g = int(row.get("n_goblin") or row.get("n_g") or 0)
        if n_d <= 0 or n_s or n_g:
            continue
        px = _f(row.get("power_x") or row.get("sweep_payout_x") or row.get("power_min_x"))
        if px is None or px <= 1:
            continue
        if best is None or px < best:
            best = px
    return best


def load_boards(root: Path, date: str) -> list[dict[str, Any]]:
    import rank_best_props_today as R

    rows: list[dict[str, Any]] = []
    loaders = (
        ("NBA", lambda: R.load_nba(root, date)),
        ("WNBA", lambda: R.load_sport(root, date, "WNBA", "wnba", "step8_wnba_direction.csv")),
        ("NHL", lambda: R.load_nhl(root, date)),
        ("NFL", lambda: R.load_nfl(root, date)),
        ("CFB", lambda: R.load_cfb(root, date)),
        ("MLB", lambda: R.load_mlb(root, date)),
        ("Soccer", lambda: R.load_sport(root, date, "SOCCER", "soccer", "step8_soccer_direction.csv")),
        ("Tennis", lambda: R.load_sport(root, date, "TENNIS", "tennis", "step8_tennis_direction.csv")),
    )
    for sport, fn in loaders:
        try:
            df = fn()
        except Exception:
            continue
        if df is None or getattr(df, "empty", True):
            continue
        recs = R.recs(df)
        games_by: dict[tuple[str, str, float | None], dict[str, Any]] = {}
        for raw in df.to_dict("records"):
            key = (
                _fold(raw.get("player") or raw.get("Player")),
                str(raw.get("prop_type") or raw.get("prop") or "").strip().lower(),
                _f(raw.get("line") or raw.get("Line")),
            )
            extra: dict[str, Any] = {}
            for i in range(1, 6):
                for col in (f"current_stat_g{i}", f"Current G{i}", f"stat_g{i}", f"Stat G{i}"):
                    if raw.get(col) is not None and str(raw.get(col)).strip() not in {"", "nan"}:
                        extra[col] = raw.get(col)
            for col in (
                "fetched_at",
                "line_asof",
                "as_of",
                "slate_as_of",
                "standard_line",
                "Standard Line",
                "line_distance",
                "line_discount_vs_standard",
                "goblin_delta",
                "tip_utc",
            ):
                val = raw.get(col)
                if val is not None and str(val).strip() not in {"", "nan", "None"}:
                    extra[col] = val
            if extra:
                games_by[key] = extra
        for rec in recs:
            rec["sport"] = rec.get("sport") or sport
            extra = games_by.get(
                (
                    _fold(rec.get("player")),
                    str(rec.get("prop") or "").strip().lower(),
                    _f(rec.get("line")),
                )
            )
            if extra:
                rec.update(extra)
            rows.append(rec)
    return rows


def team_prop_ranks(board: list[dict[str, Any]]) -> dict[tuple[str, str, str], int]:
    by: dict[tuple[str, str], dict[str, float]] = {}
    for row in board:
        sport = str(row.get("sport") or "").strip().upper()
        team = _fold(row.get("team"))
        player = _fold(row.get("player"))
        avg = _f(row.get("avg_l5") or row.get("stat_last5_avg") or row.get("l5_avg"))
        if not sport or not team or not player or avg is None:
            continue
        slot = by.setdefault((sport, team), {})
        prev = slot.get(player)
        if prev is None or avg > prev:
            slot[player] = avg
    ranks: dict[tuple[str, str, str], int] = {}
    for (sport, team), pmap in by.items():
        ordered = sorted(pmap.items(), key=lambda kv: kv[1], reverse=True)
        for i, (player, _s) in enumerate(ordered, start=1):
            ranks[(sport, team, player)] = i
    return ranks


def injury_raw(row: dict[str, Any]) -> str:
    for key in (
        "player_injury_status",
        "injury_status",
        "injury",
        "status",
        "player_status",
    ):
        raw = str(row.get(key) or "").strip()
        if raw:
            return raw
    return ""


def injury_blocks_player(row: dict[str, Any]) -> bool:
    classified = classify_injury_status(injury_raw(row))
    if classified in {"OUT", "DOUBTFUL", "QUESTIONABLE"}:
        return True
    fold = _fold(injury_raw(row))
    return fold in {"q", "d", "out", "questionable", "doubtful"}


def teammate_role_flagged(
    row: dict[str, Any],
    board: list[dict[str, Any]],
    ranks: dict[tuple[str, str, str], int],
) -> str:
    sport = str(row.get("sport") or "").strip().upper()
    team = _fold(row.get("team"))
    player = _fold(row.get("player"))
    pos = _fold(row.get("position") or row.get("pos"))
    flagged: list[str] = []
    for other in board:
        if str(other.get("sport") or "").strip().upper() != sport:
            continue
        if _fold(other.get("team")) != team:
            continue
        op = _fold(other.get("player"))
        if not op or op == player:
            continue
        share = False
        rank = ranks.get((sport, team, op))
        if rank is not None and rank <= 2:
            share = True
        if pos and _fold(other.get("position") or other.get("pos")) == pos:
            share = True
        if not share:
            continue
        if injury_blocks_player(other):
            flagged.append(str(other.get("player") or op))
    return ",".join(sorted(set(flagged)))


def load_line_snapshot(path: Path | None = None) -> dict[str, float]:
    dest = path or LINE_SNAP
    if not dest.is_file():
        return {}
    try:
        raw = json.loads(dest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    lines = raw.get("lines") if isinstance(raw, dict) else raw
    if not isinstance(lines, dict):
        return {}
    out: dict[str, float] = {}
    for key, val in lines.items():
        num = _f(val)
        if num is not None:
            out[str(key)] = num
    return out


def line_key(row: dict[str, Any], rung: str) -> str:
    return "|".join(
        (
            str(row.get("sport") or "").upper(),
            _fold(row.get("player")),
            canon_prop(row.get("sport"), row.get("prop") or row.get("prop_type")),
            rung,
        )
    )


def line_dropped(row: dict[str, Any], rung: str, prev: dict[str, float]) -> bool:
    key = line_key(row, rung)
    if key not in prev:
        return False
    now = _f(row.get("line"))
    if now is None:
        return False
    return now < prev[key] - 1e-9


def nba_role_confirmed(row: dict[str, Any], lookup: dict[tuple[str, str, str], dict[str, Any]]) -> tuple[bool, str]:
    slate = str(row.get("game_date") or row.get("date") or "")[:10]
    rec = None
    for team in (
        _norm_team(row.get("team")),
        _fold(row.get("team")),
        str(row.get("team") or "").strip().upper(),
    ):
        for name in (
            _norm_name(row.get("player")),
            _fold(row.get("player")),
            str(row.get("player") or "").strip().lower(),
        ):
            rec = lookup.get((slate, team, name))
            if rec:
                break
        if rec:
            break
    if not rec:
        return False, "nba_t30_missing"
    starter = str(rec.get("tonight_starter") or "").strip().lower()
    inj = classify_injury_status(rec.get("player_injury_status"))
    if inj in {"OUT", "DOUBTFUL"} or starter in {"out", "bench"}:
        return False, f"nba_role_{starter or 'unknown'}"
    if starter in {"confirmed", "projected"}:
        return True, f"nba_{starter}"
    return False, "nba_role_unconfirmed"


def nfl_inactive(row: dict[str, Any], inactives: set[str]) -> tuple[bool, str]:
    if not inactives:
        return False, "nfl_inactives_unposted"
    name = _fold(row.get("player"))
    if name and name in inactives:
        return False, "nfl_inactive"
    return True, "nfl_active"


def role_confirmed(
    row: dict[str, Any],
    *,
    nba_lookup: dict[tuple[str, str, str], dict[str, Any]],
    nfl_inactives: set[str],
) -> tuple[bool, str]:
    sport = str(row.get("sport") or "").strip().upper()
    if sport == "NBA":
        return nba_role_confirmed(row, nba_lookup)
    if sport == "NFL":
        return nfl_inactive(row, nfl_inactives)
    return True, "n_a"


def p_clear_cell(
    sport: str,
    prop: str,
    role: str,
    k: int,
) -> dict[str, Any] | None:
    for try_role in (role, "top2" if role == "lead" else "", "all"):
        if not try_role:
            continue
        hit = lookup_p_clear(sport, prop, try_role, k)
        if hit:
            hit = dict(hit)
            hit["role_used"] = try_role
            return hit
    return None


def validity(row: dict[str, Any], *, face_ok: bool) -> tuple[bool, str]:
    ok, why = row_qualifies_for_list(row)
    if not ok:
        return False, why or "stat_status_not_ok"
    if not face_ok:
        return False, "face_unverified"
    sport = str(row.get("sport") or "").strip()
    prop = row.get("prop") or row.get("prop_type")
    if is_analysis_invalid_prop(sport, prop):
        return False, "mapping_fail"
    league = str(row.get("league") or sport).strip().upper()
    if league.endswith(("1H", "1Q", "2H", "4Q")):
        return False, "period_slate"
    if _side(row) == "UNDER":
        return False, "under"
    if _pick(row.get("pick_type")) not in {"goblin", "standard", "demon"}:
        return False, "no_rung"
    if _f(row.get("line")) is None:
        return False, "no_line"
    if l5_floor(row) is None:
        return False, "no_floor"
    return True, "ok"


def adverse_checks(
    row: dict[str, Any],
    *,
    rung: str,
    k: int,
    board: list[dict[str, Any]],
    ranks: dict[tuple[str, str, str], int],
    prev_lines: dict[str, float],
    nba_lookup: dict[tuple[str, str, str], dict[str, Any]],
    nfl_inactives: set[str],
) -> dict[str, Any]:
    teammate = teammate_role_flagged(row, board, ranks)
    role_ok, role_why = role_confirmed(row, nba_lookup=nba_lookup, nfl_inactives=nfl_inactives)
    checks = {
        "injury_player": not injury_blocks_player(row),
        "injury_teammate_role": not bool(teammate),
        "line_dropped": not line_dropped(row, rung, prev_lines),
        "role_confirmed": role_ok,
    }
    return {
        "checks": checks,
        "all_pass": all(checks.values()),
        "teammate_flagged": teammate,
        "role_why": role_why,
        "injury_raw": injury_raw(row),
        "k": k,
    }


def is_gift(rung: str, k: int, p_clear: float, be: float | None) -> bool:
    if be is None:
        return False
    if rung == "goblin" and (k is None or k < GOBLIN_MIN_K):
        return False
    return (p_clear - be) >= EDGE


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def push_ntfy(body: str, *, title: str = "gift") -> bool:
    topic = str(os.environ.get("PROPORACLE_NTFY_TOPIC") or "").strip()
    if not topic:
        return False
    url = str(os.environ.get("PROPORACLE_NTFY_URL") or f"https://ntfy.sh/{topic}").strip()
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"Title": title, "Tags": "gift,warning"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return 200 <= int(resp.status) < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def alert_body(gift: dict[str, Any]) -> str:
    return (
        f"{gift.get('player')} {gift.get('prop')} {gift.get('rung')} "
        f"O{gift.get('line')}  L5min={gift.get('l5_floor')} k={gift.get('k')}  "
        f"P(clear)={gift.get('p_clear')}  BE={gift.get('breakeven')}  "
        f"{gift.get('sport')} {gift.get('role_tag')}"
    )


def suggest_slips(
    clean: list[dict[str, Any]],
    *,
    demon_x: float | None,
    now: str,
) -> list[dict[str, Any]]:
    """2–3 gift legs, different games, one player, Flex default."""
    slips: list[dict[str, Any]] = []
    used_players: set[str] = set()
    pool = [g for g in clean if g.get("all_pass")]
    pool.sort(key=lambda g: (-(g.get("k") or 0), -(g.get("edge") or 0)))
    i = 0
    while i < len(pool):
        legs: list[dict[str, Any]] = []
        games: set[str] = set()
        players: set[str] = set()
        for g in pool[i:]:
            player = _fold(g.get("player"))
            game = str(g.get("start_time") or g.get("opponent") or g.get("team") or "")
            if player in used_players or player in players:
                continue
            if game and game in games:
                continue
            legs.append(g)
            players.add(player)
            if game:
                games.add(game)
            if len(legs) == 3:
                break
        if len(legs) < 2:
            break
        n = len(legs)
        rungs = [str(l.get("rung")) for l in legs]
        be_parts = [l.get("p_clear") for l in legs]
        if any(p is None for p in be_parts):
            i += 1
            continue
        sweep = 1.0
        for p in be_parts:
            sweep *= float(p)
        # Quote: Standard-only 3x/6x; mixed / Demon needs a scrape.
        quote = None
        quote_src = ""
        if all(r == "standard" for r in rungs):
            quote = 3.0 if n == 2 else 6.0
            quote_src = "canonical_standard_n_correct"
        elif all(r == "goblin" for r in rungs) and n == 3:
            quote = 1.65
            quote_src = "scraped_goblin_power_p25"
        elif all(r == "demon" for r in rungs) and demon_x:
            quote = demon_x
            quote_src = "scraped_demon_rate_card"
        if quote is None or quote < MIN_QUOTE:
            i += 1
            continue
        ev = sweep * quote - 1.0
        if ev <= 0:
            i += 1
            continue
        rec = {
            "ts": now,
            "kind": "slip",
            "placed": False,
            "n_legs": n,
            "quote": quote,
            "quote_source": quote_src,
            "real_quote": quote_src.startswith("scraped"),
            "sweep_p": round(sweep, 4),
            "ev": round(ev, 4),
            "legs": [
                {
                    "player": l.get("player"),
                    "sport": l.get("sport"),
                    "prop": l.get("prop"),
                    "rung": l.get("rung"),
                    "line": l.get("line"),
                    "k": l.get("k"),
                    "p_clear": l.get("p_clear"),
                }
                for l in legs
            ],
        }
        slips.append(rec)
        used_players.update(players)
        i += 1
    return slips


def scan(
    *,
    date: str,
    root: Path | None = None,
    board: list[dict[str, Any]] | None = None,
    nba_lookup: dict[tuple[str, str, str], dict[str, Any]] | None = None,
    nfl_inactives: set[str] | None = None,
    prev_lines: dict[str, float] | None = None,
    demon_x: float | None = None,
    alert: bool = True,
    write: bool = True,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    assert_prereg()
    root = root or _REPO
    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    now = as_of.isoformat()
    rows = board if board is not None else load_boards(root, date)
    ranks = team_prop_ranks(rows)
    twins = standard_twins(rows)
    prev = prev_lines if prev_lines is not None else load_line_snapshot()
    nba_lu = nba_lookup if nba_lookup is not None else load_pre_tip_lookup()
    nfl_in = nfl_inactives if nfl_inactives is not None else set()
    if demon_x is None:
        demon_x = scraped_demon_power_x()

    face_by_sport: dict[str, bool] = {}
    for sport, folder in FOLDERS.items():
        face_by_sport[sport.upper()] = face_verified_step1(root, date, folder)

    gifts: list[dict[str, Any]] = []
    skipped: dict[str, int] = {}
    new_lines: dict[str, float] = {}
    for row in rows:
        sport = str(row.get("sport") or "").strip()
        sport_u = sport.upper()
        face_ok = face_by_sport.get(sport_u, False)
        ok, why = validity(row, face_ok=face_ok)
        if not ok:
            skipped[why] = skipped.get(why, 0) + 1
            continue
        rung = _pick(row.get("pick_type"))
        line = _f(row.get("line"))
        floor = l5_floor(row)
        if line is None or floor is None:
            skipped["no_floor"] = skipped.get("no_floor", 0) + 1
            continue
        k = floor_k(floor, line)
        if k is None:
            skipped["line_above_floor"] = skipped.get("line_above_floor", 0) + 1
            continue
        new_lines[line_key(row, rung)] = line
        rank = ranks.get((sport_u, _fold(row.get("team")), _fold(row.get("player"))))
        role = "lead" if rank == 1 else ("top2" if rank is not None and rank <= 2 else "all")
        cell = p_clear_cell(sport, row.get("prop") or row.get("prop_type") or "", role, k)
        if not cell:
            skipped["no_floor_cell"] = skipped.get("no_floor_cell", 0) + 1
            continue
        p = _f(cell.get("hr"))
        if p is None:
            skipped["no_p"] = skipped.get("no_p", 0) + 1
            continue
        gdelta = goblin_line_delta(row, twins=twins) if rung == "goblin" else None
        be = rung_breakeven(
            rung,
            demon_quote_x=demon_x if rung == "demon" else None,
            goblin_delta=gdelta,
            repo=root,
        )
        if not is_gift(rung, k, p, be):
            skipped["not_above_be"] = skipped.get("not_above_be", 0) + 1
            continue
        adv = adverse_checks(
            row,
            rung=rung,
            k=k,
            board=rows,
            ranks=ranks,
            prev_lines=prev,
            nba_lookup=nba_lu,
            nfl_inactives=nfl_in,
        )
        minutes = _f(row.get("minutes") or row.get("minutes_proj") or row.get("snap_pct"))
        rec = {
            "ts": now,
            "date": date,
            "kind": "gift",
            "placed": False,
            "player": row.get("player"),
            "team": row.get("team"),
            "opponent": row.get("opp_team") or row.get("opp"),
            "sport": sport,
            "prop": row.get("prop") or row.get("prop_type"),
            "prop_canon": canon_prop(sport, row.get("prop") or row.get("prop_type")),
            "rung": rung,
            "line": line,
            "l5_floor": floor,
            "k": k,
            "p_clear": p,
            "p_n": cell.get("n"),
            "p_wilson": cell.get("wilson_low"),
            "breakeven": be,
            "edge": round(p - (be or 0), 4),
            "role": role,
            "role_tag": role_tag(rank, minutes, backup_below=40.0 if sport_u == "NFL" else 18.0),
            "role_used": cell.get("role_used"),
            "volume": is_volume_prop(sport, canon_prop(sport, row.get("prop") or row.get("prop_type"))),
            "l5_current_season_n": (
                l5_current_season_n(row) if sport_u == "NBA" else None
            ),
            "face_verified": face_ok,
            "all_pass": adv["all_pass"],
            "checks": adv["checks"],
            "teammate_flagged": adv["teammate_flagged"],
            "role_why": adv["role_why"],
            "injury_raw": adv["injury_raw"],
            "start_time": row.get("start_time") or row.get("game_time"),
            "fetched_at": row.get("fetched_at") or row.get("line_asof") or row.get("as_of"),
            "goblin_delta": gdelta,
            "published_after_start": published_after_start(row, now=as_of),
        }
        gifts.append(rec)

    measure = [g for g in gifts if not g.get("published_after_start")]
    hindsight = [g for g in gifts if g.get("published_after_start")]
    clean = [g for g in measure if g.get("all_pass")]
    flagged = [g for g in measure if not g.get("all_pass")]
    alerts = 0
    if alert:
        for g in clean:
            if push_ntfy(alert_body(g), title=f"gift {g.get('sport')} k={g.get('k')}"):
                g["alerted"] = True
                alerts += 1
            else:
                g["alerted"] = False
    slips = suggest_slips(clean, demon_x=demon_x, now=now)
    if write:
        _append_jsonl(LOG, gifts)
        _append_jsonl(SLIP_LOG, slips)
        LINE_SNAP.parent.mkdir(parents=True, exist_ok=True)
        LINE_SNAP.write_text(
            json.dumps({"date": date, "ts": now, "lines": new_lines}, indent=2),
            encoding="utf-8",
        )
        summary = {
            "date": date,
            "ts": now,
            "n_board": len(rows),
            "n_gifts": len(gifts),
            "n_measure": len(measure),
            "n_published_after_start": len(hindsight),
            "n_clean": len(clean),
            "n_flagged": len(flagged),
            "n_alerts": alerts,
            "n_slips": len(slips),
            "skipped": skipped,
            "gifts": gifts,
            "slips": slips,
        }
        (OUT_DIR / "gift_scan_latest.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        (OUT_DIR / f"gift_scan_{date}.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        _write_md(summary)
    return {
        "n_board": len(rows),
        "n_gifts": len(gifts),
        "n_measure": len(measure),
        "n_published_after_start": len(hindsight),
        "n_clean": len(clean),
        "n_flagged": len(flagged),
        "n_alerts": alerts,
        "n_slips": len(slips),
        "skipped": skipped,
        "gifts": gifts,
        "slips": slips,
    }


def _write_md(summary: dict[str, Any]) -> None:
    lines = [
        f"# Gift scan {summary.get('date')}",
        "",
        "Observe only. Clean gifts may alert. Flagged gifts stay in the log. "
        "published_after_start rows stay in the raw log and are out of the measurement pool.",
        "",
        f"Board {summary.get('n_board')} · gifts {summary.get('n_gifts')} "
        f"(measure {summary.get('n_measure')} / after-start {summary.get('n_published_after_start')}; "
        f"clean {summary.get('n_clean')} / flagged {summary.get('n_flagged')}) · "
        f"alerts {summary.get('n_alerts')} · slips {summary.get('n_slips')}",
        "",
        "| Player | Sport | Prop | Rung | Line | GΔ | Floor | k | P | BE | Pass | Why |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for g in summary.get("gifts") or []:
        fails = [k for k, v in (g.get("checks") or {}).items() if not v]
        why = ",".join(fails) or g.get("role_why") or ""
        if g.get("published_after_start"):
            why = "published_after_start" + (f",{why}" if why else "")
        gdelta = g.get("goblin_delta")
        gdelta_s = "" if gdelta is None else gdelta
        lines.append(
            f"| {g.get('player')} | {g.get('sport')} | {g.get('prop')} | {g.get('rung')} | "
            f"{g.get('line')} | {gdelta_s} | {g.get('l5_floor')} | {g.get('k')} | {g.get('p_clear')} | "
            f"{g.get('breakeven')} | {'Y' if g.get('all_pass') and not g.get('published_after_start') else ''} | "
            f"{why} |"
        )
    if summary.get("slips"):
        lines.extend(["", "## Slips", ""])
        for s in summary["slips"]:
            names = ", ".join(
                f"{l.get('player')} {l.get('rung')} O{l.get('line')}" for l in s.get("legs") or []
            )
            lines.append(
                f"- {s.get('n_legs')}-leg quote {s.get('quote')}x EV {s.get('ev')} — {names}"
            )
    dest = OUT_DIR / "gift_scan_latest.md"
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Gift scan with adverse-selection checks")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: ET today)")
    ap.add_argument("--no-alert", action="store_true")
    args = ap.parse_args()
    date = args.date
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    res = scan(date=date, alert=not args.no_alert)
    print(
        json.dumps(
            {k: v for k, v in res.items() if k not in {"gifts", "slips"}},
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
