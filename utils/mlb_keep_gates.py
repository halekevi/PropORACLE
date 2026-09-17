"""MLB Goblin OVER keep props 1–10 and per-prop gates.

Fade 11–21 (Runs, Singles, batter Walks, PA, RBIs, 1st-inning, HR/2B/3B/SB),
Standard, and Demons. Same gate for the printed Goblin list and Goblin-70.

Pitcher-side volume props use **L10≥8-primary** (L5 dropped): catalog shows
L10 alone at real n beats L5=5+L10≥8 at near-zero joint n, and L5 often hurts.

  1 Walks Allowed:     L10>=8 + Own pitch Weak|Below (leaky staff)
  2 Pitches Thrown:    L10>=8 + Own pitch Weak|Below
  3 ERA:               Goblin 0.5 + L5=5 + L10>=8 (no rank; line-bound)
  4 Pitcher Ks:        L10>=8 + Own pitch Elite|Above (stingy staff)
  5 Hits Allowed:      L10>=8 + Opp bats Strong|Above
  6 Pitching Outs:     L10>=8 + Own pitch Elite|Above
  7 H+R+RBI:           BA>=.275 + L5=5 + Opp pitch Weak|Below (80.8% n=104)
  8 Hitter Ks:         K%>=28 + L10>=8 + Opp pitch Elite|Above (76.6% n=325)
  9 Hits / 10 TB:      BA>=.275 + L5=5 + Opp pitch Weak|Below (71% n~170)
"""
from __future__ import annotations

import csv
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from utils.defense_tiers import normalize_def_tier_label
from utils.mlb_prop_matchup import batting_strength_label
from utils.prop_norm import canon_prop as _canon

# Pitching stays leaky/stingy (not inverted). Batting uses Strong|Above display.
PROD = frozenset({"Weak", "Below Avg"})
STINGY = frozenset({"Elite", "Above Avg"})
STRONG_BAT = frozenset({"Strong", "Above Avg"})

KEEP_PROPS = frozenset(
    {
        "walks_allowed",
        "pitches_thrown",
        "earned_runs",
        "pitcher_ks",
        "hits_allowed",
        "pitching_outs",
        "hits+runs+rbis",
        "hitter_ks",
        "hits",
        "total_bases",
    }
)

_ALIAS = {"ARI": "AZ", "CHW": "CWS", "OAK": "ATH", "WSN": "WSH", "WAS": "WSH"}
_REPO = Path(__file__).resolve().parents[1]
_DEF_CSV = _REPO / "Sports" / "MLB" / "mlb_defense_summary.csv"
_SLASH_CSV = _REPO / "Sports" / "MLB" / "data" / "mlb_hitter_season_slash.csv"
_ID_CACHE = _REPO / "Sports" / "MLB" / "mlb_id_cache.csv"
BA_FLOOR = 0.275
K_RATE_FLOOR = 0.28
MIN_AB = 80
_STATS_URL = (
    "https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting"
    "&season=2026&gameType=R&sportId=1&playerPool=all&limit=2000"
)


def _num(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _side(r: dict[str, Any]) -> str:
    return str(r.get("side") or "").strip().upper()


def _prop(r: dict[str, Any]) -> str:
    return _canon("MLB", str(r.get("prop") or r.get("prop_type") or ""))


def _l5(r: dict[str, Any]) -> float | None:
    if _side(r) == "UNDER":
        keys = ("l5_under", "last5_under", "l5")
    else:
        keys = ("l5_over", "last5_over", "l5")
    for k in keys:
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _l10(r: dict[str, Any]) -> float | None:
    if _side(r) == "UNDER":
        keys = ("l10_under", "last10_under", "line_hits_under_10", "l10")
    else:
        keys = ("l10_over", "last10_over", "line_hits_over_10", "l10")
    for k in keys:
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _cover(r: dict[str, Any]) -> float | None:
    v = r.get("dist_l5")
    if v is None:
        v = r.get("cover")
    return _num(v)


def _abbr(raw: object) -> str:
    s = str(raw or "").strip().upper()
    return _ALIAS.get(s, s)


def _norm_name(s: object) -> str:
    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _parse_avg(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if s.startswith("."):
        s = "0" + s
    return _num(s)


def _k_rate_from_row(r: dict[str, Any]) -> float | None:
    v = _num(r.get("k_rate") if r.get("k_rate") not in (None, "") else None)
    if v is None:
        v = _num(r.get("k_pct") if r.get("k_pct") not in (None, "") else None)
    if v is None:
        v = _num(r.get("so_rate") if r.get("so_rate") not in (None, "") else None)
    if v is None:
        return None
    if v > 1.0:
        return v / 100.0
    return v


@lru_cache(maxsize=1)
def _slash_tables() -> tuple[dict[str, dict], dict[str, dict]]:
    by_id: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    if not _SLASH_CSV.is_file():
        return by_id, by_name
    with _SLASH_CSV.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            pid = str(row.get("mlb_player_id") or "").strip()
            rec = {
                "id": pid,
                "name": str(row.get("player") or ""),
                "avg": _parse_avg(row.get("avg")),
                "ab": _num(row.get("ab")),
                "k_rate": _num(row.get("k_rate")),
                "so": _num(row.get("so")),
                "pa": _num(row.get("pa")),
            }
            if pid:
                by_id[pid] = rec
            nm = _norm_name(rec["name"])
            if nm:
                by_name[nm] = rec
    return by_id, by_name


@lru_cache(maxsize=1)
def _id_by_name() -> dict[str, str]:
    p = _ID_CACHE
    if not p.is_file():
        p = _REPO / "Sports" / "MLB" / "scripts" / "mlb_id_cache.csv"
    if not p.is_file():
        return {}
    out: dict[str, str] = {}
    with p.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            nm = _norm_name(row.get("player_norm") or row.get("player"))
            pid = str(row.get("mlb_player_id") or "").strip()
            if nm and pid:
                out[nm] = pid
    return out


def _slash_from_cache(r: dict[str, Any]) -> dict | None:
    by_id, by_name = _slash_tables()
    pid = str(r.get("mlb_player_id") or "").strip()
    if pid and pid in by_id:
        return by_id[pid]
    name = _norm_name(r.get("player") or r.get("player_name"))
    if not name:
        return None
    if name in by_name:
        return by_name[name]
    mapped = _id_by_name().get(name)
    if mapped and mapped in by_id:
        return by_id[mapped]
    return None


def player_slash(r: dict[str, Any]) -> dict[str, float | None]:
    """Season AVG / K-rate. Row fields win; else the slash cache."""
    avg = _parse_avg(r.get("batting_avg") if r.get("batting_avg") not in (None, "") else None)
    if avg is None:
        avg = _parse_avg(r.get("ba") if r.get("ba") not in (None, "") else None)
    if avg is None:
        avg = _parse_avg(r.get("season_ba") if r.get("season_ba") not in (None, "") else None)
    kr = _k_rate_from_row(r)
    ab = _num(r.get("ab") or r.get("at_bats"))
    cached = _slash_from_cache(r)
    if cached:
        if avg is None:
            avg = cached.get("avg")
        if kr is None:
            kr = cached.get("k_rate")
        if ab is None:
            ab = cached.get("ab")
    return {"avg": avg, "k_rate": kr, "ab": ab}


def _ab_ok(slash: dict[str, float | None]) -> bool:
    ab = slash.get("ab")
    if ab is None:
        return True
    return ab >= MIN_AB


def _ba_ok(r: dict[str, Any]) -> bool:
    slash = player_slash(r)
    avg = slash.get("avg")
    if avg is None or not _ab_ok(slash):
        return False
    return avg >= BA_FLOOR


def _k28_ok(r: dict[str, Any]) -> bool:
    slash = player_slash(r)
    kr = slash.get("k_rate")
    if kr is None or not _ab_ok(slash):
        return False
    return kr >= K_RATE_FLOOR


def refresh_hitter_slash_cache() -> Path:
    """Fetch 2026 Stats API season hitting and write the slash cache."""
    import json
    import urllib.request

    req = urllib.request.Request(
        _STATS_URL, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = json.loads(resp.read().decode())
    _SLASH_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for split in (raw.get("stats") or [{}])[0].get("splits") or []:
        st = split.get("stat") or {}
        player = split.get("player") or {}
        pid = str(player.get("id") or "").strip()
        pa = _num(st.get("plateAppearances")) or 0.0
        so = _num(st.get("strikeOuts")) or 0.0
        rec = {
            "mlb_player_id": pid,
            "player": str(player.get("fullName") or ""),
            "avg": _parse_avg(st.get("avg")),
            "ab": _num(st.get("atBats")) or 0.0,
            "k_rate": (so / pa) if pa else "",
            "so": so,
            "pa": pa,
        }
        rows.append(rec)
    with _SLASH_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["mlb_player_id", "player", "avg", "ab", "k_rate", "so", "pa"]
        )
        w.writeheader()
        w.writerows(rows)
    _slash_tables.cache_clear()
    return _SLASH_CSV


@lru_cache(maxsize=1)
def _defense_by_team() -> dict[str, dict[str, str]]:
    import pandas as pd

    if not _DEF_CSV.is_file():
        return {}
    d = pd.read_csv(_DEF_CSV)
    d["TEAM_ABBREVIATION"] = d["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
    out: dict[str, dict[str, str]] = {}
    for row in d.to_dict("records"):
        rec = {
            "pitch": normalize_def_tier_label(row.get("DEF_TIER") or row.get("def_tier")) or "",
            "hits": normalize_def_tier_label(row.get("OFF_HITS_TIER")) or "",
        }
        ab = str(row["TEAM_ABBREVIATION"]).upper()
        out[ab] = rec
        for a, b in _ALIAS.items():
            if ab == b:
                out[a] = rec
            if ab == a:
                out[b] = rec
    return out


def own_pitching_tier(r: dict[str, Any]) -> str:
    t = normalize_def_tier_label(r.get("own_def_tier") or r.get("team_def_tier")) or ""
    if t:
        return t
    team = _abbr(r.get("team") or r.get("Team"))
    return (_defense_by_team().get(team) or {}).get("pitch") or ""


def own_off_hits_tier(r: dict[str, Any]) -> str:
    t = normalize_def_tier_label(r.get("own_off_hits_tier") or r.get("team_off_hits_tier")) or ""
    if t:
        return t
    team = _abbr(r.get("team") or r.get("Team"))
    return (_defense_by_team().get(team) or {}).get("hits") or ""


def opp_pitching_tier(r: dict[str, Any]) -> str:
    t = normalize_def_tier_label(
        r.get("opp_def_tier") or r.get("opp_pitch_tier") or r.get("opp_pitching_tier")
    ) or ""
    if t:
        return t
    opp = _abbr(r.get("opp_team") or r.get("Opp"))
    looked = (_defense_by_team().get(opp) or {}).get("pitch") or ""
    if looked:
        return looked
    axis = str(r.get("def_axis") or "")
    if axis.startswith("opp_offense"):
        return ""
    return normalize_def_tier_label(r.get("def") or r.get("d") or r.get("def_tier")) or ""


def _own_pitch(r: dict[str, Any]) -> str:
    return own_pitching_tier(r)


def _own_hits(r: dict[str, Any]) -> str:
    return own_off_hits_tier(r)


def _opp_pitch(r: dict[str, Any]) -> str:
    return opp_pitching_tier(r)


def _opp_hits(r: dict[str, Any]) -> str:
    t = normalize_def_tier_label(r.get("opp_off_hits_tier")) or ""
    if t:
        return t
    opp = _abbr(r.get("opp_team") or r.get("Opp"))
    looked = (_defense_by_team().get(opp) or {}).get("hits") or ""
    if looked:
        return looked
    axis = str(r.get("def_axis") or "")
    if axis == "opp_offense_hits" or _prop(r) == "hits_allowed":
        return normalize_def_tier_label(r.get("def") or r.get("def_tier")) or ""
    return ""


def opp_off_hits_tier(r: dict[str, Any]) -> str:
    return _opp_hits(r)


def _own_bat_strong(r: dict[str, Any]) -> bool:
    return batting_strength_label(_own_hits(r)) in STRONG_BAT


def _opp_bats_strong(r: dict[str, Any]) -> bool:
    return batting_strength_label(_opp_hits(r)) in STRONG_BAT


def mlb_keep_prop(r: dict[str, Any]) -> bool:
    return _prop(r) in KEEP_PROPS


def mlb_goblin_keep_eligible(r: dict[str, Any]) -> bool:
    """True when this Goblin OVER clears the locked 1–10 keep gate."""
    if str(r.get("pick_type") or "").strip() != "Goblin":
        return False
    if _side(r) != "OVER":
        return False
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in {"MLB", ""}:
        return False
    prop = _prop(r)
    if prop not in KEEP_PROPS:
        return False
    l5 = _l5(r)
    l10 = _l10(r)

    if prop == "walks_allowed":
        return l10 is not None and l10 >= 8 and _own_pitch(r) in PROD
    if prop == "pitches_thrown":
        return l10 is not None and l10 >= 8 and _own_pitch(r) in PROD
    if prop == "earned_runs":
        try:
            line = float(r.get("line"))
        except (TypeError, ValueError):
            return False
        if abs(line - 0.5) > 1e-9:
            return False
        return l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8
    if prop == "pitcher_ks":
        return l10 is not None and l10 >= 8 and _own_pitch(r) in STINGY
    if prop == "hits_allowed":
        return l10 is not None and l10 >= 8 and _opp_bats_strong(r)
    if prop == "pitching_outs":
        return l10 is not None and l10 >= 8 and _own_pitch(r) in STINGY
    if prop == "hits+runs+rbis":
        return l5 is not None and l5 >= 5 and _ba_ok(r) and _opp_pitch(r) in PROD
    if prop == "hitter_ks":
        return (
            l10 is not None
            and l10 >= 8
            and _k28_ok(r)
            and _opp_pitch(r) in STINGY
        )
    if prop in {"hits", "total_bases"}:
        return l5 is not None and l5 >= 5 and _ba_ok(r) and _opp_pitch(r) in PROD
    return False
