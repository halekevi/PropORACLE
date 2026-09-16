"""
build_leg_correlation_matrix.py

Builds a leg-correlation lookup table from historical graded props, for use in:
  (A) the ticket diversity/packer filter -- penalize/block correlated same-game stacks
  (B) EV pricing -- adjust joint P(A and B) instead of naive product P(A)*P(B)

Also runs a backtest: naive vs. correlation-adjusted joint probability against
historical tickets, to check whether the adjustment actually predicts the
May->June 2-leg win-rate bleed before this ships to the live packer/pricer.

PropORACLE schema (normalized in load_graded_props / load_tickets):
  graded_props_*.json  ->  {date, props:[{sport, player, team, opp_team, prop,
                            direction|over_under, ml_prob, hit|result, ...}]}
  tickets JSON         ->  {date, groups:[{tickets:[{ticket_id, legs:[{
                            sport, player, team, opp, prop_type, direction,
                            ml_prob|leg_prob_used, ...}]}]}]}
  ticket_eval HTML     ->  decided <article class="ticket-card all-hit|card-missed">
                            with <div class="legrow ..."> legs (May–June Lever B source)
  Derived: game_id = date|sport|sorted(team, opp); player_id = folded player name.
  No native game_id / player_id / model_prob on graded rows.

Lift independence baseline uses EMPIRICAL hit rates per (sport, stat, direction),
not model_prob — otherwise calibration drift leaks into shrunk_lift as fake
correlation. model_prob still drives adjusted_joint_prob pricing; cells also
emit calibration_gap_* diagnostics.

Usage:
    py -3.14 scripts/build_leg_correlation_matrix.py \\
        --graded-dir ui_runner/templates \\
        --out data/cache/leg_correlation_matrix.json \\
        --tickets ui_runner/templates/tickets_latest.json \\
        --backtest-out data/reports/corr_backtest.md

    # Lever B May–June population (matches grade_history daily n/wins):
    py -3.14 scripts/build_leg_correlation_matrix.py \\
        --graded-dir ui_runner/templates \\
        --reuse-matrix data/cache/leg_correlation_matrix.json \\
        --ticket-eval "ui_runner/templates/ticket_eval_2026-0[56]-*.html" \\
        --backtest-out data/reports/corr_backtest_may_june_ticket_eval.md \\
        --simulate-gates \\
        --composition-report data/reports/composition_may_june.csv \\
        --composition-min-n 30 \\
        --payout-table data/reports/composition_payout_n_correct.json \\
        --min-payout 2.0

Requires: pandas
"""

from __future__ import annotations

import argparse
import glob
import html as html_lib
import itertools
import json
import math
import os
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# 1. Load + normalize
# ---------------------------------------------------------------------------

REQUIRED_COLS = [
    "sport",
    "date",
    "game_id",
    "player_id",
    "team_id",
    "stat_type",
    "direction",
    "model_prob",
    "hit",
]


def _fold(s: object) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _norm_dir(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if "UNDER" in s:
        return "UNDER"
    if "OVER" in s:
        return "OVER"
    return s


def _safe_prob(raw: object) -> Optional[float]:
    if raw is None or raw == "":
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v < 0.0 or v > 1.0:
        return None
    return float(v)


def _safe_line(raw: object) -> Optional[float]:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _line_key(line: object) -> str:
    """Stable line token so 15, 15.0, and 15.00 match across graded/tickets."""
    v = _safe_line(line)
    if v is None:
        return ""
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.6g}"


def _derive_game_id(date: str, sport: str, team: object, opp: object) -> str:
    """Stable same-game key when graded/ticket rows lack espn game_id."""
    sides = sorted(x for x in (_fold(team), _fold(opp)) if x)
    if len(sides) < 1:
        return ""
    if len(sides) == 1:
        # tennis / solo: date|sport|player-as-team
        return f"{date}|{_fold(sport)}|{sides[0]}"
    return f"{date}|{_fold(sport)}|{sides[0]}|{sides[1]}"


def _row_hit(p: dict) -> Optional[bool]:
    """Decided HIT/MISS only. Voids/pending -> None (dropped)."""
    result = str(p.get("result") or "").strip().upper()
    if result in {"VOID", "PUSH", "PENDING", "CANCEL", "CANCELLED"}:
        return None
    if result == "HIT":
        return True
    if result == "MISS":
        return False
    h = p.get("hit")
    if h is None or h == "":
        return None
    if isinstance(h, bool):
        return h
    if isinstance(h, (int, float)):
        return bool(int(h))
    s = str(h).strip().lower()
    if s in {"1", "true", "hit", "win", "yes"}:
        return True
    if s in {"0", "false", "miss", "loss", "no"}:
        return False
    return None


# Core schema keys written explicitly below; other scalar JSON fields pass through
# so player-history / league / enrichment columns are not silently dropped.
_GRADED_CORE_KEYS = frozenset(
    {
        "sport",
        "date",
        "match_date",
        "game_date",
        "game_id",
        "espn_game_id",
        "player_id",
        "espn_id",
        "team_id",
        "player",
        "team",
        "opp",
        "opp_team",
        "opponent",
        "prop",
        "prop_type",
        "stat_type",
        "direction",
        "over_under",
        "pick",
        "model_prob",
        "ml_prob",
        "hit",
        "result",
        "line",
        "pick_type",
    }
)


def _passthrough_extras(p: dict) -> dict:
    """Keep scalar extras from graded JSON (skip nested / already-mapped keys)."""
    out: dict = {}
    for k, v in p.items():
        if k in _GRADED_CORE_KEYS:
            continue
        if isinstance(v, (dict, list)):
            continue
        if v is None:
            continue
        # Normalize common history aliases onto player_prior_n when present.
        kl = str(k).strip()
        if kl in {
            "player_prior_n",
            "games_played",
            "n_games",
            "prior_n",
            "props_seen",
            "player_n",
        }:
            try:
                out["player_prior_n"] = int(float(v))
            except (TypeError, ValueError):
                pass
            continue
        out[kl] = v
    return out


def load_graded_props(graded_dir: str) -> pd.DataFrame:
    """Load and concatenate all graded_props_*.json files in a directory.

    Adapter for PropORACLE ui_runner/templates/graded_props_*.json.
    Extra scalar fields on each prop pass through (not just the fixed schema).
    """
    files = sorted(glob.glob(os.path.join(graded_dir, "graded_props_*.json")))
    if not files:
        raise FileNotFoundError(f"No graded_props_*.json files found in {graded_dir}")

    rows = []
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        stem_date = Path(fp).stem.replace("graded_props_", "")[:10]
        file_date = str((data.get("date") if isinstance(data, dict) else None) or stem_date)[:10]
        props = data if isinstance(data, list) else (data.get("props") or [])
        for p in props:
            if not isinstance(p, dict):
                continue
            hit = _row_hit(p)
            if hit is None:
                continue
            # CRITICAL: pick_type is Goblin/Standard/Demon — NOT the stat.
            stat = p.get("prop") or p.get("prop_type") or p.get("stat_type") or ""
            direction = _norm_dir(p.get("direction") or p.get("over_under") or p.get("pick"))
            team = p.get("team") or ""
            opp = p.get("opp_team") or p.get("opp") or p.get("opponent") or ""
            player = p.get("player") or ""
            # Prefer ml_prob (graded schema); model_prob almost never present.
            prob = _safe_prob(p.get("model_prob"))
            if prob is None:
                prob = _safe_prob(p.get("ml_prob"))
            date = str(p.get("date") or p.get("match_date") or p.get("game_date") or file_date)[:10]
            sport = str(p.get("sport") or "").strip()
            game_id = str(p.get("game_id") or p.get("espn_game_id") or "").strip()
            if not game_id:
                game_id = _derive_game_id(date, sport, team, opp)
            player_id = str(p.get("player_id") or p.get("espn_id") or "").strip()
            if not player_id:
                player_id = _fold(player)
            team_id = str(p.get("team_id") or "").strip() or _fold(team)
            row = {
                "sport": sport,
                "date": date,
                "game_id": game_id,
                "player_id": player_id,
                "team_id": team_id,
                "stat_type": str(stat).strip(),
                "direction": direction,
                "model_prob": prob,
                "hit": hit,
                # keep for ticket matching / debug
                "player": player,
                "line": _safe_line(p.get("line")),
                # Goblin/Standard/Demon (line difficulty) — NOT market S/A/B/C
                "pick_type": str(p.get("pick_type") or "").strip(),
            }
            row.update(_passthrough_extras(p))
            rows.append(row)

    df = pd.DataFrame(rows)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns after load: {missing}")

    before = len(df)
    df = df.dropna(subset=["game_id", "stat_type", "direction", "hit", "model_prob"])
    df = df[(df["game_id"].astype(str).str.len() > 0) & (df["stat_type"].astype(str).str.len() > 0)]
    df = df[df["direction"].isin(["OVER", "UNDER"])]
    dropped = before - len(df)
    if dropped:
        print(f"[load] dropped {dropped} rows missing game_id/stat_type/direction/hit/model_prob")

    df["hit"] = df["hit"].astype(bool)
    df["team_id"] = df["team_id"].fillna("").astype(str)
    df["player_id"] = df["player_id"].fillna("").astype(str)
    df["game_id"] = df["game_id"].astype(str)
    df["sport"] = df["sport"].astype(str)
    return df


def shrink_prob_toward_baseline(
    model_prob: float,
    prior_n: float,
    baseline: float,
    prior_strength: float = 150.0,
) -> float:
    """Shrink model_prob toward baseline: w=n/(n+k), shrunk = w*p + (1-w)*baseline.

    Same form as correlation ``shrunk_lift``; used for thin-player confidence.
    Canonical implementation lives in ``utils.player_prob_shrinkage`` (live Standard packing).
    """
    from utils.player_prob_shrinkage import shrink_prob_toward_baseline as _shrink

    return _shrink(model_prob, prior_n, baseline, prior_strength=prior_strength)


def build_calibration_deciles(
    graded_df: pd.DataFrame,
    *,
    pick_type: str = "standard",
    group_by: str | None = None,
    n_deciles: int = 10,
    date_start: str | None = None,
    date_end: str | None = None,
    min_n: int = 30,
) -> pd.DataFrame:
    """Reusable model_prob vs realized-hit decile table, optionally split by a column.

    ``group_by`` examples: ``direction``, ``stat_type``, ``sport``.
    Output columns: [group_by?], decile, n, mean_pred, mean_hit, gap, pick_type.
    """
    if "pick_type" not in graded_df.columns:
        return pd.DataFrame()
    want = _norm_pick_family(pick_type) or str(pick_type or "").strip().lower()
    df = graded_df.copy()
    df["_family"] = df["pick_type"].map(_norm_pick_family)
    df = df[df["_family"] == want]
    df["date"] = df["date"].astype(str).str[:10]
    if date_start:
        df = df[df["date"] >= date_start]
    if date_end:
        df = df[df["date"] <= date_end]
    df["model_prob"] = pd.to_numeric(df["model_prob"], errors="coerce")
    df["hit"] = df["hit"].astype(bool)
    df = df.dropna(subset=["model_prob"])
    if df.empty:
        return pd.DataFrame()

    group_cols: list[str] = []
    if group_by:
        gb = str(group_by).strip()
        if gb not in df.columns:
            raise ValueError(f"group_by column not in graded props: {gb}")
        group_cols = [gb]

    def _one_slice(sub: pd.DataFrame) -> pd.DataFrame:
        if len(sub) < max(min_n, n_deciles):
            return pd.DataFrame()
        s = sub.sort_values("model_prob").copy()
        try:
            s["decile"] = pd.qcut(s["model_prob"], n_deciles, labels=False, duplicates="drop") + 1
        except ValueError:
            return pd.DataFrame()
        g = (
            s.groupby("decile", dropna=False)
            .agg(
                n=("hit", "count"),
                mean_pred=("model_prob", "mean"),
                mean_hit=("hit", "mean"),
            )
            .reset_index()
        )
        g["gap"] = g["mean_hit"] - g["mean_pred"]
        g["pick_type"] = want
        return g

    if not group_cols:
        out = _one_slice(df)
        return out

    parts = []
    for key, sub in df.groupby(group_cols, dropna=False):
        piece = _one_slice(sub)
        if piece.empty:
            continue
        if not isinstance(key, tuple):
            key = (key,)
        for col, val in zip(group_cols, key):
            piece.insert(0, col, val)
        parts.append(piece)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _normalize_ticket_leg(leg: dict, ticket_date: str) -> Optional[dict]:
    if not isinstance(leg, dict):
        return None
    sport = str(leg.get("sport") or "").strip()
    player = leg.get("player") or ""
    team = leg.get("team") or ""
    opp = leg.get("opp") or leg.get("opp_team") or leg.get("opponent") or ""
    # Parse "NYY @ MIN" / "NYY vs MIN" from matchup when opp missing
    if not opp:
        m = str(leg.get("matchup") or "")
        parts = re.split(r"\s+@\s+|\s+vs\.?\s+|\s+v\s+", m, maxsplit=1, flags=re.I)
        if len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            if _fold(a) == _fold(team):
                opp = b
            elif _fold(b) == _fold(team):
                opp = a
            elif not team:
                team, opp = a, b
    stat = leg.get("prop_type") or leg.get("prop") or leg.get("stat_type") or ""
    direction = _norm_dir(leg.get("direction") or leg.get("over_under"))
    # Pricing path on live tickets often uses leg_prob_used (Goblin-70 floor);
    # fall back to ml_prob then hit_rate.
    prob = _safe_prob(leg.get("model_prob"))
    if prob is None:
        prob = _safe_prob(leg.get("leg_prob_used"))
    if prob is None:
        prob = _safe_prob(leg.get("ml_prob"))
    if prob is None:
        prob = _safe_prob(leg.get("hit_rate"))
    date = str(leg.get("game_date") or leg.get("date") or ticket_date)[:10]
    game_id = str(leg.get("game_id") or "").strip() or _derive_game_id(date, sport, team, opp)
    player_id = str(leg.get("player_id") or "").strip() or _fold(player)
    team_id = str(leg.get("team_id") or "").strip() or _fold(team)
    if not sport or not stat or direction not in {"OVER", "UNDER"} or prob is None or not game_id:
        return None
    pick_type = str(leg.get("pick_type") or "").strip()
    return {
        "sport": sport,
        "date": date,
        "game_id": game_id,
        "player_id": player_id,
        "team_id": team_id,
        "stat_type": str(stat).strip(),
        "direction": direction,
        "model_prob": float(prob),
        "player": player,
        "line": leg.get("line"),
        "pick_type": pick_type,
    }


def _iter_raw_tickets(data: Any) -> list[dict]:
    """Accept list, {tickets:[...]}, or dual-card {groups:[{tickets:[...]}]}."""
    if isinstance(data, list):
        return [t for t in data if isinstance(t, dict)]
    if not isinstance(data, dict):
        return []
    if isinstance(data.get("tickets"), list):
        return [t for t in data["tickets"] if isinstance(t, dict)]
    out: list[dict] = []
    for g in data.get("groups") or []:
        if not isinstance(g, dict):
            continue
        for t in g.get("tickets") or []:
            if isinstance(t, dict):
                # stamp slate date from payload when ticket lacks it
                if not t.get("date") and data.get("date"):
                    t = dict(t)
                    t["date"] = data.get("date")
                out.append(t)
    return out


def load_tickets(tickets_path: str, graded_df: Optional[pd.DataFrame] = None) -> list:
    """Load published / Goblin-70 / mixer tickets and normalize legs.

    ticket_hit: uses explicit fields when present; else grades each leg against
    graded_df (same date/player/prop/direction/line) when provided.

    ``tickets_path`` may be a file, a directory, or a glob
    (e.g. ``ui_runner/data/combined_slate_tickets_2026-05*.json``).
    """
    path = Path(tickets_path)
    files: list[Path] = []
    if any(ch in str(tickets_path) for ch in "*?["):
        files = [Path(f) for f in sorted(glob.glob(tickets_path))]
    elif path.is_dir():
        files = sorted(path.glob("combined_slate_tickets_*.json")) + sorted(path.glob("tickets_*.json"))
    elif path.is_file():
        files = [path]
    else:
        files = [Path(f) for f in sorted(glob.glob(tickets_path))]

    if not files:
        raise FileNotFoundError(f"No ticket files matched: {tickets_path}")

    payloads: list[Any] = []
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            payloads.append(json.load(f))

    # Prefer exact line match; fall back to player+prop+direction when the
    # published line drifted vs the graded row (common on MLB/Tennis).
    grade_index: dict[tuple, bool] = {}
    grade_index_noline: dict[tuple, bool] = {}
    if graded_df is not None and not graded_df.empty:
        for r in graded_df.itertuples(index=False):
            base = (
                str(getattr(r, "date", ""))[:10],
                _fold(getattr(r, "sport", "")),
                _fold(getattr(r, "player", "")),
                _fold(getattr(r, "stat_type", "")),
                str(getattr(r, "direction", "")).upper(),
            )
            grade_index[base + (_line_key(getattr(r, "line", None)),)] = bool(getattr(r, "hit"))
            # last write wins on collisions (rare same-player multi-line same day)
            grade_index_noline[base] = bool(getattr(r, "hit"))

    def _lookup_leg_hit(leg: dict, t_date: str) -> Optional[bool]:
        base = (
            str(leg.get("date") or t_date)[:10],
            _fold(leg.get("sport")),
            _fold(leg.get("player")),
            _fold(leg.get("stat_type")),
            str(leg.get("direction") or "").upper(),
        )
        k = base + (_line_key(leg.get("line")),)
        if k in grade_index:
            return grade_index[k]
        if base in grade_index_noline:
            return grade_index_noline[base]
        return None

    tickets: list[dict] = []
    for data in payloads:
        slate_date = str((data.get("date") if isinstance(data, dict) else None) or "")[:10]
        for t in _iter_raw_tickets(data):
            t_date = str(t.get("date") or slate_date)[:10]
            raw_legs = t.get("legs") or t.get("rows") or []
            legs = []
            for leg in raw_legs:
                norm = _normalize_ticket_leg(leg, t_date)
                if norm:
                    legs.append(norm)
            if len(legs) < 2:
                continue
            ticket_hit = t.get("ticket_hit")
            if ticket_hit is None:
                ticket_hit = t.get("won")
            if ticket_hit is None and str(t.get("result") or "").upper() in {"HIT", "WIN", "WON"}:
                ticket_hit = True
            if ticket_hit is None and str(t.get("result") or "").upper() in {"MISS", "LOSS", "LOST"}:
                ticket_hit = False
            # Archives do NOT store settled outcomes — join graded_props by leg.
            if ticket_hit is None and (grade_index or grade_index_noline):
                leg_hits = []
                for leg in legs:
                    h = _lookup_leg_hit(leg, t_date)
                    if h is None:
                        leg_hits = []
                        break
                    leg_hits.append(h)
                if len(leg_hits) == len(legs):
                    ticket_hit = all(leg_hits)
            tickets.append(
                {
                    "date": t_date,
                    "ticket_id": t.get("ticket_id") or t.get("id"),
                    "legs": legs,
                    "ticket_hit": ticket_hit,
                }
            )
    return tickets


_TE_ARTICLE_RE = re.compile(
    r'<article class="ticket-card([^"]*)">(.*?)</article>',
    re.S | re.I,
)


def _te_plain(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", fragment or "")
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_pick_family(pick_type: object) -> str:
    """Map pick_type -> goblin|standard|demon|'' (line difficulty, not Power/Flex)."""
    s = str(pick_type or "").strip().lower()
    if not s:
        return ""
    if s.startswith("goblin") or "goblin" in s:
        return "goblin"
    if s.startswith("demon") or "demon" in s:
        return "demon"
    if s.startswith("standard") or s == "std" or "standard" in s:
        return "standard"
    return ""


def ticket_tier_mix(legs: list, *, fallback: str | None = None) -> str:
    """Derive ticket line-difficulty mix from per-leg pick_type.

    Orthogonal to Power vs Flex (payout structure). Returns:
      all_goblin | all_standard | all_demon | mixed_tier | unknown
    """
    families = {
        _norm_pick_family(leg.get("pick_type") if isinstance(leg, dict) else "")
        for leg in (legs or [])
    }
    families.discard("")
    if not families:
        fb = (fallback or "").strip().lower()
        if fb in {"all_goblin", "all_standard", "all_demon", "mixed_tier"}:
            return fb
        if fb == "goblin":
            return "all_goblin"
        if fb == "standard":
            return "all_standard"
        if fb in {"mixed", "mix"}:
            return "mixed_tier"
        return "unknown"
    if families == {"goblin"}:
        return "all_goblin"
    if families == {"standard"}:
        return "all_standard"
    if families == {"demon"}:
        return "all_demon"
    return "mixed_tier"


def _te_group_title_mix(body: str) -> str | None:
    """Fallback ticket mix from TE section/group title (e.g. 'NBA Goblin 2-Leg')."""
    m = re.search(r'<span class="tg">([^<]+)</span>', body or "", re.I)
    title = _te_plain(m.group(1)) if m else ""
    low = title.lower()
    if "goblin" in low:
        return "goblin"
    if "standard" in low:
        return "standard"
    if "mixed" in low or re.search(r"\bmix\b", low):
        return "mixed"
    return None


def _parse_ticket_eval_legrow(row_cls: str, body: str) -> Optional[dict]:
    """Parse one legrow block from ticket_eval HTML into a raw leg dict.

    Note: TE legrows do NOT render Goblin/Standard/Demon. The ``tier`` div is
    the market letter (S/A/B/C/D), not pick_type. Attach pick_type from
    graded_props (or section-title fallback at ticket level).
    """
    low = (row_cls or "").lower()
    if "leg-pend" in low or "leg-void" in low:
        return None

    sport_m = re.search(r'<span class="pill[^"]*">([^<]+)</span>', body, re.I)
    sport = _te_plain(sport_m.group(1)) if sport_m else ""

    player = ""
    for pat in (
        r'class="pl-(?:hit|miss|void|pend)[^"]*"[^>]*>(.*?)</div>',
        r'class="pl-name"[^>]*>(.*?)</span>',
    ):
        pm = re.search(pat, body, re.S | re.I)
        if pm:
            player = _te_plain(pm.group(1))
            player = re.sub(r"\bMISSED\b", "", player, flags=re.I).strip()
            player = re.sub(r"\bLimited Data\b.*$", "", player, flags=re.I).strip()
            if player:
                break

    prop_m = re.search(
        r'<div class="leg-prop-col[^"]*">\s*<div>(.*?)</div>\s*<div class="meta-muted">(.*?)</div>',
        body,
        re.S | re.I,
    )
    prop = _te_plain(prop_m.group(1)) if prop_m else ""
    matchup = _te_plain(prop_m.group(2)) if prop_m else ""
    team = opp = ""
    if matchup:
        parts = re.split(r"\s+vs\.?\s+|\s+@\s+|\s+v\s+", matchup, maxsplit=1, flags=re.I)
        if len(parts) == 2:
            team, opp = parts[0].strip(), parts[1].strip()
        else:
            team = matchup

    line = None
    direction = ""
    dir_m = re.search(
        r'<div class="leg-extra[^"]*">\s*([^<]*?)\s*<span class="dir-(over|under)"[^>]*>\s*([^<]+)\s*</span>',
        body,
        re.S | re.I,
    )
    if dir_m:
        line = _safe_line(_te_plain(dir_m.group(1)))
        direction = _norm_dir(dir_m.group(3) or dir_m.group(2))

    if not sport or not player or not prop or direction not in {"OVER", "UNDER"}:
        return None

    leg_hit = True if "leg-hit" in low else False if "leg-miss" in low else None
    return {
        "sport": sport,
        "player": player,
        "team": team,
        "opp": opp,
        "matchup": matchup,
        "prop_type": prop,
        "direction": direction,
        "line": line,
        "leg_hit": leg_hit,
        "pick_type": "",  # filled from graded_props join
    }


def _ticket_eval_money_hit(article_cls: str, body: str) -> Optional[bool]:
    """Decided money outcome for grade_history alignment.

    Prefer RESULT badge (Flex can WON with one miss). Fall back to all-hit /
    card-missed article classes.
    """
    if re.search(r"grade-ticket-result\s+won", body, re.I):
        return True
    if re.search(r"grade-ticket-result\s+lost", body, re.I):
        return False
    if re.search(r"MIN\s*GUARANTEE", body, re.I):
        return True  # counted in grade_history win_rate with wins
    low = (article_cls or "").lower()
    if "all-hit" in low:
        return True
    if "card-missed" in low:
        return False
    return None


def load_tickets_from_ticket_eval_html(
    path_glob: str,
    graded_df: Optional[pd.DataFrame] = None,
) -> list:
    """Load decided slips from ``ticket_eval_YYYY-MM-DD.html`` articles.

    This is the May–June population that matches ``grade_history`` daily n/wins
    (unlike ``combined_slate_tickets_*.json`` grade-pool or ``on_ticket`` joins).

    ``model_prob`` is not in the HTML — attach from ``graded_df`` by
    date/sport/player/prop/direction/(line). Tickets missing any leg prob are kept
    only if every leg still normalizes (otherwise dropped at normalize).
    """
    files = [Path(f) for f in sorted(glob.glob(path_glob))]
    if not files:
        # also accept a directory
        p = Path(path_glob)
        if p.is_dir():
            files = sorted(p.glob("ticket_eval_*.html"))
    files = [
        f
        for f in files
        if re.match(r"ticket_eval_\d{4}-\d{2}-\d{2}\.html$", f.name)
    ]
    if not files:
        raise FileNotFoundError(f"No ticket_eval_YYYY-MM-DD.html matched: {path_glob}")

    # model_prob + pick_type indexes from graded props
    prob_exact: dict[tuple, float] = {}
    prob_noline: dict[tuple, float] = {}
    pick_exact: dict[tuple, str] = {}
    pick_noline: dict[tuple, str] = {}
    if graded_df is not None and not graded_df.empty:
        for r in graded_df.itertuples(index=False):
            base = (
                str(getattr(r, "date", ""))[:10],
                _fold(getattr(r, "sport", "")),
                _fold(getattr(r, "player", "")),
                _fold(getattr(r, "stat_type", "")),
                str(getattr(r, "direction", "")).upper(),
            )
            pt = str(getattr(r, "pick_type", "") or "").strip()
            if pt:
                pick_exact[base + (_line_key(getattr(r, "line", None)),)] = pt
                pick_noline[base] = pt
            prob = getattr(r, "model_prob", None)
            if prob is None:
                continue
            try:
                pf = float(prob)
            except (TypeError, ValueError):
                continue
            if not (0.0 <= pf <= 1.0):
                continue
            prob_exact[base + (_line_key(getattr(r, "line", None)),)] = pf
            prob_noline[base] = pf

    tickets: list[dict] = []
    n_articles = n_decided = n_kept = 0
    n_pick_joined = 0
    n_pick_fallback = 0
    for fp in files:
        mdate = re.search(r"(20\d{2}-\d{2}-\d{2})", fp.name)
        slate_date = mdate.group(1) if mdate else ""
        text = fp.read_text(encoding="utf-8", errors="replace")
        # Drop Manual Ticket Builder section — it reuses legrow-like markup.
        cut = text.find('class="ticket-bucket sb-default manual-tb')
        if cut > 0:
            text = text[:cut]

        for am in _TE_ARTICLE_RE.finditer(text):
            n_articles += 1
            article_cls, body = am.group(1), am.group(2)
            money_hit = _ticket_eval_money_hit(article_cls, body)
            if money_hit is None:
                continue
            n_decided += 1

            raw_legs: list[dict] = []
            for lm in re.finditer(
                r'<div class="legrow([^"]*)">(.*?)(?=<div class="legrow|<(?:div class="ticket-grade|/article))',
                body,
                re.S | re.I,
            ):
                parsed = _parse_ticket_eval_legrow(lm.group(1), lm.group(2))
                if parsed:
                    raw_legs.append(parsed)
            if len(raw_legs) < 2:
                continue

            title_mix = _te_group_title_mix(body)

            # Attach model_prob + pick_type from graded history.
            for leg in raw_legs:
                base = (
                    slate_date,
                    _fold(leg.get("sport")),
                    _fold(leg.get("player")),
                    _fold(leg.get("prop_type")),
                    str(leg.get("direction") or "").upper(),
                )
                prob = prob_exact.get(base + (_line_key(leg.get("line")),))
                if prob is None:
                    prob = prob_noline.get(base)
                if prob is not None:
                    leg["model_prob"] = prob
                pt = pick_exact.get(base + (_line_key(leg.get("line")),))
                if pt is None:
                    pt = pick_noline.get(base)
                if pt:
                    leg["pick_type"] = pt
                    n_pick_joined += 1

            legs = []
            for leg in raw_legs:
                norm = _normalize_ticket_leg(leg, slate_date)
                if norm:
                    legs.append(norm)
            if len(legs) < 2:
                continue

            mix = ticket_tier_mix(legs, fallback=title_mix)
            if mix != "unknown" and not any(leg.get("pick_type") for leg in legs):
                n_pick_fallback += 1
                # Stamp inferred family onto legs so downstream sees pick_type
                stamp = {
                    "all_goblin": "Goblin",
                    "all_standard": "Standard",
                    "all_demon": "Demon",
                }.get(mix)
                if stamp:
                    for leg in legs:
                        if not leg.get("pick_type"):
                            leg["pick_type"] = stamp

            n_kept += 1
            tickets.append(
                {
                    "date": slate_date,
                    "ticket_id": f"te:{slate_date}:{n_kept}",
                    "legs": legs,
                    "ticket_hit": bool(money_hit),
                    "tier_mix": mix,
                    "source": "ticket_eval_html",
                }
            )

    print(
        f"[ticket_eval] files={len(files)} articles={n_articles} "
        f"decided={n_decided} normalized_2plus={n_kept} "
        f"pick_type_joined={n_pick_joined} title_mix_fallback_tickets={n_pick_fallback}"
    )
    return tickets


# ---------------------------------------------------------------------------
# 2. Categorize pairs
# ---------------------------------------------------------------------------

CAT_SAME_PLAYER_DIFF_STAT = "cat1_same_player_diff_stat"
CAT_SAME_TEAM_DIFF_PLAYER_SAME_STAT = "cat2_same_team_diff_player_same_stat"
CAT_OPPOSING_TEAMS_SAME_GAME = "cat3_opposing_teams_same_game"
CAT_BASELINE_DIFF_GAME = "cat4_baseline_diff_game"


def categorize(a: dict, b: dict) -> Optional[str]:
    """a, b: dicts with game_id, player_id, team_id, stat_type keys."""
    same_game = a["game_id"] == b["game_id"]
    same_player = bool(a["player_id"]) and a["player_id"] == b["player_id"]
    same_team = bool(a["team_id"]) and a["team_id"] == b["team_id"]
    same_stat = a["stat_type"] == b["stat_type"]

    if not same_game:
        # cross-game pairs are only useful as the near-zero baseline check
        if same_player or same_team:
            return CAT_BASELINE_DIFF_GAME
        return None

    if same_player and not same_stat:
        return CAT_SAME_PLAYER_DIFF_STAT
    if same_team and not same_player and same_stat:
        return CAT_SAME_TEAM_DIFF_PLAYER_SAME_STAT
    if not same_team:
        return CAT_OPPOSING_TEAMS_SAME_GAME
    return None  # same team + same player + same stat = the same prop; skip


# ---------------------------------------------------------------------------
# 3. Build correlation cells
# ---------------------------------------------------------------------------

def compute_global_marginals(df: pd.DataFrame) -> dict:
    """Empirical hit rate per (sport, stat_type, direction), pooled across the
    full dataset -- this is the correct independence baseline.

    Using the model's own ``model_prob`` for this baseline lets calibration
    error leak into the lift number: overconfident mean(model_prob) inflates
    expected_under_independence and artificially deflates raw_lift (spurious
    "negative correlation"). Soccer / MLB mid-prob OVER / tennis games-won are
    already known-biased; empirical hit rate sidesteps that entirely.
    """
    g = df.groupby(["sport", "stat_type", "direction"])["hit"].agg(["mean", "count"])
    return {
        (sport, stat, direction): {"emp_rate": float(row["mean"]), "n": int(row["count"])}
        for (sport, stat, direction), row in g.iterrows()
    }


@dataclass
class Cell:
    n: int = 0
    joint_hits: int = 0
    sum_model_pa: float = 0.0
    sum_model_pb: float = 0.0

    def add(self, hit_a: bool, hit_b: bool, model_pa: float, model_pb: float) -> None:
        self.n += 1
        self.joint_hits += int(hit_a and hit_b)
        self.sum_model_pa += model_pa
        self.sum_model_pb += model_pb

    def stats(
        self,
        emp_marginal_a: float,
        emp_marginal_b: float,
        prior_strength: int = 25,
    ) -> Optional[dict]:
        if self.n == 0:
            return None
        p_joint_emp = self.joint_hits / self.n
        mean_model_pa = self.sum_model_pa / self.n
        mean_model_pb = self.sum_model_pb / self.n
        # independence baseline from EMPIRICAL marginals, not model_prob
        expected_indep = emp_marginal_a * emp_marginal_b
        raw_lift = (p_joint_emp / expected_indep) if expected_indep > 0 else 1.0
        w = self.n / (self.n + prior_strength)
        shrunk_lift = w * raw_lift + (1 - w) * 1.0
        return {
            "n": self.n,
            "p_joint_empirical": round(p_joint_emp, 4),
            "expected_under_independence": round(expected_indep, 4),
            "raw_lift": round(raw_lift, 4),
            "shrunk_lift": round(shrunk_lift, 4),
            # diagnostic only -- NOT used in expected_indep
            "mean_model_prob_a": round(mean_model_pa, 4),
            "mean_model_prob_b": round(mean_model_pb, 4),
            "calibration_gap_a": round(emp_marginal_a - mean_model_pa, 4),
            "calibration_gap_b": round(emp_marginal_b - mean_model_pb, 4),
        }


def build_cells(df: pd.DataFrame, prior_strength: int = 25) -> dict:
    """Group rows by (sport, date, game_id) and pair up same-slate props,
    plus a separate cross-game pass for the category-4 baseline."""
    marginals = compute_global_marginals(df)
    cells: dict = defaultdict(Cell)

    for (sport, _date, _game_id), g in df.groupby(["sport", "date", "game_id"]):
        rows = g.to_dict("records")
        for a, b in itertools.combinations(rows, 2):
            cat = categorize(a, b)
            if cat is None or cat == CAT_BASELINE_DIFF_GAME:
                continue
            key = (sport, cat, a["stat_type"], a["direction"], b["stat_type"], b["direction"])
            cells[key].add(a["hit"], b["hit"], a["model_prob"], b["model_prob"])

    # category 4: same player or team, DIFFERENT game -- built separately
    for sport, sub in df.groupby("sport"):
        for key_col in ["player_id", "team_id"]:
            for _key_val, g in sub[sub[key_col] != ""].groupby(key_col):
                rows = g.to_dict("records")
                # cap pairs per entity to avoid O(n^2) blowups on high-volume sports
                if len(rows) > 80:
                    rows = rows[:80]
                for a, b in itertools.combinations(rows, 2):
                    if a["game_id"] == b["game_id"]:
                        continue
                    key = (
                        sport,
                        CAT_BASELINE_DIFF_GAME,
                        a["stat_type"],
                        a["direction"],
                        b["stat_type"],
                        b["direction"],
                    )
                    cells[key].add(a["hit"], b["hit"], a["model_prob"], b["model_prob"])

    out: dict = {}
    for key, cell in cells.items():
        sport, cat, stat_a, dir_a, stat_b, dir_b = key
        marg_a = marginals.get((sport, stat_a, dir_a))
        marg_b = marginals.get((sport, stat_b, dir_b))
        if marg_a is None or marg_b is None:
            continue
        stats = cell.stats(
            marg_a["emp_rate"],
            marg_b["emp_rate"],
            prior_strength=prior_strength,
        )
        if stats is None:
            continue
        out.setdefault(sport, {}).setdefault(cat, []).append(
            {
                "stat_a": stat_a,
                "direction_a": dir_a,
                "stat_b": stat_b,
                "direction_b": dir_b,
                **stats,
            }
        )
    return out


# ---------------------------------------------------------------------------
# 4. Lookup + EV adjustment
# ---------------------------------------------------------------------------

def sport_mix_decomposition(
    df: pd.DataFrame,
    *,
    base_month: str = "2026-05",
    compare_month: str = "2026-06",
) -> tuple[str, dict[str, float]]:
    """Kitagawa mix-vs-within decomposition of ticket WR change across months.

    Counterfactual WR = compare-month sport shares × base-month per-sport WRs.
    mix_effect          = CF − base WR   (more tickets from worse/better sports)
    within_sport_effect = compare WR − CF (sports individually changing)

    Sports present only in the compare month have no base WR; they use the base
    pooled WR as a fallback (noted in the report).
    """
    months = sorted(df["month"].dropna().unique())
    if base_month not in months or compare_month not in months:
        # fall back to first two months present
        if len(months) < 2:
            return ("## Sport-mix decomposition\n\nNeed ≥2 months.\n", {})
        base_month, compare_month = months[0], months[1]

    base = df[df["month"] == base_month]
    comp = df[df["month"] == compare_month]
    n0, n1 = len(base), len(comp)
    if n0 == 0 or n1 == 0:
        return ("## Sport-mix decomposition\n\nEmpty month slice.\n", {})

    wr0 = float(base["actual_hit"].mean())
    wr1 = float(comp["actual_hit"].mean())

    base_sport = (
        base.groupby("sport")
        .agg(n=("actual_hit", "count"), wr=("actual_hit", "mean"))
        .assign(share=lambda x: x["n"] / n0)
    )
    comp_sport = (
        comp.groupby("sport")
        .agg(n=("actual_hit", "count"), wr=("actual_hit", "mean"))
        .assign(share=lambda x: x["n"] / n1)
    )

    sports = sorted(set(base_sport.index) | set(comp_sport.index))
    fallback_used: list[str] = []
    rows = []
    cf = 0.0
    for sp in sports:
        share0 = float(base_sport.loc[sp, "share"]) if sp in base_sport.index else 0.0
        share1 = float(comp_sport.loc[sp, "share"]) if sp in comp_sport.index else 0.0
        wr_s0 = float(base_sport.loc[sp, "wr"]) if sp in base_sport.index else None
        wr_s1 = float(comp_sport.loc[sp, "wr"]) if sp in comp_sport.index else None
        n_s0 = int(base_sport.loc[sp, "n"]) if sp in base_sport.index else 0
        n_s1 = int(comp_sport.loc[sp, "n"]) if sp in comp_sport.index else 0

        if wr_s0 is None:
            wr_s0_used = wr0
            fallback_used.append(sp)
        else:
            wr_s0_used = wr_s0

        cf += share1 * wr_s0_used
        # per-sport contribution to mix / within (standard Kitagawa terms)
        mix_contrib = (share1 - share0) * wr_s0_used
        within_contrib = share1 * ((wr_s1 if wr_s1 is not None else wr_s0_used) - wr_s0_used)
        rows.append(
            {
                "sport": sp,
                "n_base": n_s0,
                "n_compare": n_s1,
                "share_base": round(share0, 4),
                "share_compare": round(share1, 4),
                "wr_base": round(wr_s0_used, 4) if wr_s0 is not None or sp in fallback_used else None,
                "wr_compare": round(wr_s1, 4) if wr_s1 is not None else None,
                "mix_contrib": round(mix_contrib, 4),
                "within_contrib": round(within_contrib, 4),
            }
        )

    mix_effect = cf - wr0
    within_effect = wr1 - cf
    total = wr1 - wr0
    summary = {
        "base_month": base_month,
        "compare_month": compare_month,
        "wr_base": wr0,
        "wr_compare": wr1,
        "wr_counterfactual": cf,
        "mix_effect": mix_effect,
        "within_sport_effect": within_effect,
        "total_change": total,
    }

    detail = pd.DataFrame(rows).sort_values("mix_contrib")
    try:
        detail_md = detail.to_markdown(index=False)
    except ImportError:
        detail_md = "```\n" + detail.to_string(index=False) + "\n```"

    dominate = (
        "mix_effect dominates -- volume/exposure control on the growing weak sport "
        "is the faster lever than waiting on full recalibration."
        if abs(mix_effect) >= abs(within_effect)
        else "within_sport_effect dominates -- sports got worse individually; "
        "recalibration (not just a share cap) is the real fix."
    )
    fb_note = (
        f" Fallback base WR ({wr0:.1%}) used for sports with no {base_month} tickets: "
        + ", ".join(fallback_used)
        + "."
        if fallback_used
        else ""
    )

    lines = [
        "## Sport-mix vs within-sport decomposition",
        "",
        f"Base `{base_month}` WR={wr0:.1%} (n={n0}) -> compare `{compare_month}` "
        f"WR={wr1:.1%} (n={n1}); total d={100*total:+.1f} pp.",
        "",
        f"- **Counterfactual WR** (compare mix x base per-sport WR) = **{cf:.1%}**",
        f"- **mix_effect** = CF - base = **{100*mix_effect:+.1f} pp**",
        f"- **within_sport_effect** = compare - CF = **{100*within_effect:+.1f} pp**",
        f"- Check: mix + within = {100*(mix_effect+within_effect):+.1f} pp "
        f"(should match total {100*total:+.1f} pp).",
        "",
        f"**Read:** {dominate}{fb_note}",
        "",
        "Per-sport contributions (negative mix_contrib = share grew into a weaker "
        "base-WR sport, or share left a stronger one):",
        "",
        detail_md,
        "",
    ]
    return ("\n".join(lines), summary)


def _ticket_has_lever_a(legs: list) -> bool:
    """Same-player, different-stat on one ticket (shipped Lever A / cat1 block).

    Matches ``utils.ticket_diversity._has_same_player_multistat`` — game_id does
    not matter; any two different props on the same player hard-block.
    """
    props_by_player: dict[str, set[str]] = {}
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        pid = str(leg.get("player_id") or "").strip() or _fold(leg.get("player"))
        stat = str(leg.get("stat_type") or leg.get("prop_type") or leg.get("prop") or "").strip()
        if not pid or not stat:
            continue
        props_by_player.setdefault(pid, set()).add(_fold(stat) or stat)
    return any(len(props) >= 2 for props in props_by_player.values())


def _leg_is_thin_sport(leg: dict, thin_sport: str) -> bool:
    sp = str(leg.get("sport") or "").strip().upper()
    want = str(thin_sport or "").strip().upper()
    if not want:
        return False
    if want in {"SOCCER", "SOC"}:
        return sp in {"SOCCER", "SOC"} or sp.startswith("SOCCER") or sp.startswith("WORLDCUP")
    return sp == want or sp.startswith(want)


def simulate_current_gates(
    tickets: list,
    *,
    thin_sport: str | None = "Soccer",
    thin_start: str | None = "2026-06-11",
    thin_end: str | None = "2026-07-19",
) -> dict | None:
    """Counterfactual WR if tickets violating shipped construction rules are removed.

    Rules:
      (a) Lever A — same-player different-stat pair on the ticket
      (b) Thin-competition date proxy — any ``thin_sport`` leg on a ticket whose
          slate date falls in [thin_start, thin_end] (WC window default). League
          is not on the ticket_eval historical population; that window was
          confirmed ~100% national-team Soccer inside / club outside.

    Caveat: removes existing tickets only — does not model packer substitutions.
    """
    rows = []
    for t in tickets:
        legs = t.get("legs") or []
        if len(legs) < 2:
            continue
        if t.get("ticket_hit") is None:
            continue
        blocked_a = _ticket_has_lever_a(legs)
        blocked_thin = False
        if thin_sport:
            date = str(t.get("date") or "")[:10]
            for leg in legs:
                if not _leg_is_thin_sport(leg, thin_sport):
                    continue
                if thin_start and date < thin_start:
                    continue
                if thin_end and date > thin_end:
                    continue
                blocked_thin = True
                break
        sports = {
            str(leg.get("sport") or "").strip()
            for leg in legs
            if isinstance(leg, dict) and leg.get("sport")
        }
        sports.discard("")
        sport_label = next(iter(sports)) if len(sports) == 1 else "mixed"
        rows.append(
            {
                "date": str(t.get("date") or "")[:10],
                "month": (t.get("date") or "")[:7],
                "sport": sport_label,
                "blocked_a": blocked_a,
                "blocked_thin": blocked_thin,
                "blocked_any": blocked_a or blocked_thin,
                "actual_hit": bool(t.get("ticket_hit")),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return None

    baseline = (
        df.groupby("month")["actual_hit"]
        .agg(n="count", win_rate="mean")
        .round(4)
    )
    survivors = df[~df["blocked_any"]].copy()
    filtered = (
        survivors.groupby("month")["actual_hit"]
        .agg(n="count", win_rate="mean")
        .round(4)
    )
    by_sport_surv = (
        survivors.groupby(["sport", "month"])["actual_hit"]
        .agg(n="count", win_rate="mean")
        .round(4)
    )

    # Pooled
    n_tot = len(df)
    n_surv = len(survivors)
    wr_base = float(df["actual_hit"].mean()) if n_tot else 0.0
    wr_filt = float(survivors["actual_hit"].mean()) if n_surv else 0.0

    survivors_decomp = None
    if not survivors.empty:
        decomp_md, decomp_summary = sport_mix_decomposition(survivors)
        survivors_decomp = {"md": decomp_md, "summary": decomp_summary}

    return {
        "baseline_by_month": baseline,
        "filtered_by_month": filtered,
        "by_sport_survivors": by_sport_surv,
        "survivors_df": survivors,
        "survivors_decomp": survivors_decomp,
        "n_total": n_tot,
        "n_survivors": n_surv,
        "n_blocked_lever_a": int(df["blocked_a"].sum()),
        "n_blocked_thin_competition": int(df["blocked_thin"].sum()),
        "n_blocked_any": int(df["blocked_any"].sum()),
        "wr_baseline": round(wr_base, 4),
        "wr_filtered": round(wr_filt, 4),
        "thin_sport": thin_sport,
        "thin_start": thin_start,
        "thin_end": thin_end,
    }


def format_simulate_gates_report(sim: dict) -> str:
    lines = [
        "## Simulate current construction gates",
        "",
        "Counterfactual: drop tickets that violate shipped rules, recompute WR on survivors.",
        f"- Lever A: same-player different-stat (`block_same_player_multistat`)",
        f"- Thin competition proxy: `{sim.get('thin_sport')}` legs with slate date in "
        f"[{sim.get('thin_start')}, {sim.get('thin_end')}] (WC window; league not on TE HTML)",
        "",
        f"Tickets: {sim['n_total']} -> survivors {sim['n_survivors']} "
        f"(blocked any={sim['n_blocked_any']}: "
        f"lever_a={sim['n_blocked_lever_a']}, "
        f"thin_competition={sim['n_blocked_thin_competition']})",
        f"Pooled WR: {sim['wr_baseline']:.1%} -> **{sim['wr_filtered']:.1%}** "
        f"({100*(sim['wr_filtered']-sim['wr_baseline']):+.1f} pp)",
        "",
        "### Baseline (all tickets)",
        "",
        "```",
        sim["baseline_by_month"].to_string(),
        "```",
        "",
        "### Filtered (current-rules survivors only)",
        "",
        "```",
        sim["filtered_by_month"].to_string(),
        "```",
        "",
    ]
    by_sp = sim.get("by_sport_survivors")
    if by_sp is not None and not by_sp.empty:
        lines += [
            "### Survivors by sport and month",
            "",
            "```",
            by_sp.to_string(),
            "```",
            "",
        ]
    sd = sim.get("survivors_decomp") or {}
    if sd.get("md"):
        lines += [
            "### Mix vs within on SURVIVORS (remaining gap after the cut)",
            "",
            sd["md"],
        ]
    lines += [
        "**Caveat (load-bearing):** this only removes tickets that already existed. "
        "Production would rebuild from remaining eligible legs, not publish a thinner "
        "card of the same slips. Treat WR lift as a ceiling on how much these rules "
        "hurt *this* published batch -- not a forecast of live WR. Watch volume drop "
        "(`n` baseline vs filtered) as closely as the WR number.",
        "",
    ]
    # Keep console/report ASCII-safe on Windows cp1252 consoles.
    return "\n".join(lines).encode("ascii", "replace").decode("ascii")


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """Lower bound of the Wilson score interval for a win rate.

    Ranks composition buckets so a small-n lucky 100% does not outrank a
    larger bucket with a slightly lower but far more reliable win rate —
    same discipline as shrinkage on correlation cells.
    """
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1.0 + z * z / n
    center = phat + z * z / (2.0 * n)
    margin = z * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * n)) / n)
    return max(0.0, (center - margin) / denom)


def load_payout_table(path: str) -> dict:
    """Load composition payout multipliers (N-correct To Win, never 1st-place).

    Accepted shapes:
      flat:   {"2": 3.0, "6": 40.0}
      nested: {"2": {"standard": 3.0, "goblin": 2.0}, "6": {"standard": 40.0}}

    Keys may be int or str. Nested tier keys are lowercased on lookup.
    Metadata keys starting with ``_`` are ignored.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"payout table must be a JSON object: {path}")
    out: dict = {}
    for k, v in raw.items():
        if str(k).startswith("_"):
            continue
        key = str(k).strip()
        if isinstance(v, dict):
            out[key] = {
                str(tk).strip().lower(): float(tv)
                for tk, tv in v.items()
                if not str(tk).startswith("_")
            }
        else:
            out[key] = float(v)
    return out


def resolve_payout_x(
    n_legs: int,
    tier_mix: str,
    payout_table: dict,
) -> float | None:
    """Resolve payout multiplier for (n_legs, tier_mix).

    Nested tables key off line-difficulty mix (all_goblin / all_standard /
    mixed_tier / ...), NOT Power/Flex. Flat tables apply to any mix.
    """
    if not payout_table:
        return None
    entry = payout_table.get(str(n_legs))
    if entry is None:
        return None
    if isinstance(entry, (int, float)):
        return float(entry)
    if not isinstance(entry, dict):
        return None
    t = (tier_mix or "unknown").strip().lower()
    aliases = [t]
    if t == "all_goblin":
        aliases += ["goblin", "goblins"]
    elif t == "all_standard":
        aliases += ["standard", "std"]
    elif t == "mixed_tier":
        aliases += ["mixed", "mix"]
    elif t == "all_demon":
        aliases += ["demon"]
    aliases += ["unknown", "default", "any", "standard"]
    for cand in aliases:
        if cand in entry:
            return float(entry[cand])
    if len(entry) == 1:
        return float(next(iter(entry.values())))
    return None


def composition_breakdown(
    tickets: list,
    group_cols: Optional[list] = None,
    min_n: int = 20,
    payout_table: Optional[dict] = None,
    min_payout: float | None = None,
) -> pd.DataFrame:
    """Win rate (and optional EV) by n_legs x sport x tier_mix.

    tier_mix comes from per-leg pick_type (all_goblin / all_standard /
    mixed_tier / ...). Power vs Flex is orthogonal and not encoded here.

    Without payout_table: rank by wilson_lower (consistent axis).
    With payout_table: rank by ev_conservative; min_payout drops low-payout
    buckets (YOLO floor).
    """
    if group_cols is None:
        group_cols = ["n_legs", "sport", "tier_mix"]

    rows = []
    for t in tickets:
        legs = t.get("legs") or []
        if len(legs) < 2:
            continue
        if t.get("ticket_hit") is None:
            continue
        sports = {
            str(leg.get("sport") or "").strip()
            for leg in legs
            if isinstance(leg, dict) and leg.get("sport")
        }
        sports.discard("")
        sport_label = next(iter(sports)) if len(sports) == 1 else "mixed"
        mix = str(t.get("tier_mix") or "").strip().lower()
        if not mix or mix == "unknown":
            mix = ticket_tier_mix(legs, fallback=t.get("tier"))
        rows.append(
            {
                "month": (t.get("date") or "")[:7],
                "n_legs": len(legs),
                "sport": sport_label,
                "tier_mix": mix,
                "actual_hit": bool(t.get("ticket_hit")),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    cols = [c for c in group_cols if c in df.columns]
    if not cols:
        return pd.DataFrame()

    g = df.groupby(cols, dropna=False)["actual_hit"].agg(
        n="count", wins="sum", win_rate="mean"
    )
    g["wilson_lower"] = [
        wilson_lower_bound(int(w), int(n)) for w, n in zip(g["wins"], g["n"])
    ]

    if payout_table:
        payouts: list[float | None] = []
        for idx in g.index:
            idx_t = idx if isinstance(idx, tuple) else (idx,)
            row_map = dict(zip(cols, idx_t))
            payouts.append(
                resolve_payout_x(
                    int(row_map.get("n_legs") or 0),
                    str(row_map.get("tier_mix") or "unknown"),
                    payout_table,
                )
            )
        g["payout_x"] = payouts
        if min_payout is not None:
            keep = [
                (px is not None and float(px) >= float(min_payout))
                for px in g["payout_x"]
            ]
            g = g.loc[keep]
        g["ev_raw"] = [
            (float(wr) * float(px) - 1.0) if px is not None else None
            for wr, px in zip(g["win_rate"], g["payout_x"])
        ]
        g["ev_conservative"] = [
            (float(wl) * float(px) - 1.0) if px is not None else None
            for wl, px in zip(g["wilson_lower"], g["payout_x"])
        ]
        g = g.sort_values(
            by=["ev_conservative", "wilson_lower"],
            ascending=[False, False],
            na_position="last",
        ).round(4)
    else:
        g = g.sort_values("wilson_lower", ascending=False).round(4)

    g.attrs["min_n"] = int(min_n)
    g.attrs["has_ev"] = bool(payout_table)
    g.attrs["min_payout"] = min_payout
    return g


def write_composition_report(
    tickets: list,
    out_path: str,
    *,
    min_n: int = 20,
    payout_table: Optional[dict] = None,
    min_payout: float | None = None,
) -> pd.DataFrame | None:
    """Write composition CSV; print ranked preview + consistent/YOLO archetypes."""
    full = composition_breakdown(
        tickets, min_n=min_n, payout_table=payout_table, min_payout=None
    )
    if full.empty:
        print("[composition] no eligible 2+ leg tickets found")
        return None

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    full.to_csv(out_path)
    print(f"[composition] wrote {out_path} ({len(full)} buckets)")

    comp = full
    if min_payout is not None and payout_table:
        comp = composition_breakdown(
            tickets, min_n=min_n, payout_table=payout_table, min_payout=min_payout
        )
        print(
            f"[composition] --min-payout {min_payout}: "
            f"{len(full)} buckets -> {len(comp)} after floor "
            f"(dropped {len(full) - len(comp)})"
        )

    reliable = comp[comp["n"] >= min_n]
    rank_col = "ev_conservative" if payout_table else "wilson_lower"
    rank_label = (
        "ev_conservative (wilson_lower * payout_x - 1)"
        if payout_table
        else "Wilson lower bound"
    )
    print("")
    print(f"[composition] top buckets by {rank_label} (n >= {min_n}):")
    print(reliable.head(10).to_string() if not reliable.empty else "(none)")
    print("")
    print(f"[composition] bottom buckets by {rank_label} (n >= {min_n}):")
    print(reliable.tail(10).to_string() if not reliable.empty else "(none)")
    small_n = full[full["n"] < min_n]
    if len(small_n):
        print("")
        print(
            f"[composition] {len(small_n)} buckets below min-n still in CSV; "
            f"rank by {rank_col}, not win_rate."
        )
    if payout_table:
        print(
            "[composition] NOTE: EV uses N-correct To Win multipliers "
            "(never 1st-place). TE money-hit includes Flex partials."
        )

    flat = full.reset_index()
    if "tier_mix" in flat.columns:
        print("")
        print("[composition] tier_mix coverage (ticket counts):")
        print(
            flat.groupby("tier_mix")["n"]
            .sum()
            .sort_values(ascending=False)
            .to_string()
        )

        cons = flat[
            (flat["tier_mix"] == "all_goblin") & (flat["n"] >= min_n)
        ].sort_values("wilson_lower", ascending=False)
        print("")
        print(
            f"[composition] CONSISTENT (all_goblin, wilson_lower, n>={min_n}):"
        )
        cols_c = [
            c
            for c in (
                "n_legs",
                "sport",
                "tier_mix",
                "n",
                "win_rate",
                "wilson_lower",
            )
            if c in cons.columns
        ]
        print(
            cons[cols_c].head(8).to_string(index=False)
            if not cons.empty
            else "(none)"
        )

        yolo = flat[
            (flat["tier_mix"].isin(["all_standard", "mixed_tier"]))
            & (flat["n"] >= min_n)
        ].copy()
        if payout_table and "payout_x" in yolo.columns:
            floor = float(min_payout) if min_payout is not None else 2.0
            before = len(yolo)
            yolo = yolo[yolo["payout_x"].fillna(0) >= floor]
            yolo = yolo.sort_values("ev_conservative", ascending=False)
            print("")
            print(
                f"[composition] YOLO (all_standard|mixed_tier, payout_x>={floor}, "
                f"ev_conservative, n>={min_n}; {before}->{len(yolo)} after floor):"
            )
            cols_y = [
                c
                for c in (
                    "n_legs",
                    "sport",
                    "tier_mix",
                    "n",
                    "win_rate",
                    "payout_x",
                    "ev_raw",
                    "ev_conservative",
                )
                if c in yolo.columns
            ]
            print(
                yolo[cols_y].head(8).to_string(index=False)
                if not yolo.empty
                else "(none)"
            )
        else:
            print("")
            print("[composition] YOLO needs --payout-table to rank by EV.")

    months = sorted(
        {
            (t.get("date") or "")[:7]
            for t in tickets
            if (t.get("date") or "")[:7]
        }
    )
    if len(months) >= 2:
        by_month = composition_breakdown(
            tickets,
            group_cols=["month", "n_legs", "sport", "tier_mix"],
            min_n=min_n,
            payout_table=payout_table,
            min_payout=None,
        )
        month_path = str(
            Path(out_path).with_name(Path(out_path).stem + "_by_month.csv")
        )
        by_month.to_csv(month_path)
        print(f"[composition] wrote month-split {month_path}")

    return full



def build_lookup(cells: dict) -> dict:
    """Flatten cells into (sport, cat, stat_a, dir_a, stat_b, dir_b) -> shrunk_lift,
    keyed both directions since leg order within a ticket is arbitrary."""
    lookup = {}
    for sport, cats in cells.items():
        for cat, entries in cats.items():
            if cat == CAT_BASELINE_DIFF_GAME:
                continue  # baseline is for the sanity check only, not applied at pricing time
            for e in entries:
                fwd = (sport, cat, e["stat_a"], e["direction_a"], e["stat_b"], e["direction_b"])
                rev = (sport, cat, e["stat_b"], e["direction_b"], e["stat_a"], e["direction_a"])
                lookup[fwd] = e["shrunk_lift"]
                lookup[rev] = e["shrunk_lift"]
    return lookup


def adjusted_joint_prob(legs: list, lookup: dict) -> float:
    """legs: list of dicts with sport, game_id, player_id, team_id, stat_type,
    direction, model_prob.

    Approximates joint P by starting from the naive product and applying a
    pairwise lift correction for every same-game pair:
        P_adj(A and B) = P(A) * P(B) * shrunk_lift
    For 3+ leg tickets this multiplies in every pairwise correction found --
    an approximation (true clique/latent-factor modeling is phase 2), good
    enough to test whether pairwise correction alone recovers the bleed.
    """
    p = 1.0
    for leg in legs:
        p *= leg["model_prob"]

    for a, b in itertools.combinations(legs, 2):
        if a["game_id"] != b["game_id"]:
            continue
        cat = categorize(a, b)
        if cat is None or cat == CAT_BASELINE_DIFF_GAME:
            continue
        key = (a["sport"], cat, a["stat_type"], a["direction"], b["stat_type"], b["direction"])
        lift = lookup.get(key, 1.0)
        p *= lift

    return max(0.0, min(1.0, p))


# ---------------------------------------------------------------------------
# 5. Backtest: does the adjustment anticipate the May->June bleed?
# ---------------------------------------------------------------------------

def backtest(
    tickets: list,
    lookup: dict,
    out_path: str,
    *,
    population_note: str | None = None,
) -> None:
    rows = []
    skipped_ungraded = 0
    # (month -> {total, resolved}) for resolution-rate drift check
    month_res: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "resolved": 0})
    for t in tickets:
        legs = t.get("legs", [])
        if len(legs) < 2:
            continue
        month = (t.get("date") or "")[:7]
        month_res[month]["total"] += 1
        if t.get("ticket_hit") is None:
            skipped_ungraded += 1
            continue
        month_res[month]["resolved"] += 1
        naive_p = 1.0
        for leg in legs:
            naive_p *= leg["model_prob"]
        adj_p = adjusted_joint_prob(legs, lookup)
        sports = {str(leg.get("sport") or "").strip() for leg in legs if leg.get("sport")}
        sports.discard("")
        sport_label = next(iter(sports)) if len(sports) == 1 else "mixed"
        rows.append(
            {
                "date": t.get("date"),
                "month": month,
                "sport": sport_label,
                "n_legs": len(legs),
                "naive_p": naive_p,
                "adjusted_p": adj_p,
                "actual_hit": bool(t.get("ticket_hit")),
            }
        )

    if skipped_ungraded:
        print(f"[backtest] skipped {skipped_ungraded} tickets with unknown ticket_hit")

    df = pd.DataFrame(rows)
    if df.empty:
        print("[backtest] no eligible 2+ leg tickets found")
        return

    # Split by sport as well as month -- soccer serial-form residual (or any
    # single dominant sport) shouldn't get averaged into the pooled number.
    by_sport_month = (
        df.groupby(["sport", "month"])
        .agg(
            n_tickets=("actual_hit", "count"),
            actual_win_rate=("actual_hit", "mean"),
            mean_naive_p=("naive_p", "mean"),
            mean_adjusted_p=("adjusted_p", "mean"),
        )
        .round(4)
    )

    pooled_monthly = (
        df.groupby("month")
        .agg(
            n_tickets=("actual_hit", "count"),
            actual_win_rate=("actual_hit", "mean"),
            mean_naive_p=("naive_p", "mean"),
            mean_adjusted_p=("adjusted_p", "mean"),
        )
        .round(4)
    )

    # Leg-count mix (and the n_legs=2 bleed segment the original diagnostic used).
    by_legs_month = (
        df.groupby(["n_legs", "month"])
        .agg(
            n_tickets=("actual_hit", "count"),
            actual_win_rate=("actual_hit", "mean"),
            mean_naive_p=("naive_p", "mean"),
            mean_adjusted_p=("adjusted_p", "mean"),
        )
        .round(4)
    )

    # 2-leg bleed segment, also by sport
    two = df[df["n_legs"] == 2]
    two_by_sport = None
    two_pooled = None
    if not two.empty:
        two_by_sport = (
            two.groupby(["sport", "month"])
            .agg(
                n_tickets=("actual_hit", "count"),
                actual_win_rate=("actual_hit", "mean"),
                mean_naive_p=("naive_p", "mean"),
                mean_adjusted_p=("adjusted_p", "mean"),
            )
            .round(4)
        )
        two_pooled = (
            two.groupby("month")
            .agg(
                n_tickets=("actual_hit", "count"),
                actual_win_rate=("actual_hit", "mean"),
                mean_naive_p=("naive_p", "mean"),
                mean_adjusted_p=("adjusted_p", "mean"),
            )
            .round(4)
        )

    sport_counts = df["sport"].value_counts()
    mixed_n = int(sport_counts.get("mixed", 0))
    mixed_share = mixed_n / max(1, len(df))

    # Leg-count mix shift May→June (share of graded tickets)
    mix_lines = ["## Leg-count mix (graded tickets)", ""]
    for month, sub in df.groupby("month"):
        parts = []
        for n, c in sub["n_legs"].value_counts().sort_index().items():
            parts.append(f"{int(n)}-leg={int(c)} ({c/len(sub):.1%})")
        mix_lines.append(f"- {month}: " + ", ".join(parts))
    mix_lines.append("")

    def _md_table(frame: pd.DataFrame) -> str:
        """Markdown table without requiring the optional ``tabulate`` package."""
        try:
            return frame.to_markdown()
        except ImportError:
            return "```\n" + frame.to_string() + "\n```"

    note = population_note or (
        "**Population note:** ticket source not labeled — check the loader used for this run."
    )
    lines = [
        "# Leg-correlation backtest",
        "",
        "Naive vs. correlation-adjusted joint probability vs. actual ticket win rate.",
        "If `mean_adjusted_p` tracks `actual_win_rate` (and its May->June drop) better than",
        "`mean_naive_p` does, the correlation adjustment is the right lever. If not, the",
        "categories or join keys need a second look before this ships to the packer/pricer.",
        "",
        note,
        "",
        f"Tickets graded: {len(df)} | skipped ungraded: {skipped_ungraded} | "
        f"mixed (cross-sport): {mixed_n} ({mixed_share:.1%})",
        f"Resolution rate (among 2+ leg normalized): "
        f"{len(df) / max(1, len(df) + skipped_ungraded):.1%}",
        "",
        "## Resolution rate by month",
        *(
            [
                f"- {m}: {v['resolved']}/{v['total']} "
                f"({v['resolved']/max(1,v['total']):.1%})"
                for m, v in sorted(month_res.items())
            ]
            or ["- (none)"]
        ),
        "",
        *mix_lines,
        "## By sport and month",
        "(check this first -- a single sport's residual shouldn't hide in the pooled number)",
        "",
        _md_table(by_sport_month),
        "",
        "## By leg-count and month",
        "(original diagnostic was specifically 2-leg — read that row before pooled)",
        "",
        _md_table(by_legs_month),
        "",
        "## Pooled by month",
        "",
        _md_table(pooled_monthly),
    ]
    if two_by_sport is not None:
        lines += [
            "",
            "## 2-leg only — by sport and month",
            "",
            _md_table(two_by_sport),
            "",
            "## 2-leg only — pooled by month",
            "",
            _md_table(two_pooled),
        ]

    decomp_md, decomp_summary = sport_mix_decomposition(df)
    lines += ["", decomp_md]

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[backtest] wrote {out_path}")
    print(f"[backtest] mixed share={mixed_share:.1%} ({mixed_n}/{len(df)})")
    print(by_sport_month)
    print("--- pooled ---")
    print(pooled_monthly)
    if decomp_summary:
        print("--- sport-mix decomposition ---")
        print(
            f"  {decomp_summary.get('base_month')} WR={decomp_summary['wr_base']:.1%} -> "
            f"{decomp_summary.get('compare_month')} WR={decomp_summary['wr_compare']:.1%} "
            f"(d={100*decomp_summary['total_change']:+.1f} pp)"
        )
        print(
            f"  CF={decomp_summary['wr_counterfactual']:.1%}  "
            f"mix_effect={100*decomp_summary['mix_effect']:+.1f} pp  "
            f"within_sport_effect={100*decomp_summary['within_sport_effect']:+.1f} pp"
        )


# ---------------------------------------------------------------------------
# 5b. Top-confidence construction: does selectivity beat population average?
# ---------------------------------------------------------------------------

def load_calibration_curve(path: str, pick_type: str = "standard") -> list[tuple[float, float]]:
    """Load decile calibration table -> sorted (mean_pred, mean_hit) knots.

    Accepts CSV with columns mean_pred/mean_hit (and optional pick_type, decile).
    Maps raw model_prob to calibrated rate via piecewise-linear interpolation
    (isotonic-style from the decile table).
    """
    table = pd.read_csv(path)
    fam = _norm_pick_family(pick_type) or str(pick_type or "standard").strip().lower()
    if "pick_type" in table.columns:
        table = table[
            table["pick_type"].astype(str).str.lower().map(_norm_pick_family) == fam
        ]
    pred_col = next(
        (c for c in table.columns if c.lower() in {"mean_pred", "pred", "model_prob"}),
        None,
    )
    hit_col = next(
        (c for c in table.columns if c.lower() in {"mean_hit", "hit", "emp_rate"}),
        None,
    )
    if pred_col is None or hit_col is None:
        raise ValueError(
            f"calibration table needs mean_pred and mean_hit columns: {path}"
        )
    if "decile" in table.columns:
        table = table.sort_values("decile")
    else:
        table = table.sort_values(pred_col)
    knots = [
        (float(r[pred_col]), float(r[hit_col]))
        for _, r in table.iterrows()
        if pd.notna(r[pred_col]) and pd.notna(r[hit_col])
    ]
    if len(knots) < 2:
        raise ValueError(f"calibration table too small for {pick_type}: {path}")
    return knots


def calibrate_model_prob(prob: float, knots: list[tuple[float, float]]) -> float:
    """Piecewise-linear map raw model_prob -> calibrated hit rate."""
    if prob is None or (isinstance(prob, float) and math.isnan(prob)):
        return 0.0
    p = float(prob)
    xs = [k[0] for k in knots]
    ys = [k[1] for k in knots]
    if p <= xs[0]:
        return ys[0]
    if p >= xs[-1]:
        return ys[-1]
    for i in range(len(xs) - 1):
        if xs[i] <= p <= xs[i + 1]:
            if xs[i + 1] == xs[i]:
                return ys[i + 1]
            t = (p - xs[i]) / (xs[i + 1] - xs[i])
            return ys[i] + t * (ys[i + 1] - ys[i])
    return ys[-1]


def report_standard_decile_sample_depth(
    graded_df: pd.DataFrame,
    *,
    date_start: str = "2026-05-01",
    date_end: str = "2026-06-30",
) -> pd.DataFrame:
    """Compare walk-forward (sport,stat,dir) and player history depth by decile.

    Tests whether high model_prob deciles skew toward thinner pre-game sample.
    """
    if "pick_type" not in graded_df.columns:
        return pd.DataFrame()
    std = graded_df.copy()
    std["_family"] = std["pick_type"].map(_norm_pick_family)
    std = std[std["_family"] == "standard"]
    std["date"] = std["date"].astype(str).str[:10]
    std = std[
        (std["date"] >= date_start)
        & (std["date"] <= date_end)
    ].copy()
    std["model_prob"] = pd.to_numeric(std["model_prob"], errors="coerce")
    std = std.dropna(subset=["model_prob"])
    if std.empty:
        return pd.DataFrame()

    hist = graded_df.copy()
    hist["date"] = hist["date"].astype(str).str[:10]
    hist = hist.sort_values("date")

    cell_counts: dict[tuple, int] = defaultdict(int)
    player_counts: dict[str, int] = defaultdict(int)
    rows = []
    for r in hist.itertuples(index=False):
        d = str(getattr(r, "date", ""))[:10]
        key = (
            getattr(r, "sport", ""),
            getattr(r, "stat_type", ""),
            getattr(r, "direction", ""),
        )
        pid = str(getattr(r, "player_id", "") or _fold(getattr(r, "player", "")))
        fam = _norm_pick_family(getattr(r, "pick_type", ""))
        mp = getattr(r, "model_prob", None)
        in_window = date_start <= d <= date_end
        if (
            in_window
            and fam == "standard"
            and mp is not None
            and not (isinstance(mp, float) and math.isnan(float(mp)))
        ):
            rows.append(
                {
                    "date": d,
                    "model_prob": float(mp),
                    "cell_prior_n": int(cell_counts.get(key, 0)),
                    "player_prior_n": int(player_counts.get(pid, 0)) if pid else 0,
                }
            )
        cell_counts[key] += 1
        if pid:
            player_counts[pid] += 1

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = out.sort_values("model_prob")
    out["decile"] = pd.qcut(out["model_prob"], 10, labels=False, duplicates="drop") + 1
    summary = (
        out.groupby("decile")
        .agg(
            n=("model_prob", "count"),
            mean_pred=("model_prob", "mean"),
            median_cell_prior_n=("cell_prior_n", "median"),
            median_player_prior_n=("player_prior_n", "median"),
            pct_cell_prior_lt20=("cell_prior_n", lambda s: float((s < 20).mean())),
        )
        .reset_index()
    )
    return summary


def _leg_conflicts_lever_a(selected: list[dict], cand: dict) -> bool:
    """True if adding cand would create same-player multi-stat (shipped Lever A)."""
    pid = str(cand.get("player_id") or "").strip()
    if not pid:
        return False
    c_stat = _fold(cand.get("stat_type") or cand.get("prop_type") or cand.get("prop"))
    for s in selected:
        spid = str(s.get("player_id") or "").strip()
        if not spid or spid != pid:
            continue
        # Same player already on the ticket -- block (covers same/diff stat).
        return True
    return False


def simulate_top_confidence_tickets(
    graded_df: pd.DataFrame,
    *,
    n_legs: int,
    pick_type: str = "standard",
    sport: str | None = None,
    rank_by: str = "model_prob",
    date_start: str | None = "2026-05-01",
    date_end: str | None = "2026-06-30",
    thin_sport: str | None = "Soccer",
    thin_start: str | None = "2026-06-11",
    thin_end: str | None = "2026-07-19",
    payout_table: dict | None = None,
    tickets_per_day: int = 1,
    empiric_min_n: int = 20,
    calibration_curve: list[tuple[float, float]] | None = None,
    player_history_col: str = "player_prior_n",
    player_shrinkage_prior: float = 150.0,
) -> dict:
    """Greedy daily N-leg tickets from highest-confidence eligible legs.

    Contrasts with composition_breakdown (population average of published slips):
    this asks whether *selecting* the best available legs each day can clear
    breakeven where the unfiltered shape does not.

    Rules:
      - filter by pick_type family (standard/goblin/demon) via pick_type column
      - optional sport filter (omit = cross-sport)
      - rank_by model_prob (default), calibrated (decile table), empirical walk-forward
        (sport,stat,direction hit rate), or player_shrunk (shrink model_prob toward
        walk-forward cell baseline by player prior-n — same form as shrunk_lift)
      - Lever A: unique player_id on the ticket
      - thin-competition: drop thin_sport legs inside [thin_start, thin_end]
      - ticket_hit = all legs hit (sweep) -- comparable to N-correct payout

    Returns summary + per-day ticket rows.
    """
    if n_legs < 2:
        raise ValueError("n_legs must be >= 2")
    rank_by = (rank_by or "model_prob").strip().lower()
    if rank_by not in {"model_prob", "empirical", "calibrated", "player_shrunk"}:
        raise ValueError(
            "rank_by must be model_prob, calibrated, empirical, or player_shrunk"
        )

    want_family = _norm_pick_family(pick_type) or str(pick_type or "").strip().lower()
    if "pick_type" not in graded_df.columns:
        raise ValueError("graded_df missing pick_type -- reload graded props")

    base = graded_df.copy()
    base["_family"] = base["pick_type"].map(_norm_pick_family)
    base = base[base["_family"] == want_family]
    base["date"] = base["date"].astype(str).str[:10]

    # Eligibility pool for construction (window + sport + thin gate)
    df = base.copy()
    if sport:
        want_sp = str(sport).strip().upper()
        df = df[df["sport"].astype(str).str.upper() == want_sp]
    if date_start:
        df = df[df["date"] >= date_start]
    if date_end:
        df = df[df["date"] <= date_end]

    if thin_sport and not df.empty:
        sp_u = df["sport"].astype(str).str.upper()
        want = str(thin_sport or "").strip().upper()
        if want in {"SOCCER", "SOC"}:
            is_thin_sport = (
                sp_u.isin(["SOCCER", "SOC"])
                | sp_u.str.startswith("SOCCER")
                | sp_u.str.startswith("WORLDCUP")
            )
        else:
            is_thin_sport = (sp_u == want) | sp_u.str.startswith(want)
        in_window = pd.Series(True, index=df.index)
        if thin_start:
            in_window &= df["date"] >= thin_start
        if thin_end:
            in_window &= df["date"] <= thin_end
        df = df[~(is_thin_sport & in_window)]

    # Walk-forward empirical state: (sport, stat_type, direction) -> [hits, n]
    # Warm up from same-family history strictly before date_start (live-realistic).
    counts: dict[tuple, list[int]] = {}
    player_counts: dict[str, int] = defaultdict(int)
    hist_col = str(player_history_col or "player_prior_n").strip()
    use_persisted_prior = (
        rank_by == "player_shrunk"
        and hist_col in base.columns
        and pd.to_numeric(base[hist_col], errors="coerce").notna().any()
    )

    if rank_by in {"empirical", "player_shrunk"}:
        warmup = base
        if date_start:
            warmup = warmup[warmup["date"] < date_start]
        if not warmup.empty:
            g = warmup.groupby(["sport", "stat_type", "direction"], dropna=False)[
                "hit"
            ].agg(["sum", "count"])
            for key, row in g.iterrows():
                counts[key] = [int(row["sum"]), int(row["count"])]
            if rank_by == "player_shrunk" and not use_persisted_prior:
                for pid, n in warmup.groupby("player_id").size().items():
                    if pid:
                        player_counts[str(pid)] = int(n)

    day_rows: list[dict] = []
    n_days_skipped_no_rank = 0
    for date, day in df.groupby("date", sort=True):
        day = day.copy()
        if rank_by == "empirical":
            ranks = []
            for r in day.itertuples(index=False):
                key = (r.sport, r.stat_type, r.direction)
                h_n = counts.get(key)
                if h_n is None or h_n[1] < int(empiric_min_n):
                    ranks.append(0.0)
                else:
                    ranks.append(h_n[0] / h_n[1])
            day["_rank"] = ranks
            # If nothing has enough history yet, skip the day
            if (day["_rank"] <= 0).all():
                n_days_skipped_no_rank += 1
                # still update counts with today's outcomes so later days learn
                g_today = day.groupby(["sport", "stat_type", "direction"], dropna=False)[
                    "hit"
                ].agg(["sum", "count"])
                for key, row in g_today.iterrows():
                    cur = counts.setdefault(key, [0, 0])
                    cur[0] += int(row["sum"])
                    cur[1] += int(row["count"])
                continue
        elif rank_by == "calibrated":
            if not calibration_curve:
                raise ValueError("rank_by=calibrated requires calibration_curve knots")
            mp = pd.to_numeric(day["model_prob"], errors="coerce").fillna(0.0)
            day["_rank"] = mp.map(lambda p: calibrate_model_prob(float(p), calibration_curve))
        elif rank_by == "player_shrunk":
            ranks = []
            prior_ns = []
            for r in day.itertuples(index=False):
                key = (r.sport, r.stat_type, r.direction)
                h_n = counts.get(key)
                if h_n is None or h_n[1] < 1:
                    baseline = 0.5
                else:
                    baseline = h_n[0] / h_n[1]
                pid = str(getattr(r, "player_id", "") or "")
                if use_persisted_prior:
                    raw_n = getattr(r, hist_col, None)
                    try:
                        prior_n = float(raw_n) if raw_n is not None and str(raw_n) != "nan" else 0.0
                    except (TypeError, ValueError):
                        prior_n = 0.0
                else:
                    prior_n = float(player_counts.get(pid, 0))
                mp = getattr(r, "model_prob", None)
                try:
                    mp_f = float(mp) if mp is not None else 0.0
                except (TypeError, ValueError):
                    mp_f = 0.0
                ranks.append(
                    shrink_prob_toward_baseline(
                        mp_f, prior_n, baseline, prior_strength=player_shrinkage_prior
                    )
                )
                prior_ns.append(prior_n)
            day["_rank"] = ranks
            day["_player_prior_n"] = prior_ns
        else:
            day["_rank"] = pd.to_numeric(day["model_prob"], errors="coerce").fillna(0.0)

        day = day.sort_values("_rank", ascending=False)
        used_idx: set = set()
        for _t_i in range(max(1, int(tickets_per_day))):
            selected: list[dict] = []
            for idx, row in day.iterrows():
                if idx in used_idx:
                    continue
                if rank_by == "empirical" and float(row.get("_rank") or 0) <= 0:
                    continue
                cand = row.to_dict()
                if _leg_conflicts_lever_a(selected, cand):
                    continue
                selected.append(cand)
                used_idx.add(idx)
                if len(selected) >= n_legs:
                    break
            if len(selected) < n_legs:
                break
            hits = [bool(s.get("hit")) for s in selected]
            all_hit = all(hits)
            sports = {str(s.get("sport") or "").strip() for s in selected}
            sports.discard("")
            sport_label = next(iter(sports)) if len(sports) == 1 else "mixed"
            mean_rank = float(sum(float(s.get("_rank") or 0) for s in selected) / n_legs)
            mean_raw = float(
                sum(float(s.get("model_prob") or 0) for s in selected) / n_legs
            )
            mean_prior = None
            if rank_by == "player_shrunk":
                mean_prior = float(
                    sum(float(s.get("_player_prior_n") or 0) for s in selected) / n_legs
                )
            day_rows.append(
                {
                    "date": str(date)[:10],
                    "month": str(date)[:7],
                    "sport": sport_label,
                    "n_legs": n_legs,
                    "tier_mix": f"all_{want_family}" if want_family else "unknown",
                    "ticket_hit": all_hit,
                    "legs_hit": int(sum(hits)),
                    "mean_rank": round(mean_rank, 4),
                    "mean_model_prob": round(mean_raw, 4),
                    "mean_player_prior_n": round(mean_prior, 1) if mean_prior is not None else None,
                    "players": [
                        str(s.get("player") or s.get("player_id") or "") for s in selected
                    ],
                    "stats": [str(s.get("stat_type") or "") for s in selected],
                }
            )

        # Update walk-forward counts AFTER decisions for this date
        if rank_by in {"empirical", "player_shrunk"}:
            g_today = day.groupby(["sport", "stat_type", "direction"], dropna=False)[
                "hit"
            ].agg(["sum", "count"])
            for key, row in g_today.iterrows():
                cur = counts.setdefault(key, [0, 0])
                cur[0] += int(row["sum"])
                cur[1] += int(row["count"])
            if rank_by == "player_shrunk" and not use_persisted_prior:
                for pid, n in day.groupby("player_id").size().items():
                    if pid:
                        player_counts[str(pid)] = player_counts.get(str(pid), 0) + int(n)

    out_df = pd.DataFrame(day_rows)
    n = len(out_df)
    wins = int(out_df["ticket_hit"].sum()) if n else 0
    wr = float(out_df["ticket_hit"].mean()) if n else 0.0
    wl = wilson_lower_bound(wins, n) if n else 0.0

    tier_mix = f"all_{want_family}" if want_family else "unknown"
    payout_x = resolve_payout_x(n_legs, tier_mix, payout_table) if payout_table else None
    breakeven = (1.0 / float(payout_x)) if payout_x and payout_x > 0 else None
    ev_raw = (wr * float(payout_x) - 1.0) if payout_x else None
    ev_cons = (wl * float(payout_x) - 1.0) if payout_x else None

    mean_rank_all = float(out_df["mean_rank"].mean()) if n and "mean_rank" in out_df else None
    mean_raw_all = (
        float(out_df["mean_model_prob"].mean()) if n and "mean_model_prob" in out_df else None
    )
    mean_prior_all = (
        float(out_df["mean_player_prior_n"].mean())
        if n and "mean_player_prior_n" in out_df and out_df["mean_player_prior_n"].notna().any()
        else None
    )

    summary = {
        "n_legs": n_legs,
        "pick_type": want_family or pick_type,
        "tier_mix": tier_mix,
        "sport": (str(sport).strip().upper() if sport else "ALL"),
        "rank_by": rank_by,
        "date_start": date_start,
        "date_end": date_end,
        "n_tickets": n,
        "wins": wins,
        "win_rate": wr,
        "wr": wr,
        "wilson_lower": wl,
        "payout_x": payout_x,
        "breakeven_wr": breakeven,
        "clears_breakeven_raw": (wr >= breakeven) if breakeven is not None else None,
        "clears_breakeven_wilson": (wl >= breakeven) if breakeven is not None else None,
        "ev_raw": ev_raw,
        "ev_conservative": ev_cons,
        "mean_rank": mean_rank_all,
        "mean_rank_signal": mean_rank_all,
        "mean_model_prob": mean_raw_all,
        "mean_player_prior_n": mean_prior_all,
        "empiric_min_n": int(empiric_min_n) if rank_by == "empirical" else None,
        "n_days_skipped_no_empiric_history": n_days_skipped_no_rank,
        "n_days_skipped_no_rank": n_days_skipped_no_rank,
        "player_shrinkage_prior": (
            float(player_shrinkage_prior) if rank_by == "player_shrunk" else None
        ),
        "player_history_col": hist_col if rank_by == "player_shrunk" else None,
        "player_prior_source": (
            ("persisted:" + hist_col)
            if rank_by == "player_shrunk" and use_persisted_prior
            else ("walkforward_count" if rank_by == "player_shrunk" else None)
        ),
    }
    return {"summary": summary, "tickets": out_df}


def format_top_confidence_report(
    result: dict,
    *,
    population_csv: str | None = None,
) -> str:
    s = result["summary"]
    lines = [
        "## Top-confidence construction simulation",
        "",
        "Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).",
        "ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.",
        "",
        f"- shape: {s['n_legs']}-leg `{s['tier_mix']}` sport={s['sport']} rank_by={s['rank_by']}",
        f"- window: {s['date_start']} .. {s['date_end']}",
        f"- n={s['n_tickets']}  WR={s['win_rate']:.1%}  wilson_lower={s['wilson_lower']:.1%}",
    ]
    if s.get("mean_model_prob") is not None:
        lines.append(
            f"- selected mean raw model_prob={s['mean_model_prob']:.1%}  "
            f"mean rank_signal={s.get('mean_rank')}"
        )
    if s.get("rank_by") == "empirical":
        lines.append(
            f"- walk-forward empiric_min_n={s.get('empiric_min_n')} "
            f"(skipped days with no prior history: "
            f"{s.get('n_days_skipped_no_empiric_history', 0)})"
        )
    if s.get("rank_by") == "player_shrunk":
        lines.append(
            f"- player_shrunk prior_strength={s.get('player_shrinkage_prior')} "
            f"history={s.get('player_prior_source')} "
            f"mean_player_prior_n={s.get('mean_player_prior_n')}"
        )
    if s.get("payout_x") is not None:
        lines += [
            f"- payout_x={s['payout_x']}  breakeven WR={s['breakeven_wr']:.1%}",
            f"- clears breakeven (raw)? {s['clears_breakeven_raw']}  "
            f"(wilson)? {s['clears_breakeven_wilson']}",
            f"- ev_raw={s['ev_raw']}  ev_conservative={s['ev_conservative']}",
        ]
    # Compare to population composition row if CSV present
    if population_csv and Path(population_csv).is_file():
        try:
            pop = pd.read_csv(population_csv)
            if "tier_mix" not in pop.columns or "n_legs" not in pop.columns:
                lines += [
                    "",
                    f"(population CSV missing n_legs/tier_mix columns: {population_csv})",
                ]
            else:
                mask = (
                    (pop["n_legs"] == s["n_legs"])
                    & (pop["tier_mix"].astype(str) == s["tier_mix"])
                )
                if s["sport"] != "ALL" and "sport" in pop.columns:
                    mask = mask & (
                        pop["sport"].astype(str).str.upper() == str(s["sport"]).upper()
                    )
                sub = pop.loc[mask]
                if not sub.empty:
                    # if sport=ALL, aggregate or show best-matching pooled note
                    if s["sport"] == "ALL":
                        # weight by n across sports for same n_legs+tier_mix
                        nn = int(sub["n"].sum())
                        ww = int((sub["win_rate"] * sub["n"]).sum()) if nn else 0
                        pop_wr = ww / nn if nn else 0.0
                        lines += [
                            "",
                            f"Population (same n_legs+tier_mix, all sports in CSV): "
                            f"n={nn} WR={pop_wr:.1%}",
                            f"Selectivity lift vs population WR: "
                            f"{100*(s['win_rate']-pop_wr):+.1f} pp",
                        ]
                    else:
                        row = sub.sort_values("n", ascending=False).iloc[0]
                        pop_wr = float(row["win_rate"])
                        lines += [
                            "",
                            f"Population row {s['sport']} {s['n_legs']}-leg {s['tier_mix']}: "
                            f"n={int(row['n'])} WR={pop_wr:.1%} "
                            f"wilson={float(row.get('wilson_lower') or 0):.1%}",
                            f"Selectivity lift vs population WR: "
                            f"{100*(s['win_rate']-pop_wr):+.1f} pp",
                        ]
        except OSError:
            pass
    lines.append("")
    return "\n".join(lines)



# ---------------------------------------------------------------------------
# 6. Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--graded-dir", default="ui_runner/templates")
    ap.add_argument("--out", default="data/cache/leg_correlation_matrix.json")
    ap.add_argument(
        "--tickets",
        default=None,
        help="path to tickets JSON (dual-card / groups) or a directory of combined_slate_tickets_*.json",
    )
    ap.add_argument(
        "--ticket-eval",
        default=None,
        help=(
            "Glob/dir of ticket_eval_YYYY-MM-DD.html (Lever B May–June population). "
            "Takes precedence over --tickets when set."
        ),
    )
    ap.add_argument("--backtest-out", default="data/reports/corr_backtest.md")
    ap.add_argument(
        "--prior-strength",
        type=int,
        default=25,
        help="shrinkage prior strength; higher = more shrink toward "
        "independence for low-n cells",
    )
    ap.add_argument(
        "--matrix-only",
        action="store_true",
        help="Rebuild correlation cells only (skip backtest even if tickets given).",
    )
    ap.add_argument(
        "--reuse-matrix",
        default=None,
        help="Load existing leg_correlation_matrix.json and skip cell rebuild.",
    )
    ap.add_argument(
        "--simulate-gates",
        action="store_true",
        help=(
            "Also report counterfactual WR after removing tickets that violate "
            "shipped rules (Lever A + thin-competition date proxy)."
        ),
    )
    ap.add_argument(
        "--thin-sport",
        default="Soccer",
        help="Sport for thin-competition date-range proxy (default Soccer).",
    )
    ap.add_argument(
        "--thin-start",
        default="2026-06-11",
        help="Thin-competition window start inclusive (default WC kickoff).",
    )
    ap.add_argument(
        "--thin-end",
        default="2026-07-19",
        help="Thin-competition window end inclusive (default WC final).",
    )
    ap.add_argument(
        "--composition-report",
        default=None,
        help=(
            "Write win-rate-by-composition CSV (n_legs x sport x tier), ranked by "
            "Wilson lower bound. Also writes a *_by_month.csv sibling when 2+ months."
        ),
    )
    ap.add_argument(
        "--composition-min-n",
        type=int,
        default=20,
        help=(
            "Min ticket count for console top/bottom composition preview "
            "(full CSV stays unfiltered; default 20)."
        ),
    )
    ap.add_argument(
        "--payout-table",
        default=None,
        help=(
            "JSON N-correct To Win multipliers for composition EV ranking "
            '(flat {"2": 3.0} or nested {"2": {"all_goblin": 2.2, "all_standard": 3.0}}). '
            "Keys are tier_mix (line difficulty), not Power/Flex. Never 1st-place."
        ),
    )
    ap.add_argument(
        "--top-confidence-n-legs",
        type=int,
        default=None,
        help="If set, simulate greedy daily N-leg tickets from top-confidence graded legs.",
    )
    ap.add_argument(
        "--top-confidence-pick-type",
        default="standard",
        help="Line-difficulty filter: standard|goblin|demon (default standard).",
    )
    ap.add_argument(
        "--top-confidence-sport",
        default=None,
        help="Optional sport filter (omit = cross-sport pool per day).",
    )
    ap.add_argument(
        "--top-confidence-rank-by",
        default="model_prob",
        choices=["model_prob", "calibrated", "empirical", "player_shrunk"],
        help=(
            "Rank legs by model_prob (default), calibrated (decile table), "
            "empirical (sport,stat,dir) walk-forward hit rate, or player_shrunk "
            "(shrink model_prob toward cell baseline by player prior-n)."
        ),
    )
    ap.add_argument(
        "--player-history-col",
        default="player_prior_n",
        help=(
            "Column for rank_by=player_shrunk when persisted on graded props "
            "(default player_prior_n). If missing, walk-forward prop counts are used."
        ),
    )
    ap.add_argument(
        "--player-shrinkage-prior",
        type=float,
        default=150.0,
        help="prior_strength k in n/(n+k) for rank_by=player_shrunk (default 150).",
    )
    ap.add_argument(
        "--build-calibration-deciles",
        default=None,
        help="Write model_prob vs hit decile CSV to this path (reusable calibration builder).",
    )
    ap.add_argument(
        "--calibration-group-by",
        default=None,
        help="Optional column to split deciles (direction, stat_type, sport, ...).",
    )
    ap.add_argument(
        "--calibration-pick-type",
        default=None,
        help="Pick-type family for --build-calibration-deciles (default: --top-confidence-pick-type).",
    )
    ap.add_argument(
        "--calibration-n-deciles",
        type=int,
        default=10,
        help="Number of quantile bins for --build-calibration-deciles (default 10).",
    )
    ap.add_argument(
        "--skip-matrix",
        action="store_true",
        help="Load graded props only; skip correlation cell build (faster for decile/top-confidence).",
    )
    ap.add_argument(
        "--calibration-table",
        default=None,
        help=(
            "CSV with mean_pred/mean_hit (optional pick_type) for rank_by=calibrated "
            "(e.g. data/reports/calibration_standard_goblin_deciles_may_june.csv)."
        ),
    )
    ap.add_argument(
        "--decile-sample-depth",
        action="store_true",
        help=(
            "Print Standard model_prob decile vs walk-forward (sport,stat,dir) "
            "and player prior-n (May-June window)."
        ),
    )
    ap.add_argument(
        "--top-confidence-start",
        default="2026-05-01",
        help="Inclusive start date for top-confidence simulation.",
    )
    ap.add_argument(
        "--top-confidence-end",
        default="2026-06-30",
        help="Inclusive end date for top-confidence simulation.",
    )
    ap.add_argument(
        "--top-confidence-out",
        default=None,
        help="Optional CSV of constructed daily tickets.",
    )
    ap.add_argument(
        "--top-confidence-compare",
        default=None,
        help="Population composition CSV to compare selectivity lift against.",
    )
    ap.add_argument(
        "--min-payout",
        type=float,
        default=None,
        help=(
            "Drop composition buckets with payout_x below this floor "
            "(YOLO filter; requires --payout-table). Also used as YOLO archetype floor "
            "(default 2.0 when unset)."
        ),
    )
    args = ap.parse_args()

    if args.reuse_matrix:
        with open(args.reuse_matrix, "r", encoding="utf-8") as f:
            cells = json.load(f)
        print(f"[cache] loaded matrix from {args.reuse_matrix}")
        df = load_graded_props(args.graded_dir)
        print(f"[load] {len(df)} graded props (for ticket prob join / optional)")
    elif args.skip_matrix or args.build_calibration_deciles or args.top_confidence_n_legs or args.decile_sample_depth:
        # Analysis-only paths: skip expensive correlation cell build unless also needed.
        need_cells = bool(args.ticket_eval or args.tickets or args.backtest_out or args.simulate_gates)
        df = load_graded_props(args.graded_dir)
        print(f"[load] {len(df)} graded props across {df['game_id'].nunique()} games")
        cells = {}
        if need_cells and not args.skip_matrix:
            cells = build_cells(df, prior_strength=args.prior_strength)
            os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(cells, f, indent=2)
            print(f"[cells] wrote {args.out}")
        elif args.skip_matrix:
            print("[load] --skip-matrix: correlation cells not built")
    else:
        df = load_graded_props(args.graded_dir)
        print(f"[load] {len(df)} graded props across {df['game_id'].nunique()} games")

        cells = build_cells(df, prior_strength=args.prior_strength)
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(cells, f, indent=2)
        print(f"[cells] wrote {args.out}")

        # sanity: cat4 baseline should sit near 1.0 with empirical marginals.
        # Drift now points at join/grouping bugs, not calibration leakage.
        for sport, cats in cells.items():
            baseline = cats.get(CAT_BASELINE_DIFF_GAME, [])
            if baseline:
                wsum = sum(e["n"] for e in baseline) or 1
                avg_lift = sum(e["shrunk_lift"] * e["n"] for e in baseline) / wsum
                flag = "  <-- CHECK: baseline should be ~1.0" if abs(avg_lift - 1.0) > 0.05 else ""
                print(f"[sanity] {sport} baseline n-w shrunk_lift={avg_lift:.3f}{flag}")

    if args.matrix_only:
        return

    if args.build_calibration_deciles:
        cal_pt = args.calibration_pick_type or args.top_confidence_pick_type or "standard"
        cal = build_calibration_deciles(
            df,
            pick_type=cal_pt,
            group_by=args.calibration_group_by,
            n_deciles=int(args.calibration_n_deciles),
            date_start=args.top_confidence_start,
            date_end=args.top_confidence_end,
        )
        if cal.empty:
            print(
                f"[calibration-deciles] empty for pick_type={cal_pt} "
                f"group_by={args.calibration_group_by}"
            )
        else:
            out_cal = Path(args.build_calibration_deciles)
            out_cal.parent.mkdir(parents=True, exist_ok=True)
            cal.to_csv(out_cal, index=False)
            print(f"[calibration-deciles] wrote {out_cal} rows={len(cal)} pick_type={cal_pt} "
                  f"group_by={args.calibration_group_by or '(none)'}")
            print(cal.head(20).to_string(index=False))

    if args.decile_sample_depth:
        depth = report_standard_decile_sample_depth(
            df,
            date_start=args.top_confidence_start,
            date_end=args.top_confidence_end,
        )
        if depth.empty:
            print("[decile-sample-depth] no Standard legs in window (need pick_type on graded props)")
        else:
            print("[decile-sample-depth] Standard model_prob decile vs prior sample (May-June window)")
            print(depth.to_string(index=False))
            hi = depth[depth["decile"].isin([9, 10])]
            mid = depth[depth["decile"].isin([6, 7, 8])]
            if not hi.empty and not mid.empty:
                print(
                    "[decile-sample-depth] deciles 9-10 vs 6-8: "
                    f"median_cell_prior {hi['median_cell_prior_n'].median():.0f} vs "
                    f"{mid['median_cell_prior_n'].median():.0f}; "
                    f"median_player_prior {hi['median_player_prior_n'].median():.0f} vs "
                    f"{mid['median_player_prior_n'].median():.0f}; "
                    f"pct_cell_lt20 {hi['pct_cell_prior_lt20'].mean():.1%} vs "
                    f"{mid['pct_cell_prior_lt20'].mean():.1%}"
                )

    if args.top_confidence_n_legs:
        payout_table = None
        if args.payout_table:
            payout_table = load_payout_table(args.payout_table)
        calibration_curve = None
        if args.top_confidence_rank_by == "calibrated" or args.calibration_table:
            if not args.calibration_table:
                raise SystemExit("--calibration-table required for rank_by=calibrated")
            calibration_curve = load_calibration_curve(
                args.calibration_table,
                pick_type=args.top_confidence_pick_type,
            )
            print(
                f"[calibration] loaded {len(calibration_curve)} knots from "
                f"{args.calibration_table} ({args.top_confidence_pick_type})"
            )
        tc = simulate_top_confidence_tickets(
            df,
            n_legs=int(args.top_confidence_n_legs),
            pick_type=args.top_confidence_pick_type,
            sport=args.top_confidence_sport,
            rank_by=args.top_confidence_rank_by,
            date_start=args.top_confidence_start,
            date_end=args.top_confidence_end,
            thin_sport=args.thin_sport,
            thin_start=args.thin_start,
            thin_end=args.thin_end,
            payout_table=payout_table,
            calibration_curve=calibration_curve,
            player_history_col=args.player_history_col,
            player_shrinkage_prior=float(args.player_shrinkage_prior),
        )
        report = format_top_confidence_report(
            tc, population_csv=args.top_confidence_compare
        )
        print(report)
        if args.top_confidence_out:
            os.makedirs(os.path.dirname(args.top_confidence_out) or ".", exist_ok=True)
            tc["tickets"].to_csv(args.top_confidence_out, index=False)
            print(f"[top-confidence] wrote {args.top_confidence_out}")
        # also append to backtest-out when set
        try:
            with open(args.backtest_out, "a", encoding="utf-8") as f:
                f.write("\n" + report)
        except OSError:
            pass

    ticket_source = args.ticket_eval or args.tickets
    if ticket_source:
        if args.ticket_eval:
            tickets = load_tickets_from_ticket_eval_html(args.ticket_eval, graded_df=df)
            pop_note = (
                "**Population note:** `ticket_eval_YYYY-MM-DD.html` decided articles "
                "(grade_history-aligned May–June source). "
                "`combined_slate_tickets_*.json` is the oversized grade-pool and is not used here."
            )
        else:
            tickets = load_tickets(args.tickets, graded_df=df)
            pop_note = (
                "**Population note:** JSON ticket source (often `combined_slate_tickets_*.json` "
                "grade-pool). Original May→June bleed was on realized/published outcomes — "
                "prefer `--ticket-eval` for Lever B validation."
            )
        print(f"[tickets] loaded {len(tickets)} normalized 2+ leg tickets")
        lookup = build_lookup(cells)
        backtest(tickets, lookup, args.backtest_out, population_note=pop_note)

        if args.composition_report:
            payout_table = None
            if args.payout_table:
                payout_table = load_payout_table(args.payout_table)
                print(
                    f"[composition] loaded payout table "
                    f"{args.payout_table} ({len(payout_table)} n_legs keys)"
                )
            write_composition_report(
                tickets,
                args.composition_report,
                min_n=args.composition_min_n,
                payout_table=payout_table,
                min_payout=args.min_payout,
            )

        if args.simulate_gates:
            sim = simulate_current_gates(
                tickets,
                thin_sport=args.thin_sport,
                thin_start=args.thin_start,
                thin_end=args.thin_end,
            )
            if sim is None:
                print("[simulate-gates] no eligible tickets found")
            else:
                report = format_simulate_gates_report(sim)
                # Append to backtest markdown
                try:
                    with open(args.backtest_out, "a", encoding="utf-8") as f:
                        f.write("\n" + report)
                except OSError:
                    pass
                print(report)
                print(
                    f"[simulate-gates] {sim['n_total']} tickets | "
                    f"blocked any={sim['n_blocked_any']} "
                    f"(lever_a={sim['n_blocked_lever_a']}, "
                    f"thin={sim['n_blocked_thin_competition']}) | "
                    f"WR {sim['wr_baseline']:.1%} -> {sim['wr_filtered']:.1%}"
                )
                sd = (sim.get("survivors_decomp") or {}).get("summary") or {}
                if sd:
                    print(
                        f"[simulate-gates] survivors decomp "
                        f"{sd.get('base_month')}->{sd.get('compare_month')}: "
                        f"mix={100*float(sd.get('mix_effect') or 0):+.1f}pp "
                        f"within={100*float(sd.get('within_sport_effect') or 0):+.1f}pp "
                        f"total={100*float(sd.get('total_change') or 0):+.1f}pp"
                    )


if __name__ == "__main__":
    main()
