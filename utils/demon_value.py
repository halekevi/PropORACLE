"""Demon value scanner. ALERT ONLY — never tickets, Goblin-70, or mixer.

PrizePicks prices each Demon on its own. Back out implied_p from a 2-leg
Power slip pairing that Demon with one fixed reference leg, then compare
to the player's as-of hit rate on the same line.

  implied_p = 1 / (quote_2leg * p_ref)

Quote is Power "2 correct pays" = To Win / entry. Never 1st-place.
Never Balance. No quote → skip (fail closed). No universal Demon table.

STEP 0 (before any CDP scrape): measured hit rate on the exact line,
current-season games weight 2 / last-season 1. Only scrape a quote when
Wilson 95% low >= 0.50 (cheapest implied ~43% × 1.15), n >= 15, n_cur >= 3,
current-season hit rate >= 0.60, no team change, and current minutes
>= 0.85 × last-season minutes. Team-changed rows (Jaquez / DeRozan /
Brown / Ayton) are dropped — that filter removed fake gems.

A Demon is interesting only when Wilson 95% low >= implied_p * 1.15,
with n >= 15, confirmed role/minutes, no injury, no recent team change.

After 50 graded alerts, if they do not beat average implied_p, the
scanner turns off.
"""
from __future__ import annotations

import json
import math
import os
import re
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from utils.comparison_tracks import wilson_band
from utils.player_prob_shrinkage import shrink_prob_toward_baseline

TRACK = "demon_value"
PREREG_DATE = "2026-10-27"
ALERT_ONLY = True
NEVER_TICKET = True
# Two Standards on a Power 2-pick pay 3x, so each is priced at 1/√3.
STANDARD_2LEG_POWER_X = 3.0
STANDARD_P_REF = STANDARD_2LEG_POWER_X ** -0.5  # 0.577350...
CUSHION = 1.15
MIN_GAMES = 15
PROOF_N = 50
CURRENT_W = 2.0
LAST_W = 1.0
# Cheapest Demon implied ~43% × 1.15 cushion ≈ 50%. Below this, a quote cannot alert.
SCRAPE_WILSON_FLOOR = 0.50
MIN_N_CUR = 3
MIN_CUR_HR = 0.60
MINUTES_ROLE_RATIO = 0.85
SKIP_WILSON_SCRAPE_FLOOR = "wilson_low_below_scrape_floor"
SKIP_N_CUR = "n_cur"
SKIP_CUR_HR = "cur_hr"
SKIP_TEAM_CHANGED = "team_changed"
SKIP_MINUTES_ROLE = "minutes_role"
SHRINK_K = 8.0
NBA_MINUTES_FLOOR = 18.0
# Same Demon vs two Standards must match; two-Demon quote ≈ 1/(p_a × p_b).
SAME_DEMON_QUOTE_TOL = 0.08
TWO_DEMON_ABS_TOL = 0.25
TWO_DEMON_REL_TOL = 0.03
REED_AUSAR_QUOTE = 8.76
FORMULA_SAME_GAME_CONFOUNDED = "same_game_confounded"
FORMULA_DIFFERENT_GAME_BELOW = "different_game_below_product"
ALERT_SLIP_SHAPE = "demon_plus_standard_2pick"
COUNTING_PROPS = frozenset(
    {
        "fg3m",
        "fg3a",
        "fgm",
        "fga",
        "fg2m",
        "fg2a",
        "ftm",
        "fta",
        "stl",
        "blk",
        "tov",
        "ast",
        "reb",
        "oreb",
        "dreb",
    }
)

_ROOT = Path(__file__).resolve().parents[1]
PREREG_JSON = _ROOT / "ml" / "prereg" / "demon_value_2026-10-27.json"
OUT_DIR = _ROOT / "data" / "reports"
LOG = OUT_DIR / "demon_value_forward.jsonl"
LATEST = OUT_DIR / "demon_value_latest.json"
REFERENCE = OUT_DIR / "demon_value_reference.json"
PRICING_CHECK = OUT_DIR / "demon_value_pricing_check_latest.json"


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _as_date(d: date | datetime | str | None) -> date | None:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    s = str(d).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def frozen_spec() -> dict[str, Any]:
    return {
        "track": TRACK,
        "prereg_date": PREREG_DATE,
        "alert_only": True,
        "ticketed": False,
        "never_ticket": True,
        "never_1st_place": True,
        "never_balance_as_to_win": True,
        "quote": "2-leg Power N-correct: Demon + Standard other team; To Win/entry",
        "p_ref": "standard_2leg_power_3x = 1/sqrt(3) ≈ 0.577",
        "implied_p": "1 / (quote_2leg * 0.577)",
        "sanity": "two Standards Power 2-pick must quote 3x → 57.7%",
        "pricing_check": "Demon + Standard must be different games; 3–5 unconnected two-Demon pairs over nights",
        "never_submit": True,
        "cushion": CUSHION,
        "min_games": MIN_GAMES,
        "wilson": "95% lower bound on unweighted hits/n",
        "weights": {"current_season": CURRENT_W, "last_season": LAST_W},
        "scrape_prefilter": {
            "wilson_low_min": SCRAPE_WILSON_FLOOR,
            "min_n_cur": MIN_N_CUR,
            "min_cur_hr": MIN_CUR_HR,
            "minutes_role_ratio": MINUTES_ROLE_RATIO,
            "drop_team_changed": True,
        },
        "shrink_k": SHRINK_K,
        "proof_n": PROOF_N,
        "same_team_or_game": "flag_not_block",
        "play_hint": "2-pick small stakes, one Demon plus best other leg; do not stack Demons",
        "placed_default": False,
    }


def assert_prereg(*, path: Path | None = None) -> dict[str, Any]:
    spec = json.loads((path or PREREG_JSON).read_text(encoding="utf-8"))
    assert spec["track"] == TRACK
    assert spec["prereg_date"] == PREREG_DATE
    assert spec["alert_only"] is True
    assert spec["ticketed"] is False
    assert spec["never_ticket"] is True
    assert spec["placed_default"] is False
    assert spec.get("never_submit") is True
    assert abs(float(spec["p_ref"]) - STANDARD_P_REF) < 0.002
    assert abs(float(spec["cushion"]) - CUSHION) < 1e-9
    assert int(spec["min_games"]) == MIN_GAMES
    assert int(spec["proof_n"]) == PROOF_N
    return spec


def implied_p_from_pair(quote_2leg: float, p_ref: float) -> float | None:
    """Break-even of the target Demon after dividing out the reference.

    Independent 2-leg Power: p_ref * p_demon * quote = 1.
    """
    q = _f(quote_2leg)
    p = _f(p_ref)
    if q is None or q <= 1.0 or p is None or p <= 0.0 or p >= 1.0:
        return None
    out = 1.0 / (q * p)
    if out <= 0.0 or out >= 1.0:
        return None
    return float(out)


def same_demon_two_standards(quote_a: float | None, quote_b: float | None) -> dict[str, Any]:
    """Same Demon + two different Standards must quote the same N-correct."""
    a = _f(quote_a)
    b = _f(quote_b)
    delta = None if a is None or b is None else abs(a - b)
    ok = bool(a is not None and b is not None and delta is not None and delta <= SAME_DEMON_QUOTE_TOL)
    return {
        "ok": ok,
        "quote_a": a,
        "quote_b": b,
        "delta": None if delta is None else round(delta, 6),
        "tol": SAME_DEMON_QUOTE_TOL,
    }


def two_demon_product_check(
    quote_ab: float | None,
    implied_a: float | None,
    implied_b: float | None,
) -> dict[str, Any]:
    """Two Demons together should pay about 1 / (implied_a × implied_b)."""
    q = _f(quote_ab)
    pa = _f(implied_a)
    pb = _f(implied_b)
    expected = None
    if pa is not None and pb is not None and pa > 0 and pb > 0:
        expected = 1.0 / (pa * pb)
    delta = None if q is None or expected is None else q - expected
    tol = None
    if expected is not None:
        tol = max(TWO_DEMON_ABS_TOL, TWO_DEMON_REL_TOL * expected)
    ok = bool(
        q is not None
        and expected is not None
        and delta is not None
        and tol is not None
        and abs(delta) <= tol + 1e-12
    )
    below = bool(
        q is not None
        and expected is not None
        and tol is not None
        and (expected - q) > tol + 1e-12
    )
    return {
        "ok": ok,
        "below_product": below,
        "quote_ab": q,
        "implied_a": None if pa is None else round(pa, 6),
        "implied_b": None if pb is None else round(pb, 6),
        "expected": None if expected is None else round(expected, 6),
        "delta": None if delta is None else round(delta, 6),
        "tol": None if tol is None else round(tol, 6),
    }


def geometric_mean_p(quote_2leg: float) -> float | None:
    """Equal-price bootstrap of a 2-leg quote. Not a player price."""
    q = _f(quote_2leg)
    if q is None or q <= 1.0:
        return None
    return float(q ** (-0.5))


def _fold_prop(raw: object) -> str:
    k = _fold(raw)
    if k in {"3ptm", "3ptmade", "threepointersmade", "threes"}:
        return "3ptm"
    return k


def quote_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            _fold(row.get("sport") or "nba"),
            _fold(row.get("player")),
            _fold_prop(row.get("prop") or row.get("prop_type")),
            str(_f(row.get("line")) if _f(row.get("line")) is not None else row.get("line") or ""),
        )
    )


def one_per_player(
    rows: list[dict[str, Any]],
    *,
    key: str = "wilson_lo",
) -> list[dict[str, Any]]:
    """Keep the highest-`key` row for each player, first-seen player order."""
    best: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows:
        player = _fold(row.get("player"))
        if not player:
            continue
        score = _f(row.get(key))
        prev = best.get(player)
        prev_score = _f(prev.get(key)) if prev else None
        if prev is None:
            best[player] = row
            order.append(player)
        elif score is not None and (prev_score is None or score > prev_score):
            best[player] = row
    return [best[p] for p in order]


def default_standard_reference() -> dict[str, Any]:
    return {
        "pick_type": "standard",
        "implied_p": round(STANDARD_P_REF, 6),
        "quote_2leg": STANDARD_2LEG_POWER_X,
        "lineup_type": "power",
        "source": "two_standards_power_2pick_pay_3x",
        "p_ref_exact": "1/sqrt(3)",
        "player": None,
        "team": None,
        "prop": None,
        "line": None,
    }


def load_reference(*, path: Path | None = None) -> dict[str, Any]:
    base = default_standard_reference()
    p = path or REFERENCE
    if not p.exists():
        return base
    try:
        rec = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return base
    if not isinstance(rec, dict):
        return base
    out = dict(base)
    out.update(rec)
    if reference_p(out) is None:
        out["implied_p"] = round(STANDARD_P_REF, 6)
        out["pick_type"] = "standard"
    return out


def reference_p(ref: dict[str, Any]) -> float | None:
    p = _f(ref.get("implied_p"))
    if p is None:
        if _pick(ref.get("pick_type")) == "standard":
            return STANDARD_P_REF
        return None
    if p <= 0.0 or p >= 1.0:
        return None
    if abs(p - STANDARD_P_REF) <= 0.002:
        return STANDARD_P_REF
    return p


def trust_mix_n_correct(parsed: dict[str, Any]) -> dict[str, Any] | None:
    """Demon + Standard: use Power '2 correct pays' when To Win is blank.

    1st-place 3x is the Standard decoy. A mix quote differs (e.g. 3.25x).
    """
    q = trusted_quote(parsed)
    if q is not None:
        return q
    raw = _f(parsed.get("n_correct_raw"))
    first = _f(parsed.get("first_place_x"))
    if raw is None or raw <= 1.0:
        return None
    if first is not None and abs(raw - first) <= 0.05:
        return None
    return {
        "power_x": raw,
        "to_win": parsed.get("to_win"),
        "entry": parsed.get("entry") or 20.0,
        "lineup_type": "power",
        "reversion_disclaimer": bool(parsed.get("reversion_disclaimer")),
        "quote_trusted": True,
        "n_correct_raw": raw,
        "first_place_x": first,
    }


def trust_two_standard_n_correct(parsed: dict[str, Any]) -> dict[str, Any] | None:
    """Two Standards: '2 correct pays 3X' is 57.7% even if To Win is blank."""
    q = trusted_quote(parsed)
    if q is not None:
        return q
    raw = _f(parsed.get("n_correct_raw") or parsed.get("power_x"))
    if raw is None or abs(raw - STANDARD_2LEG_POWER_X) > 0.08:
        return None
    return {
        "power_x": STANDARD_2LEG_POWER_X,
        "to_win": parsed.get("to_win"),
        "entry": parsed.get("entry") or 20.0,
        "lineup_type": "power",
        "reversion_disclaimer": bool(parsed.get("reversion_disclaimer")),
        "quote_trusted": True,
        "n_correct_raw": STANDARD_2LEG_POWER_X,
    }


def two_standard_sanity(quote_2leg: float | None, *, p_ref: float | None = None) -> dict[str, Any]:
    """Two Standards on Power 2-pick must be 3x → 57.7%. Else the parser is off."""
    pref = STANDARD_P_REF if p_ref is None else float(p_ref)
    q = _f(quote_2leg)
    implied = implied_p_from_pair(q or 0.0, pref) if q else None
    ok = bool(
        q is not None
        and abs(q - STANDARD_2LEG_POWER_X) <= 0.08
        and implied is not None
        and abs(implied - STANDARD_P_REF) < 0.005
    )
    return {
        "ok": ok,
        "quote_2leg": q,
        "implied_p": None if implied is None else round(implied, 6),
        "expected_x": STANDARD_2LEG_POWER_X,
        "expected_p": round(STANDARD_P_REF, 6),
        "p_ref": round(pref, 6),
    }


def list_standards(board: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in board:
        if _pick(row.get("pick_type")) != "standard":
            continue
        if _side(row) != "OVER":
            continue
        team = str(row.get("team") or "").strip()
        player = str(row.get("player") or "").strip()
        if not team or not player:
            continue
        key = quote_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def pick_standards_for_demon(
    demon: dict[str, Any],
    standards: list[dict[str, Any]],
    *,
    n: int = 1,
    also_avoid: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Other team and other game. PrizePicks requires two teams; same-game
    Standards confound implied_p (Towns NYK + Robinson DET tonight).
    """
    dt = _fold(demon.get("team"))
    if not dt:
        return []
    sport = _fold(demon.get("sport"))
    used = {dt}
    avoid = [x for x in (also_avoid or []) if x]
    out: list[dict[str, Any]] = []
    ordered = sorted(
        standards,
        key=lambda row: 0 if sport and _fold(row.get("sport")) == sport else 1,
    )
    for row in ordered:
        team = _fold(row.get("team"))
        if not team or team in used:
            continue
        if _fold(row.get("player")) == _fold(demon.get("player")):
            continue
        if same_game(demon, row):
            continue
        if any(same_game(row, other) for other in avoid):
            continue
        out.append(row)
        used.add(team)
        if len(out) >= n:
            break
    return out


def pick_standard_for_demon(
    demon: dict[str, Any],
    standards: list[dict[str, Any]],
    *,
    also_avoid: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Other team and other game. Same sport preferred."""
    got = pick_standards_for_demon(demon, standards, n=1, also_avoid=also_avoid)
    return got[0] if got else None


def game_teams(row: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for k in ("team", "opp", "opp_team", "opponent"):
        t = _fold(row.get(k))
        if len(t) >= 2:
            out.add(t)
    if len(out) >= 2:
        return out
    blob = str(row.get("matchup") or "")
    parsed: set[str] = set()
    for part in re.split(r"\b(?:vs\.?|@|v)\b", blob, flags=re.I):
        t = _fold(part)
        if 2 <= len(t) <= 4:
            parsed.add(t)
    team = _fold(row.get("team"))
    if team and parsed and team not in parsed:
        return out
    out.update(parsed)
    return out


def game_key(row: dict[str, Any]) -> str:
    gid = str(row.get("pp_game_id") or row.get("game_id") or "").strip()
    if gid:
        return "id:" + gid
    teams = game_teams(row)
    if len(teams) >= 2:
        return "t:" + "|".join(sorted(teams))
    return ""


def same_game(a: dict[str, Any], b: dict[str, Any]) -> bool:
    ka, kb = game_key(a), game_key(b)
    if ka and kb:
        return ka == kb
    ta, tb = game_teams(a), game_teams(b)
    if not ta or not tb or not (ta & tb):
        return False
    # Shared team is same-game when at least one side names both clubs
    # (Towns NYK + Robinson DET). Same team with no opponent is a flag,
    # not a game match — PrizePicks already requires two teams.
    return len(ta) >= 2 or len(tb) >= 2


def pair_id(a: dict[str, Any], b: dict[str, Any]) -> str:
    ka, kb = quote_key(a), quote_key(b)
    return "|".join(sorted((ka, kb)))


def pick_two_demons_different_games(
    demons: list[dict[str, Any]],
    *,
    exclude_ids: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Two Demons that do not share a game. Same-game pairs confound the product."""
    skip = exclude_ids or set()
    for i, a in enumerate(demons):
        if not game_key(a) and not game_teams(a):
            continue
        for b in demons[i + 1 :]:
            if _fold(a.get("player")) == _fold(b.get("player")):
                continue
            if same_game(a, b):
                continue
            if not game_key(b) and not game_teams(b):
                continue
            if pair_id(a, b) in skip:
                continue
            return a, b
    return None


UNCONNECTED_PAIR_NEED = 5


def pair_is_unconnected(
    a: dict[str, Any],
    b: dict[str, Any],
    std_a: dict[str, Any] | None,
    std_b: dict[str, Any] | None,
) -> bool:
    """Every implied price from a Demon + Standard in a different game."""
    if same_game(a, b):
        return False
    if std_a is None or std_b is None:
        return False
    if same_game(a, std_a) or same_game(b, std_b):
        return False
    if same_game(a, std_b) or same_game(b, std_a):
        return False
    if same_game(std_a, std_b):
        return False
    return True


def refs_unconnected_from_demon(
    demon: dict[str, Any] | None,
    *standards: dict[str, Any] | None,
) -> bool:
    """Each reference Standard is a different game than the Demon and the others."""
    if not demon:
        return False
    live = [s for s in standards if s]
    if len(live) != len(standards) or not live:
        return False
    for std in live:
        if same_game(demon, std):
            return False
    for i, a in enumerate(live):
        for b in live[i + 1 :]:
            if same_game(a, b):
                return False
    return True


def pair_quotes_complete(check: dict[str, Any] | None) -> bool:
    """Both Demon+Standard quotes and the two-Demon mix were readable."""
    if not isinstance(check, dict):
        return False
    return (
        check.get("quote_ab") is not None
        and check.get("quote_a_vs_std") is not None
        and check.get("quote_b_vs_std") is not None
        and check.get("error") != "unreadable_quote"
    )


def pair_is_settled(check: dict[str, Any] | None) -> bool:
    """Unconnected and complete — do not scrape this pair again."""
    if not isinstance(check, dict):
        return False
    return bool(check.get("unconnected") and pair_quotes_complete(check))


def formula_ok_status(
    *,
    different_game: dict[str, Any] | None,
) -> bool | str:
    """same_game_confounded until a different-game pair has been scraped."""
    if not different_game or different_game.get("error") or different_game.get("quote_ab") is None:
        return FORMULA_SAME_GAME_CONFOUNDED
    if different_game.get("below_product"):
        return FORMULA_DIFFERENT_GAME_BELOW
    if different_game.get("ok"):
        return True
    return False


def pick_reed_ausar_pair(demons: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Historical 8.76x two-Demon slip: Reed + Ausar 0.5 3PTM."""
    reed = ausar = None
    for row in demons:
        pn = _fold(row.get("player"))
        prop = _fold_prop(row.get("prop") or row.get("prop_type"))
        line = _f(row.get("line"))
        if prop != "3ptm" or line != 0.5:
            continue
        if pn in {"paulreed", "paulreedjr"}:
            reed = row
        elif pn in {"ausarthompson", "ausar"}:
            ausar = row
        if reed is not None and ausar is not None:
            return reed, ausar
    return None


def pick_two_demons_different_teams(
    demons: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    for i, a in enumerate(demons):
        ta = _fold(a.get("team"))
        if not ta:
            continue
        for b in demons[i + 1 :]:
            tb = _fold(b.get("team"))
            if not tb or tb == ta:
                continue
            if _fold(a.get("player")) == _fold(b.get("player")):
                continue
            return a, b
    return None


def two_standards_different_teams(
    standards: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    for i, a in enumerate(standards):
        ta = _fold(a.get("team"))
        if not ta:
            continue
        for b in standards[i + 1 :]:
            tb = _fold(b.get("team"))
            if not tb or tb == ta:
                continue
            if _fold(a.get("player")) == _fold(b.get("player")):
                continue
            if _fold(a.get("sport")) and _fold(a.get("sport")) != _fold(b.get("sport")):
                continue
            return a, b
    return None


def poisson_sf(k: int, lam: float) -> float:
    """P(X >= k) for Poisson(lam)."""
    if k <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    term = math.exp(-lam)
    cdf = 0.0
    for i in range(0, k):
        cdf += term
        term *= lam / (i + 1.0)
        if cdf >= 1.0:
            return 0.0
    return max(0.0, min(1.0, 1.0 - cdf))


def normal_sf(x: float, mu: float, sigma: float) -> float:
    """P(X > x) for Normal(mu, sigma)."""
    if sigma <= 0:
        return 1.0 if mu > x else 0.0
    z = (x - mu) / (sigma * math.sqrt(2.0))
    return max(0.0, min(1.0, 0.5 * math.erfc(z)))


def projection_p(
    *,
    line: float,
    proj_stat: float | None,
    prop_col: str | None,
    sample_std: float | None = None,
) -> float | None:
    if proj_stat is None or not math.isfinite(proj_stat) or proj_stat < 0:
        return None
    col = str(prop_col or "")
    if col in COUNTING_PROPS or (line % 1.0) == 0.5:
        k = int(math.floor(line) + 1)
        return poisson_sf(k, proj_stat)
    sigma = sample_std if sample_std and sample_std > 0 else max(1.0, 0.4 * max(proj_stat, 1.0))
    return normal_sf(line, proj_stat, sigma)


def _pick(raw: object) -> str:
    k = str(raw or "").strip().lower()
    if "dem" in k:
        return "demon"
    if "gob" in k:
        return "goblin"
    if "std" in k or "standard" in k:
        return "standard"
    return k


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or row.get("over_under") or "").strip().upper()
    if raw in {"U", "UNDER", "LESS", "LOWER"}:
        return "UNDER"
    return "OVER"


def is_demon_over(row: dict[str, Any]) -> bool:
    if _pick(row.get("pick_type")) != "demon":
        return False
    return _side(row) == "OVER"


# Local copy so STEP 0 does not depend on utils.nba_early_l5 (not on every tree).
_PROP_COL = {
    "points": "pts",
    "pts": "pts",
    "rebounds": "reb",
    "reb": "reb",
    "assists": "ast",
    "ast": "ast",
    "steals": "stl",
    "stl": "stl",
    "blocked shots": "blk",
    "blocks": "blk",
    "blk": "blk",
    "turnovers": "tov",
    "tov": "tov",
    "3-pt made": "fg3m",
    "3ptm": "fg3m",
    "3-ptm": "fg3m",
    "3pm": "fg3m",
    "fg3m": "fg3m",
    "fg attempted": "fga",
    "field goals attempted": "fga",
    "fga": "fga",
    "fg made": "fgm",
    "field goals made": "fgm",
    "fgm": "fgm",
    "two pointers attempted": "fg2a",
    "2pt attempted": "fg2a",
    "2pa": "fg2a",
    "fg2a": "fg2a",
    "two pointers made": "fg2m",
    "2pt made": "fg2m",
    "2pm": "fg2m",
    "fg2m": "fg2m",
    "3-pt attempted": "fg3a",
    "3pt attempted": "fg3a",
    "three pointers attempted": "fg3a",
    "3pa": "fg3a",
    "fg3a": "fg3a",
    "free throws attempted": "fta",
    "ft attempted": "fta",
    "fta": "fta",
    "free throws made": "ftm",
    "ft made": "ftm",
    "ftm": "ftm",
    "pts+rebs+asts": "pra",
    "pts+reb+ast": "pra",
    "pra": "pra",
    "pts+rebs": "pr",
    "pts+reb": "pr",
    "pr": "pr",
    "pts+asts": "pa",
    "pts+ast": "pa",
    "pa": "pa",
    "rebs+asts": "ra",
    "reb+ast": "ra",
    "ra": "ra",
    "offensive rebounds": "oreb",
    "oreb": "oreb",
}


def prop_column(prop: object) -> str | None:
    key = str(prop or "").strip().lower().replace("(combo)", "").strip()
    if key in _PROP_COL:
        return _PROP_COL[key]
    return _PROP_COL.get(_fold_prop(prop))


def measured_from_games(
    games: list[dict[str, Any]],
    *,
    line: float,
    as_of: date,
    current_opener: date,
    last_opener: date,
    proj_stat: float | None = None,
    prop_col: str | None = None,
    minutes_proj: float | None = None,
) -> dict[str, Any]:
    """As-of hit rate: current season weighted 2, last season 1. No preseason."""
    hits = 0
    n = 0
    hits_w = 0.0
    n_w = 0.0
    mins_sum = 0.0
    stat_sum = 0.0
    sq = 0.0
    n_cur = 0
    hits_cur = 0
    n_last = 0
    hits_last = 0
    mins_cur_sum = 0.0
    mins_cur_n = 0
    mins_last_sum = 0.0
    mins_last_n = 0
    last_team = ""
    last_team_date: date | None = None
    for g in games:
        gd = _as_date(g.get("game_date") or g.get("date"))
        if gd is None or gd >= as_of:
            continue
        window = str(g.get("window") or "regular").strip().lower()
        if window not in {"regular", "playoffs", "playoff"}:
            continue
        mins = _f(g.get("minutes"))
        if mins is not None and mins <= 0:
            continue
        actual = _f(g.get("actual") if g.get("actual") is not None else g.get("value"))
        if actual is None:
            continue
        if gd >= current_opener:
            w = CURRENT_W
            season = "current"
        elif gd >= last_opener:
            w = LAST_W
            season = "last"
        else:
            continue
        hit = 1 if actual > line else 0
        hits += hit
        n += 1
        hits_w += w * hit
        n_w += w
        if mins is not None and mins > 0:
            mins_sum += mins
            stat_sum += actual
        sq += (actual - line) ** 2
        if season == "current":
            n_cur += 1
            hits_cur += hit
            if mins is not None and mins > 0:
                mins_cur_sum += mins
                mins_cur_n += 1
        else:
            n_last += 1
            hits_last += hit
            if mins is not None and mins > 0:
                mins_last_sum += mins
                mins_last_n += 1
            tm = str(g.get("team") or "").strip()
            if tm and (last_team_date is None or gd >= last_team_date):
                last_team = tm
                last_team_date = gd
    raw = (hits_w / n_w) if n_w > 0 else None
    band = wilson_band(hits, n) if n > 0 else None
    per_min = (stat_sum / mins_sum) if mins_sum > 0 else None
    if proj_stat is None and minutes_proj is not None and per_min is not None:
        proj_stat = minutes_proj * per_min
    sample_std = math.sqrt(sq / n) if n >= 2 else None
    prior = projection_p(
        line=line, proj_stat=proj_stat, prop_col=prop_col, sample_std=sample_std
    )
    shrunk = raw
    if raw is not None and prior is not None:
        shrunk = shrink_prob_toward_baseline(raw, n_w, prior, SHRINK_K)
    cur_hr = (hits_cur / n_cur) if n_cur > 0 else None
    minutes_cur = (mins_cur_sum / mins_cur_n) if mins_cur_n > 0 else None
    minutes_last = (mins_last_sum / mins_last_n) if mins_last_n > 0 else None
    return {
        "n": n,
        "hits": hits,
        "hit_rate_raw": None if raw is None else round(raw, 4),
        "hit_rate": None if shrunk is None else round(float(shrunk), 4),
        "wilson_low": None if band is None else round(band[0], 4),
        "wilson_high": None if band is None else round(band[1], 4),
        "proj_stat": None if proj_stat is None else round(float(proj_stat), 3),
        "proj_p": None if prior is None else round(float(prior), 4),
        "n_weighted": round(n_w, 2),
        "n_cur": n_cur,
        "hits_cur": hits_cur,
        "cur_hr": None if cur_hr is None else round(cur_hr, 4),
        "n_last": n_last,
        "hits_last": hits_last,
        "minutes_cur": None if minutes_cur is None else round(minutes_cur, 3),
        "minutes_last": None if minutes_last is None else round(minutes_last, 3),
        "last_season_team": last_team,
    }


def _name_keys(player: str) -> set[str]:
    """Match 'Mikel Brown' to box 'Mikel Brown Jr.' and the reverse."""
    from utils.nba_minutes_role import _norm_name

    pn = _norm_name(player)
    if not pn:
        return set()
    stripped = re.sub(r"\s+(jr\.?|sr\.?|iii|ii|iv)$", "", pn).strip()
    keys = {pn, stripped}
    if stripped:
        keys.update({stripped + " jr", stripped + " jr.", stripped + " sr", stripped + " sr."})
    return {k for k in keys if k}


def nba_games_for_player(
    boxes: Any,
    player: str,
    *,
    prop: str,
    as_of: date,
) -> list[dict[str, Any]]:
    col = prop_column(prop)
    if col is None or boxes is None or getattr(boxes, "empty", True):
        return []
    if col not in boxes.columns or "_pn" not in boxes.columns:
        return []
    keys = _name_keys(player)
    if not keys:
        return []
    g = boxes[boxes["_pn"].isin(keys)]
    out: list[dict[str, Any]] = []
    for _, row in g.iterrows():
        gd = _as_date(row.get("game_date"))
        if gd is None or gd >= as_of:
            continue
        out.append(
            {
                "game_date": gd.isoformat(),
                "actual": row.get(col),
                "minutes": row.get("minutes"),
                "window": row.get("window") or "regular",
                "team": row.get("team"),
            }
        )
    return out


def injury_blocks(row: dict[str, Any]) -> bool:
    raw = ""
    for key in ("player_injury_status", "injury_status", "injury", "status", "player_status"):
        raw = str(row.get(key) or "").strip()
        if raw:
            break
    fold = _fold(raw)
    if fold in {"q", "d", "out", "questionable", "doubtful", "gtd", "dtd"}:
        return True
    up = raw.strip().upper()
    if not up or up in {"NAN", "NONE", "HEALTHY", "ACTIVE"}:
        return False
    if "OUT" in up or up in {"O"}:
        return True
    if any(tok in up for tok in ("DOUBTFUL", "QUESTIONABLE")):
        return True
    return False


def role_confirmed(row: dict[str, Any]) -> bool:
    sport = str(row.get("sport") or "").strip().upper()
    mins = _f(
        row.get("minutes_proj")
        or row.get("projected_minutes")
        or row.get("minutes")
        or row.get("proj_min")
    )
    if sport in {"NBA", "WNBA", "NBA1H", "NBA1Q", "WNBA1H", "WNBA1Q"}:
        if mins is not None and mins >= NBA_MINUTES_FLOOR:
            return True
    elif sport == "NFL":
        snaps = _f(row.get("snap_pct") or row.get("snaps"))
        if snaps is not None and snaps >= 40:
            return True
        if str(row.get("inactive") or "").strip().lower() in {"1", "true", "yes"}:
            return False
    tier = str(row.get("minutes_tier") or row.get("min_tier") or "").strip().upper()
    if any(tok in tier for tok in ("HIGH", "STAR", "STARTER")):
        return True
    role = str(row.get("role_fit") or row.get("off_role") or row.get("role") or "").strip().lower()
    if role in {"starter", "high", "high_vol", "confirmed", "projected"}:
        return True
    return False


def team_change_blocks(row: dict[str, Any], *, slate: str) -> tuple[bool, str]:
    sport = str(row.get("sport") or "").strip().upper()
    if sport not in {"NBA", "NBA1Q", "NBA1H"}:
        return False, ""
    board_team = str(row.get("team") or "").strip()
    if not board_team:
        return False, ""
    try:
        from utils.nba_board_team import team_changed_status
    except Exception:
        return False, ""
    st = team_changed_status(str(row.get("player") or ""), board_team, slate=slate)
    if st.get("power_l5_blocked"):
        return True, str(st.get("block_reason") or "team_changed")
    return False, ""


def _norm_board_team(raw: object) -> str:
    try:
        from utils.roster_change import norm_nba_team

        return norm_nba_team(raw)
    except Exception:
        return str(raw or "").strip().upper()


def _scrape_team_changed(
    row: dict[str, Any],
    *,
    slate: str,
    last_season_team: str,
    team_changed_fn: Callable[..., tuple[bool, str]] | None = None,
) -> tuple[bool, str]:
    """Drop last-season team != board team. Permanent — not a 5-game unblock."""
    board_tm = _norm_board_team(row.get("team"))
    last_tm = _norm_board_team(last_season_team)
    if board_tm and last_tm and board_tm != last_tm:
        return True, SKIP_TEAM_CHANGED
    if team_changed_fn is not None:
        blocked, _why = team_changed_fn(row, slate=slate)
        if blocked:
            return True, SKIP_TEAM_CHANGED
        return False, ""
    sport = str(row.get("sport") or "").strip().upper()
    if sport not in {"NBA", "NBA1Q", "NBA1H"}:
        return False, ""
    if not board_tm:
        return False, ""
    try:
        from utils.nba_board_team import team_changed_status
    except Exception:
        return False, ""
    st = team_changed_status(str(row.get("player") or ""), str(row.get("team") or ""), slate=slate)
    if st.get("team_changed"):
        return True, SKIP_TEAM_CHANGED
    return False, ""


def scrape_prefilter(
    row: dict[str, Any],
    *,
    games: list[dict[str, Any]] | None,
    date_str: str,
    current_opener: date,
    last_opener: date,
    team_changed_fn: Callable[..., tuple[bool, str]] | None = None,
) -> dict[str, Any]:
    """STEP 0: is this Demon OVER worth a CDP quote scrape?

    Wilson 95% low >= 0.50, n >= 15, n_cur >= 3, current-season HR >= 0.60,
    no team change, current minutes >= 0.85 × last-season minutes.
    """
    why: list[str] = []
    as_of = _as_date(date_str) or date.today()
    line = _f(row.get("line"))
    measured: dict[str, Any] | None = None
    if line is not None and games:
        measured = measured_from_games(
            games,
            line=line,
            as_of=as_of,
            current_opener=current_opener,
            last_opener=last_opener,
            prop_col=prop_column(row.get("prop") or row.get("prop_type")),
        )
    n = n_cur = 0
    cur_hr = wilson_low = minutes_cur = minutes_last = None
    last_season_team = ""
    if measured is None:
        why.append("no_measured_rate")
    else:
        n = int(measured.get("n") or 0)
        n_cur = int(measured.get("n_cur") or 0)
        cur_hr = measured.get("cur_hr")
        wilson_low = measured.get("wilson_low")
        minutes_cur = measured.get("minutes_cur")
        minutes_last = measured.get("minutes_last")
        last_season_team = str(measured.get("last_season_team") or "")
        if n < MIN_GAMES:
            why.append(f"n<{MIN_GAMES}")
        if n_cur < MIN_N_CUR:
            why.append(SKIP_N_CUR)
        if n_cur > 0 and (cur_hr is None or float(cur_hr) + 1e-12 < MIN_CUR_HR):
            why.append(SKIP_CUR_HR)
        if wilson_low is None:
            why.append("no_wilson")
        elif float(wilson_low) + 1e-12 < SCRAPE_WILSON_FLOOR:
            why.append(SKIP_WILSON_SCRAPE_FLOOR)
        if minutes_last is not None and float(minutes_last) > 0:
            if minutes_cur is None or float(minutes_cur) + 1e-12 < MINUTES_ROLE_RATIO * float(minutes_last):
                why.append(SKIP_MINUTES_ROLE)
    team_block, _team_why = _scrape_team_changed(
        row,
        slate=date_str,
        last_season_team=last_season_team,
        team_changed_fn=team_changed_fn,
    )
    if team_block:
        why.append(SKIP_TEAM_CHANGED)
    return {
        "ok": not why,
        "why": why,
        "measured": measured,
        "n": None if measured is None else n,
        "n_cur": None if measured is None else n_cur,
        "cur_hr": None if measured is None else cur_hr,
        "wilson_low": None if measured is None else wilson_low,
        "minutes_cur": None if measured is None else minutes_cur,
        "minutes_last": None if measured is None else minutes_last,
        "last_season_team": last_season_team,
        "team_changed": team_block,
    }


def _prefilter_skip_record(
    row: dict[str, Any],
    *,
    date_str: str,
    pre: dict[str, Any],
) -> dict[str, Any]:
    measured = pre.get("measured") if isinstance(pre.get("measured"), dict) else None
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "date": date_str,
        "track": TRACK,
        "kind": "scan",
        "alert_only": True,
        "ticketed": False,
        "placed": False,
        "player": row.get("player"),
        "team": row.get("team"),
        "sport": row.get("sport") or "NBA",
        "prop": row.get("prop") or row.get("prop_type"),
        "line": _f(row.get("line")),
        "pick_type": "demon",
        "side": "OVER",
        "qualified": False,
        "why": ",".join(pre.get("why") or ["skip"]),
        "n": pre.get("n"),
        "n_cur": pre.get("n_cur"),
        "cur_hr": pre.get("cur_hr"),
        "wilson_low": pre.get("wilson_low"),
        "minutes_cur": pre.get("minutes_cur"),
        "minutes_last": pre.get("minutes_last"),
        "team_changed": bool(pre.get("team_changed")),
        "hits": None if measured is None else measured.get("hits"),
        "measured_rate": None if measured is None else measured.get("hit_rate"),
        "hit_rate_raw": None if measured is None else measured.get("hit_rate_raw"),
        "implied_p": None,
        "quote_2leg": None,
        "result": "pending",
        "prefilter": True,
    }


def _row_games(
    row: dict[str, Any],
    *,
    as_of: date,
    boxes: Any = None,
    games_by_player: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]] | None:
    player = str(row.get("player") or "")
    if games_by_player is not None:
        return games_by_player.get(_fold(player)) or games_by_player.get(player)
    if boxes is not None:
        return nba_games_for_player(
            boxes,
            player,
            prop=str(row.get("prop") or row.get("prop_type") or ""),
            as_of=as_of,
        )
    return None


def scrape_candidates(
    board: list[dict[str, Any]],
    *,
    date_str: str,
    boxes: Any = None,
    games_by_player: dict[str, list[dict[str, Any]]] | None = None,
    current_opener: date | None = None,
    last_opener: date | None = None,
    team_changed_fn: Callable[..., tuple[bool, str]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """STEP 0 board filter. Return (passers, skip records). Never scrapes."""
    as_of = _as_date(date_str) or date.today()
    if current_opener is None or last_opener is None:
        cur, last = _opener_pair(as_of)
        current_opener = current_opener or cur
        last_opener = last_opener or last
    passers: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in board:
        if _pick(row.get("pick_type")) != "demon":
            continue
        if _side(row) == "UNDER":
            continue
        games = _row_games(
            row, as_of=as_of, boxes=boxes, games_by_player=games_by_player
        )
        pre = scrape_prefilter(
            row,
            games=games,
            date_str=date_str,
            current_opener=current_opener,
            last_opener=last_opener,
            team_changed_fn=team_changed_fn,
        )
        if pre["ok"]:
            passers.append(row)
        else:
            skipped.append(_prefilter_skip_record(row, date_str=date_str, pre=pre))
    return passers, skipped


def trusted_quote(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not raw or not isinstance(raw, dict):
        return None
    if raw.get("quote_trusted") is False:
        return None
    x = _f(raw.get("power_x") or raw.get("quote") or raw.get("implied_x"))
    if x is None or x <= 1.0:
        return None
    if raw.get("first_place_used"):
        return None
    return {
        "power_x": x,
        "to_win": _f(raw.get("to_win")),
        "entry": _f(raw.get("entry")),
        "lineup_type": str(raw.get("lineup_type") or "power").strip().lower() or "power",
        "reversion_disclaimer": bool(raw.get("reversion_disclaimer")),
        "quote_trusted": True,
        "legs": raw.get("legs") or [],
    }


def alert_bar(wilson_low: float | None, implied_p: float | None) -> bool:
    if wilson_low is None or implied_p is None:
        return False
    return wilson_low + 1e-12 >= implied_p * CUSHION


def skip_reasons(
    *,
    is_demon: bool,
    side: str,
    quote: dict[str, Any] | None,
    p_ref: float | None,
    implied: float | None,
    measured: dict[str, Any] | None,
    role_ok: bool,
    injured: bool,
    team_block: bool,
    scanner_on: bool,
) -> list[str]:
    why: list[str] = []
    if not scanner_on:
        why.append("scanner_off")
    if not is_demon:
        why.append("not_demon")
    if side != "OVER":
        why.append("demon_under_invalid")
    if p_ref is None:
        why.append("no_reference_implied_p")
    if quote is None:
        why.append("no_trusted_2leg_quote")
    if implied is None:
        why.append("implied_p_fail_closed")
    if measured is None:
        why.append("no_measured_rate")
    else:
        if int(measured.get("n") or 0) < MIN_GAMES:
            why.append(f"n<{MIN_GAMES}")
        if measured.get("wilson_low") is None:
            why.append("no_wilson")
        elif implied is not None and not alert_bar(measured.get("wilson_low"), implied):
            why.append("wilson_low_below_implied_cushion")
    if not role_ok:
        why.append("role_or_minutes_unconfirmed")
    if injured:
        why.append("injury_tag")
    if team_block:
        why.append("team_changed_last_5")
    return why


def _same_flag(row: dict[str, Any], ref: dict[str, Any], field: str) -> bool:
    a = _fold(row.get(field))
    b = _fold(ref.get(field))
    return bool(a and b and a == b)


def evaluate_row(
    row: dict[str, Any],
    *,
    date_str: str,
    quote: dict[str, Any] | None,
    reference: dict[str, Any],
    games: list[dict[str, Any]] | None,
    current_opener: date,
    last_opener: date,
    scanner_on: bool = True,
    team_changed_fn: Callable[..., tuple[bool, str]] | None = None,
    price_scope: str | None = None,
) -> dict[str, Any]:
    as_of = _as_date(date_str) or date.today()
    p_ref = reference_p(reference)
    q = trusted_quote(quote)
    ref_leg = quote.get("ref_leg") if isinstance(quote, dict) else None
    ref_row = dict(reference)
    if isinstance(ref_leg, dict):
        for k, v in ref_leg.items():
            if v is not None:
                ref_row[k] = v
    # Same-game is vs the Standard on the 2-leg slip, not the catalog p_ref row.
    ref_same_game = bool(q) and isinstance(ref_leg, dict) and same_game(row, ref_row)
    if ref_same_game:
        q = None
    implied = implied_p_from_pair(q["power_x"], p_ref) if q and p_ref else None
    line = _f(row.get("line"))
    col = prop_column(row.get("prop") or row.get("prop_type"))
    minutes_proj = _f(row.get("minutes_proj") or row.get("projected_minutes") or row.get("minutes"))
    if minutes_proj is None and games:
        dated: list[tuple[date, float]] = []
        for g in games:
            gd = _as_date(g.get("game_date"))
            mins = _f(g.get("minutes"))
            if gd is None or gd >= as_of or mins is None or mins <= 0:
                continue
            dated.append((gd, mins))
        dated.sort(key=lambda x: x[0], reverse=True)
        if dated:
            take = dated[:5]
            minutes_proj = sum(m for _, m in take) / len(take)
            row = dict(row)
            row["minutes_proj"] = minutes_proj
    measured = None
    if line is not None and games is not None:
        measured = measured_from_games(
            games,
            line=line,
            as_of=as_of,
            current_opener=current_opener,
            last_opener=last_opener,
            minutes_proj=minutes_proj,
            prop_col=col,
        )
    injured = injury_blocks(row)
    role_ok = role_confirmed(row)
    team_fn = team_changed_fn if team_changed_fn is not None else team_change_blocks
    team_block, team_why = team_fn(row, slate=date_str)
    why = skip_reasons(
        is_demon=is_demon_over(row) or (_pick(row.get("pick_type")) == "demon" and _side(row) == "OVER"),
        side=_side(row),
        quote=q,
        p_ref=p_ref,
        implied=implied,
        measured=measured,
        role_ok=role_ok,
        injured=injured,
        team_block=team_block,
        scanner_on=scanner_on,
    )
    # is_demon_over already requires OVER; still catch UNDER Demons.
    if _pick(row.get("pick_type")) == "demon" and _side(row) == "UNDER":
        if "demon_under_invalid" not in why:
            why.append("demon_under_invalid")
    if _pick(row.get("pick_type")) != "demon" and "not_demon" not in why:
        why.append("not_demon")
    same_team = _same_flag(row, ref_row, "team")
    flags = []
    if same_team:
        flags.append("same_team_as_ref")
    if ref_same_game:
        flags.append("same_game_as_ref")
        if "ref_same_game" not in why:
            why.append("ref_same_game")
    qualified = scanner_on and not why
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "date": date_str,
        "track": TRACK,
        "kind": "alert" if qualified else "scan",
        "alert_only": True,
        "ticketed": False,
        "placed": False,
        "player": row.get("player"),
        "team": row.get("team"),
        "opponent": row.get("opp_team") or row.get("opp") or row.get("opponent"),
        "sport": row.get("sport") or "NBA",
        "prop": row.get("prop") or row.get("prop_type"),
        "line": line,
        "pick_type": "demon",
        "side": "OVER",
        "implied_p": None if implied is None else round(implied, 4),
        "quote_2leg": None if q is None else q["power_x"],
        "p_ref": p_ref,
        "lineup_type": None if q is None else q["lineup_type"],
        "reversion_disclaimer": False if q is None else q["reversion_disclaimer"],
        "measured_rate": None if measured is None else measured.get("hit_rate"),
        "hit_rate_raw": None if measured is None else measured.get("hit_rate_raw"),
        "wilson_low": None if measured is None else measured.get("wilson_low"),
        "n": None if measured is None else measured.get("n"),
        "hits": None if measured is None else measured.get("hits"),
        "proj_p": None if measured is None else measured.get("proj_p"),
        "qualified": qualified,
        "why": (
            "Wilson low "
            f"{None if measured is None else measured.get('wilson_low')} "
            f">= implied {None if implied is None else round(implied, 4)} "
            f"x {CUSHION}"
            if qualified
            else ",".join(why)
        ),
        "flags": flags,
        "role_confirmed": role_ok,
        "injury": injured,
        "team_changed": team_block,
        "team_changed_why": team_why,
        "result": "pending",
        "play_hint": (
            (
                "2-pick small stakes, Demon + Standard only. Do not stack Demons. "
                "Alert price is valid only for that slip shape."
                if price_scope == ALERT_SLIP_SHAPE
                else "2-pick small stakes, one Demon plus best other leg. Do not stack Demons."
            )
            if qualified
            else None
        ),
        "alert_price_valid_for": price_scope,
        "never_1st_place": True,
        "ref_player": reference.get("player"),
        "ref_prop": reference.get("prop"),
        "ref_line": reference.get("line"),
        "game_date": str(row.get("game_date") or date_str)[:10],
        "start_time": row.get("start_time") or row.get("game_time"),
    }
    return rec


def load_latest(*, path: Path | None = None) -> dict[str, Any]:
    p = path or LATEST
    if not p.exists():
        return {}
    try:
        rec = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return rec if isinstance(rec, dict) else {}


def alert_price_valid_for(*, path: Path | None = None) -> str | None:
    """Set only after a different-game two-Demon pair prices below the product."""
    p = path or PRICING_CHECK
    if not p.exists():
        return None
    try:
        rec = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(rec, dict):
        return None
    if rec.get("formula_ok") == FORMULA_DIFFERENT_GAME_BELOW:
        return str(rec.get("alert_price_valid_for") or ALERT_SLIP_SHAPE)
    return None


def write_pricing_check(
    rec: dict[str, Any],
    *,
    date_str: str,
    path: Path | None = None,
) -> Path:
    dest = path or PRICING_CHECK
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(rec, indent=2)
    dest.write_text(payload, encoding="utf-8")
    if dest == PRICING_CHECK:
        (OUT_DIR / f"demon_value_pricing_check_{date_str}.json").write_text(payload, encoding="utf-8")
    return dest


def load_log(*, path: Path | None = None) -> list[dict[str, Any]]:
    p = path or LOG
    if not p.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            out.append(rec)
    return out


def proof_report(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    graded = [
        a
        for a in alerts
        if str(a.get("result") or "").lower() in {"hit", "miss"}
        and a.get("qualified")
    ]
    n = len(graded)
    if n < PROOF_N:
        return {
            "ready": False,
            "n_graded": n,
            "need": PROOF_N,
            "beats_price": None,
            "scanner_enabled": True,
            "note": f"need {PROOF_N} graded alerts; have {n}",
        }
    hits = sum(1 for a in graded if str(a.get("result")).lower() == "hit")
    band = wilson_band(hits, n)
    imps = [_f(a.get("implied_p")) for a in graded]
    imps = [p for p in imps if p is not None]
    avg_imp = sum(imps) / len(imps) if imps else None
    low = None if band is None else band[0]
    beats = bool(low is not None and avg_imp is not None and low >= avg_imp)
    return {
        "ready": True,
        "n_graded": n,
        "hits": hits,
        "hit_rate": round(hits / n, 4),
        "wilson_low": None if low is None else round(low, 4),
        "avg_implied_p": None if avg_imp is None else round(avg_imp, 4),
        "beats_price": beats,
        "scanner_enabled": beats,
        "note": (
            "alerts beat their average implied_p"
            if beats
            else "alerts did not beat their price; scanner off"
        ),
    }


def scanner_enabled(*, log_path: Path | None = None, latest_path: Path | None = None) -> bool:
    latest = load_latest(path=latest_path)
    if latest.get("scanner_enabled") is False:
        return False
    proof = proof_report([a for a in load_log(path=log_path) if a.get("qualified")])
    return bool(proof.get("scanner_enabled", True))


def format_list_section(rec: dict[str, Any] | None, *, date: str) -> str:
    lines = [
        "",
        "===== Demon value (alert only, not a ticket) =====",
    ]
    if not rec or str(rec.get("date") or "")[:10] != str(date)[:10]:
        lines.append("  (none — no scan for this date)")
        return "\n".join(lines)
    if rec.get("scanner_enabled") is False:
        lines.append("  scanner off: 50 graded alerts did not beat their price")
        return "\n".join(lines)
    alerts = [a for a in (rec.get("alerts") or []) if a.get("qualified")]
    if not alerts:
        n_skip = int(rec.get("n_skipped") or 0)
        n_priced = int(rec.get("n_priced") or 0)
        lines.append(
            f"  (none cleared Wilson low >= implied x {CUSHION}; "
            f"{n_priced} priced, {n_skip} fail-closed skips in json)"
        )
        return "\n".join(lines)
    lines.append(
        "  Play as a 2-pick with small stakes: one Demon + best other leg. "
        "Do not stack Demons. Demons stay off /tickets."
    )
    for a in alerts:
        flags = ",".join(a.get("flags") or []) or "none"
        lines.append(
            f"  {a.get('player')}  {a.get('prop')} {a.get('line')} Demon OVER  "
            f"implied={_pct(a.get('implied_p'))}  measured={_pct(a.get('measured_rate'))}  "
            f"Wilson_lo={_pct(a.get('wilson_low'))}  n={a.get('n')}  "
            f"why={a.get('why')}  flags={flags}"
        )
    return "\n".join(lines)


def _pct(raw: object) -> str:
    v = _f(raw)
    if v is None:
        return "—"
    return f"{v * 100:.1f}%"


def alert_body(rec: dict[str, Any]) -> str:
    return (
        f"DEMON VALUE (alert only, not a ticket)\n"
        f"{rec.get('player')} {rec.get('prop')} O{rec.get('line')}\n"
        f"implied {_pct(rec.get('implied_p'))}  measured {_pct(rec.get('measured_rate'))}  "
        f"Wilson lo {_pct(rec.get('wilson_low'))}  n={rec.get('n')}\n"
        f"{rec.get('why')}\n"
        f"{rec.get('play_hint')}"
    )


def push_ntfy(body: str, *, title: str = "demon value") -> bool:
    topic = str(os.environ.get("PROPORACLE_NTFY_TOPIC") or "").strip()
    if not topic:
        return False
    url = str(os.environ.get("PROPORACLE_NTFY_URL") or f"https://ntfy.sh/{topic}").strip()
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"Title": title, "Tags": "demon,warning"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return 200 <= int(resp.status) < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _opener_pair(as_of: date) -> tuple[date, date]:
    """Current RS opener and the prior season's opener (not current-minus-one-day).

    ``first_regular_season_date_public(as_of=current-1)`` still lands in this
    campaign and returns the same Oct date, which would drop last season.
    """
    from utils.nba_board_team import first_regular_season_date_public, season_start_year

    y = season_start_year(as_of)
    current = first_regular_season_date_public(
        as_of=as_of, season_start_year_arg=y
    ) or date(y, 10, 1)
    last = first_regular_season_date_public(
        as_of=date(y, 6, 1), season_start_year_arg=y - 1
    ) or date(y - 1, 10, 1)
    return current, last


def scan(
    *,
    date_str: str,
    board: list[dict[str, Any]],
    quotes: dict[str, dict[str, Any]] | None = None,
    reference: dict[str, Any] | None = None,
    boxes: Any = None,
    games_by_player: dict[str, list[dict[str, Any]]] | None = None,
    alert: bool = True,
    write: bool = True,
    team_changed_fn: Callable[..., tuple[bool, str]] | None = None,
    current_opener: date | None = None,
    last_opener: date | None = None,
    log_path: Path | None = None,
    latest_path: Path | None = None,
) -> dict[str, Any]:
    assert_prereg()
    as_of = _as_date(date_str) or date.today()
    if current_opener is None or last_opener is None:
        cur, last = _opener_pair(as_of)
        current_opener = current_opener or cur
        last_opener = last_opener or last
    ref = reference if reference is not None else load_reference()
    quotes = quotes or {}
    on = scanner_enabled(log_path=log_path, latest_path=latest_path)
    p_ref = reference_p(ref)
    price_scope = alert_price_valid_for()
    skipped = 0
    skip_counts: dict[str, int] = {}
    for row in board:
        if _pick(row.get("pick_type")) != "demon":
            continue
        if _side(row) == "UNDER":
            skipped += 1
            skip_counts["demon_under_invalid"] = skip_counts.get("demon_under_invalid", 0) + 1
    candidates, pre_skipped = scrape_candidates(
        board,
        date_str=date_str,
        boxes=boxes,
        games_by_player=games_by_player,
        current_opener=current_opener,
        last_opener=last_opener,
        team_changed_fn=team_changed_fn,
    )
    scanned: list[dict[str, Any]] = []
    for rec in pre_skipped:
        scanned.append(rec)
        skipped += 1
        reason = str(rec.get("why") or "skip").split(",")[0]
        skip_counts[reason] = skip_counts.get(reason, 0) + 1
    for row in candidates:
        key = quote_key(row)
        q = quotes.get(key) or quotes.get(_fold(row.get("player")))
        games = _row_games(
            row, as_of=as_of, boxes=boxes, games_by_player=games_by_player
        )
        row_ref = dict(ref)
        if isinstance(q, dict) and isinstance(q.get("ref_leg"), dict):
            for k in (
                "player",
                "team",
                "prop",
                "line",
                "pick_type",
                "opp",
                "opp_team",
                "pp_game_id",
                "game_id",
                "matchup",
            ):
                if q["ref_leg"].get(k) is not None:
                    row_ref[k] = q["ref_leg"][k]
        rec = evaluate_row(
            row,
            date_str=date_str,
            quote=q,
            reference=row_ref,
            games=games,
            current_opener=current_opener,
            last_opener=last_opener,
            scanner_on=on,
            team_changed_fn=team_changed_fn,
            price_scope=price_scope,
        )
        scanned.append(rec)
        if not rec.get("qualified"):
            skipped += 1
            reason = str(rec.get("why") or "skip").split(",")[0]
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
    alerts = [r for r in scanned if r.get("qualified")]
    ntfy = 0
    if alert:
        for a in alerts:
            if push_ntfy(alert_body(a)):
                a["alerted"] = True
                ntfy += 1
            else:
                a["alerted"] = False
    proof = proof_report(
        [a for a in load_log(path=log_path) if a.get("qualified")] + alerts
    )
    summary = {
        "track": TRACK,
        "date": date_str,
        "alert_only": True,
        "ticketed": False,
        "placed": False,
        "never_ticket": True,
        "scanner_enabled": proof.get("scanner_enabled", True) and on,
        "n_board_demons": len(scanned),
        "n_scrape_candidates": len(candidates),
        "n_prefilter_skipped": len(pre_skipped),
        "n_alerts": len(alerts),
        "n_skipped": skipped,
        "n_priced": sum(1 for r in scanned if r.get("implied_p") is not None),
        "n_ntfy": ntfy,
        "cushion": CUSHION,
        "proof": proof,
        "alerts": alerts,
        "skip_reasons": skip_counts,
        "priced": [r for r in scanned if r.get("implied_p") is not None],
        "scanned": scanned,
        "alert_price_valid_for": price_scope,
        "reference": {
            "player": ref.get("player"),
            "prop": ref.get("prop"),
            "line": ref.get("line"),
            "implied_p": reference_p(ref),
        },
    }
    if write:
        dest_log = log_path or LOG
        dest_latest = latest_path or LATEST
        _append_jsonl(dest_log, alerts)
        dest_latest.parent.mkdir(parents=True, exist_ok=True)
        disk = {k: v for k, v in summary.items() if k != "scanned"}
        prev = load_latest(path=dest_latest)
        if prev.get("pricing_check") and "pricing_check" not in disk:
            disk["pricing_check"] = prev["pricing_check"]
        dest_latest.write_text(json.dumps(disk, indent=2), encoding="utf-8")
        dated = OUT_DIR / f"demon_value_{date_str}.json"
        if dest_latest == LATEST:
            dated.write_text(json.dumps(disk, indent=2), encoding="utf-8")
    return summary


def grade_pending(
    *,
    log_path: Path | None = None,
    actuals: dict[str, float] | None = None,
    boxes: Any = None,
    as_of: date | str | None = None,
) -> dict[str, Any]:
    """Fill HIT/MISS on pending alerts once the game is in the books."""
    path = log_path or LOG
    rows = load_log(path=path)
    cut = _as_date(as_of) or date.today()
    actuals = actuals or {}
    graded = 0
    for rec in rows:
        if str(rec.get("result") or "").lower() != "pending":
            continue
        gd = _as_date(rec.get("game_date") or rec.get("date"))
        if gd is None or gd >= cut:
            continue
        key = quote_key(rec)
        actual = actuals.get(key)
        if actual is None and boxes is not None:
            games = nba_games_for_player(
                boxes,
                str(rec.get("player") or ""),
                prop=str(rec.get("prop") or ""),
                as_of=cut,
            )
            for g in games:
                if _as_date(g.get("game_date")) == gd:
                    actual = _f(g.get("actual"))
                    break
        if actual is None:
            continue
        line = _f(rec.get("line"))
        if line is None:
            continue
        rec["actual"] = actual
        rec["result"] = "hit" if actual > line else "miss"
        rec["placed"] = bool(rec.get("placed"))
        graded += 1
    if graded and path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for rec in rows:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return {"n_graded": graded, "n_log": len(rows)}
