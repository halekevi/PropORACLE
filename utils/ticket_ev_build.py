"""Stages 2–6: shrunk gate-cell p, rate-card price, EV slips.

model_prob is a tie break only. Goblin and Standard may share a slip.
The pool is every high-hit-rate leg. Each round takes the best-EV 2- or
3-leg, not the first names in p order. Stop when nothing left clears
EV > 0 and an all-hit payout of at least 1.4x. Stake is $20.
One player-prop once: the same player can add a different prop.
"""
from __future__ import annotations

import json
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Any, Callable

from utils.category_hit_rate import load_category_hr_index, lookup_category_hr
from utils.player_prob_shrinkage import shrink_prob_toward_baseline

# Correlation pair-lift prior, reused here. Not the player-history fit
# (k=150 in utils/player_prob_shrinkage.py). Thin-cell check of k=25
# against real L5=5 outcomes is still open.
GATE_SHRINK_K = 25.0
MIN_ALL_HIT_X = 1.4
STAKE = 20
# All-Goblin 3-leg Power is priced at 2.3x. A live quote within ±0.5 still
# belongs to that cell (1.8x–2.8x).
GOBLIN_3_POWER_X = 2.3
GOBLIN_3_BAND = 0.5
# Mixed Power floors. These replace the rate-card averages.
_MIX_FLOOR = {(2, 1): 2.0, (3, 1): 3.25, (3, 2): 2.5}

_CARD = Path(__file__).resolve().parents[1] / "data" / "reports" / "payout_rate_card.json"

# Canonical all-Standard Power when the rate card has no all-standard cell.
_STD_POWER = {2: 3.0, 3: 6.0}
# All-Standard Flex N-correct. 2-leg Flex has no partial.
_STD_FLEX = {2: {2: 3.0}, 3: {3: 2.25, 2: 1.25}}
# All-Goblin Flex fallback (N-correct, not 1st place).
_GOB_FLEX = {2: {2: 1.5}, 3: {3: 1.7, 2: 0.5}}


@lru_cache(maxsize=1)
def _rate_card() -> dict[str, Any]:
    if not _CARD.is_file():
        return {}
    try:
        raw = json.loads(_CARD.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _parent_hr(
    index: dict[tuple[str, str, str, str], dict[str, float]],
    *,
    sport: object,
    pick_type: object,
    direction: object,
) -> tuple[float, float] | None:
    from utils.category_hit_rate import _norm_dir, _norm_pick, _norm_sport

    sp = _norm_sport(sport)
    pick = _norm_pick(pick_type)
    direction_u = _norm_dir(direction)
    n = 0.0
    weighted = 0.0
    for (s, _prop, pk, d), rec in index.items():
        if s == sp and pk == pick and d == direction_u:
            nn = float(rec.get("n") or 0)
            n += nn
            weighted += float(rec.get("hr") or 0) * nn
    if n <= 0:
        return None
    return weighted / n, n


def attach_leg_p(r: dict[str, Any], *, recipe: str, index: dict | None = None) -> None:
    """p = gate-cell hit rate shrunk with the correlation prior (k=25).

    Not the player-history prior (k=150). Stores source cell and n on the row.
    """
    idx = index if index is not None else load_category_hr_index()
    side = r.get("side") or r.get("direction")
    child = lookup_category_hr(
        idx,
        sport=r.get("sport"),
        prop=r.get("prop") or r.get("prop_type"),
        pick_type=r.get("pick_type"),
        direction=side,
    )
    parent = _parent_hr(
        idx,
        sport=r.get("sport"),
        pick_type=r.get("pick_type"),
        direction=side,
    )
    parent_hr = parent[0] if parent else None
    parent_n = parent[1] if parent else 0.0
    sport = str(r.get("sport") or "")
    prop = str(r.get("prop") or r.get("prop_type") or "")
    pick = str(r.get("pick_type") or "")
    if child and parent_hr is not None:
        p = shrink_prob_toward_baseline(
            float(child["hr"]), float(child["n"]), parent_hr, GATE_SHRINK_K
        )
        r["p"] = p
        r["p_n"] = float(child["n"])
        r["p_source"] = "gate_cell"
    elif child:
        r["p"] = float(child["hr"])
        r["p_n"] = float(child["n"])
        r["p_source"] = "gate_cell"
    elif parent_hr is not None:
        r["p"] = float(parent_hr)
        r["p_n"] = 0.0
        r["p_source"] = "parent_cell"
    else:
        r["p"] = None
        r["p_n"] = 0.0
        r["p_source"] = "none"
    r["p_cell"] = f"{sport}|{prop}|{side}|{pick}|{recipe}"
    r["p_parent_n"] = parent_n
    r["gate_recipe"] = recipe


def _hit_dist(ps: list[float]) -> list[float]:
    dp = [1.0]
    for p in ps:
        nxt = [0.0] * (len(dp) + 1)
        q = 1.0 - p
        for k, mass in enumerate(dp):
            nxt[k] += mass * q
            nxt[k + 1] += mass * p
        dp = nxt
    return dp


def _is_goblin(r: dict[str, Any]) -> bool:
    return "goblin" in str(r.get("pick_type") or "").lower()


def slip_family(legs: list[dict[str, Any]]) -> str:
    kinds = {"goblin" if _is_goblin(r) else "standard" for r in legs}
    if kinds == {"goblin"}:
        return "goblin"
    if kinds == {"standard"}:
        return "standard"
    return "mix"


def power_multiplier(n_legs: int, *, family: str, n_goblin: int | None = None) -> tuple[float, str]:
    """All-hit Power multiple from the rate card for this Goblin count."""
    card = _rate_card()
    n_g = n_legs if family == "goblin" else 0 if family == "standard" else int(n_goblin or 0)
    if n_legs == 3 and n_g == 3:
        return GOBLIN_3_POWER_X, "goblin_3"
    if 0 < n_g < n_legs:
        pinned = _MIX_FLOOR.get((n_legs, n_g))
        if pinned is not None:
            return float(pinned), "mix_floor"
    if n_g <= 0:
        base = (card.get("baselines_power_min_x") or {}).get(f"{n_legs}S")
        if base:
            return float(base), "payout_rate_card"
        return float(_STD_POWER.get(n_legs) or 0.0), "canonical_standard"
    floors = card.get("composition_floors") or {}
    key = f"{n_legs}L_{n_g}G"
    cell = floors.get(key) or {}
    if cell.get("avg_min_x"):
        return float(cell["avg_min_x"]), "payout_rate_card"
    fallback = {(2, 2): 1.5, (3, 3): GOBLIN_3_POWER_X, (3, 2): 2.5, (3, 1): 3.25, (2, 1): 2.0}
    return float(fallback.get((n_legs, n_g), 0.0)), "mixed_fallback"


def flex_table(n_legs: int, *, family: str) -> dict[int, float]:
    src = _STD_FLEX if family == "standard" else _GOB_FLEX
    return dict(src.get(n_legs) or {})


def score_slip(legs: list[dict[str, Any]], *, family: str | None = None) -> dict[str, Any] | None:
    """Best of Power vs Flex. None when EV <= 0 or all-hit payout < 1.4x.

    ``family`` is taken from the legs. A Goblin plus a Standard is ``mix``.
    Flex is only compared on an all-Goblin or all-Standard slip.
    """
    ps = []
    for leg in legs:
        try:
            ps.append(float(leg["p"]))
        except (KeyError, TypeError, ValueError):
            return None
    if any(p <= 0 or p > 1 for p in ps):
        return None
    n = len(ps)
    dist = _hit_dist(ps)
    p_all = dist[n]
    fam = slip_family(legs) if family is None else family
    n_g = sum(1 for leg in legs if _is_goblin(leg))
    if fam == "standard":
        n_g = 0
    elif fam == "goblin":
        n_g = n
    best: dict[str, Any] | None = None
    px, src = power_multiplier(n, family=fam, n_goblin=n_g)
    if px >= MIN_ALL_HIT_X:
        multiple = p_all * px
        ev = multiple - 1.0
        if ev > 0:
            best = {
                "product": "Power",
                "ev": ev,
                "expected_multiple": multiple,
                "all_hit_x": px,
                "payout_source": src,
                "n_correct": {n: px},
                "p_win": p_all,
            }
    table = {} if fam == "mix" else flex_table(n, family=fam)
    sweep = float(table.get(n) or 0.0)
    if table and sweep >= MIN_ALL_HIT_X:
        multiple = sum(dist[k] * float(m) for k, m in table.items() if k < len(dist))
        ev = multiple - 1.0
        if ev > 0 and (best is None or ev > best["ev"]):
            best = {
                "product": "Flex",
                "ev": ev,
                "expected_multiple": multiple,
                "all_hit_x": sweep,
                "payout_source": "flex_n_correct",
                "n_correct": table,
                "p_win": sum(dist[k] for k in table if k < len(dist) and table[k] > 0),
            }
    if best is None:
        return None
    best["legs"] = legs
    best["family"] = fam
    best["stake"] = STAKE
    best["payout_note"] = (
        f"{best['product']} all-hit {best['all_hit_x']}x from {best['payout_source']}. "
        "EV = expected N-correct multiple - 1."
    )
    return best


def _mlb_construction_banned(r: dict[str, Any]) -> bool:
    if str(r.get("sport") or "").strip().upper() != "MLB":
        return False
    try:
        from combined_slate_tickets import _leg_mlb_construction_banned
    except Exception:
        return False
    return bool(_leg_mlb_construction_banned(r))


def leg_id(r: dict[str, Any], fold: Callable[[object], str]) -> str:
    """One player-prop. PRA and assists for the same player are different ids."""
    from utils.prop_norm import canon_prop

    player = fold(r.get("player"))
    prop = canon_prop(r.get("sport"), r.get("prop") or r.get("prop_type"))
    if not player or not prop:
        return ""
    return f"{player}|{prop}"


def build_ev_slips(
    pool: list[dict[str, Any]],
    *,
    taken: set[str],
    fold: Callable[[object], str],
    family: str | None = None,
) -> list[dict[str, Any]]:
    """Best-EV 2- or 3-leg from the whole pool. Goblin and Standard may share a slip.

    Same game is allowed. One player-prop once, so one player can contribute
    two props. ``taken`` stores those player|prop ids. Pass ``family`` only
    to force a pure pool. MLB construction ban runs before ranking. Stop when
    no remaining group clears EV > 0 and a 1.4x all-hit payout.
    """
    best_by: dict[str, dict[str, Any]] = {}
    for r in pool:
        if r.get("p") is None:
            continue
        if _mlb_construction_banned(r):
            continue
        lid = leg_id(r, fold)
        if not lid or lid in taken:
            continue
        prev = best_by.get(lid)
        if prev is None or float(r["p"]) > float(prev["p"]):
            best_by[lid] = r
    rows = list(best_by.values())
    out: list[dict[str, Any]] = []
    while True:
        open_rows = [r for r in rows if leg_id(r, fold) not in taken]
        if len(open_rows) < 2:
            return out
        best: dict[str, Any] | None = None
        for n in (2, 3):
            if len(open_rows) < n:
                continue
            for combo in combinations(open_rows, n):
                scored = score_slip(list(combo), family=family)
                if scored is None:
                    continue
                if best is None or scored["ev"] > best["ev"]:
                    best = scored
        if best is None:
            return out
        for leg in best["legs"]:
            taken.add(leg_id(leg, fold))
        out.append(best)
