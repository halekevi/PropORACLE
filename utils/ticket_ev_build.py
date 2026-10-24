"""Stages 2–6: shrunk gate-cell p, rate-card price, EV slips.

model_prob is a tie break only. Goblin and Standard may share a slip.
The pool is every high-hit-rate leg. Each round takes the best-EV 2- or
3-leg that clears its composition test. The dollar-for-dollar test
(expected N-correct multiple minus 1) is stored on every slip.
All-Goblin slips clear on an all-hit payout of at least 1.4x; the
dollar test is recorded and does not drop them, because each Goblin
cuts the multiplier. Mixed slips and all-Standard slips must clear
that test at their own payout. A mix or Standard that returns a dollar
outranks an all-Goblin slip that does not. Stake is $20.
One player-prop once. The same player can add a second prop and stops at 2.
A sport fills its own slips first. Legs that cannot fill stay in one pool
with every other sport.
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
# (k=150 in utils/player_prob_shrinkage.py). The k=25 re-fit is closed:
# Aug 26–Sep 27 gated legs hit 78% against a category rate near 70%, and
# k=25 cannot close an 8-point gap on cells with n around 57.
GATE_SHRINK_K = 25.0
MIN_ALL_HIT_X = 1.4
MAX_LEGS_PER_PLAYER = 2
STAKE = 20
# Fallback only, when the scrape log has no 3-leg Goblin rows. The live quote
# is the 25th percentile of those rows (1.65x on the current log, n=27,
# range 1.3–3.5). The old 2.3x ± 0.5 band (1.8x–2.8x) is retired.
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
    n_s = max(0, n_legs - n_g)
    from utils.n_correct_payout import lookup_composition_p25, uses_composition_p25

    if uses_composition_p25(n_s, n_g, n_legs):
        found = lookup_composition_p25(n_s, n_g, n_legs, "Power")
        if found:
            return float(found["n_correct"][n_legs]), "n_correct_p25"
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
    # 2-leg all-Goblin 1.5x is the rate-card average. Logged scrapes run
    # 1.2x–1.5x and straddle 1.4x, so this number is not a settled N-correct.
    fallback = {(2, 2): 1.5, (3, 3): GOBLIN_3_POWER_X, (3, 2): 2.5, (3, 1): 3.25, (2, 1): 2.0}
    return float(fallback.get((n_legs, n_g), 0.0)), "mixed_fallback"


def flex_table(n_legs: int, *, family: str) -> dict[int, float]:
    src = _STD_FLEX if family == "standard" else _GOB_FLEX
    return dict(src.get(n_legs) or {})


def _clears_composition(family: str, all_hit_x: float, ev: float) -> bool:
    """All-Goblin keeps the 1.4x floor. Mix and Standard must also return a dollar."""
    if all_hit_x < MIN_ALL_HIT_X:
        return False
    if family == "goblin":
        return True
    return ev > 0


def score_slip(
    legs: list[dict[str, Any]],
    *,
    family: str | None = None,
    require_dollar: bool | None = None,
) -> dict[str, Any] | None:
    """Best of Power vs Flex under that composition's dollar test.

    ``family`` is taken from the legs. A Goblin plus a Standard is ``mix``.
    Flex is only compared on an all-Goblin or all-Standard slip.
    All-Goblin: all-hit payout at least 1.4x. The dollar test is stored
    and does not drop the slip. Mix and Standard: the same 1.4x floor,
    and the expected N-correct multiple has to be over 1.
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
    if px > 0:
        multiple = p_all * px
        ev = multiple - 1.0
        if require_dollar is True:
            clears = px >= MIN_ALL_HIT_X and ev > 0
        elif require_dollar is False:
            clears = px >= MIN_ALL_HIT_X
        else:
            clears = _clears_composition(fam, px, ev)
        if clears:
            best = {
                "product": "Power",
                "ev": ev,
                "expected_multiple": multiple,
                "all_hit_x": px,
                "payout_source": src,
                "n_correct": {n: px},
                "p_win": p_all,
            }
    # A hardcoded Flex table is a second median. Once Power is the 25th
    # percentile, do not let Flex replace it. Flex still has to clear a
    # dollar, including on an all-Goblin slip whose Power quote does not.
    table = {} if fam == "mix" or src == "n_correct_p25" else flex_table(n, family=fam)
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
    best["dollar_test"] = best["ev"] > 0
    best["dollar_test_required"] = fam != "goblin"
    if fam == "goblin":
        dollar_note = "All-Goblin: 1.4x floor, dollar test recorded."
    else:
        dollar_note = "Dollar test required at this composition's payout."
    best["payout_note"] = (
        f"{best['product']} all-hit {best['all_hit_x']}x from {best['payout_source']}. "
        "EV = expected N-correct multiple - 1. "
        f"{dollar_note}"
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


def _combo_within_player_cap(
    combo: list[dict[str, Any]],
    fold: Callable[[object], str],
    uses: dict[str, int],
    cap: int,
) -> bool:
    seen: dict[str, int] = {}
    for leg in combo:
        name = fold(leg.get("player"))
        if not name:
            return False
        seen[name] = seen.get(name, 0) + 1
        if uses.get(name, 0) + seen[name] > cap:
            return False
    return True


def build_ev_slips(
    pool: list[dict[str, Any]],
    *,
    taken: set[str],
    fold: Callable[[object], str],
    family: str | None = None,
    player_uses: dict[str, int] | None = None,
    max_player_legs: int = MAX_LEGS_PER_PLAYER,
    require_dollar: bool | None = None,
    max_slips: int | None = None,
) -> list[dict[str, Any]]:
    """Highest-probability 2- or 3-leg. Goblin and Standard may share a slip.

    Same game is allowed. One player-prop once. The same player can add a
    second prop and stops at ``max_player_legs``. ``taken`` stores player|prop
    ids. ``player_uses`` counts legs already placed for each player. Pass
    ``family`` only to force a pure pool. MLB construction ban runs before
    ranking. All-Goblin groups need a 1.4x all-hit payout. Mixed and
    all-Standard groups must also clear the dollar test. Among groups that
    clear, the higher EV is taken. At one payout, that is the higher
    sweep probability. A mix or Standard that returns a dollar outranks
    an all-Goblin slip that does not.
    """
    from utils.gap_ranker import prefer_slip

    uses = player_uses if player_uses is not None else {}
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
                if not _combo_within_player_cap(list(combo), fold, uses, max_player_legs):
                    continue
                scored = score_slip(list(combo), family=family, require_dollar=require_dollar)
                if scored is None:
                    continue
                if best is None or prefer_slip(scored, best):
                    best = scored
        if best is None:
            return out
        for leg in best["legs"]:
            taken.add(leg_id(leg, fold))
            name = fold(leg.get("player"))
            if name:
                uses[name] = uses.get(name, 0) + 1
        out.append(best)
        if max_slips is not None and len(out) >= max_slips:
            return out


def build_sport_then_mix(
    pool: list[dict[str, Any]],
    *,
    taken: set[str],
    fold: Callable[[object], str],
    max_player_legs: int = MAX_LEGS_PER_PLAYER,
    max_slips: int | None = None,
) -> list[dict[str, Any]]:
    """Each sport fills its own slips. Legs that cannot fill mix across sports.

    A player still stops at ``max_player_legs`` on the whole card.
    """
    uses: dict[str, int] = {}
    by_sport: dict[str, list[dict[str, Any]]] = {}
    for row in pool:
        by_sport.setdefault(str(row.get("sport") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    def _room() -> int | None:
        if max_slips is None:
            return None
        return max(0, max_slips - len(out))

    for rows in by_sport.values():
        room = _room()
        if room == 0:
            return out
        out.extend(
            build_ev_slips(
                rows,
                taken=taken,
                fold=fold,
                player_uses=uses,
                max_player_legs=max_player_legs,
                max_slips=room,
            )
        )
    if _room() == 0:
        return out
    rest = [row for row in pool if leg_id(row, fold) not in taken]
    out.extend(
        build_ev_slips(
            rest,
            taken=taken,
            fold=fold,
            player_uses=uses,
            max_player_legs=max_player_legs,
            max_slips=_room(),
        )
    )
    if _room() == 0:
        return out
    # A higher-priced face that never landed on a slip gives way to the
    # next face of that same player-prop (Goblin 74.5 unused, Standard 78.5
    # can still pair). Faces that did land stay taken.
    best: dict[str, dict[str, Any]] = {}
    alts: dict[str, list[dict[str, Any]]] = {}
    for row in pool:
        if row.get("p") is None:
            continue
        lid = leg_id(row, fold)
        if not lid or lid in taken:
            continue
        prev = best.get(lid)
        if prev is None or float(row["p"]) > float(prev["p"]):
            if prev is not None:
                alts.setdefault(lid, []).append(prev)
            best[lid] = row
        else:
            alts.setdefault(lid, []).append(row)
    if alts:
        nxt: list[dict[str, Any]] = []
        for lid, row in best.items():
            if lid in alts:
                nxt.append(max(alts[lid], key=lambda item: float(item["p"])))
            else:
                nxt.append(row)
        out.extend(
            build_ev_slips(
                nxt,
                taken=taken,
                fold=fold,
                player_uses=uses,
                max_player_legs=max_player_legs,
                max_slips=_room(),
            )
        )
    return out
