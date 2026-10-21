"""Replay current ticket construction vs the four proposed changes.

Walk-forward leg p uses only graded results from earlier dates.
Slips are graded on that day's HIT/MISS. Payout is the rate card, or the
2026-09-27 live all-Goblin N-correct numbers on the live-price variants.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(REPO)]

import scripts.rank_best_props_today as R  # noqa: E402
from scripts.build_goblin70_tickets import (  # noqa: E402
    can_add,
    fold_name,
    pack,
    unique_by_player,
)
from utils.nfl_keep_gates import nfl_goblin_ticket_eligible  # noqa: E402
from utils.player_prob_shrinkage import shrink_prob_toward_baseline  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    P_GOBLIN_L5EQ5,
    P_STANDARD_GATE,
    goblin_70_eligible,
    goblin_sort_key,
    standard_sort_key,
    standard_ticket_eligible,
)
from utils.ticket_ev_build import (  # noqa: E402
    GATE_SHRINK_K,
    MIN_ALL_HIT_X,
    _hit_dist,
    _is_goblin,
    build_ev_slips,
    flex_table,
    score_slip,
    slip_family,
)

TEMPLATES = REPO / "ui_runner" / "templates"
OUT = REPO / "data" / "reports" / "ticket_construction_backtest.json"
LOADERS = (
    "load_nfl",
    "load_cfb",
    "load_cfb1h",
    "load_golf",
    "load_wnba1q",
    "load_wnba1h",
)
# 2026-09-27 board: all-Goblin Power 3-correct 1.8x, 2-leg about 1.5x.
# Mixes and Standards stay on the rate card. We did not scrape a mix series.
LIVE_GOBLIN = {2: 1.5, 3: 1.8}


def sport_key(v: object) -> str:
    s = str(v or "").strip().upper()
    if s == "SOC":
        return "SOCCER"
    return s


def prop_key(sport: object, prop: object) -> str:
    return canon_prop(sport_key(sport), str(prop or ""))


def line_key(v: object) -> str:
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return ""


def pick_key(v: object) -> str:
    s = str(v or "").lower()
    if "gob" in s:
        return "Goblin"
    if "std" in s or "standard" in s:
        return "Standard"
    return ""


def side_key(v: object) -> str:
    s = str(v or "").strip().upper()
    return s if s in {"OVER", "UNDER"} else ""


def cell_key(r: dict) -> tuple[str, str, str, str]:
    return (
        sport_key(r.get("sport")),
        prop_key(r.get("sport"), r.get("prop")),
        pick_key(r.get("pick_type")),
        side_key(r.get("side") or r.get("direction")),
    )


def load_grades() -> tuple[dict, dict]:
    """date -> grade rows, and (date, player, prop, line, pick, side) -> 1/0."""
    by_date: dict[str, list[tuple]] = defaultdict(list)
    exact: dict[tuple, int] = {}
    for path in sorted(TEMPLATES.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : len("graded_props_") + 10]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for raw in data.get("props") or []:
            res = str(raw.get("result") or "")
            if res not in {"HIT", "MISS"}:
                continue
            hit = 1 if res == "HIT" else 0
            sport = raw.get("sport")
            prop = raw.get("prop")
            player = fold_name(raw.get("player"))
            line = line_key(raw.get("line"))
            pick = pick_key(raw.get("pick_type"))
            side = side_key(raw.get("direction") or raw.get("over_under"))
            ck = (
                sport_key(sport),
                prop_key(sport, prop),
                pick,
                side,
            )
            if not ck[1] or not player or not pick or not side:
                continue
            by_date[date].append((ck, hit))
            exact[(date, player, ck[1], line, pick, side)] = hit
    return by_date, exact


def load_board(date: str) -> list[dict]:
    root = REPO
    rows: list[dict] = []
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(root, date, sport, folder, fname)
        if df is not None and not df.empty:
            rows.extend(R.recs(df))
    for name in LOADERS:
        loader = getattr(R, name, None)
        if not callable(loader):
            continue
        extra = loader(root, date)
        if extra is not None and not extra.empty:
            rows.extend(R.recs(extra))
    return rows


def attach_grade(rows: list[dict], exact: dict, date: str) -> tuple[int, int]:
    matched = 0
    for r in rows:
        key = (
            date,
            fold_name(r.get("player")),
            prop_key(r.get("sport"), r.get("prop")),
            line_key(r.get("line")),
            pick_key(r.get("pick_type")),
            side_key(r.get("side")),
        )
        hit = exact.get(key)
        r["grade_hit"] = hit
        if hit is not None:
            matched += 1
    return matched, len(rows)


class Cells:
    def __init__(self) -> None:
        self.child: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
        self.parent: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])

    def add(self, ck: tuple, hit: int) -> None:
        c = self.child[ck]
        c[0] += hit
        c[1] += 1
        p = self.parent[(ck[0], ck[2], ck[3])]
        p[0] += hit
        p[1] += 1

    def p_of(self, r: dict) -> tuple[float | None, str, float]:
        ck = cell_key(r)
        hits, n = self.child.get(ck, [0, 0])
        ph, pn = self.parent.get((ck[0], ck[2], ck[3]), [0, 0])
        if n > 0 and pn > 0:
            p = shrink_prob_toward_baseline(hits / n, float(n), ph / pn, GATE_SHRINK_K)
            return p, "gate_cell", float(n)
        if n > 0:
            return hits / n, "gate_cell", float(n)
        if pn > 0:
            return ph / pn, "parent_cell", 0.0
        return None, "none", 0.0


def with_floor(rows: list[dict], cells: Cells) -> list[dict]:
    out = []
    for r in rows:
        rec = dict(r)
        p, src, n = cells.p_of(rec)
        gob = pick_key(rec.get("pick_type")) == "Goblin"
        if gob:
            floor = P_GOBLIN_L5EQ5
            if p is None or p < floor:
                rec["p"] = floor
                rec["p_source"] = "l5eq5_floor"
            else:
                rec["p"] = p
                rec["p_source"] = src
        else:
            if p is None:
                rec["p"] = P_STANDARD_GATE
                rec["p_source"] = "strict_floor"
            else:
                rec["p"] = p
                rec["p_source"] = src
        rec["p_n"] = n
        out.append(rec)
    return out


def raw_p(rows: list[dict], cells: Cells) -> list[dict]:
    out = []
    for r in rows:
        rec = dict(r)
        p, src, n = cells.p_of(rec)
        rec["p"] = p
        rec["p_source"] = src
        rec["p_n"] = n
        if p is not None:
            out.append(rec)
    return out


def score_live(legs: list[dict]) -> dict | None:
    """score_slip, with all-Goblin Power replaced by the 2026-09-27 board."""
    fam = slip_family(legs)
    n_g = sum(1 for leg in legs if _is_goblin(leg))
    n = len(legs)
    if fam != "goblin":
        return score_slip(legs)
    ps = []
    for leg in legs:
        try:
            ps.append(float(leg["p"]))
        except (KeyError, TypeError, ValueError):
            return None
    if any(p <= 0 or p > 1 for p in ps):
        return None
    dist = _hit_dist(ps)
    p_all = dist[n]
    px = float(LIVE_GOBLIN.get(n) or 0.0)
    best = None
    if px >= MIN_ALL_HIT_X:
        multiple = p_all * px
        ev = multiple - 1.0
        if ev > 0:
            best = {
                "product": "Power",
                "ev": ev,
                "expected_multiple": multiple,
                "all_hit_x": px,
                "payout_source": "live_2026-09-27",
                "n_correct": {n: px},
                "p_win": p_all,
            }
    table = flex_table(n, family="goblin")
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
    best["stake"] = 10
    return best


def combo_ok(legs: tuple[dict, ...] | list[dict]) -> bool:
    acc: list[dict] = []
    for leg in legs:
        if not can_add(acc, leg, wnba_cap=2):
            return False
        acc.append(leg)
    return True


def search_slips(rows: list[dict], *, scorer, same_game: bool) -> list[dict]:
    pool = [r for r in rows if r.get("p") is not None]
    taken: set[str] = set()
    out: list[dict] = []
    while True:
        open_rows = []
        seen: set[str] = set()
        ordered = sorted(pool, key=lambda r: -float(r["p"]))
        for r in ordered:
            pn = fold_name(r.get("player"))
            if not pn or pn in taken or pn in seen:
                continue
            seen.add(pn)
            open_rows.append(r)
        gobs = [r for r in open_rows if _is_goblin(r)]
        stds = [r for r in open_rows if not _is_goblin(r)]
        if len(open_rows) > 40:
            cand = gobs[:22] + stds[:22]
        else:
            cand = open_rows
        best = None
        for k in (2, 3):
            if len(cand) < k:
                continue
            for combo in combinations(cand, k):
                if same_game and not combo_ok(combo):
                    continue
                scored = scorer(list(combo))
                if scored is None:
                    continue
                if best is None or scored["ev"] > best["ev"]:
                    best = scored
        if best is None:
            return out
        for leg in best["legs"]:
            taken.add(fold_name(leg.get("player")))
        out.append(best)


def current_slips(gob: list[dict], std: list[dict], pool: list[dict]) -> list[dict]:
    taken: set[str] = set()
    out: list[dict] = []
    nfl_wave = [
        r
        for r in gob
        if sport_key(r.get("sport")) == "NFL" and nfl_goblin_ticket_eligible(r)
    ]
    while True:
        combo = pack(nfl_wave, 3, taken, mix_sports=False, wnba_cap=99)
        if len(combo) != 3:
            break
        scored = score_slip(combo, family="goblin")
        if scored is None:
            break
        for r in combo:
            taken.add(fold_name(r.get("player")))
        out.append(scored)
    out.extend(build_ev_slips(pool, taken=taken, fold=fold_name))
    return out


def realize(scored: dict) -> tuple[float, int, int] | None:
    legs = scored["legs"]
    if any(leg.get("grade_hit") is None for leg in legs):
        return None
    hits = sum(int(leg["grade_hit"]) for leg in legs)
    table = scored["n_correct"]
    mult = 0.0
    for k, m in table.items():
        if int(k) == hits:
            mult = float(m)
            break
    return (mult - 1.0) * 10.0, hits, len(legs)


def blank() -> dict:
    return {
        "slips": 0,
        "graded": 0,
        "ungraded": 0,
        "all_hit": 0,
        "profit": 0.0,
        "staked": 0.0,
        "pred_ev_sum": 0.0,
        "families": defaultdict(int),
        "products": defaultdict(int),
        "floored_legs": 0,
        "legs": 0,
    }


def add_slip(bucket: dict, scored: dict) -> None:
    bucket["slips"] += 1
    bucket["families"][scored.get("family") or "?"] += 1
    bucket["products"][scored.get("product") or "?"] += 1
    bucket["pred_ev_sum"] += float(scored.get("ev") or 0.0)
    for leg in scored["legs"]:
        bucket["legs"] += 1
        if leg.get("p_source") == "l5eq5_floor":
            bucket["floored_legs"] += 1
    got = realize(scored)
    if got is None:
        bucket["ungraded"] += 1
        return
    profit, hits, n = got
    bucket["graded"] += 1
    bucket["staked"] += 10.0
    bucket["profit"] += profit
    if hits == n:
        bucket["all_hit"] += 1


def finish(bucket: dict) -> dict:
    g = bucket["graded"]
    return {
        "slips": bucket["slips"],
        "graded": g,
        "ungraded": bucket["ungraded"],
        "all_hit": bucket["all_hit"],
        "all_hit_rate": round(bucket["all_hit"] / g, 3) if g else None,
        "profit": round(bucket["profit"], 2),
        "roi": round(bucket["profit"] / bucket["staked"], 3) if bucket["staked"] else None,
        "mean_pred_ev": round(bucket["pred_ev_sum"] / bucket["slips"], 3) if bucket["slips"] else None,
        "families": dict(bucket["families"]),
        "products": dict(bucket["products"]),
        "floored_leg_share": round(bucket["floored_legs"] / bucket["legs"], 3) if bucket["legs"] else None,
    }


def main() -> None:
    print("loading grades", flush=True)
    graded_by_date, exact = load_grades()
    out_days = sorted(
        p.name
        for p in (REPO / "outputs").iterdir()
        if p.is_dir() and len(p.name) == 10 and p.name[0] == "2"
    )
    days = [d for d in out_days if d in graded_by_date and d <= "2026-09-14"]
    print(f"overlap days {len(days)} {days[0]}..{days[-1]}", flush=True)
    cells = Cells()
    # Warm the prior with graded dates that fall before the first board day.
    first = days[0]
    for date in sorted(graded_by_date):
        if date >= first:
            break
        for ck, hit in graded_by_date[date]:
            cells.add(ck, hit)
    totals = {k: blank() for k in ("current", "search", "nofloor", "live", "live_game")}
    day_rows = []
    pool_players = 0
    pool_matched = 0
    pool_rows = 0
    for date in sorted(set(days) | set(d for d in graded_by_date if first <= d <= days[-1])):
        if date in days and (REPO / "outputs" / date).is_dir():
            board = load_board(date)
            eligible = []
            for r in board:
                if pick_key(r.get("pick_type")) not in {"Goblin", "Standard"}:
                    continue
                if "+" in str(r.get("player") or ""):
                    continue
                side = side_key(r.get("side"))
                l5v = r.get("l5_over") if side == "OVER" else r.get("l5_under")
                try:
                    if float(l5v) < 5:
                        continue
                except (TypeError, ValueError):
                    continue
                try:
                    ok = goblin_70_eligible(r) or standard_ticket_eligible(r)
                except Exception:
                    ok = False
                if ok:
                    eligible.append(r)
            gob = unique_by_player(
                [r for r in eligible if pick_key(r.get("pick_type")) == "Goblin"],
                goblin_sort_key,
            )
            std = unique_by_player(
                [r for r in eligible if pick_key(r.get("pick_type")) == "Standard"],
                standard_sort_key,
            )
            # One face per player inside each builder, but both faces stay
            # available until p is attached (same as build()).
            matched, n_el = attach_grade(gob + std, exact, date)
            pool_rows += len(gob) + len(std)
            pool_matched += matched
            floored = with_floor(gob + std, cells)
            gob_f = [r for r in floored if _is_goblin(r)]
            std_f = [r for r in floored if not _is_goblin(r)]
            naked = raw_p(gob + std, cells)
            variants = {
                "current": current_slips(gob_f, std_f, floored),
                "search": search_slips(floored, scorer=score_slip, same_game=False),
                "nofloor": search_slips(naked, scorer=score_slip, same_game=False),
                "live": search_slips(naked, scorer=score_live, same_game=False),
                "live_game": search_slips(naked, scorer=score_live, same_game=True),
            }
            snap = {"date": date, "eligible": len(gob) + len(std), "matched": matched}
            for name, slips in variants.items():
                for s in slips:
                    add_slip(totals[name], s)
                snap[name] = len(slips)
            players = {fold_name(r.get("player")) for r in gob + std}
            pool_players += len(players)
            day_rows.append(snap)
            print(
                f"{date} elig {snap['eligible']} matched {matched} "
                + " ".join(f"{k}={snap[k]}" for k in variants),
                flush=True,
            )
        for ck, hit in graded_by_date.get(date, []):
            cells.add(ck, hit)
    summary = {k: finish(v) for k, v in totals.items()}
    payload = {
        "window": [days[0], days[-1]] if days else [],
        "n_days": len(days),
        "eligible_rows": pool_rows,
        "grade_matched_rows": pool_matched,
        "unique_player_days": pool_players,
        "note": (
            "Walk-forward p from earlier graded HIT/MISS only. "
            "Current floors Goblin p up to 0.763 and only scores the top 2 and top 3. "
            "Search scores 2- and 3-leg combinations, keeping the best EV slip. "
            "Live prices all-Goblin Power at 1.5x (2 legs) and 1.8x (3 legs) from the 2026-09-27 board. "
            "A slip with any unmatched leg is counted as ungraded and left out of ROI."
        ),
        "variants": summary,
        "days": day_rows,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    print("wrote", OUT, flush=True)


if __name__ == "__main__":
    main()
