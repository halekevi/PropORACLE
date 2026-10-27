"""Provisional within-gate ranker. Gap orders Overs. It does not price them.

The gate, the cell price, and the break-even stay as they are. Break-even
still decides whether a slip builds, through EV. Gap may reorder an Over
only when the cell already clears the gate's own admission bar: n at least
30, and a Wilson low of at least 0.65 for a Goblin or 0.50 for a Standard.
A leg under that bar stays in the pool, at its cell-rate position.

Slip groups whose EV is within two points (0.02 of expected multiple
minus 1) break the tie on that gap. A wider EV gap still wins.

Each slate logs both orders, the legs that moved between the two top-k
sets, the EV gap between those sets, and which order was used. A slate
counts toward the stop rule only when those top-k sets differ. After 20
graded differing slates, gap turns off only when it loses to the cell-rate
order (the day-clustered band sits entirely below zero). A tie is not
evidence. The off switch is reviewed again the next month, so a sport
can turn back on.
"""
from __future__ import annotations

import json
import math
import random
from datetime import date as date_cls
from pathlib import Path
from typing import Any

from utils.mlb_keep_gates import pitcher_l10_parent_cell
from utils.prop_norm import canon_prop

_REPO = Path(__file__).resolve().parents[1]
LOG_PATH = _REPO / "data" / "reports" / "gap_ranker_log.jsonl"
STATE_PATH = _REPO / "data" / "reports" / "gap_ranker_state.json"

# Two points of EV, where EV is the expected N-correct multiple minus 1.
EV_TIE = 0.02
TOP_K = 6
MIN_LEGS = 12
MIN_GAMES = 4
MIN_SLATES = 20
RANDOM_DRAWS = 1000
BOOTSTRAP_DRAWS = 2000
# Admission bar, the same one the gate uses to let a cell in. Not the
# payout break-even (Goblin 0.845 / Standard 0.577), which stays in EV.
GOBLIN_ADMISSION = 0.65
STANDARD_ADMISSION = 0.50
ADMISSION_MIN_N = 30


def wilson_low(hits: int, n: int, z: float = 1.96) -> float | None:
    """Lower Wilson bound. Same formula as the comparison-track band."""
    if n <= 0:
        return None
    p = hits / n
    den = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / den
    margin = z * math.sqrt((p * (1.0 - p) / n) + (z * z / (4.0 * n * n))) / den
    return center - margin


def _sport(row: dict[str, Any]) -> str:
    return str(row.get("sport") or "").strip().upper()


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or row.get("over_under") or "").strip().upper()
    if raw in {"O", "OVER"}:
        return "OVER"
    if raw in {"U", "UNDER"}:
        return "UNDER"
    return raw


def _player(row: dict[str, Any]) -> str:
    return " ".join(str(row.get("player") or "").lower().split())


def _is_standard(row: dict[str, Any]) -> bool:
    return "standard" in str(row.get("pick_type") or "").lower()


def admission_floor(row: dict[str, Any]) -> float:
    """Wilson low a cell must already clear before gap may promote the leg."""
    return STANDARD_ADMISSION if _is_standard(row) else GOBLIN_ADMISSION


def _cell_n(row: dict[str, Any]) -> int | None:
    raw = row.get("cell_n")
    if raw in (None, ""):
        raw = row.get("n")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _num(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    return value


def gap_value(row: dict[str, Any]) -> float | None:
    """Play-side gap already stored on the row. Does not invent one."""
    for key in ("gap_z", "cushion_z"):
        value = _num(row.get(key))
        if value is not None:
            return value
    return None


def _stored_wilson(row: dict[str, Any]) -> float | None:
    stored = _num(row.get("cell_wilson_low"))
    if stored is not None:
        return stored
    if stored is None and row.get("cell_wilson_low") == 0:
        return 0.0
    hit = row.get("cell_hit")
    n = row.get("cell_n")
    try:
        return wilson_low(int(hit), int(n))
    except (TypeError, ValueError):
        return None


def cell_counts(row: dict[str, Any], index: dict | None = None) -> tuple[int, int] | None:
    """Hits and n for the cell that already prices the leg. Missing stays missing."""
    if row.get("cell_hit") not in (None, "") and row.get("cell_n") not in (None, ""):
        try:
            return int(row["cell_hit"]), int(row["cell_n"])
        except (TypeError, ValueError):
            return None
    parent = pitcher_l10_parent_cell(row)
    if parent is not None:
        return int(parent["hit"]), int(parent["n"])
    if index is None:
        return None
    from utils.category_hit_rate import lookup_category_hr

    child = lookup_category_hr(
        index,
        sport=row.get("sport"),
        prop=row.get("prop") or row.get("prop_type"),
        pick_type=row.get("pick_type"),
        direction=_side(row),
    )
    if not child or not child.get("n"):
        return None
    return int(child.get("hit") or 0), int(child["n"])


def load_state(path: Path | None = None) -> dict[str, Any]:
    dest = path or STATE_PATH
    if not dest.is_file():
        return {"sports": {}}
    try:
        raw = json.loads(dest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"sports": {}}
    return raw if isinstance(raw, dict) else {"sports": {}}


def sport_enabled(sport: str, state: dict[str, Any] | None = None, path: Path | None = None) -> bool:
    """Missing sports stay on. Off lasts until the next month's review."""
    book = state if state is not None else load_state(path)
    row = (book.get("sports") or {}).get(str(sport or "").upper()) or {}
    if row.get("enabled") is False:
        return False
    return True


def leg_clears_floor(row: dict[str, Any]) -> bool:
    """Over whose cell clears the admission bar. Independent of the on/off switch."""
    if _side(row) != "OVER":
        return False
    if gap_value(row) is None:
        return False
    n = _cell_n(row)
    if n is None or n < ADMISSION_MIN_N:
        return False
    low = _stored_wilson(row)
    return low is not None and low >= admission_floor(row)


def gap_can_rank(row: dict[str, Any], *, state: dict[str, Any] | None = None) -> bool:
    """Card order. A sport that has been turned off is not reordered."""
    return leg_clears_floor(row) and sport_enabled(_sport(row), state)


def stamp_rank_fields(row: dict[str, Any], index: dict | None = None) -> None:
    """Attach gap_z and the cell Wilson bound. Leaves p, floors, and the gate alone."""
    from utils.cushion_z import cushion_z

    value = gap_value(row)
    if value is None:
        side = _side(row)
        line = _num(row.get("line"))
        games = []
        for i in range(1, 11):
            slot = row.get(f"current_stat_g{i}")
            if slot in (None, ""):
                slot = row.get(f"Current G{i}")
            games.append(slot)
        if line is not None and any(g not in (None, "") for g in games):
            value = cushion_z(games, line, side)
    if value is not None:
        row["gap_z"] = value
    counts = cell_counts(row, index)
    if counts is not None:
        hit, n = counts
        row["cell_hit"] = hit
        row["cell_n"] = n
        row["cell_wilson_low"] = wilson_low(hit, n)
    row["gap_rank_eligible"] = gap_can_rank(row)


def gap_order_key(row: dict[str, Any]) -> tuple:
    """Legs gap may promote, biggest gap first. Everyone else follows by cell rate."""
    player = _player(row)
    low = _stored_wilson(row)
    cell_key = -(low if low is not None else -1.0)
    if gap_can_rank(row):
        return (0, -float(gap_value(row) or 0.0), cell_key, player)
    return (1, cell_key, player)


def cell_order_key(row: dict[str, Any]) -> tuple:
    low = _stored_wilson(row)
    return (-(low if low is not None else -1.0), _player(row))


def game_key(row: dict[str, Any]) -> str:
    for key in ("espn_event_id", "espn_game_id", "pp_game_id", "event_id", "game_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    matchup = " ".join(str(row.get("matchup") or "").upper().split())
    if matchup:
        return matchup
    return _player(row)


def top_k(rows: list[dict[str, Any]], key_fn, k: int = TOP_K) -> list[dict[str, Any]]:
    """One leg per player (anchor cap) and one player-prop, highest key first."""
    seen_player: set[str] = set()
    seen_prop: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in sorted(rows, key=key_fn):
        player = _player(row)
        prop = canon_prop(row.get("sport"), row.get("prop") or row.get("prop_type"))
        ident = f"{player}|{prop}"
        if not player or player in seen_player or ident in seen_prop:
            continue
        seen_player.add(player)
        seen_prop.add(ident)
        out.append(row)
        if len(out) >= k:
            break
    return out


def slip_gap(scored: dict[str, Any], *, state: dict[str, Any] | None = None) -> float | None:
    """Mean gap of Overs on the slip that clear the Wilson floor. None when gap cannot act."""
    values: list[float] = []
    for leg in scored.get("legs") or []:
        if not gap_can_rank(leg, state=state):
            continue
        value = gap_value(leg)
        if value is not None:
            values.append(value)
    if not values:
        return None
    return sum(values) / len(values)


def prefer_slip(candidate: dict[str, Any], incumbent: dict[str, Any]) -> bool:
    """True when the candidate slip should replace the incumbent.

    A difference above two EV points follows EV. Inside that band, a larger
    gap wins. With no usable gap on either slip, the higher EV wins and an
    exact tie keeps the incumbent.
    """
    ev_c = float(candidate["ev"])
    ev_i = float(incumbent["ev"])
    if abs(ev_c - ev_i) > EV_TIE:
        return ev_c > ev_i
    gap_c = slip_gap(candidate)
    gap_i = slip_gap(incumbent)
    if gap_c is None and gap_i is None:
        return ev_c > ev_i
    if gap_c is None:
        return False
    if gap_i is None:
        return True
    if gap_c != gap_i:
        return gap_c > gap_i
    return ev_c > ev_i


def _leg_log(row: dict[str, Any]) -> dict[str, Any]:
    prop = canon_prop(row.get("sport"), row.get("prop") or row.get("prop_type"))
    player = _player(row)
    low = _stored_wilson(row)
    return {
        "id": f"{player}|{prop}",
        "player": player,
        "prop": prop,
        "side": _side(row),
        "pick_type": str(row.get("pick_type") or ""),
        "line": row.get("line"),
        "gap_z": gap_value(row),
        "wilson_low": low,
        "cell_n": _cell_n(row),
        "p": _num(row.get("p")),
        "clears_floor": bool(leg_clears_floor(row)),
        "game": game_key(row),
        "hit": row.get("hit") if row.get("hit") in (0, 1) else None,
    }


def _mean_p(legs: list[dict[str, Any]]) -> float | None:
    vals = []
    for leg in legs:
        value = _num(leg.get("p"))
        if value is not None:
            vals.append(value)
    if not vals:
        return None
    return sum(vals) / len(vals)


def slate_record(sport: str, legs: list[dict[str, Any]], *, date: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
    games = {game_key(leg) for leg in legs if game_key(leg)}
    enabled = sport_enabled(sport, state)
    logged = [_leg_log(leg) for leg in legs]
    gap_top = _pick_stored(logged, _stored_gap_key)
    cell_top = _pick_stored(logged, _stored_cell_key)
    gap_ids = [leg["id"] for leg in gap_top]
    cell_ids = [leg["id"] for leg in cell_top]
    gap_set = set(gap_ids)
    cell_set = set(cell_ids)
    gap_p = _mean_p(gap_top)
    cell_p = _mean_p(cell_top)
    ev_gap = None if gap_p is None or cell_p is None else round(gap_p - cell_p, 4)
    return {
        "date": date,
        "sport": sport,
        "qualifying": len(legs) >= MIN_LEGS and len(games) >= MIN_GAMES,
        "orders_differ": gap_set != cell_set,
        "order_used": "gap" if enabled else "cell",
        "pool_n": len(legs),
        "games": len(games),
        "gap_order": [leg["id"] for leg in sorted(logged, key=_stored_gap_key)],
        "cell_order": [leg["id"] for leg in sorted(logged, key=_stored_cell_key)],
        "top_gap": gap_ids,
        "top_cell": cell_ids,
        "legs_reordered": [leg_id for leg_id in gap_ids if leg_id not in cell_set]
        + [leg_id for leg_id in cell_ids if leg_id not in gap_set],
        "ev_gap": ev_gap,
        "legs": logged,
    }


def read_log(path: Path | None = None) -> list[dict[str, Any]]:
    dest = path or LOG_PATH
    if not dest.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in dest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def write_log(rows: list[dict[str, Any]], path: Path | None = None) -> None:
    dest = path or LOG_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    dest.write_text(text, encoding="utf-8")


def log_slate(
    rows: list[dict[str, Any]],
    *,
    date: str,
    path: Path | None = None,
    state_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Replace this date's lines. Hits already stored on those lines are kept."""
    state = load_state(state_path)
    by: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sport = _sport(row)
        if sport:
            by.setdefault(sport, []).append(row)
    fresh = [slate_record(sport, legs, date=date, state=state) for sport, legs in sorted(by.items())]
    prior_hits: dict[tuple[str, str, str], int] = {}
    kept: list[dict[str, Any]] = []
    for old in read_log(path):
        if str(old.get("date") or "") == date:
            for leg in old.get("legs") or []:
                if leg.get("hit") in (0, 1):
                    prior_hits[(date, str(old.get("sport") or ""), str(leg.get("id") or ""))] = int(leg["hit"])
            continue
        kept.append(old)
    for rec in fresh:
        for leg in rec["legs"]:
            prev = prior_hits.get((date, rec["sport"], leg["id"]))
            if prev is not None and leg.get("hit") not in (0, 1):
                leg["hit"] = prev
    write_log(kept + fresh, path)
    return fresh


def _stored_gap_key(leg: dict[str, Any]) -> tuple:
    low = leg.get("wilson_low")
    cell_key = -(float(low) if low is not None else -1.0)
    if leg.get("clears_floor") and leg.get("gap_z") is not None and leg.get("side") == "OVER":
        return (0, -float(leg["gap_z"]), cell_key, str(leg.get("player") or ""))
    return (1, cell_key, str(leg.get("player") or ""))


def _stored_cell_key(leg: dict[str, Any]) -> tuple:
    low = leg.get("wilson_low")
    return (-(float(low) if low is not None else -1.0), str(leg.get("player") or ""))


def _pick_stored(legs: list[dict[str, Any]], key_fn, rng: random.Random | None = None) -> list[dict[str, Any]]:
    if rng is None:
        ordered = sorted(legs, key=key_fn)
    else:
        keys = {id(leg): rng.random() for leg in legs}
        ordered = sorted(legs, key=lambda leg: keys[id(leg)])
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for leg in ordered:
        player = str(leg.get("player") or "")
        if not player or player in seen:
            continue
        seen.add(player)
        out.append(leg)
        if len(out) >= TOP_K:
            break
    return out


def _rate(legs: list[dict[str, Any]]) -> float | None:
    if len(legs) < TOP_K:
        return None
    if any(leg.get("hit") not in (0, 1) for leg in legs):
        return None
    return sum(int(leg["hit"]) for leg in legs) / len(legs)


def _band(day_diffs: list[float], draws: int = BOOTSTRAP_DRAWS, seed: int = 0) -> tuple[float, float] | None:
    if not day_diffs:
        return None
    rng = random.Random(seed)
    n = len(day_diffs)
    stats = []
    for _ in range(draws):
        sample = [day_diffs[rng.randrange(n)] for _ in range(n)]
        stats.append(sum(sample) / n)
    stats.sort()
    lo = stats[int(0.025 * (draws - 1))]
    hi = stats[int(0.975 * (draws - 1))]
    return lo, hi


def _orders_differ(legs: list[dict[str, Any]]) -> bool:
    gap_ids = {leg.get("id") for leg in _pick_stored(legs, _stored_gap_key)}
    cell_ids = {leg.get("id") for leg in _pick_stored(legs, _stored_cell_key)}
    return gap_ids != cell_ids


def score_sport(records: list[dict[str, Any]], sport: str) -> dict[str, Any]:
    """Day-clustered hit rates. Only slates whose top-k sets differ are evidence."""
    days = []
    qualifying = 0
    differing = 0
    for rec in records:
        if str(rec.get("sport") or "").upper() != sport.upper():
            continue
        if not rec.get("qualifying"):
            continue
        qualifying += 1
        legs = list(rec.get("legs") or [])
        if not _orders_differ(legs):
            continue
        differing += 1
        gap_legs = _pick_stored(legs, _stored_gap_key)
        cell_legs = _pick_stored(legs, _stored_cell_key)
        gap_rate = _rate(gap_legs)
        cell_rate = _rate(cell_legs)
        if gap_rate is None or cell_rate is None:
            continue
        rng = random.Random(f"{rec.get('date')}|{sport}")
        random_rates = []
        for _ in range(RANDOM_DRAWS):
            drawn = _pick_stored(legs, _stored_gap_key, rng)
            rate = _rate(drawn)
            if rate is not None:
                random_rates.append(rate)
        days.append(
            {
                "date": rec.get("date"),
                "gap": gap_rate,
                "cell": cell_rate,
                "random": (sum(random_rates) / len(random_rates)) if random_rates else None,
            }
        )
    gap_mean = sum(d["gap"] for d in days) / len(days) if days else None
    cell_mean = sum(d["cell"] for d in days) / len(days) if days else None
    rand_vals = [d["random"] for d in days if d["random"] is not None]
    rand_mean = sum(rand_vals) / len(rand_vals) if rand_vals else None
    diffs = [d["gap"] - d["cell"] for d in days]
    band = _band(diffs)
    beats = bool(band and band[0] > 0)
    loses = bool(band and band[1] < 0)
    return {
        "sport": sport.upper(),
        "qualifying": qualifying,
        "differing": differing,
        "differing_graded": len(days),
        "graded_days": len(days),
        "gap": gap_mean,
        "cell": cell_mean,
        "random": rand_mean,
        "gap_minus_cell_band": None if band is None else [band[0], band[1]],
        "beats_cell": beats,
        "loses_to_cell": loses,
    }


def apply_stop(records: list[dict[str, Any]], state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Turn gap off only when it loses on 20 differing slates. Review that monthly.

    A tie, or two orders that picked the same legs, is not evidence. An off
    sport stays off for the rest of the review month, then the same rule
    runs again and can turn it back on.
    """
    book = {"sports": dict((state or {}).get("sports") or {})}
    sports = sorted({str(rec.get("sport") or "").upper() for rec in records if rec.get("sport")})
    current_month = date_cls.today().strftime("%Y-%m")
    for sport in sports:
        prev = book["sports"].get(sport) or {}
        scored = score_sport(records, sport)
        held_off = prev.get("enabled") is False and prev.get("reviewed_month") == current_month
        if held_off:
            enabled = False
            decision = "gap off"
            reviewed = current_month
        elif scored["differing_graded"] >= MIN_SLATES and scored["loses_to_cell"]:
            enabled = False
            decision = "gap off"
            reviewed = current_month
        elif scored["differing_graded"] >= MIN_SLATES and scored["beats_cell"]:
            enabled = True
            decision = "gap stays on"
            reviewed = current_month
        else:
            enabled = True
            decision = "trial"
            reviewed = current_month
        book["sports"][sport] = {
            **scored,
            "enabled": enabled,
            "decision": decision,
            "reviewed_month": reviewed,
            "updated": date_cls.today().isoformat(),
        }
    return book


def save_state(state: dict[str, Any], path: Path | None = None) -> None:
    dest = path or STATE_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _graded_hit_index(dates: set[str]) -> dict[tuple[str, str, str, str, str], int]:
    """(date, sport, player, prop, side) -> 1/0 from graded_props files, when they exist."""
    roots = [
        _REPO / "ui_runner" / "templates",
        Path(r"H:\PropORACLE_main_cp\ui_runner\templates"),
    ]
    found: dict[tuple[str, str, str, str, str], int] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for day in dates:
            path = root / f"graded_props_{day}.json"
            if not path.is_file():
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for row in raw.get("props") or raw.get("rows") or []:
                if not isinstance(row, dict):
                    continue
                result = str(row.get("result") or "").upper()
                if result not in {"HIT", "MISS"}:
                    continue
                sport = str(row.get("sport") or "").strip().upper()
                player = " ".join(str(row.get("player") or "").lower().split())
                prop = canon_prop(sport, row.get("prop") or row.get("prop_type"))
                side = _side(row)
                if not (sport and player and prop and side):
                    continue
                found[(day, sport, player, prop, side)] = 1 if result == "HIT" else 0
    return found


def fill_hits(records: list[dict[str, Any]]) -> int:
    """Copy graded HIT/MISS onto log legs that do not have one yet."""
    dates = {str(rec.get("date") or "") for rec in records if rec.get("date")}
    if not dates:
        return 0
    index = _graded_hit_index(dates)
    filled = 0
    for rec in records:
        day = str(rec.get("date") or "")
        sport = str(rec.get("sport") or "").upper()
        for leg in rec.get("legs") or []:
            if leg.get("hit") in (0, 1):
                continue
            key = (day, sport, str(leg.get("player") or ""), str(leg.get("prop") or ""), str(leg.get("side") or ""))
            if key not in index:
                continue
            leg["hit"] = index[key]
            filled += 1
    return filled


def _fmt_rate(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}"


def format_line(sport: str, row: dict[str, Any]) -> str:
    status = "OFF" if row.get("enabled") is False else "ON"
    decision = str(row.get("decision") or "trial")
    differed = int(row.get("differing_graded") if row.get("differing_graded") is not None else row.get("graded_days") or 0)
    qualifying = int(row.get("qualifying") or 0)
    return (
        f"gap_rank_{sport.lower()} | {status} | "
        f"gap {_fmt_rate(row.get('gap'))} cell {_fmt_rate(row.get('cell'))} "
        f"random {_fmt_rate(row.get('random'))} | "
        f"days {differed} differing / {qualifying} qualifying | {decision}"
    )


def score_and_maybe_disable(
    *,
    log_path: Path | None = None,
    state_path: Path | None = None,
    fill: bool = True,
) -> list[str]:
    """Fill hits, score each sport, and turn gap off when the stop rule says so."""
    records = read_log(log_path)
    if fill:
        filled = fill_hits(records)
        if filled:
            write_log(records, log_path)
    state = apply_stop(records, load_state(state_path))
    save_state(state, state_path)
    lines = []
    for sport, row in sorted((state.get("sports") or {}).items()):
        lines.append(format_line(sport, row))
    return lines
