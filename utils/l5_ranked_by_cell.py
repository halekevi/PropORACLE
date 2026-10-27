"""L5=5 slips ranked by the cell's Wilson low, one leg per group.

A leg enters only when the live card offers that side at that line, and
its five stored game values all clear the posted line. A stamped L5
count with no values, or a zero-filled series, stays out. Sacks Taken
stays out until those five values are a real box score (an all-zero
series is the missing-fill, not a sack total). Hits, blocked shots, and
faceoffs won stay out the same way until a five-game series is real.

Groups are one sport, prop, side, and book. The rate is that cell's
historical hit rate, not today's pool. A cell with n under 30 is
report-only. Groups with n of at least 30 are ordered by Wilson low.
Each group gives the slip one leg, the one with the largest play-side
gap in standard deviations. A player already on the slip yields that
group's next leg. Same-game legs may share a slip (flagged
``same_game_pair``); only all-same-team and the same player twice are
hard blocks.

Standard slips use 2, 3, or 4 legs, whichever the book can price.
A 6-leg slip is built only when six groups clear the n bar. Goblin
slips are 2 or 3 legs. A short list makes a smaller slip. Nothing is
filled with a leg from a group that missed the bar. The price check is
last: a slip is placed only when it is EV-positive at the N-correct
payout. Slips are logged on ``l5_ranked_by_cell`` and stay off /tickets
until about 30 stamped slips have been compared with the gated build.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from utils.category_hit_rate import lookup_category_hr
from utils.scraped_quote import is_real_scraped_quote
from utils.cushion_z import l5_clears, play_side_gap
from utils.fantasy_prop_filter import is_fantasy_prop_label
from utils.gap_ranker import wilson_low
from utils.postseason_slate import is_regular_season_row, season_class
from utils.prop_norm import canon_prop, fold
from utils.ticket_70_pool import skip_combo_player
from utils.slip_correlation import (
    annotate_slip,
    hard_block_reason,
    pick_legs_without_hard_blocks,
    player_key as corr_player_key,
)
from utils.ticket_ev_build import score_slip

TRACK = "l5_ranked_by_cell"
MIN_CELL_N = 30
COMPARE_AFTER = 30
_LOG = Path(__file__).resolve().parents[1] / "data" / "reports" / "track_comparison.jsonl"
# Corrected board-opponent attaches. The category file still carries the
# wrong-match tennis Goblin rates.
_CORRECTED_UNGATED: dict[tuple[str, str, str, str], tuple[int, int]] = {
    ("TENNIS", "games_won", "Goblin", "OVER"): (819, 1224),
    ("TENNIS", "match_total_games", "Goblin", "OVER"): (665, 1050),
}
_SEASON_FAMILY = {
    "MLB",
    "WNBA",
    "WNBA1H",
    "WNBA1Q",
    "NBA",
    "NBA1H",
    "NBA1Q",
    "NBAP",
    "NHL",
    "NFL",
    "NFLP",
    "CFB",
    "CFB1H",
    "CBB",
    "WCBB",
}
_SACKS_TAKEN = {"sackstaken", "sacks_taken"}
# An all-zero series on these props is the empty box fill. A real sack,
# completion, or long shows up as a non-zero in the five games.
_ZERO_FILL_PROPS = _SACKS_TAKEN | {
    "sacks",
    "passcompletions",
    "pass_completions",
    "completionpercentage",
    "completion_percentage",
    "longestcompletion",
    "longest_completion",
    # NHL box columns that are still empty. A real series has a non-zero.
    "hits",
    "blockedshots",
    "blocked_shots",
    "faceoffswon",
    "faceoffs_won",
}
_BORROWED_MARKERS = ("longest", "halves", "quarters", "first")
# These labels were stored as the parent counting stat. Drop them even when
# the parent row is already off the card, so the copy cannot rank alone.
_ALWAYS_BORROWED = ("longest", "halves", "quarters")
_UNKNOWN_OPP = {"", "unknown", "unk", "unknown_opp", "unknown opp"}


def _sport(row: dict[str, Any]) -> str:
    return str(row.get("sport") or "").strip().upper()


def _pick(row: dict[str, Any]) -> str:
    raw = str(row.get("pick_type") or "").strip().lower()
    if "demon" in raw:
        return "Demon"
    if "goblin" in raw:
        return "Goblin"
    if "standard" in raw:
        return "Standard"
    return ""


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"O", "OVER"}:
        return "OVER"
    if raw in {"U", "UNDER"}:
        return "UNDER"
    return ""


def _prop(row: dict[str, Any]) -> str:
    return canon_prop(_sport(row), row.get("prop") or row.get("prop_type"))


def _player(row: dict[str, Any]) -> str:
    return " ".join(str(row.get("player") or "").lower().split())


def _line(row: dict[str, Any]) -> float | None:
    try:
        value = float(row.get("line"))
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    return value


def _finite(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    return value


def _card_last5(row: dict[str, Any]) -> tuple[float, ...] | None:
    """The five values on the PrizePicks card, newest first. Absent means unchecked."""
    raw = row.get("card_l5")
    if raw in (None, ""):
        raw = row.get("pp_last5")
    if not isinstance(raw, (list, tuple)) or len(raw) < 5:
        return None
    vals: list[float] = []
    for item in list(raw)[:5]:
        num = _finite(item)
        if num is None:
            return None
        vals.append(num)
    return tuple(vals)


def five_games(row: dict[str, Any]) -> list[float] | None:
    """The five stored game values, newest first. A blank is not a zero."""
    for prefix in ("current_stat_g", "stat_g", "g"):
        vals: list[float] = []
        present = False
        for i in range(1, 6):
            raw = row.get(f"{prefix}{i}")
            if prefix == "g" and raw in (None, ""):
                raw = row.get(f"G{i}")
            if raw not in (None, ""):
                present = True
            num = _finite(raw)
            if num is None:
                vals = []
                break
            vals.append(num)
        if present and len(vals) == 5:
            return vals
    return None


def _sacks_taken(row: dict[str, Any]) -> bool:
    return fold(_prop(row)) in _SACKS_TAKEN or fold(row.get("prop")) in _SACKS_TAKEN


def _zero_fill_prop(row: dict[str, Any]) -> bool:
    return fold(_prop(row)) in _ZERO_FILL_PROPS or fold(row.get("prop")) in _ZERO_FILL_PROPS


def sacks_box_real(values: list[float]) -> bool:
    """An all-zero sack series is the missing fill, not a box score."""
    return any(v != 0 for v in values)


def _game_abbr(sport: str, raw: object) -> str:
    token = str(raw or "").strip().upper()
    if sport == "NFL" and token == "WAS":
        return "WSH"
    return token


def _game_key(row: dict[str, Any]) -> str:
    for key in ("game_id", "event_id", "pp_game_id", "game_ppid"):
        raw = row.get(key)
        if raw not in (None, ""):
            return str(raw)
    sport = _sport(row)
    teams = tuple(
        sorted(
            part
            for part in (
                _game_abbr(sport, row.get("team")),
                _game_abbr(sport, row.get("opp_team") or row.get("opp") or row.get("opponent")),
            )
            if part
        )
    )
    start = str(row.get("start_time") or row.get("game_time") or "")[:10]
    if teams:
        return "|".join((*teams, start))
    return ""


def _season_ok(row: dict[str, Any]) -> str:
    """Empty string means the row can stay. Postseason is its own track."""
    sport = _sport(row)
    kind = season_class(row)
    if kind == "postseason":
        return "postseason_own_track"
    if sport in _SEASON_FAMILY:
        if not is_regular_season_row(row):
            return "no_regular_season"
    elif kind == "preseason":
        return "no_regular_season"
    if sport in {"TENNIS", "SOCCER"}:
        opp = str(row.get("opp_team") or row.get("opp") or row.get("opponent") or "").strip().lower()
        if opp in _UNKNOWN_OPP:
            return "no_regular_season"
    return ""


def _cell(row: dict[str, Any], index: dict) -> dict[str, float] | None:
    sport = _sport(row)
    prop = _prop(row)
    pick = _pick(row)
    side = _side(row)
    corrected = _CORRECTED_UNGATED.get((sport, prop, pick, side))
    if corrected is not None:
        hit, n = corrected
        return {"hr": hit / n, "n": float(n), "hit": float(hit)}
    found = lookup_category_hr(
        index,
        sport=sport,
        prop=row.get("prop") or prop,
        pick_type=pick,
        direction=side,
    )
    if not found or found.get("hr") is None or not found.get("n"):
        return None
    try:
        n = float(found["n"])
        hr = float(found["hr"])
        hit = float(found.get("hit") or round(hr * n))
    except (TypeError, ValueError):
        return None
    if n <= 0 or hr < 0 or hr > 1:
        return None
    return {"hr": hr, "n": n, "hit": hit}


def gap_z(values: list[float], line: float, side: str) -> float | None:
    """(play-side average gap) / sample standard deviation of these five games."""
    if len(values) < 2:
        return None
    avg = sum(values) / len(values)
    var = sum((v - avg) ** 2 for v in values) / (len(values) - 1)
    sd = math.sqrt(var)
    if sd <= 0:
        return None
    gap = play_side_gap(avg, line, side)
    if gap is None:
        return None
    return gap / sd


def screen_leg(
    row: dict[str, Any],
    index: dict,
    offers: Any = None,
) -> tuple[dict[str, Any] | None, str]:
    """Step 1 plus the cell. The five values ride on every kept row.

    ``offers`` is the live card. When it is passed, a side the card does
    not list is dropped before the cell is priced. ``None`` leaves the
    check off so a unit fixture can build a slip without a board.
    """
    if not isinstance(row, dict):
        return None, "bad_row"
    if skip_combo_player(row) or is_fantasy_prop_label(row.get("prop")):
        return None, "combo_or_fantasy"
    pick = _pick(row)
    side = _side(row)
    if pick == "Demon" or pick not in {"Goblin", "Standard"} or side not in {"OVER", "UNDER"}:
        return None, "not_standard_or_goblin"
    if pick == "Goblin" and side != "OVER":
        return None, "goblin_under"
    if offers is not None and not offers.offers_side(row):
        return None, "card_side_missing"
    from utils.nhl_excluded_props import nhl_series_held

    if nhl_series_held(row):
        return None, "nhl_log_untrusted"
    season = _season_ok(row)
    if season:
        return None, season
    status = str(row.get("stat_status") or "").strip().upper().replace(" ", "_")
    if status in {"NO_DATA", "MISSING", "NO_BOX"}:
        return None, "no_box_log"
    values = five_games(row)
    if values is None:
        return None, "no_five_values"
    line = _line(row)
    if line is None or not l5_clears(values, line, side):
        return None, "not_five_of_five"
    card_vals = _card_last5(row)
    if card_vals is not None and card_vals != tuple(values):
        return None, "card_log_disagreement"
    if _zero_fill_prop(row) and not sacks_box_real(values):
        return None, "sacks_taken_box_untrusted"
    cell = _cell(row, index)
    if cell is None:
        return None, "no_regular_season_cell"
    out = dict(row)
    out["l5_values"] = values
    out["sport"] = _sport(row)
    out["prop_canon"] = _prop(row)
    out["pick_type"] = pick
    out["side"] = side
    out["cell_hr"] = cell["hr"]
    out["cell_n"] = int(cell["n"])
    out["cell_hit"] = int(cell["hit"])
    low = wilson_low(int(cell["hit"]), int(cell["n"]))
    out["cell_wilson_low"] = low
    out["gap_z"] = gap_z(values, line, side)
    out["p"] = cell["hr"]
    out["report_only"] = int(cell["n"]) < MIN_CELL_N
    out["unproven"] = pick == "Standard" and side == "UNDER"
    out["player_key"] = _player(row)
    out["game_key"] = _game_key(row)
    out["group_key"] = (_sport(row), _prop(row), side, pick)
    return out, ""


def _group_label(key: tuple[str, str, str, str]) -> str:
    sport, prop, side, book = key
    return f"{sport} {prop} {side} {book}"


def drop_borrowed_series(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop a prop whose five games are another prop's series.

    Longest reception, halves, and first-reception yards were stored as the
    parent counting stat. The same five numbers on two props keeps the
    parent and drops the borrowed label.
    """
    by_player: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for leg in legs:
        by_player.setdefault((str(leg.get("sport") or ""), str(leg.get("player_key") or "")), []).append(leg)
    drop_ids: set[int] = set()
    for leg in legs:
        name = fold(leg.get("prop_canon") or leg.get("prop"))
        if any(marker in name for marker in _ALWAYS_BORROWED):
            drop_ids.add(id(leg))
    for group in by_player.values():
        sigs: dict[tuple[float, ...], list[dict[str, Any]]] = {}
        for leg in group:
            sigs.setdefault(tuple(leg.get("l5_values") or []), []).append(leg)
        for members in sigs.values():
            folded = {fold(leg.get("prop_canon") or leg.get("prop")) for leg in members}
            if len(folded) < 2:
                continue
            for leg in members:
                name = fold(leg.get("prop_canon") or leg.get("prop"))
                if any(marker in name for marker in _BORROWED_MARKERS):
                    drop_ids.add(id(leg))
    return [leg for leg in legs if id(leg) not in drop_ids]


def build_groups(legs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Wilson-low order for cells with n>=30. Smaller cells stay report-only."""
    buckets: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    report: list[dict[str, Any]] = []
    for leg in legs:
        if leg.get("report_only"):
            report.append(leg)
            continue
        buckets.setdefault(tuple(leg["group_key"]), []).append(leg)
    groups = []
    for key, members in buckets.items():
        members.sort(
            key=lambda leg: (
                -(leg["gap_z"] if leg.get("gap_z") is not None else -1e9),
                leg.get("player_key") or "",
            )
        )
        sample = members[0]
        groups.append(
            {
                "key": key,
                "label": _group_label(key),
                "book": key[3],
                "wilson_low": sample.get("cell_wilson_low"),
                "cell_hr": sample.get("cell_hr"),
                "cell_n": sample.get("cell_n"),
                "cell_hit": sample.get("cell_hit"),
                "legs": members,
            }
        )
    groups.sort(
        key=lambda group: (
            -(group["wilson_low"] if group["wilson_low"] is not None else -1.0),
            -(group["cell_n"] or 0),
            group["label"],
        )
    )
    for rank, group in enumerate(groups, start=1):
        group["rank"] = rank
        for leg in group["legs"]:
            leg["group_rank"] = rank
            leg["group_label"] = group["label"]
    return groups, report


def candidate_sizes(n_groups: int, n_picks: int, family: str) -> list[int]:
    """Book sizes we may try, largest first. 6 only after six groups clear."""
    if family == "goblin":
        allowed = [3, 2]
    else:
        allowed = [4, 3, 2]
        if n_groups >= 6:
            allowed = [6, *allowed]
    return [n for n in allowed if 2 <= n <= n_picks]


def _take_one(group: dict[str, Any], used_players: set[str]) -> dict[str, Any] | None:
    for leg in group["legs"]:
        if leg.get("gap_z") is None:
            continue
        player = leg.get("player_key") or corr_player_key(leg)
        if player and player in used_players:
            continue
        return leg
    return None


def walk_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One leg per group, in Wilson order. Same player twice is skipped; same game is allowed."""
    used_players: set[str] = set()
    picked: list[dict[str, Any]] = []
    for group in groups:
        leg = _take_one(group, used_players)
        if leg is None:
            continue
        player = leg.get("player_key") or corr_player_key(leg)
        if player:
            used_players.add(player)
        picked.append(leg)
    return picked


def _pack(picks: list[dict[str, Any]], *, family: str, n_groups: int) -> list[dict[str, Any]]:
    """Largest book size that is EV-positive. A miss shrinks. It does not backfill.

    Hard blocks: same player twice, all legs from one team. Same-game pairs
    are allowed and stamped ``same_game_pair`` (flag only).
    """
    rest = list(picks)
    slips: list[dict[str, Any]] = []
    while len(rest) >= 2:
        sizes = candidate_sizes(n_groups, len(rest), family)
        placed_slip: dict[str, Any] | None = None
        negative_attempt: dict[str, Any] | None = None
        for n in sizes:
            trial = pick_legs_without_hard_blocks(rest, n)
            if trial is None or hard_block_reason(trial):
                continue
            scored = score_slip(trial, family=family, require_dollar=True)
            if scored is None or not (scored.get("ev") is not None and float(scored["ev"]) > 0):
                if negative_attempt is None:
                    negative_attempt = {
                        "legs": trial,
                        "n_legs": len(trial),
                        "placed": False,
                        "ev": None if scored is None else scored.get("ev"),
                        "all_hit_x": None if scored is None else scored.get("all_hit_x"),
                        "payout_source": None if scored is None else scored.get("payout_source"),
                        "p_win": None if scored is None else scored.get("p_win"),
                        "ev_label": "EV-negative",
                        "family": family,
                    }
                    annotate_slip(negative_attempt)
                continue
            placed_slip = {
                "legs": trial,
                "n_legs": len(trial),
                "placed": True,
                "ev": scored.get("ev"),
                "all_hit_x": scored.get("all_hit_x"),
                "payout_source": scored.get("payout_source"),
                "p_win": scored.get("p_win"),
                "ev_label": "EV-positive",
                "product": scored.get("product") or "Power",
                "family": family,
            }
            annotate_slip(placed_slip)
            break
        if placed_slip is None:
            if negative_attempt is not None and not slips:
                slips.append(negative_attempt)
            break
        slips.append(placed_slip)
        used_ids = {id(leg) for leg in placed_slip["legs"]}
        rest = [leg for leg in rest if id(leg) not in used_ids]
        if family == "standard":
            break
    return slips


def build_ranked_slips(
    board: list[dict[str, Any]],
    index: dict,
    offers: Any = None,
) -> dict[str, Any]:
    """Screen the board, rank the cells, and build the Standard and Goblin slips."""
    kept: list[dict[str, Any]] = []
    dropped: dict[str, int] = {}
    card_misses: list[dict[str, Any]] = []
    for row in board:
        leg, reason = screen_leg(row, index, offers)
        if leg is None:
            dropped[reason] = dropped.get(reason, 0) + 1
            if reason == "card_log_disagreement" and isinstance(row, dict):
                card_misses.append(
                    {
                        "player": row.get("player"),
                        "sport": _sport(row),
                        "prop": row.get("prop"),
                        "pick_type": _pick(row),
                        "side": _side(row),
                        "line": row.get("line"),
                        "reason": "card_log_disagreement",
                        "l5_values": five_games(row),
                        "card_l5": list(_card_last5(row) or []),
                    }
                )
            if reason == "card_side_missing" and isinstance(row, dict):
                card_misses.append(
                    {
                        "player": row.get("player"),
                        "sport": _sport(row),
                        "prop": row.get("prop"),
                        "pick_type": _pick(row),
                        "side": _side(row),
                        "line": row.get("line"),
                    }
                )
            continue
        kept.append(leg)
    kept = drop_borrowed_series(kept)
    groups, report = build_groups(kept)
    standard = [group for group in groups if group["book"] == "Standard"]
    goblin = [group for group in groups if group["book"] == "Goblin"]
    std_picks = walk_groups(standard)
    gob_picks = walk_groups(goblin)
    slips = _pack(std_picks, family="standard", n_groups=len(standard))
    slips.extend(_pack(gob_picks, family="goblin", n_groups=len(goblin)))
    for slip in slips:
        for leg in slip["legs"]:
            leg["track"] = TRACK
            leg["placed"] = bool(slip["placed"])
    return {
        "track": TRACK,
        "pool": kept,
        "report_only": report,
        "groups": groups,
        "dropped": dropped,
        "card_misses": card_misses,
        "slips": slips,
        "n_slips": len(slips),
        "n_placed": sum(1 for slip in slips if slip.get("placed")),
    }


def _leg_log(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "player": leg.get("player"),
        "sport": leg.get("sport"),
        "prop": leg.get("prop"),
        "prop_canon": leg.get("prop_canon"),
        "pick_type": leg.get("pick_type"),
        "direction": leg.get("side"),
        "line": leg.get("line"),
        "l5_values": leg.get("l5_values"),
        "cell_hr": leg.get("cell_hr"),
        "cell_n": leg.get("cell_n"),
        "cell_hit": leg.get("cell_hit"),
        "cell_wilson_low": leg.get("cell_wilson_low"),
        "gap_z": leg.get("gap_z"),
        "group_rank": leg.get("group_rank"),
        "group_label": leg.get("group_label"),
        "unproven": bool(leg.get("unproven")),
        "p": leg.get("p"),
    }


def _slip_log(slip: dict[str, Any], *, date: str, index: int) -> dict[str, Any]:
    legs = list(slip.get("legs") or [])
    n_g = sum(1 for leg in legs if _pick(leg) == "Goblin")
    n_s = sum(1 for leg in legs if _pick(leg) == "Standard")
    annotate_slip(slip)
    return {
        "date": date,
        "id": f"L5C-{slip.get('family', 'x')[:1].upper()}{len(legs)}-{index}",
        "track": TRACK,
        "kind": "slip",
        "placed": bool(slip.get("placed")),
        "ticket_track": TRACK,
        "postseason": 0,
        "composition": f"{n_s}S+{n_g}G",
        "n_legs": len(legs),
        "n_standard": n_s,
        "n_goblin": n_g,
        "p_win": slip.get("p_win"),
        "quote": slip.get("all_hit_x"),
        "payout_source": slip.get("payout_source"),
        "captured_at": slip.get("captured_at"),
        # Pin / EV floors are not scraped. Only stamp scraped_payout when
        # payout_source is a scrape family and captured_at is non-null.
        "scraped_payout": (
            slip.get("all_hit_x")
            if (
                slip.get("placed")
                and is_real_scraped_quote(
                    slip.get("payout_source"), slip.get("captured_at")
                )
            )
            else None
        ),
        "ev": slip.get("ev"),
        "dollar_test": bool(slip.get("placed")),
        "ev_label": slip.get("ev_label"),
        "stake": 20,
        "result": None,
        "same_game_pair": bool(slip.get("same_game_pair")),
        "correlation_block": slip.get("correlation_block"),
        "sports": sorted({_sport(leg) for leg in legs}),
        "standard_sports": sorted({_sport(leg) for leg in legs if _pick(leg) == "Standard"}),
        "legs": [_leg_log(leg) for leg in legs],
        "exclude_from_book": True,
    }


def _write_day(rows: list[dict[str, Any]], *, date: str, path: Path) -> None:
    kept: list[dict[str, Any]] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(rec, dict) and rec.get("track") == TRACK and rec.get("date") == date:
                continue
            if isinstance(rec, dict):
                kept.append(rec)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = kept + rows
    text = "".join(json.dumps(rec, ensure_ascii=False) + "\n" for rec in body)
    path.write_text(text, encoding="utf-8")


def stamped_slip_count(path: Path | None = None) -> int:
    dest = path or _LOG
    if not dest.is_file():
        return 0
    n = 0
    for line in dest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            isinstance(rec, dict)
            and rec.get("track") == TRACK
            and rec.get("kind", "slip") == "slip"
        ):
            n += 1
    return n


def record_l5_ranked(
    board: list[dict[str, Any]],
    *,
    date: str,
    index: dict | None = None,
    path: Path | None = None,
    offers: Any = None,
) -> dict[str, Any]:
    """Log today's ranked slips. They stay off the live ticket card.

    Pass the live card book. Without it, this track does not confirm a
    side, so the caller that publishes a slate should pass one.
    """
    from utils.category_hit_rate import load_category_hr_index

    hr = index if index is not None else load_category_hr_index()
    built = build_ranked_slips(board, hr, offers)
    logs = [_slip_log(slip, date=date, index=i) for i, slip in enumerate(built["slips"], start=1)]
    dest = path or _LOG
    _write_day(logs, date=date, path=dest)
    built["stamped_slips"] = stamped_slip_count(dest)
    built["compare_after"] = COMPARE_AFTER
    built["ready_to_compare"] = built["stamped_slips"] >= COMPARE_AFTER
    return built
