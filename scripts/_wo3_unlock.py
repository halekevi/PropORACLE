"""W4b: board waterfall. Missing current-season n is derived from the box."""
from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.nfl_keep_gates import NFL_STD_UNDER_KEEP  # noqa: E402
from utils.postseason_slate import (  # noqa: E402
    apply_postseason_price,
    is_postseason_ticket_leg,
    season_blocks_ticket,
    season_class,
    shortlist_postseason,
)
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"
OUT = ROOT / "data" / "reports" / "unlock_waterfall.json"
DATE = "2026-10-08"
NFL_WEEK1 = date(2026, 9, 10)
CFB_WEEK1 = date(2026, 8, 29)


def iso(day: object) -> str:
    text = str(day or "").strip()
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def sample_n(row: dict):
    for key in ("l5_sample_n", "L5 Sample N", "games_used"):
        val = num(row.get(key))
        if val is not None:
            return val, "column"
    return None, ""


def box_game_counts() -> dict[tuple[str, str], int]:
    """Games strictly before the slate, current season, by sport family and player."""
    counts: dict[tuple[str, str], set[str]] = defaultdict(set)
    conn = sqlite3.connect(REF)
    for sport, table, start in (
        ("MLB", "mlb_gamelog", "2026-03-01"),
        ("NBA", "nba", "2025-10-01"),
        ("WNBA", "wnba", "2026-05-01"),
        ("NHL", "nhl", "2025-10-01"),
        ("Soccer", "soccer", "2025-07-01"),
    ):
        col_player = "player"
        col_date = "game_date"
        if table == "mlb_gamelog":
            continue
        for game_date, player in conn.execute(
            f"SELECT {col_date}, {col_player} FROM {table} WHERE {col_date} >= ? AND {col_date} < ?",
            (start, DATE),
        ):
            counts[(sport, fold(player))].add(iso(game_date))
    ids = {}
    cache = ROOT / "Sports" / "MLB" / "mlb_id_cache.csv"
    if cache.is_file():
        import csv

        with cache.open(encoding="utf-8-sig", newline="") as fh:
            for rec in csv.DictReader(fh):
                ids[str(rec.get("mlb_player_id") or "")] = fold(rec.get("player_norm"))
    for pid, game_date in conn.execute(
        "SELECT DISTINCT mlb_player_id, game_date FROM mlb_gamelog WHERE game_date >= '2026-03-01' AND game_date < ?",
        (DATE,),
    ):
        name = ids.get(str(pid))
        if name:
            counts[("MLB", name)].add(iso(game_date))
    for sport, payload in conn.execute(
        "SELECT sport, payload FROM espn_boxscore_rows WHERE sport IN ('nfl', 'cfb')"
    ):
        try:
            row = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            continue
        day = iso(row.get("game_date"))
        if not day or day >= DATE:
            continue
        start = "2026-09-01" if sport == "nfl" else "2026-08-01"
        if day < start:
            continue
        family = "NFL" if sport == "nfl" else "CFB"
        counts[(family, fold(row.get("player_norm")))].add(day)
    conn.close()
    return {key: len(days) for key, days in counts.items()}


def week_of(day: date, anchor: date) -> int:
    return (day - anchor).days // 7 + 1


def fifth_game_week(sport: str) -> dict:
    conn = sqlite3.connect(REF)
    by_player: dict[str, list[str]] = defaultdict(list)
    if sport == "NFL":
        rows = conn.execute(
            "SELECT payload FROM espn_boxscore_rows WHERE sport='nfl'"
        )
        anchor = NFL_WEEK1
        start = "2026-09-01"
    else:
        rows = conn.execute(
            "SELECT payload FROM espn_boxscore_rows WHERE sport='cfb'"
        )
        anchor = CFB_WEEK1
        start = "2026-08-01"
    for (payload,) in rows:
        try:
            row = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            continue
        day = iso(row.get("game_date"))
        if day < start:
            continue
        by_player[fold(row.get("player_norm"))].append(day)
    conn.close()
    fifths = []
    reached = 0
    for days in by_player.values():
        uniq = sorted(set(days))
        if len(uniq) >= 5:
            reached += 1
            fifths.append(uniq[4])
    fifths.sort()
    median = fifths[len(fifths) // 2] if fifths else None
    week = None
    if median:
        week = week_of(date.fromisoformat(median), anchor)
    return {
        "players": len(by_player),
        "players_with_5": reached,
        "median_fifth_game": median,
        "week": week,
    }


def directional_l5(row: dict):
    side = str(row.get("side") or row.get("direction") or "").upper()
    if side == "UNDER":
        return num(row.get("l5_under") if row.get("l5_under") is not None else row.get("l5"))
    return num(row.get("l5_over") if row.get("l5_over") is not None else row.get("l5"))


def main() -> None:
    played = box_game_counts()
    rows = load_today_board(DATE)
    post_ticket: list[dict] = []
    sports: dict[str, dict] = {}
    missing_unrecoverable = []
    for row in rows:
        sport = T.norm_sport(str(row.get("sport") or "")) or "UNKNOWN"
        bucket = sports.setdefault(
            sport,
            {
                "board": {"Goblin": 0, "Standard": 0},
                "regular_season": {"Goblin": 0, "Standard": 0},
                "l5_eq5": {"Goblin": 0, "Standard": 0},
                "has_cell": {"Goblin": 0, "Standard": 0},
                "keep_gate": {"Goblin": 0, "Standard": 0},
                "ticket_gate": {"Goblin": 0, "Standard": 0},
                "sample_column": 0,
                "sample_derived": 0,
                "sample_missing": 0,
                "sample_joined": 0,
                "unjoined_players": [],
                "season": {"regular": 0, "postseason": 0, "preseason": 0, "unknown": 0},
                "current_n_ge5": 0,
                "nfl_standard_under_allowlist": 0,
            },
        )
        if not row.get("start_time") and not row.get("game_date") and not row.get("slate_date"):
            row["slate_date"] = DATE
        pick = "Goblin" if "goblin" in str(row.get("pick_type") or "").lower() else "Standard"
        if pick not in {"Goblin", "Standard"}:
            continue
        bucket["board"][pick] += 1
        n, source = sample_n(row)
        if n is None:
            family = sport
            if sport.startswith("NBA"):
                family = "NBA"
            elif sport.startswith("WNBA"):
                family = "WNBA"
            elif sport.startswith("CFB"):
                family = "CFB"
            n = played.get((family, fold(row.get("player"))))
            if n is None:
                bucket["sample_missing"] += 1
                name = str(row.get("player") or "")
                if name and name not in bucket["unjoined_players"] and len(bucket["unjoined_players"]) < 30:
                    bucket["unjoined_players"].append(name)
            else:
                bucket["sample_derived"] += 1
                bucket["sample_joined"] += 1
        else:
            bucket["sample_column"] += 1
        if n is not None and n >= 5:
            bucket["current_n_ge5"] += 1
        if sport == "NFL" and pick == "Standard":
            prop = T.canon_prop("NFL", str(row.get("prop") or ""))
            side = str(row.get("side") or "").upper()
            if side == "UNDER" and prop in NFL_STD_UNDER_KEEP:
                bucket["nfl_standard_under_allowlist"] += 1
        kind = season_class(row) or "unknown"
        if kind not in bucket["season"]:
            kind = "unknown"
        bucket["season"][kind] += 1
        if season_blocks_ticket(row):
            continue
        if kind == "postseason":
            bucket.setdefault("postseason_legs", {"Goblin": 0, "Standard": 0})
            bucket["postseason_legs"][pick] += 1
        else:
            bucket["regular_season"][pick] += 1
        if directional_l5(row) != 5:
            continue
        bucket["l5_eq5"][pick] += 1
        rule = rule_for(sport)
        if rule is None:
            continue
        bucket["has_cell"][pick] += 1
        mode, fn = rule
        if fn is not None and not fn(row):
            continue
        bucket["keep_gate"][pick] += 1
        ok = goblin_70_eligible(row) if pick == "Goblin" else standard_ticket_eligible(row)
        if ok:
            if kind == "postseason":
                hold = bucket.setdefault("postseason_ticket", {"Goblin": 0, "Standard": 0})
                hold[pick] += 1
                post_ticket.append(row)
            else:
                bucket["ticket_gate"][pick] += 1
    for sport, bucket in sports.items():
        if bucket["sample_column"] == 0 and bucket["sample_derived"] == 0 and bucket["board"]["Goblin"] + bucket["board"]["Standard"] > 0:
            missing_unrecoverable.append(sport)
    from utils.category_hit_rate import load_category_hr_index, lookup_category_hr

    hr_index = load_category_hr_index()
    priced = []
    for row in post_ticket:
        child = lookup_category_hr(
            hr_index,
            sport=row.get("sport"),
            prop=row.get("prop"),
            pick_type=row.get("pick_type"),
            direction=row.get("side") or row.get("direction"),
        )
        cell = float(child["hr"]) if child and child.get("hr") is not None else None
        if is_postseason_ticket_leg(row) and apply_postseason_price(row, cell):
            priced.append(row)
    groups = shortlist_postseason(priced)
    report = {
        "date": DATE,
        "postseason_priced_legs": len(priced),
        "postseason_shortlist": len(groups),
        "postseason_placed": 0,
        "sports": sports,
        "sample_unrecoverable": missing_unrecoverable,
        "nfl_fifth_game": fifth_game_week("NFL"),
        "cfb_fifth_game": fifth_game_week("CFB"),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2)[:4000])


if __name__ == "__main__":
    main()
