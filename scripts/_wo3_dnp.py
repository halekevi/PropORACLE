"""W8: zero-minute rows become NO_ACTUAL. Recompute active cells three ways."""
from __future__ import annotations

import csv
import json
import math
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from utils.dnp_zero_minutes import minutes_status  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"
MLB_IDS = ROOT / "Sports" / "MLB" / "mlb_id_cache.csv"
LOG = ROOT / "data" / "reports" / "grade_corrections.jsonl"
OUT = ROOT / "data" / "reports" / "dnp_zero_minutes.json"
VOID_OUT = ROOT / "data" / "reports" / "void_reason_split.json"
ET = ZoneInfo("America/New_York")
PITCHER = {
    "pitcher_ks",
    "strikeouts",
    "hits_allowed",
    "walks_allowed",
    "earned_runs",
    "era",
    "pitching_outs",
    "pitches_thrown",
    "innings_pitched",
    "outs",
}
BE_3_22 = 2.2 ** (-1 / 3)


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


def wilson_low(h: int, n: int) -> float | None:
    if n <= 0:
        return None
    z = 1.96
    p = h / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return (center - spread) / denom


def files() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            by.setdefault(path.stem.replace("graded_props_", "")[:10], path)
    return by


def load_minutes() -> dict[tuple[str, str, str], float | None]:
    """(sport, fold player, date) -> minutes. None means the player is on the row with a blank."""
    out: dict[tuple[str, str, str], float | None] = {}
    conn = sqlite3.connect(REF)
    for sport, table, col in (
        ("NBA", "nba", "minutes"),
        ("CBB", "cbb", "minutes"),
        ("WNBA", "wnba", "minutes"),
        ("NHL", "nhl", "toi"),
        ("Soccer", "soccer", "minutes_played"),
    ):
        for game_date, player, minutes in conn.execute(
            f"SELECT game_date, player, {col} FROM {table}"
        ):
            out[(sport, fold(player), str(game_date)[:10])] = num(minutes)
    for sport, payload in conn.execute(
        "SELECT sport, payload FROM espn_boxscore_rows WHERE sport IN ('nfl', 'cfb')"
    ):
        try:
            row = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            continue
        key_sport = "NFL" if sport == "nfl" else "CFB"
        out[(key_sport, fold(row.get("player_norm")), str(row.get("game_date") or "")[:10])] = num(
            row.get("MIN")
        )
    ids: dict[str, str] = {}
    if MLB_IDS.is_file():
        with MLB_IDS.open(encoding="utf-8", newline="") as fh:
            for rec in csv.DictReader(fh):
                ids[fold(rec.get("player_norm"))] = str(rec.get("mlb_player_id") or "")
    pa: dict[tuple[str, str], float | None] = {}
    bf: dict[tuple[str, str], float | None] = {}
    for pid, game_date, prop, val in conn.execute(
        """
        SELECT mlb_player_id, game_date, prop_norm, stat_value
        FROM mlb_gamelog
        WHERE prop_norm IN ('plate_appearances', 'batters_faced')
        """
    ):
        bucket = pa if prop == "plate_appearances" else bf
        bucket[(str(pid), str(game_date)[:10])] = num(val)
    conn.close()
    out["_mlb_ids"] = ids  # type: ignore[assignment]
    out["_pa"] = pa  # type: ignore[assignment]
    out["_bf"] = bf  # type: ignore[assignment]
    return out


def mlb_minutes(indexes, player: str, day: str, prop: str):
    ids = indexes["_mlb_ids"]
    pid = ids.get(fold(player))
    if not pid:
        return None
    use_bf = prop in PITCHER or (prop == "strikeouts")
    table = indexes["_bf"] if use_bf else indexes["_pa"]
    if (pid, day) not in table and (pid, day) not in (indexes["_bf"] if not use_bf else indexes["_pa"]):
        return None
    if (pid, day) not in table:
        return None
    return table[(pid, day)]


def lookup_minutes(indexes, sport: str, player: str, day: str, prop: str):
    if sport == "MLB":
        return mlb_minutes(indexes, player, day, prop)
    family = sport
    if sport in {"NBA1Q", "NBA1H"}:
        family = "NBA"
    elif sport in {"WNBA1Q", "WNBA1H"}:
        family = "WNBA"
    elif sport == "CFB1H":
        family = "CFB"
    key = (family, fold(player), day)
    if key not in indexes:
        return None
    return indexes[key]


class Acc:
    def __init__(self) -> None:
        self.h = [0, 0, 0]
        self.n = [0, 0, 0]

    def add(self, hit: int, status: str | None) -> None:
        self.h[0] += hit
        self.n[0] += 1
        if status != "dnp":
            self.h[1] += hit
            self.n[1] += 1
        if status not in {"dnp", "minutes_unknown"}:
            self.h[2] += hit
            self.n[2] += 1

    def pack(self) -> dict:
        labels = ("as_is", "ex_zero_minute", "ex_zero_minute_and_unknown")
        out = {}
        for i, label in enumerate(labels):
            n = self.n[i]
            h = self.h[i]
            hr = (h / n) if n else None
            low = wilson_low(h, n)
            out[label] = {
                "hits": h,
                "n": n,
                "hr": None if hr is None else round(hr, 4),
                "wilson_low": None if low is None else round(low, 4),
            }
        return out


def l5(row, side: str):
    key = "l5_under" if side == "UNDER" else "l5_over"
    v = num(row.get(key))
    if v is None:
        v = num(row.get("last5_under" if side == "UNDER" else "last5_over"))
    return v


def l10(row, side: str):
    key = "l10_under" if side == "UNDER" else "l10_over"
    return num(row.get(key))


def cell_names(sport: str, prop: str, pick: str, side: str, row) -> list[str]:
    names: list[str] = []
    if sport == "Tennis" and prop == "games_won" and "goblin" in pick and side == "OVER":
        names.append("tennis_gw_goblin")
        std = num(row.get("standard_line"))
        line = num(row.get("line"))
        if std is not None and line is not None and std - line >= 4:
            names.append("tennis_gw_discount")
    if sport == "Tennis" and prop == "match_total_games" and "goblin" in pick and side == "OVER":
        if (l5(row, side) or -1) >= 4 and (l10(row, side) or -1) >= 8:
            names.append("tennis_tg_l5ge4_l10ge8")
    if sport == "Soccer" and prop == "shots":
        if l5(row, side) == 5 and (l10(row, side) or -1) >= 8:
            names.append("soccer_shots_l5eq5_l10ge8")
    if sport == "Soccer" and prop == "sog":
        if (l5(row, side) or -1) >= 4:
            names.append("soccer_sot_l5ge4")
    return names


def void_bucket(row) -> str | None:
    res = str(row.get("result") or "").upper()
    reason = str(row.get("void_reason") or "").lower()
    reading = str(row.get("retirement_reading") or "")
    if reading == "walkover_chart_silent" or "walkover" in reason:
        return "walkover"
    if reading == "readings_differ" or "retir" in reason:
        return "retirement"
    if reason == "dnp" or (res == "NO_ACTUAL" and "dnp" in reason):
        return "dnp"
    if "postpon" in reason or "cancel" in reason:
        return "postponed"
    if res in {"VOID", "NO_ACTUAL"} or reason in {"no_data", "fetch_gap"} or "no_data" in reason:
        return "fetch_gap"
    return None


def main() -> None:
    indexes = load_minutes()
    seen = set()
    if LOG.is_file():
        for line in LOG.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                seen.add(
                    (
                        str(rec.get("date") or "")[:10],
                        fold(rec.get("player")),
                        str(rec.get("prop") or ""),
                        str(rec.get("line") or ""),
                        str(rec.get("reason") or ""),
                    )
                )
    cells = {name: Acc() for name in (
        "tennis_gw_goblin",
        "tennis_gw_discount",
        "tennis_tg_l5ge4_l10ge8",
        "soccer_shots_l5eq5_l10ge8",
        "soccer_sot_l5ge4",
    )}
    counts = {"dnp": 0, "minutes_unknown": 0, "played": 0, "files": 0}
    voids = {"dnp": 0, "postponed": 0, "retirement": 0, "walkover": 0, "fetch_gap": 0}
    corrections = []
    for day, path in files().items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            bucket = void_bucket(row)
            if bucket:
                voids[bucket] += 1
            sport = T.norm_sport(str(row.get("sport") or ""))
            prop = T.canon_prop(sport, str(row.get("prop") or ""))
            actual = num(row.get("actual_value"))
            graded = str(row.get("result") or "").upper()
            side = str(row.get("direction") or row.get("over_under") or "").upper()
            status = None
            if actual == 0 and graded in {"HIT", "MISS", "PUSH"}:
                minutes = lookup_minutes(indexes, sport, str(row.get("player") or ""), day, prop)
                status = minutes_status(minutes)
                if row.get("minutes_status") != status:
                    row["minutes_status"] = status
                    changed = True
                counts[status] += 1
                if status == "dnp":
                    old = graded
                    row["result"] = "NO_ACTUAL"
                    row["void_reason"] = "dnp"
                    row["hit"] = None
                    row["dnp_reason"] = "dnp_zero_minutes"
                    key = (day, fold(row.get("player")), str(row.get("prop") or ""), str(row.get("line") or ""), "dnp_zero_minutes")
                    if old in {"HIT", "MISS"} and key not in seen:
                        corrections.append(
                            {
                                "date": day,
                                "player": row.get("player"),
                                "prop": row.get("prop"),
                                "direction": side,
                                "line": row.get("line"),
                                "sport": sport,
                                "old": old,
                                "new": "NO_ACTUAL",
                                "reason": "dnp_zero_minutes",
                                "version": 1,
                                "logged_at": datetime.now(ET).isoformat(timespec="seconds"),
                            }
                        )
                        seen.add(key)
                    voids["dnp"] += 1
            if graded in {"HIT", "MISS"}:
                pick = str(row.get("pick_type") or "").lower()
                for name in cell_names(sport, prop, pick, side, row):
                    cells[name].add(1 if graded == "HIT" else 0, status)
        if changed:
            path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            counts["files"] += 1
    if corrections:
        with LOG.open("a", encoding="utf-8") as fh:
            for rec in corrections:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    priced = {
        "tennis_gw_goblin": 0.62,
        "tennis_gw_discount": 0.62,
        "tennis_tg_l5ge4_l10ge8": 0.702,
    }
    closed = {"tennis_gw_goblin", "tennis_gw_discount"}
    report_cells = {}
    repriced = []
    for name, acc in cells.items():
        packed = acc.pack()
        before = packed["as_is"]["hr"]
        after = packed["ex_zero_minute_and_unknown"]["hr"]
        shift = None if before is None or after is None else round(after - before, 4)
        low_before = packed["as_is"]["wilson_low"]
        low_after = packed["ex_zero_minute_and_unknown"]["wilson_low"]
        cross = (
            low_before is not None
            and low_after is not None
            and (low_before > BE_3_22) != (low_after > BE_3_22)
        )
        move = shift is not None and abs(shift) > 0.03
        n_after = packed["ex_zero_minute_and_unknown"]["n"]
        do_price = (move or cross) and n_after >= 30 and name not in closed and name in priced
        packed["shift"] = shift
        packed["crosses_break_even_0.769"] = cross
        packed["reprice"] = do_price
        report_cells[name] = packed
        if do_price and after is not None:
            repriced.append((name, round(after, 4)))
    payload = {
        "counts": counts,
        "corrections": len(corrections),
        "cells": report_cells,
        "repriced": repriced,
        "break_even_3leg_2_2x": round(BE_3_22, 4),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    VOID_OUT.write_text(json.dumps(voids, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
