"""Standard L5=5 hit rates — strict DNP, leakage split, clean bucket hold.

Void only with positive DNP evidence. Power-6 stays log-only (power6_trusted=false)
until leakage-pass + true-Standard filters clear.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_graded_history import (  # noqa: E402
    _norm_dir,
    _norm_pick,
    _norm_sport,
    _parse_hit,
)
from analyze_l5_gate_rates import _directional_l5  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.dnp_zero_void import (  # noqa: E402
    is_dnp_zero_actual,
    would_old_unknown_pt_void,
)
from utils.power6_std_buckets import (  # noqa: E402
    BE_6PICK,
    POWER6_BLOCKED_SPORTS,
    UNPROVEN_BUCKETS,
)

OUT = ROOT / "data" / "reports" / "standard_l5eq5_hit_rates.json"
NFL_CACHE = ROOT / "Sports" / "NFL" / "data" / "cache" / "nfl_boxscore_cache.csv"

_NBA_PROP_COL = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocks": "blk",
    "blocked shots": "blk",
    "turnovers": "tov",
    "3-pt made": "fg3m",
    "3pt made": "fg3m",
    "fg attempted": "fga",
    "fg made": "fgm",
    "pts+rebs": "pr",
    "pts+asts": "pa",
    "rebs+asts": "ra",
    "pts+rebs+asts": "pra",
    "pra": "pra",
    "fantasy score": "fantasy_score",
}

_NHL_PROP_COL = {
    "shots_on_goal": "shots_on_goal",
    "shots on goal": "shots_on_goal",
    "sog": "shots_on_goal",
    "points": "points",
    "assists": "assists",
    "goals": "goals",
    "blocked shots": "blocked_shots",
    "hits": "hits",
    "saves": "saves",
}

_SOCCER_PROP_COL = {
    "shots": "sh",
    "shots on target": "sog",
    "shots_on_target": "sog",
    "sot": "sog",
    "goals": "g",
    "assists": "a",
    "saves": "sv",
}

_NFL_PROP_COL = {
    "passing yards": "PASS_YDS",
    "pass yards": "PASS_YDS",
    "passing attempts": "PASS_ATT",
    "pass attempts": "PASS_ATT",
    "passing completions": "PASS_CMP",
    "pass completions": "PASS_CMP",
    "passing touchdowns": "PASS_TD",
    "pass tds": "PASS_TD",
    "interceptions": "PASS_INT",
    "rushing yards": "RUSH_YDS",
    "rush yards": "RUSH_YDS",
    "rushing attempts": "RUSH_ATT",
    "rush attempts": "RUSH_ATT",
    "rushing touchdowns": "RUSH_TD",
    "receiving yards": "REC_YDS",
    "rec yards": "REC_YDS",
    "receptions": "REC",
    "receiving touchdowns": "REC_TD",
    "longest reception": "REC_LONG",
    "longest rush": "RUSH_LONG",
    "targets": "REC_TGT",
    "sacks": "SACK",
    "tackles": "TACK_TOT",
    "kicking points": "KICK_PTS",
}


def _ci_row(hits: int, n: int) -> dict[str, Any]:
    hr = (hits / n) if n else None
    band = wilson_band(hits, n) if n else None
    lo = round(band[0], 4) if band else None
    hi = round(band[1], 4) if band else None
    return {
        "hits": hits,
        "n": n,
        "hit_rate": round(hr, 4) if hr is not None else None,
        "ci95_low": lo,
        "ci95_high": hi,
        "ci_low_above_be": bool(lo is not None and lo > BE_6PICK),
    }


def _db_path() -> Path | None:
    for p in (
        ROOT / "data" / "cache" / "proporacle_ref.db",
        ROOT / "Sports" / "NBA" / "data" / "cache" / "proporacle_ref.db",
        ROOT / "proporacle_ref.db",
    ):
        if p.is_file():
            return p
    return None


def _norm_player(s: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _prop_key(s: object) -> str:
    return str(s or "").strip().lower().replace("_", " ")


def _is_defaulted_pick_type(pt_raw: object) -> bool:
    """True when pick_type is empty/dash and _norm_pick maps it to standard."""
    raw = str(pt_raw or "").strip()
    if raw.casefold() == "standard":
        return False
    return _norm_pick(pt_raw) == "standard"


def _nfl_date(v: object) -> str | None:
    s = str(v or "").strip()
    if len(s) == 10 and s[4] == "-":
        return s
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return None


_STEP8_DATE_CACHE: dict[tuple[str, str], dict[str, str]] = {}


def _step8_player_game_dates(slate: str, sport: str) -> dict[str, str]:
    key = (slate, sport.upper())
    if key in _STEP8_DATE_CACHE:
        return _STEP8_DATE_CACHE[key]
    sport_u = sport.upper()
    candidates: list[Path] = []
    if sport_u in {"NBA", "NBAP"}:
        candidates.append(ROOT / "outputs" / slate / "nba" / "step8_all_direction_clean.xlsx")
    if sport_u in {"NBA1Q", "NBA1H"}:
        seg = "1q" if sport_u == "NBA1Q" else "1h"
        candidates.extend(
            [
                ROOT / "outputs" / slate / f"step8_nba{seg}_direction_clean_{slate}.xlsx",
                ROOT / "outputs" / slate / "nba" / f"step8_nba{seg}_direction_clean.xlsx",
            ]
        )
    if sport_u == "NHL":
        candidates.extend(
            [
                ROOT / "outputs" / slate / "nhl" / "step8_nhl_direction_clean.xlsx",
                ROOT / "outputs" / slate / f"step8_nhl_direction_clean_{slate}.xlsx",
            ]
        )
    if sport_u == "NFL":
        candidates.extend(
            [
                ROOT / "outputs" / slate / "nfl" / "step8_nfl_direction_clean.xlsx",
                ROOT / "outputs" / slate / f"step8_nfl_direction_clean_{slate}.xlsx",
            ]
        )
    import pandas as pd

    out: dict[str, str] = {}
    for path in candidates:
        if not path.is_file():
            continue
        try:
            df = pd.read_excel(path, sheet_name="ALL", header=1)
        except Exception:
            try:
                df = pd.read_excel(path, sheet_name=0)
            except Exception:
                continue
        cols = {str(c).strip().lower(): c for c in df.columns}
        pcol = cols.get("player")
        dcol = cols.get("game date") or cols.get("game_date") or cols.get("start_time")
        if not pcol or not dcol:
            continue
        for _, row in df.iterrows():
            pname = str(row.get(pcol) or "").strip().lower()
            raw = str(row.get(dcol) or "")[:10]
            if pname and len(raw) == 10 and raw[4] == "-" and pname not in out:
                out[pname] = raw
        if out:
            break
    _STEP8_DATE_CACHE[key] = out
    return out


class _BoxIndex:
    """Lazy per-sport box lookup for leakage recompute."""

    def __init__(self) -> None:
        self._nba: dict[str, list[tuple[str, dict[str, float]]]] | None = None
        self._nhl: dict[str, list[tuple[str, dict[str, float]]]] | None = None
        self._soccer: dict[str, list[tuple[str, dict[str, float]]]] | None = None
        self._nfl: dict[str, list[tuple[str, dict[str, float]]]] | None = None
        self.path = _db_path()

    def _load_table(
        self, table: str, cols: list[str]
    ) -> dict[str, list[tuple[str, dict[str, float]]]]:
        out: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not self.path:
            return out
        con = sqlite3.connect(str(self.path))
        try:
            have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
            use = [c for c in cols if c in have]
            if "game_date" not in have or "player" not in have or not use:
                return out
            q = f"SELECT game_date, player, {', '.join(use)} FROM {table}"
            for row in con.execute(q):
                gd = str(row[0] or "")[:10]
                if len(gd) != 10:
                    continue
                pn = _norm_player(row[1])
                vals: dict[str, float] = {}
                for i, c in enumerate(use):
                    try:
                        v = float(row[2 + i])
                    except (TypeError, ValueError):
                        continue
                    vals[c] = v
                out[pn].append((gd, vals))
        except sqlite3.Error:
            return out
        finally:
            con.close()
        for pn in out:
            out[pn].sort(key=lambda x: x[0])
        return out

    def nba(self) -> dict[str, list[tuple[str, dict[str, float]]]]:
        if self._nba is None:
            cols = list({*_NBA_PROP_COL.values(), "minutes"})
            self._nba = self._load_table("nba", cols)
        return self._nba

    def nhl(self) -> dict[str, list[tuple[str, dict[str, float]]]]:
        if self._nhl is None:
            cols = list(dict.fromkeys([*_NHL_PROP_COL.values(), "toi"]))
            self._nhl = self._load_table("nhl", cols)
        return self._nhl

    def soccer(self) -> dict[str, list[tuple[str, dict[str, float]]]]:
        if self._soccer is None:
            cols = list(
                dict.fromkeys([*_SOCCER_PROP_COL.values(), "sh", "sog", "minutes_played"])
            )
            self._soccer = self._load_table("soccer", cols)
        return self._soccer

    def nfl(self) -> dict[str, list[tuple[str, dict[str, float]]]]:
        if self._nfl is not None:
            return self._nfl
        out: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not NFL_CACHE.is_file():
            self._nfl = out
            return out
        import pandas as pd

        df = pd.read_csv(NFL_CACHE, encoding="utf-8-sig", low_memory=False)
        cols = list(_NFL_PROP_COL.values())
        for _, row in df.iterrows():
            gd = _nfl_date(row.get("game_date"))
            if not gd:
                continue
            pn = _norm_player(row.get("player_norm") or row.get("last_name"))
            if not pn:
                continue
            vals: dict[str, float] = {}
            for c in cols:
                if c not in row.index:
                    continue
                try:
                    v = float(row[c])
                except (TypeError, ValueError):
                    continue
                if v == v:  # not NaN
                    vals[c] = v
            if vals:
                out[pn].append((gd, vals))
        for pn in out:
            out[pn].sort(key=lambda x: x[0])
        self._nfl = out
        return out

    def games_for(
        self, sport: str, player: str, prop: str
    ) -> tuple[list[tuple[str, dict[str, float]]], str | None]:
        """Return (games, box_col) for leakage recompute."""
        sport_u = sport.upper()
        prop_k = _prop_key(prop)
        if sport_u == "NBA":
            col = _NBA_PROP_COL.get(prop_k)
            if not col:
                return [], None
            return self.nba().get(_norm_player(player), []), col
        if sport_u == "NHL":
            col = _NHL_PROP_COL.get(prop_k)
            if not col:
                return [], None
            return self.nhl().get(_norm_player(player), []), col
        if sport_u == "SOCCER":
            col = _SOCCER_PROP_COL.get(prop_k)
            if not col:
                return [], None
            return self.soccer().get(_norm_player(player), []), col
        if sport_u == "NFL":
            col = _NFL_PROP_COL.get(prop_k)
            if not col:
                return [], None
            return self.nfl().get(_norm_player(player), []), col
        return [], None


def _window_hits(
    games: list[tuple[str, dict[str, float]]],
    *,
    cutoff: str,
    col: str,
    line: float,
    direction: str,
    include_cutoff: bool,
) -> tuple[int | None, list[dict[str, Any]]]:
    """Directional clears in last 5 games before (or on) cutoff."""
    if include_cutoff:
        prior = [(gd, vals) for gd, vals in games if gd <= cutoff and col in vals]
    else:
        prior = [(gd, vals) for gd, vals in games if gd < cutoff and col in vals]
    if len(prior) < 5:
        return None, []
    window = prior[-5:]
    hits = 0
    detail: list[dict[str, Any]] = []
    for gd, vals in window:
        v = vals[col]
        clear = (v > line) if direction == "OVER" else (v < line)
        if clear:
            hits += 1
        detail.append({"date": gd, "value": v, "clear": clear})
    return hits, detail


def _iter_std_l5eq5_raw(
    graded_dir: Path, boxes: _BoxIndex
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    paths = sorted(p for p in graded_dir.glob("graded_props_*.json") if ".bak_" not in p.name)
    rows: list[dict[str, Any]] = []
    audits: dict[str, Any] = {
        "true_dnp_void_n": 0,
        "old_unknown_pt_would_void_n": 0,
        "pick_type_labels": defaultdict(int),
        "defaulted_pick_type_n": 0,
        "std_line_eq_goblin_n": 0,
        "std_over_line_lt_goblin_n": 0,
        "leakage_checked_n": 0,
        "leakage_ok_n": 0,
        "leakage_fail_n": 0,
        "leakage_unchecked_n": 0,
        "leakage_fail_samples": [],
        "old_unknown_pt_by_bucket": defaultdict(int),
        "true_dnp_by_bucket": defaultdict(int),
        "lookahead_rescues_n": 0,
    }

    for path in paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        file_date = str(raw.get("date") or path.stem.replace("graded_props_", ""))[:10]
        chunk = raw.get("props", raw.get("rows", []))
        if not isinstance(chunk, list):
            continue

        goblin_lines: dict[tuple[str, str], set[float]] = defaultdict(set)
        for r in chunk:
            if not isinstance(r, dict):
                continue
            if _norm_pick(r.get("pick_type")) != "goblin":
                continue
            try:
                gl = float(r.get("line"))
            except (TypeError, ValueError):
                continue
            key = (str(r.get("player") or "").strip().lower(), _prop_key(r.get("prop")))
            goblin_lines[key].add(gl)

        for r in chunk:
            if not isinstance(r, dict):
                continue
            pt_raw = str(r.get("pick_type") or "")
            if _norm_pick(r.get("pick_type")) != "standard":
                continue
            l5 = _directional_l5(r)
            if l5 is None or int(l5) != 5:
                continue
            direction = _norm_dir(r.get("direction") or r.get("over_under"))
            if direction not in {"OVER", "UNDER"}:
                continue
            sport = _norm_sport(r.get("sport"))
            result = str(r.get("result") or "").strip().upper()
            if result in {"VOID", "PUSH", "DNP", "CANCELLED", "CANCELED"}:
                continue

            actual = r.get("actual")
            if actual is None or str(actual).strip() in {"", "—", "-"}:
                actual = r.get("actual_value")
            if actual is None or str(actual).strip() in {"", "—", "-"}:
                actual = r.get("stat_value")
            try:
                actual_f = (
                    float(actual)
                    if actual is not None and str(actual).strip() not in {"", "—", "-"}
                    else None
                )
            except (TypeError, ValueError):
                actual_f = None

            bucket_key = f"{sport}|{direction}"

            if is_dnp_zero_actual(actual_f, r):
                audits["true_dnp_void_n"] += 1
                audits["true_dnp_by_bucket"][bucket_key] += 1
                continue

            if would_old_unknown_pt_void(actual_f, r):
                audits["old_unknown_pt_would_void_n"] += 1
                audits["old_unknown_pt_by_bucket"][bucket_key] += 1

            hit = _parse_hit(result)
            if hit is None:
                continue

            audits["pick_type_labels"][pt_raw] += 1
            defaulted = _is_defaulted_pick_type(r.get("pick_type"))
            if defaulted:
                audits["defaulted_pick_type_n"] += 1

            try:
                line_f = float(r.get("line"))
            except (TypeError, ValueError):
                line_f = None

            player = str(r.get("player") or "").strip()
            prop = _prop_key(r.get("prop"))
            gkey = (player.lower(), prop)
            glines = goblin_lines.get(gkey) or set()
            std_eq_goblin = bool(
                line_f is not None and any(abs(line_f - g) < 1e-9 for g in glines)
            )
            std_over_lt_goblin = bool(
                direction == "OVER"
                and line_f is not None
                and glines
                and line_f < min(glines)
            )
            if std_eq_goblin:
                audits["std_line_eq_goblin_n"] += 1
            if std_over_lt_goblin:
                audits["std_over_line_lt_goblin_n"] += 1

            source_board_date = file_date
            game_date = str(r.get("game_date") or r.get("Game Date") or "").strip()[:10]
            if len(game_date) != 10:
                game_date = (
                    _step8_player_game_dates(file_date, sport).get(player.lower()) or ""
                )
            board_mismatch = bool(
                game_date and source_board_date and game_date != source_board_date
            )

            leak_status = "unchecked"
            leak_before: int | None = None
            leak_incl: int | None = None
            leak_detail_before: list[dict[str, Any]] = []
            leak_detail_incl: list[dict[str, Any]] = []
            lookahead_rescue = False
            cutoff = game_date or source_board_date
            box_col: str | None = None
            if line_f is not None and cutoff and len(cutoff) == 10:
                games, box_col = boxes.games_for(sport, player, prop)
                if box_col and games:
                    leak_before, leak_detail_before = _window_hits(
                        games,
                        cutoff=cutoff,
                        col=box_col,
                        line=line_f,
                        direction=direction,
                        include_cutoff=False,
                    )
                    leak_incl, leak_detail_incl = _window_hits(
                        games,
                        cutoff=cutoff,
                        col=box_col,
                        line=line_f,
                        direction=direction,
                        include_cutoff=True,
                    )

            if leak_before is None:
                audits["leakage_unchecked_n"] += 1
                leak_status = "unchecked"
            else:
                audits["leakage_checked_n"] += 1
                if leak_before == 5:
                    audits["leakage_ok_n"] += 1
                    leak_status = "ok"
                else:
                    audits["leakage_fail_n"] += 1
                    leak_status = "fail"
                    if leak_incl == 5:
                        lookahead_rescue = True
                        audits["lookahead_rescues_n"] += 1
                    if len(audits["leakage_fail_samples"]) < 40:
                        audits["leakage_fail_samples"].append(
                            {
                                "sport": sport,
                                "player": player,
                                "prop": prop,
                                "direction": direction,
                                "line": line_f,
                                "game_date": cutoff,
                                "actual": actual_f,
                                "hit": int(hit),
                                "stored_l5": 5,
                                "recomputed_l5_before": leak_before,
                                "recomputed_l5_incl_game": leak_incl,
                                "lookahead_rescue": lookahead_rescue,
                                "box_col": box_col,
                                "window_before": leak_detail_before,
                                "window_incl": leak_detail_incl,
                            }
                        )

            rows.append(
                {
                    "sport": sport,
                    "direction": direction,
                    "hit": int(hit),
                    "file_date": file_date,
                    "source_board_date": source_board_date,
                    "game_date": game_date or None,
                    "board_date_mismatch": board_mismatch,
                    "player": player,
                    "prop": prop,
                    "line": line_f,
                    "pick_type": pt_raw,
                    "defaulted_pick_type": defaulted,
                    "actual": actual_f,
                    "old_unknown_pt_would_void": would_old_unknown_pt_void(actual_f, r),
                    "std_line_eq_goblin": std_eq_goblin,
                    "std_over_line_lt_goblin": std_over_lt_goblin,
                    "leakage_status": leak_status,
                    "leakage_recomputed_l5": leak_before,
                    "leakage_incl_game_l5": leak_incl,
                    "lookahead_rescue": lookahead_rescue,
                }
            )
    return rows, audits


def _bucket_table(
    rows: list[dict[str, Any]],
    *,
    old_void_counts: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[int]] = defaultdict(list)
    old_void_in: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        buckets[(r["sport"], r["direction"])].append(int(r["hit"]))
        if r.get("old_unknown_pt_would_void"):
            old_void_in[(r["sport"], r["direction"])] += 1
    out = []
    for (sport, direction), hits_list in sorted(buckets.items()):
        hits = sum(hits_list)
        n = len(hits_list)
        key = f"{sport}|{direction}"
        old_n = (
            int((old_void_counts or {}).get(key, 0))
            if old_void_counts is not None
            else old_void_in[(sport, direction)]
        )
        out.append(
            {
                "sport": sport,
                "direction": direction,
                **_ci_row(hits, n),
                "old_unknown_pt_would_void_n": old_n,
                "unproven": (sport.upper(), direction) in UNPROVEN_BUCKETS
                or (sport.upper() == "TENNIS" and n < 80),
            }
        )
    return out


def _leak_split_by_sport(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, list[int]]] = defaultdict(lambda: {"ok": [], "fail": []})
    for r in rows:
        st = r.get("leakage_status")
        if st not in {"ok", "fail"}:
            continue
        groups[r["sport"]][st].append(int(r["hit"]))
    out = []
    for sport in sorted(groups):
        ok = groups[sport]["ok"]
        fail = groups[sport]["fail"]
        out.append(
            {
                "sport": sport,
                "pass": _ci_row(sum(ok), len(ok)),
                "fail": _ci_row(sum(fail), len(fail)),
                "delta_hr": (
                    round((sum(fail) / len(fail)) - (sum(ok) / len(ok)), 4)
                    if ok and fail
                    else None
                ),
            }
        )
    return out


def _hand_check_ten(
    fail_rows: list[dict[str, Any]], boxes: _BoxIndex
) -> list[dict[str, Any]]:
    """Pick up to 10 fail rows across sports; annotate lookahead vs mismatch."""
    by_sport: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in fail_rows:
        by_sport[s["sport"]].append(s)
    picked: list[dict[str, Any]] = []
    quotas = [("NHL", 3), ("NBA", 3), ("SOCCER", 2), ("NFL", 2)]
    for sport, n in quotas:
        for s in by_sport.get(sport, [])[:n]:
            picked.append(s)
    if len(picked) < 10:
        seen = {
            (p["sport"], p["player"], p["prop"], p.get("game_date"), p.get("file_date"))
            for p in picked
        }
        for s in fail_rows:
            key = (
                s["sport"],
                s["player"],
                s["prop"],
                s.get("game_date"),
                s.get("file_date"),
            )
            if key in seen:
                continue
            picked.append(s)
            seen.add(key)
            if len(picked) >= 10:
                break

    out = []
    for s in picked[:10]:
        games, col = boxes.games_for(s["sport"], s["player"], s["prop"])
        cutoff = s.get("game_date") or s.get("file_date") or ""
        before_n, window_before = (None, [])
        incl_n, window_incl = (None, [])
        if col and cutoff and s.get("line") is not None:
            before_n, window_before = _window_hits(
                games,
                cutoff=cutoff,
                col=col,
                line=float(s["line"]),
                direction=s["direction"],
                include_cutoff=False,
            )
            incl_n, window_incl = _window_hits(
                games,
                cutoff=cutoff,
                col=col,
                line=float(s["line"]),
                direction=s["direction"],
                include_cutoff=True,
            )
        on_date = None
        if col and cutoff:
            for gd, vals in games:
                if gd == cutoff and col in vals:
                    on_date = vals[col]
                    break
        lookahead_rescue = before_n is not None and before_n < 5 and incl_n == 5
        if lookahead_rescue:
            verdict = "lookahead"
        elif incl_n is not None and before_n is not None and incl_n > before_n:
            verdict = "partial_lookahead_not_to_5"
        else:
            verdict = "recompute_mismatch"

        out.append(
            {
                "sport": s["sport"],
                "player": s["player"],
                "prop": s["prop"],
                "direction": s["direction"],
                "line": s.get("line"),
                "game_date": cutoff,
                "actual": s.get("actual"),
                "hit": s.get("hit"),
                "recomputed_l5_before": before_n,
                "recomputed_l5_incl_game": incl_n,
                "lookahead_rescue": lookahead_rescue,
                "window_before": window_before,
                "window_incl": window_incl,
                "box_value_on_game_date": on_date,
                "actual_matches_box": (
                    on_date is not None
                    and s.get("actual") is not None
                    and abs(float(on_date) - float(s["actual"])) < 1e-6
                ),
                "verdict": verdict,
            }
        )
    return out


def _run(label: str, graded_dir: Path) -> dict[str, Any]:
    boxes = _BoxIndex()
    raw_rows, audits = _iter_std_l5eq5_raw(graded_dir, boxes)
    n_mismatch = sum(1 for r in raw_rows if r["board_date_mismatch"])
    base = [r for r in raw_rows if not r["board_date_mismatch"]]

    old_void = dict(audits["old_unknown_pt_by_bucket"])
    by_sd_base = _bucket_table(base, old_void_counts=old_void)

    # Clean pool: true Standard label, not below-Goblin OVER, leakage-pass only.
    n_drop_defaulted = sum(1 for r in base if r.get("defaulted_pick_type"))
    n_drop_below_gob = sum(1 for r in base if r.get("std_over_line_lt_goblin"))
    n_drop_leak_fail = sum(1 for r in base if r.get("leakage_status") == "fail")
    n_drop_leak_unchecked = sum(1 for r in base if r.get("leakage_status") == "unchecked")

    clean = [
        r
        for r in base
        if not r.get("defaulted_pick_type")
        and not r.get("std_over_line_lt_goblin")
        and r.get("leakage_status") == "ok"
    ]
    by_sd_clean = _bucket_table(clean, old_void_counts=old_void)

    leak_split = _leak_split_by_sport(base)
    # Overall pass vs fail
    pass_rows = [r for r in base if r.get("leakage_status") == "ok"]
    fail_rows = [r for r in base if r.get("leakage_status") == "fail"]
    leak_split_overall = {
        "pass": _ci_row(sum(r["hit"] for r in pass_rows), len(pass_rows)),
        "fail": _ci_row(sum(r["hit"] for r in fail_rows), len(fail_rows)),
        "delta_hr": (
            round(
                (sum(r["hit"] for r in fail_rows) / len(fail_rows))
                - (sum(r["hit"] for r in pass_rows) / len(pass_rows)),
                4,
            )
            if pass_rows and fail_rows
            else None
        ),
        "lookahead_rescues_among_fails": audits["lookahead_rescues_n"],
        "lookahead_rescue_rate_among_fails": (
            round(audits["lookahead_rescues_n"] / len(fail_rows), 4)
            if fail_rows
            else None
        ),
    }

    hand = _hand_check_ten(fail_rows, boxes)
    hand_verdicts: dict[str, int] = defaultdict(int)
    for h in hand:
        hand_verdicts[str(h.get("verdict"))] += 1

    clears_clean = [
        r for r in by_sd_clean if r.get("ci_low_above_be") and not r.get("unproven")
    ]
    recommended = [r for r in clears_clean if r["sport"] not in POWER6_BLOCKED_SPORTS]

    return {
        "source": label,
        "date_range": {
            "min": min((r["file_date"] for r in base), default=None),
            "max": max((r["file_date"] for r in base), default=None),
        },
        "break_even_6pick_37_5x": BE_6PICK,
        "power6_trusted": False,
        "filters": {
            "dnp_zero_void": "positive_evidence_only",
            "exclude_source_board_date_mismatch": True,
            "board_date_mismatch_excluded_n": n_mismatch,
            "n_base": len(base),
            "true_dnp_void_n": audits["true_dnp_void_n"],
            "old_unknown_pt_would_void_n": audits["old_unknown_pt_would_void_n"],
            "clean_exclusions": {
                "defaulted_pick_type_n": n_drop_defaulted,
                "std_over_line_lt_goblin_n": n_drop_below_gob,
                "leakage_fail_n": n_drop_leak_fail,
                "leakage_unchecked_n": n_drop_leak_unchecked,
                "n_after_clean": len(clean),
            },
        },
        "audits": {
            "pick_type_labels": dict(audits["pick_type_labels"]),
            "defaulted_pick_type_n": audits["defaulted_pick_type_n"],
            "std_line_eq_goblin_n": audits["std_line_eq_goblin_n"],
            "std_over_line_lt_goblin_n": audits["std_over_line_lt_goblin_n"],
            "leakage": {
                "checked_n": audits["leakage_checked_n"],
                "ok_n": audits["leakage_ok_n"],
                "fail_n": audits["leakage_fail_n"],
                "unchecked_n": audits["leakage_unchecked_n"],
                "fail_rate": (
                    round(audits["leakage_fail_n"] / audits["leakage_checked_n"], 4)
                    if audits["leakage_checked_n"]
                    else None
                ),
                "sports": ["NBA", "NHL", "SOCCER", "NFL"],
                "note": (
                    "L5 recomputed from boxes using games strictly before game_date. "
                    "NBA1Q/1H stay unchecked (full-game boxes only)."
                ),
            },
            "leakage_pass_vs_fail": {
                "overall": leak_split_overall,
                "by_sport": leak_split,
            },
            "hand_check_10_fails": hand,
            "hand_check_verdict_counts": dict(hand_verdicts),
            "nfl_under_clean_note": (
                "NFL UNDER leakage-pass cell is ~99% — not trusted. Pass filter "
                "selects easy lines where box L5 already=5; fail/unchecked NFL "
                "UNDER still ~75–79%. Treat as selection artifact until resolved."
            ),
            "true_dnp_by_bucket": dict(audits["true_dnp_by_bucket"]),
            "old_unknown_pt_by_bucket": dict(audits["old_unknown_pt_by_bucket"]),
            "box_db": str(boxes.path) if boxes.path else None,
            "nfl_cache": str(NFL_CACHE) if NFL_CACHE.is_file() else None,
        },
        "overall_base": _ci_row(sum(r["hit"] for r in base), len(base)),
        "overall_clean": _ci_row(sum(r["hit"] for r in clean), len(clean)),
        "by_sport_direction": by_sd_base,
        "by_sport_direction_clean": by_sd_clean,
        "buckets_ci_low_above_be_clean": clears_clean,
        "recommended_6pick_buckets": recommended,
        "hold_note": (
            "power6_trusted=false — log-only. Clean table uses leakage-pass + "
            "explicit Standard pick_type + drop OVER Standard-below-Goblin."
        ),
    }


def main() -> None:
    primary = _run("ui_runner/templates", ROOT / "ui_runner" / "templates")
    print(f"PRIMARY: {primary['source']}  dates {primary['date_range']}")
    fx = primary["filters"]
    print(
        f"filters: DNP=positive_evidence_only  true_dnp={fx['true_dnp_void_n']}  "
        f"old_unk_void={fx['old_unknown_pt_would_void_n']}  n_base={fx['n_base']}"
    )
    cx = fx["clean_exclusions"]
    print(
        f"clean drops: defaulted={cx['defaulted_pick_type_n']}  "
        f"std_below_goblin={cx['std_over_line_lt_goblin_n']}  "
        f"leak_fail={cx['leakage_fail_n']}  leak_unchecked={cx['leakage_unchecked_n']}  "
        f"-> n_clean={cx['n_after_clean']}"
    )

    aud = primary["audits"]
    split = aud["leakage_pass_vs_fail"]
    ov = split["overall"]
    print("\n=== (1) leakage FAIL vs PASS hit rates ===")
    print(
        f"  PASS: {ov['pass']['hits']}/{ov['pass']['n']} = "
        f"{100 * (ov['pass']['hit_rate'] or 0):.1f}%  "
        f"CI_lo={100 * (ov['pass']['ci95_low'] or 0):.1f}%"
    )
    print(
        f"  FAIL: {ov['fail']['hits']}/{ov['fail']['n']} = "
        f"{100 * (ov['fail']['hit_rate'] or 0):.1f}%  "
        f"CI_lo={100 * (ov['fail']['ci95_low'] or 0):.1f}%"
    )
    print(
        f"  delta (fail-pass)={ov['delta_hr']}  "
        f"lookahead_rescues={ov['lookahead_rescues_among_fails']} "
        f"({100 * (ov['lookahead_rescue_rate_among_fails'] or 0):.1f}% of fails)"
    )
    print("  by sport:")
    for r in split["by_sport"]:
        p, f = r["pass"], r["fail"]
        print(
            f"    {r['sport']:<8}  "
            f"pass {p['hits']}/{p['n']}={100 * (p['hit_rate'] or 0):.1f}%  "
            f"fail {f['hits']}/{f['n']}={100 * (f['hit_rate'] or 0):.1f}%  "
            f"delta={r['delta_hr']}"
        )

    print("\n=== (1b) hand-check 10 leakage fails ===")
    print(f"  verdicts: {aud.get('hand_check_verdict_counts')}")
    for h in aud["hand_check_10_fails"]:
        print(
            f"  {h['verdict']:<22} {h['sport']:<6} {h['player']:<20} "
            f"{h['prop']:<16} {h['direction']:<5} line={h['line']}  "
            f"before={h['recomputed_l5_before']} incl={h['recomputed_l5_incl_game']}  "
            f"actual={h.get('actual')} box_on_date={h.get('box_value_on_game_date')}  "
            f"match={h.get('actual_matches_box')} hit={h.get('hit')}"
        )
        wb = h.get("window_before") or []
        if wb:
            print(
                "    before: "
                + ", ".join(
                    f"{x['date']}={x['value']}{'*' if x['clear'] else ''}" for x in wb
                )
            )
    if aud.get("nfl_under_clean_note"):
        print(f"\n  NOTE: {aud['nfl_under_clean_note']}")

    print("\n=== (3) CLEAN buckets (leak-pass + true Standard + drop below-Goblin) ===")
    o = primary["overall_clean"]
    print(
        f"OVERALL clean: {o['hits']}/{o['n']} = {100 * (o['hit_rate'] or 0):.1f}% "
        f"CI95 [{100 * (o['ci95_low'] or 0):.1f}, {100 * (o['ci95_high'] or 0):.1f}]"
    )
    for r in primary["by_sport_direction_clean"]:
        flag = (
            "CLEAR"
            if r["ci_low_above_be"] and not r.get("unproven")
            else ("UNPRV" if r.get("unproven") else "     ")
        )
        print(
            f"  {flag} {r['sport']:<8} {r['direction']:<5} "
            f"{r['hits']}/{r['n']} = {100 * r['hit_rate']:.1f}% "
            f"CI_lo={100 * r['ci95_low']:.1f}%"
        )

    print("\n=== Power-6 (HELD — power6_trusted=false, log-only) ===")
    for r in primary["recommended_6pick_buckets"]:
        print(
            f"  would-clear {r['sport']:<8} {r['direction']:<5} "
            f"{r['hits']}/{r['n']} = {100 * r['hit_rate']:.1f}% "
            f"CI_lo={100 * r['ci95_low']:.1f}%"
        )
    if not primary["recommended_6pick_buckets"]:
        print("  (none clear on clean leakage-pass pool)")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "break_even_6pick_37_5x": BE_6PICK,
        "ci_method": "wilson_95",
        "power6_trusted": False,
        "primary": primary["source"],
        "sources": [primary],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
