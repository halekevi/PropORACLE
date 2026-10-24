"""Does the average-to-line gap still move hit rate after the gate?

Gate: directional L5=5, directional L10>=8, and today's sport keep
(ticket_gate_passes). Terciles and the logistic model use a chronological
holdout. Tennis corrected rows use the Sackmann match against the step8
board opponent. Nothing here is wired into a ticket.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

import cushion_z_discovery as D
from cushion_log_window import iter_history, last10, load_series, norm_player
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr
from utils.comparison_tracks import wilson_band
from utils.cushion_z import _mean_sd, _window
from utils.fantasy_prop_filter import is_fantasy_prop_label
from utils.gate_shuffle import shuffle_cutoff
from utils.mlb_keep_gates import pitcher_l10_parent_cell
from utils.ticket_70_pool import ticket_gate_passes

ROOT = Path(__file__).resolve().parents[1]
OUT_JSON = ROOT / "data" / "reports" / "gap_inside_gate.json"
OUT_XLSX = ROOT / "data" / "reports" / "gap_inside_gate.xlsx"
MODEL_CUT = "2026-09-21"
EDGES = (-5, -3, -2, -1, -0.5, 0, 0.5, 1, 2, 3, 5, 8)
LABELS = (
    "avg 5+ under the line",
    "avg 3 to 5 under",
    "avg 2 to 3 under",
    "avg 1 to 2 under",
    "avg 0.5 to 1 under",
    "avg 0 to 0.5 under",
    "avg 0 to 0.5 over",
    "avg 0.5 to 1 over",
    "avg 1 to 2 over",
    "avg 2 to 3 over",
    "avg 3 to 5 over",
    "avg 5 to 8 over",
    "avg 8+ over the line",
)


def _gap_label(gap: float) -> str:
    for edge, label in zip(EDGES, LABELS):
        if gap < edge:
            return label
    return LABELS[-1]


def _counts(games, line: float, side: str) -> tuple[int, int] | None:
    vals = _window(games)
    if vals is None or len(vals) < 5:
        return None
    over = side == "OVER"
    l5 = sum((v > line) if over else (v < line) for v in vals[:5])
    l10 = sum((v > line) if over else (v < line) for v in vals)
    return l5, l10


def _avg_z(games, line: float, side: str) -> tuple[float, float] | None:
    vals = _window(games)
    if vals is None:
        return None
    moments = _mean_sd(vals)
    if moments is None:
        return None
    avg, sd = moments
    raw = avg - float(line)
    play = raw if side == "OVER" else -raw
    return raw, play / sd


def _passes(src: dict, sport: str, prop: str, book: str, side: str, line: float, l5: int, l10: int, player: str) -> bool:
    if l5 != 5 or l10 < 8:
        return False
    row = {
        "sport": sport,
        "prop": prop,
        "pick_type": book.split()[0],
        "side": side,
        "direction": side,
        "line": line,
        "player": player,
        "player_name": player,
        "team": src.get("team") or src.get("team_abbr"),
        "opp_team": src.get("opp_team") or src.get("opponent"),
        "l5": l5,
        "l10": l10,
        "l5_over": l5 if side == "OVER" else None,
        "l5_under": l5 if side == "UNDER" else None,
        "l10_over": l10 if side == "OVER" else None,
        "l10_under": l10 if side == "UNDER" else None,
        "def_tier": src.get("def_tier") or src.get("def") or src.get("DEF_TIER"),
        "checks": src.get("checks") if isinstance(src.get("checks"), dict) else {},
        "league": src.get("league") or src.get("competition"),
    }
    try:
        return bool(ticket_gate_passes(row))
    except Exception:
        return False


def _num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def _game_key(g: dict, player: str) -> str:
    event = str(g.get("event_id") or g.get("game_id") or g.get("espn_game_id") or "").strip()
    if event and event.lower() not in {"nan", "none"}:
        return event
    team = norm_player(g.get("team") or g.get("team_abbr"))
    opp = norm_player(g.get("opp_team") or g.get("opponent") or g.get("opp"))
    if team and opp:
        return "|".join(sorted((team, opp)))
    if opp:
        return norm_player(player) + "|" + opp
    return ""


def collect() -> list[dict]:
    D._CORRECTIONS = D.load_corrections()
    print("loading logs", flush=True)
    series = load_series()
    dates = sorted(p.stem.replace("graded_props_", "")[:10] for p in D.GRADED.glob("graded_props_20*.json"))
    rows: list[dict] = []
    boards: dict = {}

    def take(g: dict, date: str) -> None:
        sport = D.T.norm_sport(str(g.get("sport") or ""))
        if sport not in D.SPORTS:
            return
        result = str(g.get("result") or "").upper()
        side = str(g.get("direction") or g.get("over_under") or "").upper()
        if side == "O":
            side = "OVER"
        if side == "U":
            side = "UNDER"
        prop_key = "".join(ch for ch in str(g.get("prop") or "").lower() if ch.isalnum())
        line = D.fnum(g.get("line"))
        fixed = D.corrections_for(g, date, prop_key, side, line)
        if fixed:
            result = fixed
        if result not in {"HIT", "MISS"}:
            return
        player = str(g.get("player") or "")
        prop_name = str(g.get("prop") or "")
        if "+" in player or "combo" in prop_name.lower():
            return
        if D.row_excluded(g) or is_fantasy_prop_label(prop_name):
            return
        book = D.book_of(g.get("pick_type"), side)
        if book is None or line is None:
            return
        prop = D.T.canon_prop(sport, prop_name)
        if not prop:
            return
        key = (date, sport)
        if key not in boards:
            path = D.find_step8(date, D.SPORTS[sport])
            boards[key] = D.load_games(path, sport) if path else {}
        games = boards[key].get((norm_player(player), prop, book.split()[0], round(line, 1)))
        if not games:
            games = last10(series, sport, norm_player(player), prop, date)
        if not games or _window(games) is None:
            return
        counted = _counts(games, line, side)
        moments = _avg_z(games, line, side)
        if counted is None or moments is None:
            return
        l5, l10 = counted
        raw, z = moments
        rows.append(
            {
                "date": date,
                "sport": sport,
                "prop": prop,
                "book": book,
                "side": side,
                "player": norm_player(player),
                "line": line,
                "hit": 1 if result == "HIT" else 0,
                "l5": l5,
                "l10": l10,
                "gap": raw,
                "gap_z": z,
                "actual": _num(g.get("actual") if g.get("actual") not in (None, "") else g.get("actual_value")),
                "opp": str(g.get("opp_team") or g.get("opponent") or ""),
                "team": str(g.get("team") or ""),
                "game": _game_key(g, player),
                "src": g,
            }
        )

    for date in dates:
        raw = json.loads((D.GRADED / f"graded_props_{date}.json").read_text(encoding="utf-8"))
        for g in raw.get("props") or raw.get("rows") or []:
            take(g, date)
        print(date, len(rows), flush=True)
    before = len(rows)
    for g in iter_history(set(dates)):
        take(g, g["date"])
    print("archive added", len(rows) - before, flush=True)
    return rows


def _pack(parts: list[dict]) -> list[dict]:
    out = []
    for row in parts:
        n = row["n"]
        band = wilson_band(row["hits"], n) if n else None
        out.append(
            {
                **row,
                "rate": None if n == 0 else round(row["hits"] / n, 4),
                "wilson": None if band is None else [round(band[0], 4), round(band[1], 4)],
                "report_only": n < 30,
            }
        )
    return out


def _bins(rows: list[dict], gap_key: str = "gap") -> list[dict]:
    acc: dict[str, list] = defaultdict(lambda: [0, 0, 0.0, set(), set()])
    for r in rows:
        cell = acc[_gap_label(r[gap_key])]
        cell[0] += r["hit"]
        cell[1] += 1
        cell[2] += r[gap_key]
        cell[3].add(r["player"])
        cell[4].add(r["date"])
    packed = []
    for label in LABELS:
        if label not in acc:
            continue
        hits, n, gap_sum, players, dates = acc[label]
        player_hits = defaultdict(list)
        date_hits = defaultdict(list)
        for r in rows:
            if _gap_label(r[gap_key]) != label:
                continue
            player_hits[r["player"]].append(r["hit"])
            date_hits[r["date"]].append(r["hit"])
        packed.append(
            {
                "band": label,
                "hits": hits,
                "n": n,
                "mean_gap": round(gap_sum / n, 2),
                "players": len(players),
                "dates": len(dates),
                "player_rate": round(sum(sum(v) / len(v) for v in player_hits.values()) / len(player_hits), 4),
                "date_rate": round(sum(sum(v) / len(v) for v in date_hits.values()) / len(date_hits), 4),
            }
        )
    return _pack(packed)


def _by_prop(rows: list[dict], gap_key: str = "gap") -> list[dict]:
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(r["sport"], r["prop"], r["book"])].append(r)
    out = []
    for (sport, prop, book), group in sorted(groups.items()):
        if len(group) < 30:
            continue
        out.append({"sport": sport, "prop": prop, "book": book, "n": len(group), "bands": _bins(group, gap_key)})
    return out


def _holdout_terciles(rows: list[dict]) -> list[dict]:
    by_sport: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        by_sport[r["sport"]].append(r["date"])
    cuts = {}
    for sport, seen in by_sport.items():
        ordered = sorted(set(seen))
        cuts[sport] = ordered[int(len(ordered) * 0.7)] if len(ordered) >= 4 else ordered[-1]
    for r in rows:
        r["split"] = "holdout" if r["date"] >= cuts[r["sport"]] else "train"
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(r["sport"], r["prop"], r["book"])].append(r)
    cells = []
    for (sport, prop, book), group in sorted(groups.items()):
        train = [r for r in group if r["split"] == "train"]
        hold = [r for r in group if r["split"] == "holdout"]
        edges = D.tercile_edges([r["gap_z"] for r in train])
        if edges is None or len(hold) < 15:
            continue
        buckets = {name: [] for name in ("low", "mid", "high")}
        for r in hold:
            buckets[D.assign_tercile(r["gap_z"], edges)].append(r)
        packed = []
        for name in ("low", "mid", "high"):
            chunk = buckets[name]
            hits = sum(r["hit"] for r in chunk)
            n = len(chunk)
            band = wilson_band(hits, n) if n else None
            packed.append(
                {
                    "bucket": name,
                    "hits": hits,
                    "n": n,
                    "rate": None if n == 0 else round(hits / n, 4),
                    "wilson": None if band is None else [round(band[0], 4), round(band[1], 4)],
                    "report_only": n < 30,
                }
            )
        rates = [b["rate"] for b in packed]
        rising = all(r is not None for r in rates) and rates[0] <= rates[1] <= rates[2]
        high = packed[2]
        beats = None
        p95 = None
        if len(hold) >= 15 and high["n"] > 0:
            labels = [r["hit"] for r in hold]

            def score(shuffled: list[int]) -> float:
                picked = [
                    shuffled[i]
                    for i, r in enumerate(hold)
                    if D.assign_tercile(r["gap_z"], edges) == "high"
                ]
                return 0.0 if not picked else sum(picked) / len(picked)

            p95 = round(shuffle_cutoff([r["date"] for r in hold], labels, score, draws=200, seed=0), 4)
            beats = high["rate"] is not None and high["rate"] > p95
        cells.append(
            {
                "sport": sport,
                "prop": prop,
                "book": book,
                "cut": cuts[sport],
                "train_n": len(train),
                "holdout_n": len(hold),
                "edges": [round(edges[0], 3), round(edges[1], 3)],
                "buckets": packed,
                "rising": rising,
                "shuffle_p95": p95,
                "beats_shuffle": beats,
                "wired": False,
            }
        )
    return cells


def _logit(X: np.ndarray, y: np.ndarray, l2: float = 1.0) -> np.ndarray:
    n, k = X.shape
    w = np.zeros(k)
    penalty = np.full(k, l2)
    penalty[0] = 0.0
    for _ in range(30):
        z = np.clip(X @ w, -20, 20)
        p = 1.0 / (1.0 + np.exp(-z))
        var = np.maximum(p * (1.0 - p), 1e-6)
        H = (X.T * var) @ X
        H.flat[:: k + 1] += penalty
        grad = X.T @ (p - y) + penalty * w
        step = np.linalg.solve(H, grad)
        w -= step
        if float(np.max(np.abs(step))) < 1e-6:
            break
    return w


def _logloss(X: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    z = np.clip(X @ w, -20, 20)
    p = np.clip(1.0 / (1.0 + np.exp(-z)), 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _model(rows: list[dict]) -> dict:
    usable = [r for r in rows if r["gap_z"] == r["gap_z"]]
    train = [r for r in usable if r["date"] < MODEL_CUT]
    hold = [r for r in usable if r["date"] >= MODEL_CUT]
    if len(train) < 40 or len(hold) < 20:
        return {"train_n": len(train), "holdout_n": len(hold), "note": "too few gated legs around Sep 21"}
    disc = [r["discount"] for r in train if r.get("discount") is not None]
    disc_fill = float(np.median(disc)) if disc else 0.0

    def frame(group: list[dict], with_z: bool) -> tuple[np.ndarray, np.ndarray]:
        raw = []
        for r in group:
            missing = r.get("discount") is None
            raw.append(
                [
                    1.0,
                    r["l5"],
                    r["l10"],
                    r["line"],
                    disc_fill if missing else r["discount"],
                    1.0 if missing else 0.0,
                ]
                + ([r["gap_z"]] if with_z else [])
            )
        return np.asarray(raw, dtype=float), np.asarray([r["hit"] for r in group], dtype=float)

    def scale(train_x, hold_x):
        mu = train_x.mean(axis=0)
        sd = train_x.std(axis=0)
        sd[0] = 1.0
        mu[0] = 0.0
        sd[sd == 0] = 1.0
        return (train_x - mu) / sd, (hold_x - mu) / sd

    out = {"train_n": len(train), "holdout_n": len(hold), "cut": MODEL_CUT, "discount_rows": len(disc)}
    for name, with_z in (("without_gap_z", False), ("with_gap_z", True)):
        xt, yt = frame(train, with_z)
        xh, yh = frame(hold, with_z)
        xt, xh = scale(xt, xh)
        w = _logit(xt, yt)
        out[name] = {"train_log_loss": round(_logloss(xt, yt, w), 4), "holdout_log_loss": round(_logloss(xh, yh, w), 4)}
    out["holdout_log_loss_delta"] = round(
        out["without_gap_z"]["holdout_log_loss"] - out["with_gap_z"]["holdout_log_loss"], 4
    )
    out["gap_z_helps_holdout"] = out["holdout_log_loss_delta"] > 0
    return out


def _discount(rows: list[dict]) -> None:
    lines: dict[tuple, dict] = defaultdict(dict)
    for r in rows:
        lines[(r["date"], r["player"], r["prop"])][r["book"].split()[0]] = r["line"]
    for r in rows:
        pair = lines[(r["date"], r["player"], r["prop"])]
        if "Standard" in pair and "Goblin" in pair:
            r["discount"] = round(pair["Standard"] - pair["Goblin"], 2)
        else:
            r["discount"] = None


def _discount_cross(rows: list[dict]) -> list[dict]:
    goblin = [r for r in rows if r["book"] == "Goblin OVER"]

    def gap_band(gap: float) -> str:
        if gap < 0:
            return "avg under the line"
        if gap < 2:
            return "avg 0 to 2 over"
        return "avg 2+ over"

    def disc_band(v) -> str:
        if v is None:
            return "no standard line"
        if v < 2:
            return "discount under 2"
        if v < 4:
            return "discount 2 to 4"
        return "discount 4+"

    acc: dict[tuple, list] = defaultdict(lambda: [0, 0])
    for r in goblin:
        cell = acc[(r["sport"], r["prop"], gap_band(r["gap"]), disc_band(r["discount"]))]
        cell[0] += r["hit"]
        cell[1] += 1
    out = []
    for (sport, prop, gap, disc), (hits, n) in sorted(acc.items()):
        if n < 15:
            continue
        out.append(
            {
                "sport": sport,
                "prop": prop,
                "gap": gap,
                "discount": disc,
                "hits": hits,
                "n": n,
                "rate": round(hits / n, 4),
                "report_only": n < 30,
            }
        )
    return out


def _board_opponents() -> dict[tuple, str]:
    """(date, player) -> board opponent from tennis step8."""
    out: dict[tuple, str] = {}
    roots = [D.OUTS, ROOT / "outputs"]
    for root in roots:
        if not root.is_dir():
            continue
        for day in root.iterdir():
            if not day.is_dir() or len(day.name) != 10:
                continue
            files = list(day.glob("tennis/step8_tennis_direction.csv")) + list(day.glob("**/step8*tennis*direction*.csv"))
            for path in files[:1]:
                with path.open(encoding="utf-8-sig", newline="") as fh:
                    for row in __import__("csv").DictReader(fh):
                        player = norm_player(row.get("player") or row.get("player_name"))
                        opp = str(row.get("opp_team") or "").strip()
                        if player and opp:
                            out[(day.name, player)] = opp
    return out


def _sackmann_vs() -> dict[tuple, float]:
    """(player, opp last name, tourney date, prop) -> stat."""
    out: dict[tuple, float] = {}
    root = ROOT / "Sports" / "Tennis" / "data" / "sackmann"
    sys_path = __import__("sys")
    sys_path.path.insert(0, str(ROOT / "Sports" / "Tennis" / "scripts"))
    from tennis_shared import _parse_score_both_sides, norm_key

    import csv

    for name in ("atp_matches_combined.csv", "wta_matches_combined.csv"):
        path = root / name
        if not path.is_file():
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                day = str(row.get("tourney_date") or "")
                if len(day) == 8 and day.isdigit():
                    day = f"{day[:4]}-{day[4:6]}-{day[6:]}"
                if day < "2026-01-01":
                    continue
                parsed = _parse_score_both_sides(str(row.get("score") or ""))
                if not parsed:
                    continue
                pairs = (
                    (row.get("winner_name"), row.get("loser_name"), parsed["winner"], parsed["match_total_games"]),
                    (row.get("loser_name"), row.get("winner_name"), parsed["loser"], parsed["match_total_games"]),
                )
                for player, opp, side, total in pairs:
                    pk = norm_player(norm_key(str(player or "")))
                    opp_last = norm_player(str(opp or "")).split()[-1] if opp else ""
                    if not pk or not opp_last:
                        continue
                    gw = side.get("games_won")
                    if gw is not None:
                        out[(pk, opp_last, day, "games_won")] = float(gw)
                    if total is not None:
                        out[(pk, opp_last, day, "match_total_games")] = float(total)
    return out


def _corrected_tennis(rows: list[dict]) -> list[dict]:
    """Regrade tennis from the cached ESPN board opponent. A missing board drops the leg."""
    import json as _json
    from collections import defaultdict

    from _wrong_match_and_cap_effect import _iter_comps, _norm

    folder = ROOT / "data" / "cache" / "espn_tennis_scoreboard"
    board: dict[tuple, dict] = {}
    for path in folder.glob("*.json"):
        ymd = path.stem.split("_")[-1]
        if len(ymd) != 8 or not ymd.isdigit():
            continue
        day = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
        try:
            payload = _json.loads(path.read_text(encoding="utf-8"))
        except (OSError, _json.JSONDecodeError):
            continue
        found: dict[str, list] = defaultdict(list)
        for _ev, comp in _iter_comps(payload):
            comps = list(comp.get("competitors") or [])
            if len(comps) < 2:
                continue
            status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
            retired = any(tok in status.upper() for tok in ("RET", "ABANDON", "WALKOVER", "W/O"))
            for i, c in enumerate(comps):
                name = str(((c.get("athlete") or {}).get("displayName") or "")).strip()
                if not name:
                    continue
                lines = c.get("linescores") or []
                games = sum(float((x or {}).get("value") or 0) for x in lines)
                other = comps[1 - i]
                opp_games = sum(float((x or {}).get("value") or 0) for x in (other.get("linescores") or []))
                found[norm_player(name)].append(
                    {"games": games, "total": games + opp_games, "retired": retired, "status": status}
                )
        for player, matches in found.items():
            done = [m for m in matches if not m["retired"] and m["games"] > 0]
            if done:
                board[(day, player)] = done[-1]
    kept = []
    for r in rows:
        if r["sport"] != "Tennis" or r["prop"] not in {"games_won", "match_total_games"}:
            continue
        match = board.get((r["date"], r["player"]))
        if match is None:
            flipped = " ".join(reversed(r["player"].split()))
            match = board.get((r["date"], flipped))
        if match is None or match["retired"]:
            continue
        stat = match["games"] if r["prop"] == "games_won" else match["total"]
        if r["side"] == "OVER":
            hit = stat > r["line"]
        else:
            hit = stat < r["line"]
        kept.append({**r, "hit": 1 if hit else 0})
    return kept


def _flat(records: list[dict], kind: str) -> list[dict]:
    rows = []
    for rec in records:
        base = {k: rec[k] for k in ("sport", "prop", "book") if k in rec}
        base["table"] = kind
        for band in rec.get("bands") or []:
            rows.append({**base, **{k: band[k] for k in band if k != "wilson"}, "wilson_lo": (band.get("wilson") or [None, None])[0], "wilson_hi": (band.get("wilson") or [None, None])[1]})
    return rows


RANK_JSON = ROOT / "data" / "reports" / "gap_rank_test.json"
HOLDOUT_START = "2026-09-21"
TOP_K = 6
MIN_LEGS = 12
MIN_GAMES = 4
RANDOM_DRAWS = 1000


def _cell_wilson_low(row: dict, index: dict) -> float:
    if row["sport"] == "MLB" and row["book"].startswith("Goblin"):
        parent = pitcher_l10_parent_cell(row["prop"])
        if parent:
            band = wilson_band(int(parent["hit"]), int(parent["n"]))
            if band:
                return band[0]
    found = lookup_category_hr(
        index,
        sport=row["sport"],
        prop=row["prop"],
        pick_type=row["book"].split()[0],
        direction=row["side"],
    )
    if not found or not found.get("n"):
        return -1.0
    band = wilson_band(int(found["hit"]), int(found["n"]))
    return band[0] if band else -1.0


def _pick_top(legs: list[dict], score, k: int = TOP_K) -> list[dict]:
    ordered = sorted(legs, key=score, reverse=True)
    chosen: list[dict] = []
    seen_player: set[str] = set()
    for leg in ordered:
        if leg["player"] in seen_player:
            continue
        chosen.append(leg)
        seen_player.add(leg["player"])
        if len(chosen) >= k:
            break
    return chosen


def _distinct_slip(chosen: list[dict], k: int) -> list[dict]:
    out: list[dict] = []
    games: set[str] = set()
    for leg in chosen:
        game = leg.get("game") or ""
        if game and game in games:
            continue
        out.append(leg)
        if game:
            games.add(game)
        if len(out) >= k:
            break
    return out


def _rate(legs: list[dict]) -> float | None:
    if not legs:
        return None
    return sum(leg["hit"] for leg in legs) / len(legs)


def _boot(values: list[float], draws: int = 2000) -> list[float] | None:
    if len(values) < 2:
        return None
    rng = np.random.default_rng(0)
    arr = np.asarray(values, dtype=float)
    means = np.empty(draws)
    n = len(arr)
    for i in range(draws):
        means[i] = float(arr[rng.integers(0, n, n)].mean())
    means.sort()
    return [round(float(means[int(0.025 * (draws - 1))]), 4), round(float(means[int(0.975 * (draws - 1))]), 4)]


def _rank_one_sport(sport: str, rows: list[dict], index: dict) -> dict:
    by_day: dict[str, list] = defaultdict(list)
    for row in rows:
        if row["sport"] == sport and row["date"] >= HOLDOUT_START:
            by_day[row["date"]].append(row)
    census = []
    kept_days = []
    for day, legs in sorted(by_day.items()):
        games = {leg["game"] for leg in legs if leg.get("game")}
        census.append({"date": day, "gated_legs": len(legs), "games": len(games)})
        if len(legs) >= MIN_LEGS and len(games) >= MIN_GAMES:
            kept_days.append((day, legs))
    rules = ("gap_z", "cell_wilson_low", "cell_then_gap", "random")
    day_rows = []
    rng = np.random.default_rng(0)
    for day, legs in kept_days:
        scored = []
        for leg in legs:
            scored.append({**leg, "cell_lo": _cell_wilson_low(leg, index)})
        picks = {
            "gap_z": _pick_top(scored, lambda r: r["gap_z"] if r["gap_z"] == r["gap_z"] else -999.0),
            "cell_wilson_low": _pick_top(scored, lambda r: r["cell_lo"]),
            "cell_then_gap": _pick_top(scored, lambda r: (r["cell_lo"], r["gap_z"] if r["gap_z"] == r["gap_z"] else -999.0)),
        }
        random_rates = []
        random_slip2 = []
        random_slip3 = []
        for _ in range(RANDOM_DRAWS):
            keys = {id(leg): float(rng.random()) for leg in scored}
            drawn = _pick_top(scored, lambda r, keys=keys: keys[id(r)])
            random_rates.append(_rate(drawn))
            s2 = _distinct_slip(drawn, 2)
            s3 = _distinct_slip(drawn, 3)
            random_slip2.append(1.0 if len(s2) == 2 and all(x["hit"] for x in s2) else 0.0)
            random_slip3.append(1.0 if len(s3) == 3 and all(x["hit"] for x in s3) else 0.0)
        rec = {"date": day, "pool": len(legs), "games": len({x["game"] for x in legs if x.get("game")})}
        for name, chosen in picks.items():
            slip2 = _distinct_slip(chosen, 2)
            slip3 = _distinct_slip(chosen, 3)
            rec[name] = {
                "n": len(chosen),
                "hits": sum(x["hit"] for x in chosen),
                "rate": _rate(chosen),
                "slip2_cash": 1 if len(slip2) == 2 and all(x["hit"] for x in slip2) else 0,
                "slip3_cash": 1 if len(slip3) == 3 and all(x["hit"] for x in slip3) else 0,
                "slip2_n": len(slip2),
                "slip3_n": len(slip3),
            }
        rec["random_rate"] = float(np.mean(random_rates)) if random_rates else None
        rec["random_slip2"] = float(np.mean(random_slip2)) if random_slip2 else None
        rec["random_slip3"] = float(np.mean(random_slip3)) if random_slip3 else None
        day_rows.append(rec)

    def pooled(name: str) -> dict:
        hits = sum(d[name]["hits"] for d in day_rows)
        n = sum(d[name]["n"] for d in day_rows)
        day_rates = [d[name]["rate"] for d in day_rows if d[name]["rate"] is not None]
        return {
            "legs": f"{hits}/{n}" if n else "0/0",
            "leg_rate": None if n == 0 else round(hits / n, 4),
            "day_rate": None if not day_rates else round(float(np.mean(day_rates)), 4),
            "slip2_cash": f"{sum(d[name]['slip2_cash'] for d in day_rows)}/{len(day_rows)}",
            "slip3_cash": f"{sum(d[name]['slip3_cash'] for d in day_rows)}/{len(day_rows)}",
        }

    summary = {name: pooled(name) for name in ("gap_z", "cell_wilson_low", "cell_then_gap")}
    if day_rows:
        summary["random"] = {
            "leg_rate": round(float(np.mean([d["random_rate"] for d in day_rows])), 4),
            "day_rate": round(float(np.mean([d["random_rate"] for d in day_rows])), 4),
            "slip2_cash_rate": round(float(np.mean([d["random_slip2"] for d in day_rows])), 4),
            "slip3_cash_rate": round(float(np.mean([d["random_slip3"] for d in day_rows])), 4),
        }
    gap_minus_random = [
        d["gap_z"]["rate"] - d["random_rate"]
        for d in day_rows
        if d["gap_z"]["rate"] is not None and d["random_rate"] is not None
    ]
    gap_minus_cell = [
        d["gap_z"]["rate"] - d["cell_wilson_low"]["rate"]
        for d in day_rows
        if d["gap_z"]["rate"] is not None and d["cell_wilson_low"]["rate"] is not None
    ]
    band_random = _boot(gap_minus_random)
    band_cell = _boot(gap_minus_cell)
    beats_random = bool(band_random and band_random[0] > 0)
    loses_to_cell = bool(band_cell and band_cell[1] < 0)
    decision = "unused"
    status = "FAIL"
    if len(day_rows) < 2:
        status = "OPEN"
        decision = "too few qualifying days"
    elif beats_random and not loses_to_cell:
        status = "PASS"
        decision = "gap_z may break an exact tie"
    else:
        decision = "leave gap_z unused"
    return {
        "id": f"gap_rank_{sport.lower()}",
        "sport": sport,
        "status": status,
        "decision": decision,
        "qualifying_days": len(day_rows),
        "holdout_days": len(census),
        "census": census,
        "rules": summary,
        "gap_minus_random_band": band_random,
        "gap_minus_cell_band": band_cell,
        "beats_random": beats_random,
        "loses_to_cell": loses_to_cell,
        "days": day_rows,
    }


def rank_test(gated: list[dict]) -> dict:
    index = load_category_hr_index()
    sports = sorted({r["sport"] for r in gated if r["date"] >= HOLDOUT_START})
    by_sport = [_rank_one_sport(sport, gated, index) for sport in sports]
    return {
        "holdout": f"dates on or after {HOLDOUT_START}",
        "keep_day": f"at least {MIN_LEGS} gated legs and {MIN_GAMES} games",
        "pick": "top 6, one leg per player",
        "random_draws": RANDOM_DRAWS,
        "sports": by_sport,
    }


def main() -> None:
    rows = collect()
    _discount(rows)
    for r in rows:
        r["gated"] = _passes(r.pop("src"), r["sport"], r["prop"], r["book"], r["side"], r["line"], r["l5"], r["l10"], r["player"])
    gated = [r for r in rows if r["gated"]]
    print("rows", len(rows), "gated", len(gated), flush=True)
    print("rank test", flush=True)
    ranked = rank_test(gated)
    RANK_JSON.write_text(json.dumps(ranked, indent=2), encoding="utf-8")
    for sport in ranked["sports"]:
        rules = sport["rules"]
        gap = (rules.get("gap_z") or {}).get("leg_rate")
        cell = (rules.get("cell_wilson_low") or {}).get("leg_rate")
        rnd = (rules.get("random") or {}).get("leg_rate")
        print(
            f"{sport['id']} | {sport['status']} | gap {gap} cell {cell} random {rnd} | "
            f"days {sport['qualifying_days']} | {sport['decision']}",
            flush=True,
        )
    unders = [r for r in rows if r["book"] == "Standard UNDER"]
    for r in unders:
        r["flipped"] = -r["gap"]
    print("tennis corrected attaches", flush=True)
    tennis = _corrected_tennis(rows)
    payload = {
        "wired": False,
        "gate": "directional L5=5, directional L10>=8, and ticket_gate_passes (sport keep, D where that sport requires it)",
        "gap": "last-10 average minus the posted line. Positive means the average sits above the prop line.",
        "standard_under": "flipped gap is line minus average, so a positive number means the average is under the line",
        "eligible_with_a_window": len(rows),
        "gated_legs": len(gated),
        "gated_by_sport": {
            sport: sum(1 for r in gated if r["sport"] == sport)
            for sport in sorted({r["sport"] for r in gated})
        },
        "gated_gap_bins": _by_prop(gated),
        "gated_cushion_z_holdout": _holdout_terciles(gated),
        "model": _model(gated),
        "discount_cross": _discount_cross([r for r in rows if r["book"] == "Goblin OVER"]),
        "tennis_board_opponent": {
            "legs": len(tennis),
            "games_won_goblin_over": _rate_cell(tennis, "games_won", "Goblin OVER"),
            "total_games_goblin_over": _rate_cell(tennis, "match_total_games", "Goblin OVER"),
            "bins": _by_prop([r for r in tennis if r["book"].endswith("OVER")]),
        },
        "standard_under_flipped": _by_prop(unders, "gap"),
        "nfl_cfb": "Dated NFL and CFB boards still have no last-10 current-season list, so those sports are absent.",
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xw:
        pd.DataFrame(_flat(payload["gated_gap_bins"], "gated gap")).to_excel(xw, sheet_name="Gated gap", index=False)
        zrows = []
        for cell in payload["gated_cushion_z_holdout"]:
            for b in cell["buckets"]:
                zrows.append(
                    {
                        "sport": cell["sport"],
                        "prop": cell["prop"],
                        "book": cell["book"],
                        "cut": cell["cut"],
                        "bucket": b["bucket"],
                        "hits": b["hits"],
                        "n": b["n"],
                        "rate": b["rate"],
                        "report_only": b["report_only"],
                        "rising": cell["rising"],
                        "beats_shuffle": cell["beats_shuffle"],
                        "shuffle_p95": cell["shuffle_p95"],
                    }
                )
        pd.DataFrame(zrows).to_excel(xw, sheet_name="Gated cushion_z", index=False)
        pd.DataFrame([payload["model"]]).to_excel(xw, sheet_name="Model", index=False)
        pd.DataFrame(payload["discount_cross"]).to_excel(xw, sheet_name="Gap x discount", index=False)
        pd.DataFrame(_flat(payload["tennis_board_opponent"]["bins"], "tennis corrected")).to_excel(
            xw, sheet_name="Tennis board opponent", index=False
        )
        pd.DataFrame(_flat(payload["standard_under_flipped"], "under flipped")).to_excel(
            xw, sheet_name="Standard under flipped", index=False
        )
    print("wrote", OUT_XLSX, flush=True)
    print("model", payload["model"], flush=True)


def _rate_cell(rows: list[dict], prop: str, book: str) -> dict:
    group = [r for r in rows if r["prop"] == prop and r["book"] == book]
    hits = sum(r["hit"] for r in group)
    n = len(group)
    return {"hits": hits, "n": n, "rate": None if n == 0 else round(hits / n, 4)}


if __name__ == "__main__":
    main()
