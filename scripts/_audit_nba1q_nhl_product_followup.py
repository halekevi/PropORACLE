"""Follow-up product audit: NBA1Q Reb/Ast concentration+leakage; NHL UNDER 0.5/role/1-game.

Writes data/reports/audit_nba1q_nhl_product_followup.{json,md}
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
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
from all_ranked_prop_hit_rates import l10_dir, l5_dir  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.dnp_zero_void import is_dnp_zero_actual  # noqa: E402
from utils.pp_vegas_gap import is_grading_suspect  # noqa: E402

OUT_JSON = ROOT / "data" / "reports" / "audit_nba1q_nhl_product_followup.json"
OUT_MD = ROOT / "data" / "reports" / "audit_nba1q_nhl_product_followup.md"
DB = ROOT / "data" / "cache" / "proporacle_ref.db"
GRADED_DIRS = [
    ROOT.parent / "PropORACLE_main_cp" / "ui_runner" / "templates",
    ROOT / "ui_runner" / "templates",
]


def _ci(hits: int, n: int) -> dict[str, Any]:
    hr = (hits / n) if n else None
    band = wilson_band(hits, n) if n else None
    return {
        "hits": hits,
        "n": n,
        "hit_rate": round(hr, 4) if hr is not None else None,
        "wilson_low": round(band[0], 4) if band else None,
        "wilson_high": round(band[1], 4) if band else None,
    }


def _fmt(c: dict | None) -> str:
    if not c or not c.get("n"):
        return "n=0"
    return (
        f"{100 * c['hit_rate']:.1f}% ({c['hits']}/{c['n']}) "
        f"Wilson[{100 * c['wilson_low']:.1f}–{100 * c['wilson_high']:.1f}]"
    )


def _prop_key(s: object) -> str:
    return str(s or "").strip().lower().replace("_", " ")


def _norm_player(s: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _num(v: object) -> float | None:
    try:
        if v is None or str(v).strip() in {"", "—", "-", "nan", "None"}:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _actual(r: dict) -> float | None:
    for k in ("actual", "actual_value", "stat_value"):
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _iter_graded() -> list[tuple[str, list[dict]]]:
    seen: set[str] = set()
    out: list[tuple[str, list[dict]]] = []
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for path in sorted(d.glob("graded_props_*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            if date in seen:
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            chunk = raw.get("props", raw.get("rows", []))
            if not isinstance(chunk, list):
                continue
            seen.add(date)
            out.append((date, [r for r in chunk if isinstance(r, dict)]))
    return out


class NbaFgIndex:
    def __init__(self) -> None:
        self.by_player: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not DB.is_file():
            return
        con = sqlite3.connect(str(DB))
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
            use = [c for c in ("game_date", "player", "pts", "reb", "ast", "minutes") if c in cols]
            if len(use) < 4:
                return
            for row in con.execute(f"SELECT {', '.join(use)} FROM nba"):
                gd = str(row[0] or "")[:10]
                if len(gd) != 10:
                    continue
                pn = _norm_player(row[1])
                vals: dict[str, float] = {}
                for i, c in enumerate(use[2:], start=2):
                    try:
                        vals[c] = float(row[i])
                    except (TypeError, ValueError):
                        continue
                self.by_player[pn].append((gd, vals))
        except sqlite3.Error:
            return
        finally:
            con.close()
        for pn in self.by_player:
            self.by_player[pn].sort(key=lambda x: x[0])

    def fg_on(self, player: str, game_date: str) -> dict[str, float] | None:
        for gd, vals in self.by_player.get(_norm_player(player), []):
            if gd == game_date:
                return vals
        return None

    def games(self, player: str) -> list[tuple[str, dict[str, float]]]:
        return self.by_player.get(_norm_player(player), [])


class NhlIndex:
    def __init__(self) -> None:
        self.by_player: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not DB.is_file():
            return
        con = sqlite3.connect(str(DB))
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nhl)")}
            use = [
                c
                for c in (
                    "game_date",
                    "player",
                    "points",
                    "assists",
                    "goals",
                    "toi",
                    "shots_on_goal",
                )
                if c in cols
            ]
            if "game_date" not in use or "player" not in use:
                return
            for row in con.execute(f"SELECT {', '.join(use)} FROM nhl"):
                gd = str(row[0] or "")[:10]
                if len(gd) != 10:
                    continue
                pn = _norm_player(row[1])
                vals: dict[str, float] = {}
                for i, c in enumerate(use[2:], start=2):
                    try:
                        vals[c] = float(row[i])
                    except (TypeError, ValueError):
                        continue
                self.by_player[pn].append((gd, vals))
        except sqlite3.Error:
            return
        finally:
            con.close()
        for pn in self.by_player:
            self.by_player[pn].sort(key=lambda x: x[0])

    def games(self, player: str) -> list[tuple[str, dict[str, float]]]:
        return self.by_player.get(_norm_player(player), [])


def _leakage_l5(
    games: list[tuple[str, dict[str, float]]],
    *,
    cutoff: str,
    col: str,
    line: float,
    direction: str,
) -> int | None:
    prior = [(gd, vals) for gd, vals in games if gd < cutoff and col in vals]
    if len(prior) < 5:
        return None
    window = prior[-5:]
    hits = 0
    for _, vals in window:
        v = vals[col]
        if direction == "OVER" and v > line:
            hits += 1
        elif direction == "UNDER" and v < line:
            hits += 1
    return hits


def _role_tier_nhl(games: list[tuple[str, dict[str, float]]], game_date: str) -> str:
    """Crude role from recent TOI / points rate (no roster table)."""
    prior = [(gd, v) for gd, v in games if gd < game_date]
    if len(prior) < 3:
        return "unknown"
    window = prior[-10:]
    tois = [v.get("toi") for _, v in window if v.get("toi") is not None]
    pts = [v.get("points", 0.0) for _, v in window]
    if not tois:
        return "unknown"
    avg_toi = sum(tois) / len(tois)
    # TOI often stored as minutes float
    if avg_toi >= 18:
        return "starter"
    if avg_toi >= 12:
        return "middle"
    return "depth"


def analyze_nba1q(nba: NbaFgIndex) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for prop, col in (("rebounds", "reb"), ("assists", "ast")):
        legs: list[dict[str, Any]] = []
        join = Counter()
        leak = Counter()
        for file_date, chunk in _iter_graded():
            for r in chunk:
                if _norm_sport(r.get("sport")) != "NBA1Q":
                    continue
                if _norm_pick(r.get("pick_type")) != "goblin":
                    continue
                if _norm_dir(r.get("direction") or r.get("over_under")) != "OVER":
                    continue
                if _prop_key(r.get("prop")) != prop:
                    continue
                l5 = l5_dir(r, "OVER")
                if l5 is None or int(l5) != 5:
                    continue
                hit = _parse_hit(str(r.get("result") or ""))
                if hit is None:
                    continue
                if is_grading_suspect(r):
                    continue
                actual = _actual(r)
                if is_dnp_zero_actual(actual, r):
                    continue
                game_date = str(r.get("game_date") or "")[:10]
                if len(game_date) != 10:
                    game_date = file_date
                player = str(r.get("player") or "").strip()
                line = _num(r.get("line"))

                # FG join
                fg = nba.fg_on(player, game_date)
                if fg is None or col not in fg or actual is None:
                    join_status = "unchecked"
                elif abs(fg[col] - actual) < 1e-9:
                    join_status = "fg_match_drop"
                elif actual > fg[col] + 1e-9:
                    join_status = "actual_gt_fg_drop"
                else:
                    join_status = "plausible_1q"
                join[join_status] += 1
                if join_status in {"fg_match_drop", "actual_gt_fg_drop"}:
                    continue

                # Leakage on FULL-GAME boxes vs 1Q line (proxy — same caveat as unchecked)
                leak_status = "unchecked"
                if line is not None:
                    recomputed = _leakage_l5(
                        nba.games(player),
                        cutoff=game_date,
                        col=col,
                        line=line,
                        direction="OVER",
                    )
                    if recomputed is None:
                        leak_status = "unchecked"
                    elif recomputed == 5:
                        leak_status = "pass"
                    else:
                        leak_status = "fail"
                leak[leak_status] += 1
                if leak_status == "fail":
                    continue

                legs.append(
                    {
                        "player": player,
                        "date": game_date,
                        "line": line,
                        "actual": actual,
                        "hit": int(hit),
                        "join": join_status,
                        "leak": leak_status,
                    }
                )

        n = len(legs)
        hits = sum(l["hit"] for l in legs)
        players = Counter(l["player"] for l in legs)
        dates = Counter(l["date"] for l in legs)
        top10 = players.most_common(10)
        top10_n = sum(c for _, c in top10)
        join_n = sum(join.values())
        out[prop] = {
            "pool": "Goblin OVER L5=5, drop FG-match/suspect/DNP, require leakage pass|unchecked kept? NO — fail dropped, unchecked kept",
            "note_leakage": (
                "Leakage uses FULL-GAME nba boxes vs 1Q lines (proxy). "
                "Fail drops; unchecked kept but reported separately."
            ),
            "join_counts": dict(join),
            "join_unchecked_share": round(join["unchecked"] / join_n, 4) if join_n else None,
            "join_checked_n": join["plausible_1q"] + join["fg_match_drop"] + join["actual_gt_fg_drop"],
            "join_total_l5eq5": join_n,
            "leak_counts": dict(leak),
            "after_leak_fail_drop": _ci(hits, n),
            "after_leak_pass_only": _ci(
                sum(l["hit"] for l in legs if l["leak"] == "pass"),
                sum(1 for l in legs if l["leak"] == "pass"),
            ),
            "distinct_players": len(players),
            "distinct_dates": len(dates),
            "top10_players": [{"player": p, "n": c, "share": round(c / n, 4) if n else None} for p, c in top10],
            "top10_share": round(top10_n / n, 4) if n else None,
            "max_player_share": round(top10[0][1] / n, 4) if n and top10 else None,
            "dates_top5": dates.most_common(5),
        }
    return out


def analyze_nhl(nhl: NhlIndex) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for prop, col in (("points", "points"), ("assists", "assists")):
        legs: list[dict[str, Any]] = []
        for file_date, chunk in _iter_graded():
            for r in chunk:
                if _norm_sport(r.get("sport")) != "NHL":
                    continue
                if _norm_pick(r.get("pick_type")) != "standard":
                    continue
                if _norm_dir(r.get("direction") or r.get("over_under")) != "UNDER":
                    continue
                if _prop_key(r.get("prop")) != prop:
                    continue
                l5 = l5_dir(r, "UNDER")
                l10 = l10_dir(r, "UNDER")
                if l5 is None or int(l5) != 5:
                    continue
                if l10 is None or float(l10) < 8:
                    continue
                hit = _parse_hit(str(r.get("result") or ""))
                if hit is None:
                    continue
                actual = _actual(r)
                if is_dnp_zero_actual(actual, r):
                    continue
                line = _num(r.get("line"))
                game_date = str(r.get("game_date") or "")[:10]
                if len(game_date) != 10:
                    game_date = file_date
                player = str(r.get("player") or "").strip()
                games = nhl.games(player)
                # leakage
                if line is not None and games:
                    recomputed = _leakage_l5(
                        games,
                        cutoff=game_date,
                        col=col,
                        line=line,
                        direction="UNDER",
                    )
                    if recomputed is not None and recomputed != 5:
                        continue
                role = _role_tier_nhl(games, game_date)
                # game key: prefer game_id else date+teams
                gid = str(r.get("game_id") or r.get("espn_game_id") or "")
                if not gid:
                    team = str(r.get("team") or "")
                    opp = str(r.get("opponent") or r.get("opp_team") or "")
                    gid = f"{game_date}|{'-'.join(sorted([team, opp]))}"
                legs.append(
                    {
                        "player": player,
                        "date": game_date,
                        "game_id": gid,
                        "line": line,
                        "is_half": bool(line is not None and abs(line - 0.5) < 1e-9),
                        "role": role,
                        "hit": int(hit),
                    }
                )

        n = len(legs)
        hits = sum(l["hit"] for l in legs)
        half = [l for l in legs if l["is_half"]]
        nonhalf = [l for l in legs if not l["is_half"]]
        by_role: dict[str, list] = defaultdict(list)
        for l in legs:
            by_role[l["role"]].append(l)

        # 1 per game: keep highest-confidence? use first hit preference — prefer HIT then any
        # Deterministic: sort by (hit desc, player) and take first per game_id
        by_game: dict[str, list] = defaultdict(list)
        for l in legs:
            by_game[l["game_id"]].append(l)
        capped: list[dict] = []
        for gid, group in by_game.items():
            group_sorted = sorted(group, key=lambda x: (-x["hit"], x["player"]))
            capped.append(group_sorted[0])

        # also: random-ish alternate = first player alpha per game
        capped_alpha: list[dict] = []
        for gid, group in by_game.items():
            capped_alpha.append(sorted(group, key=lambda x: x["player"])[0])

        out[prop] = {
            "gate": "Standard UNDER L5=5 + L10>=8 + leakage pass (fail dropped)",
            "uncapped": _ci(hits, n),
            "line_0_5": {
                "n": len(half),
                "share": round(len(half) / n, 4) if n else None,
                "cell": _ci(sum(l["hit"] for l in half), len(half)),
            },
            "line_not_0_5": {
                "n": len(nonhalf),
                "share": round(len(nonhalf) / n, 4) if n else None,
                "cell": _ci(sum(l["hit"] for l in nonhalf), len(nonhalf)),
            },
            "by_role": {
                role: {
                    "n": len(rows),
                    "share": round(len(rows) / n, 4) if n else None,
                    "cell": _ci(sum(r["hit"] for r in rows), len(rows)),
                }
                for role, rows in sorted(by_role.items(), key=lambda x: -len(x[1]))
            },
            "one_per_game_prefer_hit": _ci(
                sum(l["hit"] for l in capped), len(capped)
            ),
            "one_per_game_alpha": _ci(
                sum(l["hit"] for l in capped_alpha), len(capped_alpha)
            ),
            "games_with_multi_legs": sum(1 for g in by_game.values() if len(g) > 1),
            "avg_legs_per_game": round(n / len(by_game), 3) if by_game else None,
            "rule4_note": (
                "If rule 4 (no 0.5 on role/depth) applies to NHL UNDER, "
                f"dropping depth+0.5 removes a large share — see by_role and line_0_5."
            ),
        }
        # depth ∩ 0.5
        depth_half = [l for l in legs if l["is_half"] and l["role"] == "depth"]
        out[prop]["depth_and_0_5"] = {
            "n": len(depth_half),
            "share": round(len(depth_half) / n, 4) if n else None,
            "cell": _ci(sum(l["hit"] for l in depth_half), len(depth_half)),
        }
        starter_half = [l for l in legs if l["is_half"] and l["role"] == "starter"]
        out[prop]["starter_and_0_5"] = {
            "n": len(starter_half),
            "share": round(len(starter_half) / n, 4) if n else None,
            "cell": _ci(sum(l["hit"] for l in starter_half), len(starter_half)),
        }
    return out


def write_md(payload: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# NBA1Q / NHL product follow-up")
    lines.append("")
    lines.append("## NBA1Q Goblin OVER Rebounds / Assists (L5=5)")
    lines.append("")
    for prop, r in payload["nba1q"].items():
        lines.append(f"### {prop}")
        jn = r["join_total_l5eq5"]
        unchecked = r["join_counts"].get("unchecked", 0)
        lines.append(
            f"- **FG join unchecked:** {unchecked}/{jn} = "
            f"**{100 * (r['join_unchecked_share'] or 0):.1f}%** "
            f"(checked={r['join_checked_n']}; plausible_1q={r['join_counts'].get('plausible_1q', 0)}; "
            f"fg_match_drop={r['join_counts'].get('fg_match_drop', 0)})"
        )
        lines.append(f"- Leakage counts: `{r['leak_counts']}` — {r['note_leakage']}")
        lines.append(f"- After drop leakage-fail (unchecked kept): **{_fmt(r['after_leak_fail_drop'])}**")
        lines.append(f"- Leakage-pass only: **{_fmt(r['after_leak_pass_only'])}**")
        lines.append(
            f"- Distinct players={r['distinct_players']}, dates={r['distinct_dates']}; "
            f"top-10 share=**{100 * (r['top10_share'] or 0):.1f}%**; "
            f"max player=**{100 * (r['max_player_share'] or 0):.1f}%**"
        )
        lines.append("- Top-10 players:")
        for p in r["top10_players"]:
            lines.append(f"  - {p['player']}: {p['n']} ({100 * (p['share'] or 0):.1f}%)")
        lines.append("")
    lines.append("## NHL Standard UNDER Points / Assists (L5=5+L10≥8)")
    lines.append("")
    for prop, r in payload["nhl"].items():
        lines.append(f"### {prop}")
        lines.append(f"- Uncapped: **{_fmt(r['uncapped'])}**")
        lines.append(
            f"- Line 0.5: {r['line_0_5']['n']} ({100 * (r['line_0_5']['share'] or 0):.1f}%) "
            f"→ {_fmt(r['line_0_5']['cell'])}"
        )
        lines.append(
            f"- Line ≠0.5: {r['line_not_0_5']['n']} ({100 * (r['line_not_0_5']['share'] or 0):.1f}%) "
            f"→ {_fmt(r['line_not_0_5']['cell'])}"
        )
        lines.append("- By role (TOI proxy):")
        for role, cell in r["by_role"].items():
            lines.append(
                f"  - {role}: n={cell['n']} ({100 * (cell['share'] or 0):.1f}%) → {_fmt(cell['cell'])}"
            )
        lines.append(
            f"- Depth ∩ 0.5: n={r['depth_and_0_5']['n']} "
            f"({100 * (r['depth_and_0_5']['share'] or 0):.1f}%) → {_fmt(r['depth_and_0_5']['cell'])}"
        )
        lines.append(
            f"- Starter ∩ 0.5: n={r['starter_and_0_5']['n']} "
            f"({100 * (r['starter_and_0_5']['share'] or 0):.1f}%) → {_fmt(r['starter_and_0_5']['cell'])}"
        )
        lines.append(
            f"- One leg/game (prefer HIT): **{_fmt(r['one_per_game_prefer_hit'])}** "
            f"| alpha pick: **{_fmt(r['one_per_game_alpha'])}** "
            f"(multi-leg games={r['games_with_multi_legs']}, avg legs/game={r['avg_legs_per_game']})"
        )
        lines.append(f"- Rule 4: {r['rule4_note']}")
        lines.append("")
    lines.append("## Product decision notes")
    lines.append("")
    lines.append(
        "- Cap NHL UNDER at **one leg per game** on slips (correlation)."
    )
    lines.append(
        "- Decide rule 4 for NHL UNDER 0.5 depth: if banned, product thins heavily — "
        "see depth∩0.5 share."
    )
    lines.append(
        "- NBA1Q: if unchecked FG join is majority, do not promote until period boxes join."
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    print("Loading indexes…")
    nba = NbaFgIndex()
    nhl = NhlIndex()
    print("NBA1Q analysis…")
    nba1q = analyze_nba1q(nba)
    print("NHL analysis…")
    nhl_out = analyze_nhl(nhl)
    payload = {"nba1q": nba1q, "nhl": nhl_out}
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_md(payload)
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_MD}")
    for prop, r in nba1q.items():
        print(
            f"NBA1Q {prop}: unchecked={r['join_unchecked_share']} "
            f"after={_fmt(r['after_leak_fail_drop'])} "
            f"players={r['distinct_players']} top10={r['top10_share']}"
        )
    for prop, r in nhl_out.items():
        print(
            f"NHL {prop}: uncapped={_fmt(r['uncapped'])} "
            f"0.5share={r['line_0_5']['share']} "
            f"1/game={_fmt(r['one_per_game_prefer_hit'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
