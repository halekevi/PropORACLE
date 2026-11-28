"""NBA gate search with every factor: L5/L10 hits, L5 avg, L10 avg, season avg, team and opponent prop ranks,
minutes trend, and matchup pace. Observe-only — reports, not tickets.

Filters (all directional; OVER favorable is the mirror of UNDER favorable):
  L5 hits       any | >=4 | =5
  L10 hits      any | >=7 | >=8 | >=9
  L5 avg        any | right side of line | >=10% past line
  L10 avg       any | right side of line | >=10% past line
  season avg    any | right side | >=5% | >=10%       (season gap to line)
  team rank     any | favorable | not unfavorable     (player's team per-game rank for that stat)
  opp rank      any | favorable | not unfavorable     (opponent allowed per-game rank for that stat)
  minutes       any | not down 2+ | down 2+           (L5 minutes vs season minutes)
  pace          any | favorable | not unfavorable     (avg possessions/g of both teams, as of the slate;
                                                       fast/slow = top/bottom third on the train slates)

Pick per (book, prop, side): highest Wilson lower bound on train (slates <= TRAIN_END) with n >= MIN_N.
Holdout (later slates) is the honest rate. Also writes a one-factor table: hit rate per factor level per prop.

  py -3.14 scripts/_nba_max_gates_full.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))
from _nba_matchup_tier_backtest import team_games, wilson_lo  # noqa: E402
from _nba_max_gates import BREAK_EVEN, FAV_DEF, FAV_OFF, MIN_HOLD_N, MIN_N, PROP_DISPLAY, TRAIN_END, load_legs  # noqa: E402

REPORT = ROOT / "data" / "reports" / "nba_max_gates_full.json"
REPORT_MD = ROOT / "data" / "reports" / "nba_max_gates_full.md"
PLAYER_CSV = ROOT / "data" / "reports" / "nba_player_prop_ranks.csv"
PLAYER_MD = ROOT / "data" / "reports" / "nba_player_prop_ranks.md"
PLAYER_MIN_NIGHTS = 5
PLAYER_TOP = 10

LEVELS = {
    "l5": ["any", ">=4", "=5"],
    "l10": ["any", ">=7", ">=8", ">=9"],
    "l5avg": ["any", "right side", ">=10%"],
    "l10avg": ["any", "right side", ">=10%"],
    "season": ["any", "right side", ">=5%", ">=10%"],
    "off": ["any", "favorable", "not unfavorable"],
    "def": ["any", "favorable", "not unfavorable"],
    "min": ["any", "not down 2+", "down 2+"],
    "pace": ["any", "favorable", "not unfavorable"],
}
LABEL = {
    "l5": "L5 hits", "l10": "L10 hits", "l5avg": "L5 avg", "l10avg": "L10 avg", "season": "season avg",
    "off": "team rank", "def": "opp rank", "min": "minutes", "pace": "pace",
}


def pace_asof(slates: list[str]) -> pd.DataFrame:
    g = team_games()
    out = []
    for s in slates:
        p = g[g.game_date < s].groupby("team").agg(n=("poss", "size"), poss=("poss", "mean"))
        p = p[p.n >= 3]
        if len(p) < 20:
            continue
        out.append(pd.DataFrame({"slate": s, "team": p.index, "pace": p.poss.values}))
    return pd.concat(out, ignore_index=True)


def attach_pace(legs: pd.DataFrame, pace: pd.DataFrame) -> pd.DataFrame:
    own = pace.rename(columns={"team": "team_n", "pace": "pace_own"})
    opp = pace.rename(columns={"team": "opp_n", "pace": "pace_opp"})
    legs = legs.merge(own, on=["slate", "team_n"], how="inner").merge(opp, on=["slate", "opp_n"], how="inner")
    legs["pace_match"] = (legs.pace_own + legs.pace_opp) / 2
    return legs


def rel(avg: pd.Series, line: pd.Series) -> pd.Series:
    return (avg - line) / line.where(line != 0)


def masks(d: pd.DataFrame, side: str, pace_lo: float, pace_hi: float) -> dict[str, dict[str, np.ndarray]]:
    up = side == "OVER"
    sgn = 1 if up else -1
    l5 = d.l5_over if up else d.l5_under
    l10 = d.l10_over if up else d.l10_under
    r5, r10, rs = sgn * rel(d.l5_avg, d.line), sgn * rel(d.l10_avg, d.line), sgn * d.rel_gap
    other = "UNDER" if up else "OVER"
    off_f, def_f = d.own_off_pt.isin(FAV_OFF[side]), d.opp_def_pt.isin(FAV_DEF[side])
    off_u, def_u = d.own_off_pt.isin(FAV_OFF[other]), d.opp_def_pt.isin(FAV_DEF[other])
    fast, slow = d.pace_match >= pace_hi, d.pace_match <= pace_lo
    pace_f, pace_u = (fast, slow) if up else (slow, fast)
    md = d.min_l5 - d.min_season
    t = np.ones(len(d), dtype=bool)
    v = lambda s: s.fillna(False).values.astype(bool)  # noqa: E731
    return {
        "l5": {"any": t, ">=4": v(l5 >= 4), "=5": v(l5 == 5)},
        "l10": {"any": t, ">=7": v(l10 >= 7), ">=8": v(l10 >= 8), ">=9": v(l10 >= 9)},
        "l5avg": {"any": t, "right side": v(r5 > 0), ">=10%": v(r5 >= 0.10)},
        "l10avg": {"any": t, "right side": v(r10 > 0), ">=10%": v(r10 >= 0.10)},
        "season": {"any": t, "right side": v(rs > 0), ">=5%": v(rs >= 0.05), ">=10%": v(rs >= 0.10)},
        "off": {"any": t, "favorable": v(off_f), "not unfavorable": v(~off_u)},
        "def": {"any": t, "favorable": v(def_f), "not unfavorable": v(~def_u)},
        "min": {"any": t, "not down 2+": v(md > -2), "down 2+": v(md <= -2)},
        "pace": {"any": t, "favorable": v(pace_f), "not unfavorable": v(~pace_u)},
    }


def search(d: pd.DataFrame, side: str, book: str, mk: dict, hit: np.ndarray, tr: np.ndarray) -> dict | None:
    keys = list(LEVELS)
    min_n = MIN_N[book]
    best: dict | None = None

    def dfs(i: int, m: np.ndarray, combo: list[str]) -> None:
        nonlocal best
        mt = m & tr
        n = int(mt.sum())
        if n < min_n:
            return
        if i == len(keys):
            k_ = int(hit[mt].sum())
            lo = wilson_lo(k_, n)
            if best is None or lo > best["train_low"] + 1e-9:
                best = {"gate": dict(zip(keys, combo)), "train_n": n, "train_hit": 100 * k_ / n,
                        "train_low": lo, "mask": m}
            return
        for lv in LEVELS[keys[i]]:
            dfs(i + 1, m if lv == "any" else m & mk[keys[i]][lv], combo + [lv])

    dfs(0, np.ones(len(d), dtype=bool), [])
    if best is None:
        return None
    mh = best.pop("mask") & ~tr
    hn = int(mh.sum())
    hk = int(hit[mh].sum())
    best["hold_n"] = hn
    best["hold_hit"] = round(100 * hk / hn, 1) if hn else None
    best["hold_low"] = round(100 * wilson_lo(hk, hn), 1) if hn else None
    best["train_low"] = round(100 * best["train_low"], 1)
    best["train_hit"] = round(best["train_hit"], 1)
    best["base_train"] = round(100 * hit[tr].mean(), 1)
    best["base_hold"] = round(100 * hit[~tr].mean(), 1) if (~tr).any() else None
    best["holds"] = bool(hn >= MIN_HOLD_N and best["hold_hit"] >= BREAK_EVEN[book]
                         and best["train_hit"] >= BREAK_EVEN[book])
    return best


def factor_table(mk: dict, hit: np.ndarray) -> dict:
    out = {}
    for k, lv in mk.items():
        out[k] = {}
        for name, m in lv.items():
            n = int(m.sum())
            if n:
                out[k][name] = {"n": n, "hit": round(100 * hit[m].mean(), 1)}
    return out


def player_ranks(legs: pd.DataFrame, book: str, side: str) -> pd.DataFrame:
    """One row per player x prop: graded hit rate on this book/side, ranked within the prop by Wilson low."""
    d = legs.copy()
    d["hit"] = (d.over_hit if side == "OVER" else d.under_hit).astype(int)
    sgn = 1 if side == "OVER" else -1
    d["gap_l5"] = sgn * (d.l5_avg - d.line)
    d["gap_l10"] = sgn * (d.l10_avg - d.line)
    d["gap_season"] = sgn * (d.season_avg - d.line)
    d["min_delta"] = d.min_l5 - d.min_season
    g = d.sort_values("slate").groupby(["prop", "player"]).agg(
        team=("team_n", "last"), nights=("slate", "nunique"), legs=("hit", "size"), hits=("hit", "sum"),
        avg_line=("line", "mean"), avg_actual=("actual", "mean"), gap_l5=("gap_l5", "mean"),
        gap_l10=("gap_l10", "mean"), gap_season=("gap_season", "mean"), min_l5=("min_l5", "mean"),
        min_delta=("min_delta", "mean"), team_rank=("own_off_p", "mean"), opp_rank=("opp_def_p", "mean"),
        pace=("pace_match", "mean"), last_slate=("slate", "max"),
    ).reset_index()
    g = g[g.nights >= PLAYER_MIN_NIGHTS].copy()
    if g.empty:
        return g
    g["hit_pct"] = 100 * g.hits / g.legs
    g["wilson_low"] = [100 * wilson_lo(int(k), int(n)) for k, n in zip(g.hits, g.legs)]
    g.insert(0, "side", side)
    g.insert(0, "book", book)
    g["prop_display"] = g.prop.map(lambda p: PROP_DISPLAY.get(p, p))
    g = g.sort_values(["prop", "wilson_low", "hit_pct"], ascending=[True, False, False])
    g["rank"] = g.groupby("prop").cumcount() + 1
    g["players_ranked"] = g.groupby("prop").player.transform("size")
    num = ["avg_line", "avg_actual", "gap_l5", "gap_l10", "gap_season", "min_l5", "min_delta",
           "team_rank", "opp_rank", "pace", "hit_pct", "wilson_low"]
    g[num] = g[num].round(1)
    return g


def player_md(p: pd.DataFrame) -> list[str]:
    out = ["# NBA player rankings by prop", "",
           f"Graded legs {p.last_slate.min() if len(p) else ''}..{p.last_slate.max() if len(p) else ''}; "
           f"players with >= {PLAYER_MIN_NIGHTS} graded nights on that prop. Ranked by Wilson 95% low of hit rate. "
           "Gaps are signed toward the side (positive = past the line on that side). Team rank = player's team "
           "per-game rank for the stat (1 = most); opp rank = opponent allowed rank (1 = allows least). "
           "Goblin legs include every ladder line posted that night.", ""]
    for (book, side, prop), g in p.groupby(["book", "side", "prop_display"], sort=True):
        out += [f"## {book} {prop} {side} — top {min(PLAYER_TOP, len(g))} of {len(g)}", "",
                "| # | Player | Team | Hit | Legs/Nights | Low | Avg line | L5 gap | L10 gap | Season gap | Min L5 | Min Δ | Team rk | Opp rk | Pace |",
                "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for r in g.head(PLAYER_TOP).itertuples(index=False):
            out.append(f"| {r.rank} | {r.player} | {r.team} | {r.hit_pct}% | {r.legs}/{r.nights} | {r.wilson_low}% | "
                       f"{r.avg_line} | {r.gap_l5:+} | {r.gap_l10:+} | {r.gap_season:+} | {r.min_l5} | {r.min_delta:+} | "
                       f"{r.team_rank} | {r.opp_rank} | {r.pace} |")
        out.append("")
    return out


def gate_str(gate: dict) -> str:
    return " · ".join(f"{LABEL[k]} {v}" for k, v in gate.items() if v != "any") or "no filter"


def main() -> None:
    gates, factors, players = [], [], []
    for book in ("Goblin", "Standard"):
        legs = load_legs(book)
        legs = attach_pace(legs, pace_asof(sorted(legs.slate.unique())))
        for side in (("OVER",) if book == "Goblin" else ("OVER", "UNDER")):
            players.append(player_ranks(legs, book, side))
        trp = legs[legs.slate <= TRAIN_END].drop_duplicates(["slate", "team_n", "opp_n"]).pace_match
        pace_lo, pace_hi = float(trp.quantile(1 / 3)), float(trp.quantile(2 / 3))
        print(f"{book}: {len(legs)} legs, {legs.slate.nunique()} slates "
              f"({legs.slate.min()}..{legs.slate.max()}), pace slow<={pace_lo:.1f} fast>={pace_hi:.1f}")
        for prop, grp in legs.groupby("prop"):
            grp = grp.reset_index(drop=True)
            tr = (grp.slate <= TRAIN_END).values
            for side in ("OVER", "UNDER"):
                if book == "Goblin" and side == "UNDER":
                    continue
                hit = (grp.over_hit if side == "OVER" else grp.under_hit).values.astype(int)
                mk = masks(grp, side, pace_lo, pace_hi)
                factors.append({"book": book, "prop": prop, "side": side, "n": len(grp),
                                "base": round(100 * hit.mean(), 1), "factors": factor_table(mk, hit)})
                g = search(grp, side, book, mk, hit, tr)
                if g:
                    gates.append({"book": book, "prop": prop, "prop_display": PROP_DISPLAY.get(prop, prop),
                                  "side": side, "gate_text": gate_str(g["gate"]), **g})
    gates.sort(key=lambda g: (g["book"], not g["holds"], -(g["hold_hit"] or 0)))

    lines = ["# NBA max gates — all factors", "",
             f"Train slates <= {TRAIN_END}; holdout = later slates. Break-even: Standard {BREAK_EVEN['Standard']}%, "
             f"Goblin {BREAK_EVEN['Goblin']}%. Observe-only.", ""]
    for book in ("Goblin", "Standard"):
        lines += [f"## {book}", "", "| Prop | Side | Gate | Train | Holdout | Base | Holds |", "|---|---|---|---:|---:|---:|---|"]
        for g in [x for x in gates if x["book"] == book]:
            hh = f"{g['hold_hit']}% (n={g['hold_n']})" if g["hold_hit"] is not None else "-"
            lines.append(f"| {g['prop_display']} | {g['side']} | {g['gate_text']} | {g['train_hit']}% (n={g['train_n']}) "
                         f"| {hh} | {g['base_train']}% | {'YES' if g['holds'] else ''} |")
        lines.append("")
    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
    REPORT.write_text(json.dumps({"train_end": TRAIN_END, "break_even": BREAK_EVEN, "gates": gates,
                                  "factors": factors}, indent=1, default=str), encoding="utf-8")
    print("\n".join(lines))
    print(f"Saved -> {REPORT}\nSaved -> {REPORT_MD}")

    p = pd.concat([x for x in players if not x.empty], ignore_index=True)
    cols = ["book", "side", "prop", "prop_display", "rank", "players_ranked", "player", "team", "hit_pct",
            "wilson_low", "hits", "legs", "nights", "avg_line", "avg_actual", "gap_l5", "gap_l10", "gap_season",
            "min_l5", "min_delta", "team_rank", "opp_rank", "pace", "last_slate"]
    p[cols].to_csv(PLAYER_CSV, index=False)
    PLAYER_MD.write_text("\n".join(player_md(p)), encoding="utf-8")
    print(f"Saved -> {PLAYER_CSV} ({len(p)} player-prop rows)\nSaved -> {PLAYER_MD}")


if __name__ == "__main__":
    main()
