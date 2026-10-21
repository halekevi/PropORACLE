"""A∩B: live G70 vs season avg >= line+cover_unit. Assists poss>=18 vs ticket.

Entering-game ESPN only. Keep Goblin OVER. A = goblin_70_eligible (Reb 3.5 skip D,
Assists 2.5 fade, PRA Off+S/A, last5 cover). B = L5=5 AND season_avg >= line + cover_need.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num, pick_book  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
STAT = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "pra": "pra",
    "pts+reb": "pr",
    "pts+ast": "pa",
    "reb+ast": "ra",
    "threes": "fg3m",
    "threes_att": "fg3a",
    "fga": "fga",
}
CSV_CAT = dict(STAT)
KEEP = list(STAT)
POSS_FADE = 18.0


def _norm(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def canon_team(raw):
    s = str(raw or "").strip().upper()
    return TEAM_MAP.get(s, s) if s and "/" not in s else ""


def load_csv():
    df = pd.read_csv(default_csv_path())
    df["team"] = df["team"].astype(str).str.strip().str.upper().map(lambda t: TEAM_MAP.get(t, t))
    return {rec["team"]: rec for rec in df.to_dict("records")}


def load_prior():
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df["PLAYER_NORM"] = df["PLAYER_NORM"].fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm)
    cols = [
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TO",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
    ]
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.sort_values(["PLAYER_NORM", "game_date", "event_id"])
    prior = {}
    run: dict[str, list] = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        games = run.setdefault(pn, [])
        n = len(games)
        last5 = games[-5:]

        def s(k, gs=None):
            src = games if gs is None else gs
            return sum(g[k] for g in src)

        def a(k, gs=None):
            src = games if gs is None else gs
            return (sum(g[k] for g in src) / len(src)) if src else None

        fga, fta, tov = s("fga"), s("fta"), s("tov")
        prior[(pn, d)] = {
            "n": n,
            "poss_g": ((fga + 0.44 * fta + tov) / n) if n else None,
            "pts": a("pts"),
            "reb": a("reb"),
            "ast": a("ast"),
            "pra": a("pra"),
            "pr": a("pr"),
            "pa": a("pa"),
            "ra": a("ra"),
            "fga": a("fga"),
            "fg3m": a("fg3m"),
            "fg3a": a("fg3a"),
            "l5_pts": a("pts", last5),
            "l5_reb": a("reb", last5),
            "l5_ast": a("ast", last5),
            "l5_pra": a("pra", last5),
            "l5_pr": a("pr", last5),
            "l5_pa": a("pa", last5),
            "l5_ra": a("ra", last5),
            "l5_fga": a("fga", last5),
            "l5_fg3m": a("fg3m", last5),
            "l5_fg3a": a("fg3a", last5),
        }
        if float(rec.MIN) >= 1:
            games.append(
                {
                    "pts": float(rec.PTS),
                    "reb": float(rec.REB),
                    "ast": float(rec.AST),
                    "pra": float(rec.PTS) + float(rec.REB) + float(rec.AST),
                    "pr": float(rec.PTS) + float(rec.REB),
                    "pa": float(rec.PTS) + float(rec.AST),
                    "ra": float(rec.REB) + float(rec.AST),
                    "fga": float(rec.FGA),
                    "fg3m": float(rec.FG3M),
                    "fg3a": float(rec.FG3A),
                    "fta": float(rec.FTA),
                    "tov": float(rec.TO),
                }
            )
    return prior


def collect(prior, by_team):
    rows = []
    seen = set()
    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        props = raw.get("props") or raw.get("rows") or []
        if not isinstance(props, list):
            continue
        for r in props:
            if not isinstance(r, dict):
                continue
            if str(r.get("result") or "").strip().upper() not in {"HIT", "MISS"}:
                continue
            if T.norm_sport(str(r.get("sport") or "")) != "WNBA":
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side not in {"O", "OVER"}:
                continue
            if pick_book(str(r.get("pick_type") or ""), "OVER") != "Goblin OVER":
                continue
            prop = T.canon_prop("WNBA", str(r.get("prop") or r.get("prop_type") or ""))
            if prop not in STAT:
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, prop, line)
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_dir(r, "OVER")
            l10 = l10_dir(r, "OVER")
            st = prior.get((_norm(player), date)) or {}
            savg = st.get(STAT[prop])
            last5 = st.get(f"l5_{STAT[prop]}")
            cover_gap = (last5 - line) if last5 is not None and line is not None else None
            unit = T.cover_need("WNBA", prop)
            opp = canon_team(r.get("opp_team"))
            csv = by_team.get(opp) or {}
            csv_tier = normalize_def_tier_label(csv.get(f"{CSV_CAT[prop]}_tier"))
            d_raw = csv_tier or normalize_def_tier_label(r.get("def_tier") or r.get("def")) or ""
            rec = {
                "sport": "WNBA",
                "player": player,
                "prop": prop,
                "side": "OVER",
                "line": line,
                "pick_type": "Goblin",
                "l5_over": l5,
                "l10_over": l10,
                "cover": cover_gap,
                "dist_l5": cover_gap,
                "def": d_raw,
                "usage_tier": r.get("usage_tier"),
                "minutes_tier": r.get("minutes_tier"),
                "prop_tier": r.get("tier") or r.get("prop_tier") or "A",
                "poss_g": st.get("poss_g"),
                "game_date": date,
            }
            l5eq5 = l5 is not None and l5 >= 5
            gap = (savg - line) if savg is not None and line is not None else None
            rows.append(
                {
                    "prop": prop,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "l5eq5": l5eq5,
                    "a": goblin_70_eligible(rec),
                    "b": bool(l5eq5 and gap is not None and gap >= unit),
                    "b_ge_line": bool(l5eq5 and gap is not None and gap >= 0),
                    "poss18": bool(
                        l5eq5
                        and st.get("poss_g") is not None
                        and st["poss_g"] >= POSS_FADE
                    ),
                    "poss_known": st.get("poss_g") is not None,
                    "unit": unit,
                }
            )
    return rows


def show(label, sub, width=52):
    h = sum(r["hit"] for r in sub)
    print(f"  {label:{width}} {hr(h, len(sub))}")


def overlap_block(title, pool):
    a = [r for r in pool if r["a"]]
    b = [r for r in pool if r["b"]]
    both = [r for r in pool if r["a"] and r["b"]]
    a_only = [r for r in pool if r["a"] and not r["b"]]
    b_only = [r for r in pool if r["b"] and not r["a"]]
    l5 = [r for r in pool if r["l5eq5"]]
    print(f"\n========== {title} ==========")
    show("L5=5", l5)
    show("A  live G70 (L5=5+L10+D+cover)", a)
    show("B  L5=5 + avg >= line+cover_unit", b)
    show("A ∩ B", both)
    show("A only (ticket, fails avg+cover)", a_only)
    show("B only (avg+cover, fails ticket)", b_only)
    n_a, n_b, n_both = len(a), len(b), len(both)
    pct_b_in_a = (100.0 * n_both / n_b) if n_b else 0.0
    pct_a_in_b = (100.0 * n_both / n_a) if n_a else 0.0
    print(
        f"  overlap: |A|={n_a}  |B|={n_b}  |A∩B|={n_both}"
        f"  B already in A={n_both}/{n_b} ({pct_b_in_a:.1f}%)"
        f"  A that also B={n_both}/{n_a} ({pct_a_in_b:.1f}%)"
    )
    print(f"  incremental faces B adds on top of A: {n_b - n_both}")
    b_line = [r for r in pool if r["b_ge_line"]]
    show("B0 L5=5 + avg >= line (no unit)", b_line)
    show("A ∩ B0", [r for r in pool if r["a"] and r["b_ge_line"]])
    show("A only vs B0", [r for r in pool if r["a"] and not r["b_ge_line"]])


def assists_poss(pool):
    rs = [r for r in pool if r["prop"] == "assists"]
    l5 = [r for r in rs if r["l5eq5"]]
    a = [r for r in rs if r["a"]]
    hi = [r for r in l5 if r["poss18"]]
    lo = [r for r in l5 if r["poss_known"] and not r["poss18"]]
    a_hi = [r for r in a if r["poss18"]]
    a_lo = [r for r in a if not r["poss18"]]
    print("\n========== ASSISTS poss>=18 vs live ticket ==========")
    show("L5=5", l5)
    show("L5=5 + poss>=18  (fade candidate)", hi)
    show("L5=5 + poss<18", lo)
    show("A  live G70 Assists", a)
    show("A ∩ poss>=18  (would drop)", a_hi)
    show("A minus poss>=18", a_lo)
    show("poss>=18, not ticket (B-only analog)", [r for r in hi if not r["a"]])
    n_a, n_drop = len(a), len(a_hi)
    print(f"  G70 Assists dropped by poss>=18: {n_drop}/{n_a}")


def main():
    rows = collect(load_prior(), load_csv())
    print(f"collected {len(rows)} keep Goblin OVER faces")
    overlap_block("ALL KEEP PROPS  ticket ∩ (avg>=line+cover_unit)", rows)
    for prop in KEEP:
        overlap_block(display_prop(prop, "WNBA") or prop, [r for r in rows if r["prop"] == prop])
    assists_poss(rows)


if __name__ == "__main__":
    main()
