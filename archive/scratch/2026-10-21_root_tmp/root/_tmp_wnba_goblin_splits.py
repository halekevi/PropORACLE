"""WNBA Goblin OVER diagnostics: 3PA volume vs 3P%, Assists miss shape, line-tier vs cover.

Same honesty bar as FTM: real n, listed vs ticket stack vs G70+cover, kill if it
doesn't beat a coin at usable sample.
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import (  # noqa: E402
    graded_paths,
    l10_dir,
    l5_dir,
    num,
    pick_book,
    row_def_tier,
)
from utils.defense_tiers import d_aligned  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
PROPS = {"threes", "assists", "rebounds", "reb+ast", "points", "pts+ast"}
STAT = {
    "threes": "fg3m",
    "assists": "ast",
    "rebounds": "reb",
    "reb+ast": "ra",
    "points": "pts",
    "pts+ast": "pa",
}


def _norm_name(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def bucket(rows, pred):
    sub = [r for r in rows if pred(r)]
    return sum(r["hit"] for r in sub), len(sub)


def load_cache():
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df["PLAYER_NORM"] = df["PLAYER_NORM"].fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm_name)
    for c in ("FTM", "FTA", "FG3M", "FG3A", "AST", "REB", "PTS", "FGM", "FGA", "MIN"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["TEAM"] = df["TEAM"].fillna("").astype(str).str.upper().str.strip()
    df = df.sort_values(["PLAYER_NORM", "game_date", "event_id"])
    return df


def build_prior(df: pd.DataFrame):
    prior: dict[tuple[str, str], dict] = {}
    running: dict[str, list] = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        played = float(rec.MIN) >= 1.0
        games = running.setdefault(pn, [])
        last5 = [g for g in games[-5:]]
        last10 = [g for g in games[-10:]]

        def avg(gs, key):
            if not gs:
                return None
            return sum(g[key] for g in gs) / len(gs)

        def sums(gs, *keys):
            return {k: sum(g[k] for g in gs) for k in keys}

        s10 = sums(last10, "fg3m", "fg3a", "ast", "reb", "pts", "fgm", "fga")
        s_all = sums(games, "fg3m", "fg3a", "ast", "reb", "pts", "fgm", "fga")
        prior[(pn, d)] = {
            "n_prior": len(games),
            "l5_fg3m": avg(last5, "fg3m"),
            "l5_ast": avg(last5, "ast"),
            "l5_reb": avg(last5, "reb"),
            "l5_ra": avg(last5, "ra"),
            "l5_pts": avg(last5, "pts"),
            "l5_pa": avg(last5, "pa"),
            "l10_fg3m": avg(last10, "fg3m"),
            "l10_fg3a": avg(last10, "fg3a"),
            "l10_ast": avg(last10, "ast"),
            "l10_3pp": (s10["fg3m"] / s10["fg3a"]) if s10["fg3a"] else None,
            "l10_3pa": s10["fg3a"],
            "season_3pp": (s_all["fg3m"] / s_all["fg3a"]) if s_all["fg3a"] else None,
            "season_3pa": s_all["fg3a"],
            "season_3pa_g": (s_all["fg3a"] / len(games)) if games else None,
            "matched": True,
            "team": rec.TEAM,
        }
        if played:
            games.append(
                {
                    "fg3m": float(rec.FG3M),
                    "fg3a": float(rec.FG3A),
                    "ast": float(rec.AST),
                    "reb": float(rec.REB),
                    "ra": float(rec.REB) + float(rec.AST),
                    "pts": float(rec.PTS),
                    "pa": float(rec.PTS) + float(rec.AST),
                    "fgm": float(rec.FGM),
                    "fga": float(rec.FGA),
                }
            )
    return prior


def build_team_nights(df: pd.DataFrame):
    """Per team-date: that night's FG% and entering season FG%."""
    g = (
        df.groupby(["TEAM", "game_date", "event_id"], as_index=False)
        .agg(fgm=("FGM", "sum"), fga=("FGA", "sum"), ast=("AST", "sum"))
        .sort_values(["TEAM", "game_date"])
    )
    night = {}
    running: dict[str, dict] = {}
    for rec in g.itertuples(index=False):
        team = rec.TEAM
        d = rec.game_date
        st = running.setdefault(team, {"fgm": 0.0, "fga": 0.0, "n": 0})
        prior_pct = (st["fgm"] / st["fga"]) if st["fga"] else None
        game_pct = (rec.fgm / rec.fga) if rec.fga else None
        night[(team, d)] = {
            "game_fg": game_pct,
            "prior_fg": prior_pct,
            "game_ast": rec.ast,
            "event_id": rec.event_id,
        }
        st["fgm"] += float(rec.fgm)
        st["fga"] += float(rec.fga)
        st["n"] += 1
    return night


def collect(df, prior, nights):
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
            if side in ("O", "OVER"):
                side = "OVER"
            else:
                continue
            if pick_book(str(r.get("pick_type") or ""), side) != "Goblin OVER":
                continue
            prop = T.canon_prop("WNBA", str(r.get("prop") or r.get("prop_type") or ""))
            if prop not in PROPS:
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, prop, line)
            if key in seen:
                continue
            seen.add(key)
            pn = _norm_name(player)
            st = prior.get((pn, date)) or {}
            team = (st.get("team") or str(r.get("team") or "")).upper().strip()
            tn = nights.get((team, date)) or {}
            l5 = l5_dir(r, "OVER")
            l10 = l10_dir(r, "OVER")
            actual = num(r.get("actual_value") if r.get("actual_value") is not None else r.get("actual"))
            d_raw = row_def_tier(r, sport="WNBA")
            d_ok = d_aligned("WNBA", "OVER", d_raw, prop)
            last5 = st.get(f"l5_{STAT[prop]}")
            cover_gap = (last5 - line) if last5 is not None and line is not None else None
            cover_ok = T.cover_clears_floor("WNBA", cover_gap, "OVER", prop)
            listed = l5 is not None and l5 >= 4
            l5eq5 = l5 is not None and l5 >= 5
            l10_ok = l10 is not None and l10 >= 8
            stack = l5eq5 and l10_ok and d_ok
            g70 = stack and cover_ok
            hit = 1 if str(r.get("result")).strip().upper() == "HIT" else 0
            rows.append(
                {
                    "prop": prop,
                    "player": player,
                    "date": date,
                    "line": line,
                    "hit": hit,
                    "actual": actual,
                    "listed": listed,
                    "l5eq5": l5eq5,
                    "l10_ok": l10_ok,
                    "stack": stack,
                    "g70": g70,
                    "d_ok": d_ok,
                    "cover_ok": cover_ok,
                    "cover_gap": cover_gap,
                    "l5": l5,
                    "l10": l10,
                    "matched": bool(st),
                    "l10_3pa_g": st.get("l10_fg3a"),
                    "l10_3pp": st.get("l10_3pp"),
                    "l10_3pa": st.get("l10_3pa"),
                    "season_3pp": st.get("season_3pp"),
                    "season_3pa": st.get("season_3pa") or 0,
                    "season_3pa_g": st.get("season_3pa_g"),
                    "l10_ast": st.get("l10_ast"),
                    "game_fg": tn.get("game_fg"),
                    "prior_fg": tn.get("prior_fg"),
                    "def": d_raw,
                }
            )
    return rows


def by_prop(rows, prop):
    return [r for r in rows if r["prop"] == prop]


def print_hr_row(label, rows):
    listed = [r for r in rows if r["listed"]]
    l5eq5 = [r for r in rows if r["l5eq5"]]
    stack = [r for r in rows if r["stack"]]
    g70 = [r for r in rows if r["g70"]]
    print(
        f"  {label:22} all {hr(sum(r['hit'] for r in rows), len(rows)):18}  "
        f"listed {hr(sum(r['hit'] for r in listed), len(listed)):18}  "
        f"L5=5 {hr(sum(r['hit'] for r in l5eq5), len(l5eq5)):18}  "
        f"L5=5+L10+D {hr(sum(r['hit'] for r in stack), len(stack)):18}  "
        f"G70+cover {hr(sum(r['hit'] for r in g70), len(g70))}"
    )


def threes_report(rows):
    rs = by_prop(rows, "threes")
    print("\n========== 3PM  (Goblin OVER)  3PA volume vs make-rate ==========")
    print(f"cache match {sum(1 for r in rs if r['matched'])}/{len(rs)}")
    print_hr_row("baseline", rs)

    print("\n  L10 3PA/g (attempt volume, not makes):")
    cuts = [
        ("<2 looks", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] < 2),
        ("2–3 looks", lambda r: r["l10_3pa_g"] is not None and 2 <= r["l10_3pa_g"] < 4),
        ("4+ looks", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 4),
        ("4–5 looks", lambda r: r["l10_3pa_g"] is not None and 4 <= r["l10_3pa_g"] < 5),
        ("5+ looks", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 5),
        ("6+ looks", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 6),
    ]
    for lab, pred in cuts:
        print_hr_row(lab, [r for r in rs if pred(r)])

    print("\n  season 3P% floors (min 40 3PA entering) — make-rate, FTM-style:")
    for floor in (0.30, 0.33, 0.35, 0.38, 0.40):
        print_hr_row(
            f"3P% >= {floor:.0%}",
            [
                r
                for r in rs
                if r["season_3pa"] >= 40 and r["season_3pp"] is not None and r["season_3pp"] >= floor
            ],
        )
    print_hr_row(
        "3P% < 33%",
        [
            r
            for r in rs
            if r["season_3pa"] >= 40 and r["season_3pp"] is not None and r["season_3pp"] < 0.33
        ],
    )

    print("\n  L10 3P% floors (min 15 L10 3PA):")
    for floor in (0.30, 0.35, 0.40):
        print_hr_row(
            f"L10 3P% >= {floor:.0%}",
            [
                r
                for r in rs
                if (r["l10_3pa"] or 0) >= 15 and r["l10_3pp"] is not None and r["l10_3pp"] >= floor
            ],
        )

    print("\n  volume + make-rate:")
    combos = [
        ("4+ looks + 3P%>=33", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 4 and r["season_3pa"] >= 40 and (r["season_3pp"] or 0) >= 0.33),
        ("4+ looks + 3P%>=35", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 4 and r["season_3pa"] >= 40 and (r["season_3pp"] or 0) >= 0.35),
        ("5+ looks + 3P%>=33", lambda r: r["l10_3pa_g"] is not None and r["l10_3pa_g"] >= 5 and r["season_3pa"] >= 40 and (r["season_3pp"] or 0) >= 0.33),
    ]
    for lab, pred in combos:
        print_hr_row(lab, [r for r in rs if pred(r)])

    print("\n  ticket stack (L5=5+L10>=8+D) split by L10 3PA/g:")
    stack = [r for r in rs if r["stack"]]
    for lab, pred in cuts[:5]:
        print_hr_row(lab, [r for r in stack if pred(r)])


def assists_report(rows):
    rs = by_prop(rows, "assists")
    print("\n========== ASSISTS  (Goblin OVER)  miss shape ==========")
    print(f"cache match {sum(1 for r in rs if r['matched'])}/{len(rs)}")
    print_hr_row("Assists", rs)
    for p, lab in (("points", "Points"), ("pts+ast", "Pts+Asts"), ("reb+ast", "Reb+Ast")):
        print_hr_row(lab, by_prop(rows, p))

    print("\n  D split (already on the ticket):")
    print_hr_row("D pass", [r for r in rs if r["d_ok"]])
    print_hr_row("D fail", [r for r in rs if not r["d_ok"]])

    print("\n  pregame team FG% (entering, not tonight — non-circular):")
    for lab, pred in [
        ("team FG <42%", lambda r: r["prior_fg"] is not None and r["prior_fg"] < 0.42),
        ("team FG 42–46%", lambda r: r["prior_fg"] is not None and 0.42 <= r["prior_fg"] < 0.46),
        ("team FG >=46%", lambda r: r["prior_fg"] is not None and r["prior_fg"] >= 0.46),
    ]:
        print_hr_row(lab, [r for r in rs if pred(r)])

    print("\n  L10 AST volume vs line:")
    for lab, pred in [
        ("L10 AST < line", lambda r: r["l10_ast"] is not None and r["line"] is not None and r["l10_ast"] < r["line"]),
        ("L10 AST >= line", lambda r: r["l10_ast"] is not None and r["line"] is not None and r["l10_ast"] >= r["line"]),
        ("L10 AST >= line+1", lambda r: r["l10_ast"] is not None and r["line"] is not None and r["l10_ast"] >= r["line"] + 1),
    ]:
        print_hr_row(lab, [r for r in rs if pred(r)])

    print("\n  miss distance on LISTED misses (proxy, not potential-assists):")
    listed_miss = [r for r in rs if r["listed"] and r["hit"] == 0 and r["actual"] is not None and r["line"] is not None]
    l5eq5_miss = [r for r in rs if r["l5eq5"] and r["hit"] == 0 and r["actual"] is not None and r["line"] is not None]
    stack_miss = [r for r in rs if r["stack"] and r["hit"] == 0 and r["actual"] is not None and r["line"] is not None]
    g70_miss = [r for r in rs if r["g70"] and r["hit"] == 0 and r["actual"] is not None and r["line"] is not None]

    def miss_buckets(misses, title):
        near = [r for r in misses if r["actual"] >= r["line"] - 1]  # one extra make would cash
        mid = [r for r in misses if r["line"] - 2 <= r["actual"] < r["line"] - 1]
        dead = [r for r in misses if r["actual"] < r["line"] - 2]
        zero = [r for r in misses if r["actual"] <= 0]
        print(f"    {title} n_miss={len(misses)}")
        print(f"      near (within 1 of line) {len(near)}/{len(misses)}  mid {len(mid)}/{len(misses)}  dead (>2 short) {len(dead)}/{len(misses)}  zero-AST {len(zero)}/{len(misses)}")
        if misses:
            gaps = [r["line"] - r["actual"] for r in misses]
            print(f"      median shortfall {sorted(gaps)[len(gaps)//2]:.1f}   mean {sum(gaps)/len(gaps):.2f}")

    miss_buckets(listed_miss, "listed L5>=4")
    miss_buckets(l5eq5_miss, "L5=5")
    miss_buckets(stack_miss, "L5=5+L10+D")
    miss_buckets(g70_miss, "G70+cover")

    print("\n  same-game teammate FG% on misses vs hits (CIRCULAR — assists are made shots; description only):")
    for lab, pred in [
        ("hits, game FG", lambda r: r["hit"] == 1 and r["game_fg"] is not None),
        ("misses, game FG", lambda r: r["hit"] == 0 and r["game_fg"] is not None),
    ]:
        sub = [r for r in rs if r["listed"] and pred(r)]
        if sub:
            avg = sum(r["game_fg"] for r in sub) / len(sub)
            print(f"    listed {lab}: n={len(sub)}  mean FG% {100*avg:.1f}%")
    for lab, pred in [
        ("hits, game FG", lambda r: r["hit"] == 1 and r["game_fg"] is not None),
        ("misses, game FG", lambda r: r["hit"] == 0 and r["game_fg"] is not None),
    ]:
        sub = [r for r in rs if r["stack"] and pred(r)]
        if sub:
            avg = sum(r["game_fg"] for r in sub) / len(sub)
            print(f"    stack {lab}: n={len(sub)}  mean FG% {100*avg:.1f}%")


def line_tier_report(rows):
    print("\n========== LINE-TIER vs COVER  (Goblin OVER) ==========")
    print("cover = cache last5 avg − line vs PROP_COVER_UNIT (reb 1.3, 3s 0.7, RA 1.3)")
    for prop, lab in (("rebounds", "Rebounds"), ("threes", "3PM"), ("reb+ast", "Reb+Ast"), ("assists", "Assists")):
        rs = by_prop(rows, prop)
        print(f"\n  {lab}  n={len(rs)}  cover_need={T.cover_need('WNBA', prop)}")
        lines = sorted({r["line"] for r in rs if r["line"] is not None})
        print(f"    {'line':6} {'all':18} {'listed':18} {'L5=5':18} {'cover-ok':18} {'L5=5+L10+D':18} {'G70+cover':18}")
        for ln in lines:
            sub = [r for r in rs if r["line"] == ln]
            listed = [r for r in sub if r["listed"]]
            l5eq5 = [r for r in sub if r["l5eq5"]]
            cov = [r for r in sub if r["cover_ok"]]
            stack = [r for r in sub if r["stack"]]
            g70 = [r for r in sub if r["g70"]]
            print(
                f"    {ln:<6} {hr(sum(r['hit'] for r in sub), len(sub)):18} "
                f"{hr(sum(r['hit'] for r in listed), len(listed)):18} "
                f"{hr(sum(r['hit'] for r in l5eq5), len(l5eq5)):18} "
                f"{hr(sum(r['hit'] for r in cov), len(cov)):18} "
                f"{hr(sum(r['hit'] for r in stack), len(stack)):18} "
                f"{hr(sum(r['hit'] for r in g70), len(g70)):18}"
            )
        # coarse bands
        print("    bands:")
        bands = [
            ("<=1.5", lambda r: r["line"] is not None and r["line"] <= 1.5),
            ("2.5", lambda r: r["line"] == 2.5),
            ("3.5+", lambda r: r["line"] is not None and r["line"] >= 3.5),
        ]
        if prop == "rebounds":
            bands = [
                ("<=2.5", lambda r: r["line"] is not None and r["line"] <= 2.5),
                ("3.5", lambda r: r["line"] == 3.5),
                ("4.5+", lambda r: r["line"] is not None and r["line"] >= 4.5),
            ]
        if prop == "reb+ast":
            bands = [
                ("<=2.5", lambda r: r["line"] is not None and r["line"] <= 2.5),
                ("3.5–4.5", lambda r: r["line"] is not None and 3.5 <= r["line"] <= 4.5),
                ("5.5+", lambda r: r["line"] is not None and r["line"] >= 5.5),
            ]
        for blab, pred in bands:
            print_hr_row(blab, [r for r in rs if pred(r)])


def vol_gate(r):
    """5+ L10 3PA/g + season 3P%>=33 (min 40 3PA). Independent of L5/L10/D."""
    return (
        r.get("l10_3pa_g") is not None
        and r["l10_3pa_g"] >= 5
        and (r.get("season_3pa") or 0) >= 40
        and r.get("season_3pp") is not None
        and r["season_3pp"] >= 0.33
    )


def show(label, rows):
    print(f"  {label:48} {hr(sum(r['hit'] for r in rows), len(rows))}")


def threes_joint(rows):
    rs = by_prop(rows, "threes")
    l5 = [r for r in rs if r["l5eq5"]]
    ticket = [r for r in rs if r["stack"]]
    l5_vol = [r for r in l5 if vol_gate(r)]
    both = [r for r in ticket if vol_gate(r)]
    ticket_only = [r for r in ticket if not vol_gate(r)]
    l5_vol_not_ticket = [r for r in l5_vol if not r["stack"]]
    g70 = [r for r in rs if r["g70"]]
    g70_vol = [r for r in g70 if vol_gate(r)]
    print("\n========== 3PM JOINT  ticket ∩ (5+ looks + 3P%>=33) ==========")
    show("L5=5", l5)
    show("L5=5 + 5+looks + 3P%>=33  (B)", l5_vol)
    show("L5=5+L10>=8+D  (A, live ticket)", ticket)
    show("A ∩ B", both)
    show("A only (ticket, fails vol)", ticket_only)
    show("B only (L5=5+vol, fails L10+D)", l5_vol_not_ticket)
    print()
    show("G70+cover", g70)
    show("G70+cover ∩ vol", g70_vol)
    show("G70+cover, fails vol", [r for r in g70 if not vol_gate(r)])
    print()
    l5_l10 = [r for r in l5 if r["l10_ok"]]
    l5_d = [r for r in l5 if r["d_ok"]]
    show("L5=5 + L10>=8 (no D)", l5_l10)
    show("L5=5 + L10>=8 ∩ vol", [r for r in l5_l10 if vol_gate(r)])
    show("L5=5 + D (no L10)", l5_d)
    show("L5=5 + D ∩ vol", [r for r in l5_d if vol_gate(r)])
    n_a, n_b, n_both = len(ticket), len(l5_vol), len(both)
    pct_b_in_a = (100.0 * n_both / n_b) if n_b else 0.0
    pct_a_in_b = (100.0 * n_both / n_a) if n_a else 0.0
    print(
        f"\n  overlap: |A|={n_a}  |B|={n_b}  |A∩B|={n_both}"
        f"  B already in A={n_both}/{n_b} ({pct_b_in_a:.1f}%)"
        f"  A that also B={n_both}/{n_a} ({pct_a_in_b:.1f}%)"
    )
    print(f"  incremental faces B adds on top of A: {n_b - n_both}")


def line_d_decomp(rows, prop, line, title):
    rs = [r for r in by_prop(rows, prop) if r["line"] == line]
    print(f"\n========== {title}  Goblin {line}  D vs L10 vs cover ==========")
    show("all", rs)
    show("L5=5", [r for r in rs if r["l5eq5"]])
    show("L5=5 + D pass", [r for r in rs if r["l5eq5"] and r["d_ok"]])
    show("L5=5 + D fail", [r for r in rs if r["l5eq5"] and not r["d_ok"]])
    show("L5=5 + L10>=8", [r for r in rs if r["l5eq5"] and r["l10_ok"]])
    show("L5=5 + L10 fail", [r for r in rs if r["l5eq5"] and not r["l10_ok"]])
    show("L5=5 + cover-ok", [r for r in rs if r["l5eq5"] and r["cover_ok"]])
    show("L5=5 + cover fail", [r for r in rs if r["l5eq5"] and not r["cover_ok"]])
    show("L5=5 + L10>=8 + D  (ticket)", [r for r in rs if r["stack"]])
    show("L5=5 + L10>=8 + D fail", [r for r in rs if r["l5eq5"] and r["l10_ok"] and not r["d_ok"]])
    show("L5=5 + L10 + D, cover fail", [r for r in rs if r["stack"] and not r["cover_ok"]])
    show("G70+cover", [r for r in rs if r["g70"]])


def main():
    df = load_cache()
    prior = build_prior(df)
    nights = build_team_nights(df)
    rows = collect(df, prior, nights)
    print(f"collected {len(rows)} Goblin OVER faces")
    threes_joint(rows)
    line_d_decomp(rows, "rebounds", 3.5, "REBOUNDS")
    line_d_decomp(rows, "assists", 2.5, "ASSISTS")
    line_d_decomp(rows, "assists", 1.5, "ASSISTS")


if __name__ == "__main__":
    main()
