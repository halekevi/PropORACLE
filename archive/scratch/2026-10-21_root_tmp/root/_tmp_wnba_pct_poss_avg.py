"""WNBA Goblin OVER: season avg, shooting %, possessions — solo and combos.

Entering-game only (no same-game leak). L5=5 primary. Thin props also print
listed L5>=4 so FTM/DREB are not silent.
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
from all_ranked_prop_hit_rates import graded_paths, l5_dir, num, pick_book  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
STAT = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocks": "blk",
    "stocks": "stocks",
    "turnovers": "tov",
    "pra": "pra",
    "pts+reb": "pr",
    "pts+ast": "pa",
    "reb+ast": "ra",
    "fgm": "fgm",
    "fga": "fga",
    "threes": "fg3m",
    "threes_att": "fg3a",
    "fg2m": "fg2m",
    "fg2a": "fg2a",
    "ftm": "ftm",
    "fta": "fta",
    "oreb": "oreb",
    "dreb": "dreb",
}
PCT_FOR = {
    "ftm": ("ftp", 10, (0.70, 0.75, 0.80, 0.85)),
    "fta": ("ftp", 10, (0.70, 0.75, 0.80)),
    "threes": ("fg3p", 20, (0.30, 0.33, 0.35, 0.38)),
    "threes_att": ("fg3p", 20, (0.30, 0.33, 0.35)),
    "fgm": ("fgp", 30, (0.42, 0.45, 0.48)),
    "fga": ("fgp", 30, (0.42, 0.45)),
    "points": ("fgp", 40, (0.42, 0.45, 0.48)),
    "pts+reb": ("fgp", 40, (0.42, 0.45)),
    "pts+ast": ("fgp", 40, (0.42, 0.45)),
    "pra": ("fgp", 40, (0.42, 0.45)),
}


def _norm(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def load_prior():
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df["PLAYER_NORM"] = df["PLAYER_NORM"].fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm)
    cols = [
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TO",
        "FGM", "FGA", "FG3M", "FG3A", "FG2M", "FG2A", "FTM", "FTA", "OREB", "DREB",
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

        def s(k):
            return sum(g[k] for g in games)

        def a(k):
            return (s(k) / n) if n else None

        fga, fta, tov = s("fga"), s("fta"), s("tov")
        prior[(pn, d)] = {
            "n": n,
            "min_g": a("min"),
            "fga_g": a("fga"),
            "poss_g": ((fga + 0.44 * fta + tov) / n) if n else None,
            "fgp": (s("fgm") / fga) if fga else None,
            "fg3p": (s("fg3m") / s("fg3a")) if s("fg3a") else None,
            "ftp": (s("ftm") / fta) if fta else None,
            "fg3a_tot": s("fg3a"),
            "fta_tot": fta,
            "fga_tot": fga,
            "pts": a("pts"),
            "reb": a("reb"),
            "ast": a("ast"),
            "stl": a("stl"),
            "blk": a("blk"),
            "stocks": a("stocks"),
            "tov": a("tov"),
            "pra": a("pra"),
            "pr": a("pr"),
            "pa": a("pa"),
            "ra": a("ra"),
            "fgm": a("fgm"),
            "fga": a("fga"),
            "fg3m": a("fg3m"),
            "fg3a": a("fg3a"),
            "fg2m": a("fg2m"),
            "fg2a": a("fg2a"),
            "ftm": a("ftm"),
            "fta": a("fta"),
            "oreb": a("oreb"),
            "dreb": a("dreb"),
        }
        if float(rec.MIN) >= 1:
            games.append(
                {
                    "min": float(rec.MIN),
                    "pts": float(rec.PTS),
                    "reb": float(rec.REB),
                    "ast": float(rec.AST),
                    "stl": float(rec.STL),
                    "blk": float(rec.BLK),
                    "stocks": float(rec.STL) + float(rec.BLK),
                    "tov": float(rec.TO),
                    "pra": float(rec.PTS) + float(rec.REB) + float(rec.AST),
                    "pr": float(rec.PTS) + float(rec.REB),
                    "pa": float(rec.PTS) + float(rec.AST),
                    "ra": float(rec.REB) + float(rec.AST),
                    "fgm": float(rec.FGM),
                    "fga": float(rec.FGA),
                    "fg3m": float(rec.FG3M),
                    "fg3a": float(rec.FG3A),
                    "fg2m": float(rec.FG2M),
                    "fg2a": float(rec.FG2A),
                    "ftm": float(rec.FTM),
                    "fta": float(rec.FTA),
                    "oreb": float(rec.OREB),
                    "dreb": float(rec.DREB),
                }
            )
    return prior


def collect(prior):
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
            st = prior.get((_norm(player), date)) or {}
            savg = st.get(STAT[prop])
            rows.append(
                {
                    "prop": prop,
                    "line": line,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "listed": l5 is not None and l5 >= 4,
                    "savg": savg,
                    "gap": (savg - line) if savg is not None and line is not None else None,
                    "min_g": st.get("min_g"),
                    "poss_g": st.get("poss_g"),
                    "fga_g": st.get("fga_g"),
                    "fgp": st.get("fgp"),
                    "fg3p": st.get("fg3p"),
                    "ftp": st.get("ftp"),
                    "n_prior": st.get("n") or 0,
                    "fga_tot": st.get("fga_tot") or 0,
                    "fg3a_tot": st.get("fg3a_tot") or 0,
                    "fta_tot": st.get("fta_tot") or 0,
                }
            )
    return rows


def take(rows, pred):
    sub = [r for r in rows if pred(r)]
    return sum(x["hit"] for x in sub), len(sub)


def show(label, h, n, base_h=None, base_n=None):
    extra = ""
    if base_n and n and base_n != n:
        b = base_h / base_n
        extra = f"  Δ{(100.0 * h / n) - 100.0 * b:+.1f}pt"
    print(f"    {label:34} {hr(h, n)}{extra}")


def report(prop, pool, *, tag):
    lab = display_prop(prop, "WNBA") or prop
    h0, n0 = take(pool, lambda r: True)
    print(f"\n{lab}  {tag}  {hr(h0, n0)}")
    if n0 < 5:
        print("    (n<5 — skip splits)")
        return
    u = T.cover_need("WNBA", prop)
    # season avg vs line
    show("season avg >= line", *take(pool, lambda r: r["gap"] is not None and r["gap"] >= 0), h0, n0)
    show(f"season avg >= line+{u}", *take(pool, lambda r: r["gap"] is not None and r["gap"] >= u), h0, n0)
    show("season avg < line", *take(pool, lambda r: r["gap"] is not None and r["gap"] < 0), h0, n0)
    # minutes / possessions
    for m in (20, 24, 28):
        show(f"MIN/g >= {m}", *take(pool, lambda r, m=m: r["min_g"] is not None and r["min_g"] >= m), h0, n0)
    for p in (10, 12, 15, 18):
        show(f"poss/g >= {p}", *take(pool, lambda r, p=p: r["poss_g"] is not None and r["poss_g"] >= p), h0, n0)
    # %
    spec = PCT_FOR.get(prop)
    if spec:
        key, min_att, floors = spec
        att = {"ftp": "fta_tot", "fg3p": "fg3a_tot", "fgp": "fga_tot"}[key]
        for fl in floors:
            show(
                f"{key} >= {fl:.0%} (att>={min_att})",
                *take(
                    pool,
                    lambda r, fl=fl, key=key, att=att, min_att=min_att: (
                        r.get(key) is not None
                        and r[key] >= fl
                        and r.get(att, 0) >= min_att
                    ),
                ),
                h0,
                n0,
            )
        lo = floors[0]
        show(
            f"{key} < {lo:.0%}",
            *take(
                pool,
                lambda r, lo=lo, key=key, att=att, min_att=min_att: (
                    r.get(key) is not None and r[key] < lo and r.get(att, 0) >= min_att
                ),
            ),
            h0,
            n0,
        )
    # combos
    show(
        "avg>=line AND MIN>=24",
        *take(
            pool,
            lambda r: r["gap"] is not None and r["gap"] >= 0 and r["min_g"] is not None and r["min_g"] >= 24,
        ),
        h0,
        n0,
    )
    show(
        "avg>=line AND poss>=15",
        *take(
            pool,
            lambda r: r["gap"] is not None and r["gap"] >= 0 and r["poss_g"] is not None and r["poss_g"] >= 15,
        ),
        h0,
        n0,
    )
    if spec:
        key, min_att, floors = spec
        att = {"ftp": "fta_tot", "fg3p": "fg3a_tot", "fgp": "fga_tot"}[key]
        fl = floors[0]
        show(
            f"avg>=line AND {key}>={fl:.0%}",
            *take(
                pool,
                lambda r, fl=fl, key=key, att=att, min_att=min_att: (
                    r["gap"] is not None
                    and r["gap"] >= 0
                    and r.get(key) is not None
                    and r[key] >= fl
                    and r.get(att, 0) >= min_att
                ),
            ),
            h0,
            n0,
        )
        show(
            f"MIN>=24 AND {key}>={fl:.0%}",
            *take(
                pool,
                lambda r, fl=fl, key=key, att=att, min_att=min_att: (
                    r["min_g"] is not None
                    and r["min_g"] >= 24
                    and r.get(key) is not None
                    and r[key] >= fl
                    and r.get(att, 0) >= min_att
                ),
            ),
            h0,
            n0,
        )
        show(
            f"avg>=line AND MIN>=24 AND {key}>={fl:.0%}",
            *take(
                pool,
                lambda r, fl=fl, key=key, att=att, min_att=min_att: (
                    r["gap"] is not None
                    and r["gap"] >= 0
                    and r["min_g"] is not None
                    and r["min_g"] >= 24
                    and r.get(key) is not None
                    and r[key] >= fl
                    and r.get(att, 0) >= min_att
                ),
            ),
            h0,
            n0,
        )


def main():
    rows = collect(load_prior())
    dead = {
        "turnovers",
        "steals",
        "stocks",
        "oreb",
        "blocks",
        "ftm",
        "fta",
        "fgm",
        "dreb",
        "fg2m",
    }
    keep = [
        "points",
        "rebounds",
        "assists",
        "pra",
        "pts+reb",
        "pts+ast",
        "reb+ast",
        "threes",
        "threes_att",
        "fga",
    ]
    print("======== NOT CLEARING (Goblin OVER) ========")
    for prop in dead:
        pool5 = [r for r in rows if r["prop"] == prop and r["l5eq5"]]
        pool4 = [r for r in rows if r["prop"] == prop and r["listed"]]
        if len(pool5) >= 5:
            report(prop, pool5, tag="L5=5")
        else:
            print(f"\n{display_prop(prop, 'WNBA')}  L5=5 {hr(sum(r['hit'] for r in pool5), len(pool5))}  — fallback listed")
            report(prop, pool4, tag="listed L5>=4")
    print("\n======== KEEP SET (Goblin OVER L5=5) ========")
    for prop in keep:
        pool5 = [r for r in rows if r["prop"] == prop and r["l5eq5"]]
        report(prop, pool5, tag="L5=5")


if __name__ == "__main__":
    main()
