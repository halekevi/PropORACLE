"""WNBA Goblin OVER hit rates with live goblin_70_eligible (Reb 3.5 skip D, Ast 2.5 fade)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num, pick_book  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402
from utils.wnba_ticket_cuts import wnba_assists_25_ticket_fade, wnba_rebounds_35_skip_d  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
STAT = {
    "threes": "fg3m",
    "assists": "ast",
    "rebounds": "reb",
    "reb+ast": "ra",
    "points": "pts",
    "pts+ast": "pa",
    "pts+reb": "pr",
    "pra": "pra",
    "fga": "fga",
    "threes_att": "fg3a",
}
CSV_CAT = {
    "threes": "fg3m",
    "threes_att": "fg3a",
    "assists": "ast",
    "rebounds": "reb",
    "reb+ast": "ra",
    "points": "pts",
    "pts+ast": "pa",
    "pts+reb": "pr",
    "pra": "pra",
    "fga": "fga",
}
PROPS = set(STAT)


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


def load_cache():
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    for c in ("FG3M", "FG3A", "AST", "REB", "PTS", "FGA", "MIN"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["PLAYER_NORM"] = df.get("PLAYER_NORM", pd.Series("", index=df.index)).fillna("").astype(str)
    from utils.player_name_utils import normalize_player_name
    import re

    def _nn(name):
        base = normalize_player_name(name).lower()
        s = re.sub(r"[^a-z0-9\s]", " ", base)
        s = re.sub(r"\s+", " ", s).strip()
        return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})

    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_nn)
    df["PLAYER_NORM"] = df["PLAYER_NORM"]
    df = df.sort_values(["PLAYER_NORM", "game_date"])
    prior = {}
    running = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        games = running.setdefault(pn, [])
        last5 = games[-5:]

        def avg(gs, k):
            if not gs:
                return None
            return sum(g[k] for g in gs) / len(gs)

        prior[(pn, d)] = {f"l5_{k}": avg(last5, k) for k in ("fg3m", "ast", "reb", "ra", "pts", "pa", "pr", "pra", "fga", "fg3a")}
        if float(rec.MIN) >= 1:
            games.append(
                {
                    "fg3m": float(rec.FG3M),
                    "fg3a": float(rec.FG3A),
                    "ast": float(rec.AST),
                    "reb": float(rec.REB),
                    "ra": float(rec.REB) + float(rec.AST),
                    "pts": float(rec.PTS),
                    "pa": float(rec.PTS) + float(rec.AST),
                    "pr": float(rec.PTS) + float(rec.REB),
                    "pra": float(rec.PTS) + float(rec.REB) + float(rec.AST),
                    "fga": float(rec.FGA),
                }
            )
    return prior, _nn


def collect(prior, nn, by_team):
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
            if prop not in PROPS:
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, prop, line)
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_dir(r, "OVER")
            l10 = l10_dir(r, "OVER")
            st = prior.get((nn(player), date)) or {}
            last5 = st.get(f"l5_{STAT[prop]}")
            cover_gap = (last5 - line) if last5 is not None and line is not None else None
            opp = canon_team(r.get("opp_team"))
            cat = CSV_CAT.get(prop)
            csv = by_team.get(opp) or {}
            csv_tier = normalize_def_tier_label(csv.get(f"{cat}_tier")) if cat else ""
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
            }
            hit = 1 if str(r.get("result")).strip().upper() == "HIT" else 0
            rows.append(
                {
                    "prop": prop,
                    "line": line,
                    "hit": hit,
                    "listed": l5 is not None and l5 >= 4,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "old_g70": bool(
                        l5 is not None
                        and l5 >= 5
                        and l10 is not None
                        and l10 >= 8
                        and d_aligned("WNBA", "OVER", d_raw, prop)
                        and T.cover_clears_floor("WNBA", cover_gap, "OVER", prop)
                    ),
                    "now": goblin_70_eligible(rec),
                    "skip_d": wnba_rebounds_35_skip_d(rec),
                    "fade_25": wnba_assists_25_ticket_fade(rec),
                    "display": T.display_prop(prop) if hasattr(T, "display_prop") else prop,
                }
            )
    return rows


def main():
    by_team = load_csv()
    prior, nn = load_cache()
    rows = collect(prior, nn, by_team)
    from utils.prop_norm import display_prop

    print("WNBA Goblin OVER — listed L5>=4 vs live G70 (now = goblin_70_eligible)")
    print(f"{'Prop':22} {'Listed L5>=4':20} {'Old G70 (L5+L10+D+cover)':26} {'Now (live cuts)':22}")
    by = {}
    for r in rows:
        by.setdefault(r["prop"], []).append(r)
    order = [
        "points",
        "pts+ast",
        "pts+reb",
        "pra",
        "reb+ast",
        "rebounds",
        "assists",
        "threes",
        "threes_att",
        "fga",
    ]
    for prop in order:
        rs = by.get(prop) or []
        if not rs:
            continue
        listed = [x for x in rs if x["listed"]]
        old = [x for x in rs if x["old_g70"]]
        now = [x for x in rs if x["now"]]
        lab = display_prop(prop, "WNBA") or prop
        print(
            f"{lab:22} {hr(sum(x['hit'] for x in listed), len(listed)):20} "
            f"{hr(sum(x['hit'] for x in old), len(old)):26} "
            f"{hr(sum(x['hit'] for x in now), len(now)):22}"
        )
    print()
    all_listed = [x for x in rows if x["listed"]]
    all_old = [x for x in rows if x["old_g70"]]
    all_now = [x for x in rows if x["now"]]
    print(f"{'ALL above':22} {hr(sum(x['hit'] for x in all_listed), len(all_listed)):20} {hr(sum(x['hit'] for x in all_old), len(all_old)):26} {hr(sum(x['hit'] for x in all_now), len(all_now))}")
    reb35 = [x for x in rows if x["skip_d"]]
    ast25 = [x for x in rows if x["fade_25"]]
    reb35_now = [x for x in reb35 if x["now"]]
    reb35_old = [x for x in reb35 if x["old_g70"]]
    print()
    print("Cuts")
    print(f"  Rebounds 3.5  old G70 {hr(sum(x['hit'] for x in reb35_old), len(reb35_old))}   now {hr(sum(x['hit'] for x in reb35_now), len(reb35_now))}  (D skipped)")
    print(f"  Assists 2.5   faded from tickets  n_listed_L5=5 faces in sample {sum(1 for x in ast25 if x['l5eq5'])}  now n={sum(1 for x in ast25 if x['now'])}")


if __name__ == "__main__":
    main()
