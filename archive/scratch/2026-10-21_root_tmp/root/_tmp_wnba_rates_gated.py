"""WNBA live gates: keep vs not-clearing. goblin_70_eligible + standard_ticket_eligible."""
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
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
KEEP = {
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
}
STAT = {
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
    "fgm": "fgm",
    "fg2m": "fg2m",
    "fg2a": "fg2a",
    "ftm": "ftm",
    "fta": "fta",
    "oreb": "oreb",
    "dreb": "dreb",
    "steals": "stl",
    "blocks": "blk",
    "stocks": "stocks",
    "turnovers": "tov",
}
CSV_CAT = {
    **{k: v for k, v in STAT.items() if k != "stocks"},
    "stocks": "bs",
}


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def nn(name):
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


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
    for c in (
        "PTS", "REB", "AST", "STL", "BLK", "TO",
        "FGM", "FGA", "FG3M", "FG3A", "FG2M", "FG2A",
        "FTM", "FTA", "OREB", "DREB", "MIN",
    ):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["PLAYER_NORM"] = df.get("PLAYER_NORM", pd.Series("", index=df.index)).fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(nn)
    df = df.sort_values(["PLAYER_NORM", "game_date", "event_id"])
    prior = {}
    running: dict[str, list] = {}
    keys = (
        "pts", "reb", "ast", "ra", "pa", "pr", "pra",
        "stl", "blk", "stocks", "tov",
        "fgm", "fga", "fg3m", "fg3a", "fg2m", "fg2a",
        "ftm", "fta", "oreb", "dreb",
    )

    def avg(gs, k):
        if not gs:
            return None
        return sum(g[k] for g in gs) / len(gs)

    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        games = running.setdefault(pn, [])
        last5 = games[-5:]
        n = len(games)
        fga = sum(g["fga"] for g in games)
        fta = sum(g["fta"] for g in games)
        tov = sum(g["tov"] for g in games)
        st = {f"l5_{k}": avg(last5, k) for k in keys}
        st.update({f"avg_{k}": avg(games, k) for k in keys})
        st["poss_g"] = ((fga + 0.44 * fta + tov) / n) if n else None
        prior[(pn, d)] = st
        if float(rec.MIN) >= 1:
            games.append(
                {
                    "pts": float(rec.PTS),
                    "reb": float(rec.REB),
                    "ast": float(rec.AST),
                    "ra": float(rec.REB) + float(rec.AST),
                    "pa": float(rec.PTS) + float(rec.AST),
                    "pr": float(rec.PTS) + float(rec.REB),
                    "pra": float(rec.PTS) + float(rec.REB) + float(rec.AST),
                    "stl": float(rec.STL),
                    "blk": float(rec.BLK),
                    "stocks": float(rec.STL) + float(rec.BLK),
                    "tov": float(rec.TO),
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
            if side in {"O", "OVER"}:
                side = "OVER"
            elif side in {"U", "UNDER"}:
                side = "UNDER"
            else:
                continue
            book = pick_book(str(r.get("pick_type") or ""), side)
            if not book:
                continue
            prop = T.canon_prop("WNBA", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, prop, book, line)
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_dir(r, side)
            l10 = l10_dir(r, side)
            st = prior.get((nn(player), date)) or {}
            sk = STAT.get(prop)
            last5 = st.get(f"l5_{sk}") if sk else None
            savg = st.get(f"avg_{sk}") if sk else None
            cover_gap = (last5 - line) if last5 is not None and line is not None else num(r.get("edge"))
            opp = canon_team(r.get("opp_team"))
            cat = CSV_CAT.get(prop)
            csv = by_team.get(opp) or {}
            csv_tier = normalize_def_tier_label(csv.get(f"{cat}_tier")) if cat else ""
            d_raw = csv_tier or normalize_def_tier_label(r.get("def_tier") or r.get("def")) or ""
            rec = {
                "sport": "WNBA",
                "player": player,
                "prop": prop,
                "side": side,
                "line": line,
                "pick_type": book.split()[0],
                "l5_over": l5 if side == "OVER" else None,
                "l5_under": l5 if side == "UNDER" else None,
                "l10_over": l10 if side == "OVER" else None,
                "l10_under": l10 if side == "UNDER" else None,
                "cover": cover_gap,
                "dist_l5": cover_gap,
                "def": d_raw,
                "usage_tier": r.get("usage_tier"),
                "minutes_tier": r.get("minutes_tier"),
                "prop_tier": r.get("tier") or r.get("prop_tier") or "A",
                "avg_season": savg,
                "poss_g": st.get("poss_g"),
                "game_date": date,
            }
            if book.startswith("Goblin"):
                now = goblin_70_eligible(rec)
            elif book.startswith("Standard"):
                now = standard_ticket_eligible(rec)
            else:
                now = False
            rows.append(
                {
                    "date": date,
                    "book": book,
                    "prop": prop,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "listed": l5 is not None and l5 >= 4,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "now": now,
                }
            )
    return rows


def dump(title, rows, order=None):
    print(f"\n{title}")
    print(f"{'Prop':28} {'Listed L5>=4':20} {'L5=5':20} {'Live ticket':20}")
    by = {}
    for r in rows:
        by.setdefault(r["prop"], []).append(r)
    if order:
        items = [(p, by[p]) for p in order if p in by]
        items += sorted(((p, rs) for p, rs in by.items() if p not in set(order)), key=lambda kv: kv[0])
    else:
        items = sorted(by.items(), key=lambda kv: -sum(1 for x in kv[1] if x["now"]))
    for prop, rs in items:
        listed = [x for x in rs if x["listed"]]
        l5 = [x for x in rs if x["l5eq5"]]
        now = [x for x in rs if x["now"]]
        lab = display_prop(prop, "WNBA") or prop
        print(
            f"{lab:28} {hr(sum(x['hit'] for x in listed), len(listed)):20} "
            f"{hr(sum(x['hit'] for x in l5), len(l5)):20} "
            f"{hr(sum(x['hit'] for x in now), len(now)):20}"
        )
    listed = [x for x in rows if x["listed"]]
    l5 = [x for x in rows if x["l5eq5"]]
    now = [x for x in rows if x["now"]]
    print(
        f"{'ALL':28} {hr(sum(x['hit'] for x in listed), len(listed)):20} "
        f"{hr(sum(x['hit'] for x in l5), len(l5)):20} "
        f"{hr(sum(x['hit'] for x in now), len(now)):20}"
    )


def main():
    rows = collect(load_prior(), load_csv())
    gob = [r for r in rows if r["book"] == "Goblin OVER"]
    keep = [r for r in gob if r["prop"] in KEEP]
    dead = [r for r in gob if r["prop"] not in KEEP]
    dump(
        "CLEARING — Goblin OVER keep (live G70 = L5=5+L10>=8+D+cover; "
        "Reb 3.5+ skip D; Ast 2.5 off; Ast poss>=18 off; "
        "Pts/Reb/PR/PA avg>=line+unit; PRA Off+S/A)",
        keep,
        order=[
            "fga",
            "threes_att",
            "threes",
            "reb+ast",
            "assists",
            "rebounds",
            "pts+ast",
            "points",
            "pts+reb",
            "pra",
        ],
    )
    dump("NOT CLEARING — Goblin OVER (dead / never ticket)", dead)
    dump("Standard OVER", [r for r in rows if r["book"] == "Standard OVER"])
    dump("Standard UNDER", [r for r in rows if r["book"] == "Standard UNDER"])


if __name__ == "__main__":
    main()
