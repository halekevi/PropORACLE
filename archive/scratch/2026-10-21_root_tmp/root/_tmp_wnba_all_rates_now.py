"""Every WNBA prop: listed vs live ticket, all books."""
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
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
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
    "stocks": "bs",
    "turnovers": "tov",
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
    cols = {
        "PTS": "pts",
        "REB": "reb",
        "AST": "ast",
        "STL": "stl",
        "BLK": "blk",
        "TO": "tov",
        "FGM": "fgm",
        "FGA": "fga",
        "FG3M": "fg3m",
        "FG3A": "fg3a",
        "FG2M": "fg2m",
        "FG2A": "fg2a",
        "FTM": "ftm",
        "FTA": "fta",
        "OREB": "oreb",
        "DREB": "dreb",
        "MIN": "min",
    }
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["PLAYER_NORM"] = df.get("PLAYER_NORM", pd.Series("", index=df.index)).fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(nn)
    df = df.sort_values(["PLAYER_NORM", "game_date"])
    prior = {}
    running: dict[str, list] = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        games = running.setdefault(pn, [])
        last5 = games[-5:]

        def avg(gs, k):
            if not gs:
                return None
            return sum(g[k] for g in gs) / len(gs)

        keys = (
            "pts",
            "reb",
            "ast",
            "ra",
            "pa",
            "pr",
            "pra",
            "stl",
            "blk",
            "stocks",
            "tov",
            "fgm",
            "fga",
            "fg3m",
            "fg3a",
            "fg2m",
            "fg2a",
            "ftm",
            "fta",
            "oreb",
            "dreb",
        )
        prior[(pn, d)] = {f"l5_{k}": avg(last5, k) for k in keys}
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
            last5 = st.get(f"l5_{STAT[prop]}") if prop in STAT else None
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
                "prop_tier": r.get("tier") or r.get("prop_tier"),
            }
            if book.startswith("Goblin"):
                now = goblin_70_eligible(rec)
            elif book.startswith("Standard"):
                now = standard_ticket_eligible(rec)
            else:
                now = False
            hit = 1 if str(r.get("result")).strip().upper() == "HIT" else 0
            rows.append(
                {
                    "book": book,
                    "prop": prop,
                    "hit": hit,
                    "listed": l5 is not None and l5 >= 4,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "now": now,
                }
            )
    return rows


def dump(title, rows):
    print(f"\n{title}")
    print(f"{'Prop':28} {'L5=5':22} {'Live ticket now':20}")
    by = {}
    for r in rows:
        by.setdefault(r["prop"], []).append(r)
    items = sorted(by.items(), key=lambda kv: -sum(1 for x in kv[1] if x["l5eq5"]))
    for prop, rs in items:
        l5 = [x for x in rs if x["l5eq5"]]
        now = [x for x in rs if x["now"]]
        lab = display_prop(prop, "WNBA") or prop
        print(f"{lab:28} {hr(sum(x['hit'] for x in l5), len(l5)):22} {hr(sum(x['hit'] for x in now), len(now)):20}")
    l5 = [x for x in rows if x["l5eq5"]]
    now = [x for x in rows if x["now"]]
    print(f"{'ALL':28} {hr(sum(x['hit'] for x in l5), len(l5)):22} {hr(sum(x['hit'] for x in now), len(now)):20}")


def main():
    rows = collect(load_prior(), load_csv())
    for book in ("Goblin OVER", "Standard OVER", "Standard UNDER"):
        dump(book, [r for r in rows if r["book"] == book])


if __name__ == "__main__":
    main()
