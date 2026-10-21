"""WNBA Goblin OVER FTM: does entering-game FT% lift the 45% listed cell?"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from all_ranked_prop_hit_rates import graded_paths, l5_dir, num, pick_book  # noqa: E402
from prop_hit_tiers import canon_prop, norm_sport  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402


def _norm_name(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def load_prior_ft():
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df["PLAYER_NORM"] = df["PLAYER_NORM"].fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm_name)
    df["FTM"] = pd.to_numeric(df["FTM"], errors="coerce").fillna(0)
    df["FTA"] = pd.to_numeric(df["FTA"], errors="coerce").fillna(0)
    df = df.sort_values(["PLAYER_NORM", "game_date", "event_id"])
    prior: dict[tuple[str, str], dict] = {}
    running: dict[str, dict] = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        st = running.setdefault(pn, {"ftm": 0.0, "fta": 0.0, "games": []})
        games = st["games"]
        last10 = games[-10:]
        l10_ftm = sum(g[1] for g in last10)
        l10_fta = sum(g[2] for g in last10)
        prior[(pn, d)] = {
            "season_ftm": st["ftm"],
            "season_fta": st["fta"],
            "season_pct": (st["ftm"] / st["fta"]) if st["fta"] else None,
            "l10_ftm": l10_ftm,
            "l10_fta": l10_fta,
            "l10_pct": (l10_ftm / l10_fta) if l10_fta else None,
            "l10_fta_avg": (l10_fta / len(last10)) if last10 else None,
            "n_prior": len(games),
        }
        st["ftm"] += float(rec.FTM)
        st["fta"] += float(rec.FTA)
        st["games"].append((d, float(rec.FTM), float(rec.FTA)))
    return prior


def collect_ftm():
    prior = load_prior_ft()
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
            if norm_sport(str(r.get("sport") or "")) != "WNBA":
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            else:
                continue
            book = pick_book(str(r.get("pick_type") or ""), side)
            if book != "Goblin OVER":
                continue
            prop = canon_prop("WNBA", str(r.get("prop") or r.get("prop_type") or ""))
            if prop != "ftm":
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, line)
            if key in seen:
                continue
            seen.add(key)
            pn = _norm_name(player)
            st = prior.get((pn, date)) or {}
            l5 = l5_dir(r, "OVER")
            rows.append(
                {
                    "date": date,
                    "player": player,
                    "line": line,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "listed": l5 is not None and l5 >= 4,
                    "l5": l5,
                    "season_pct": st.get("season_pct"),
                    "season_fta": st.get("season_fta") or 0,
                    "l10_pct": st.get("l10_pct"),
                    "l10_fta": st.get("l10_fta") or 0,
                    "l10_fta_avg": st.get("l10_fta_avg"),
                    "n_prior": st.get("n_prior") or 0,
                    "matched": bool(st),
                }
            )
    return rows


def bucket(rows, pred):
    sub = [r for r in rows if pred(r)]
    h = sum(r["hit"] for r in sub)
    return h, len(sub)


def dump(title, rows):
    print(f"\n{title}  n={len(rows)}  {hr(sum(r['hit'] for r in rows), len(rows))}")
    print(f"  cache match {sum(1 for r in rows if r['matched'])}/{len(rows)}")
    listed = [r for r in rows if r["listed"]]
    print(f"  listed L5>=4  {hr(sum(r['hit'] for r in listed), len(listed))}")
    lines = defaultdict(list)
    for r in rows:
        lines[r["line"]].append(r["hit"])
    print("  by line:", {k: hr(sum(v), len(v)) for k, v in sorted(lines.items(), key=lambda x: (x[0] is None, x[0] or 0))})

    print("  season FT% floors (min 20 FTA entering):")
    for floor in (0.70, 0.75, 0.80, 0.85, 0.90):
        h, n = bucket(rows, lambda r, f=floor: (r["season_fta"] >= 20 and r["season_pct"] is not None and r["season_pct"] >= f))
        hl, nl = bucket(listed, lambda r, f=floor: (r["season_fta"] >= 20 and r["season_pct"] is not None and r["season_pct"] >= f))
        print(f"    >= {floor:.0%}  all {hr(h, n)}   listed {hr(hl, nl)}")

    print("  season FT% floors (min 40 FTA entering):")
    for floor in (0.75, 0.80, 0.85):
        h, n = bucket(rows, lambda r, f=floor: (r["season_fta"] >= 40 and r["season_pct"] is not None and r["season_pct"] >= f))
        hl, nl = bucket(listed, lambda r, f=floor: (r["season_fta"] >= 40 and r["season_pct"] is not None and r["season_pct"] >= f))
        print(f"    >= {floor:.0%}  all {hr(h, n)}   listed {hr(hl, nl)}")

    print("  below FT% (min 20 FTA):")
    for cap in (0.70, 0.75, 0.80):
        h, n = bucket(rows, lambda r, c=cap: (r["season_fta"] >= 20 and r["season_pct"] is not None and r["season_pct"] < c))
        print(f"    < {cap:.0%}  all {hr(h, n)}")

    print("  L10 FT% floors (min 10 L10 FTA):")
    for floor in (0.70, 0.75, 0.80, 0.85):
        h, n = bucket(rows, lambda r, f=floor: (r["l10_fta"] >= 10 and r["l10_pct"] is not None and r["l10_pct"] >= f))
        hl, nl = bucket(listed, lambda r, f=floor: (r["l10_fta"] >= 10 and r["l10_pct"] is not None and r["l10_pct"] >= f))
        print(f"    >= {floor:.0%}  all {hr(h, n)}   listed {hr(hl, nl)}")

    print("  L10 FTA volume (attempts, not %):")
    for vmin in (2.0, 3.0, 4.0, 5.0):
        h, n = bucket(rows, lambda r, v=vmin: (r["l10_fta_avg"] is not None and r["l10_fta_avg"] >= v))
        hl, nl = bucket(listed, lambda r, v=vmin: (r["l10_fta_avg"] is not None and r["l10_fta_avg"] >= v))
        print(f"    L10 FTA/g >= {vmin:.0f}  all {hr(h, n)}   listed {hr(hl, nl)}")

    print("  FT% + volume:")
    combos = [
        (0.80, 20, 3.0),
        (0.80, 20, 4.0),
        (0.85, 20, 3.0),
        (0.75, 40, 3.0),
    ]
    for pct, min_fta, vol in combos:
        def ok(r, pct=pct, min_fta=min_fta, vol=vol):
            return (
                r["season_fta"] >= min_fta
                and r["season_pct"] is not None
                and r["season_pct"] >= pct
                and r["l10_fta_avg"] is not None
                and r["l10_fta_avg"] >= vol
            )
        h, n = bucket(rows, ok)
        hl, nl = bucket(listed, ok)
        print(f"    season FT%>={pct:.0%} (FTA>={min_fta}) + L10 FTA/g>={vol:.0f}  all {hr(h, n)}   listed {hr(hl, nl)}")


def main():
    rows = collect_ftm()
    dump("Goblin OVER Free Throws Made", rows)


if __name__ == "__main__":
    main()
