"""Re-cut avg-vs-line hit rates by book: Goblin OVER, Standard OVER, Standard UNDER."""
from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

OPEN = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(OPEN / "scripts"))
import _avg_gap_by_badge as B  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
BOOKS = ("Goblin OVER", "Goblin UNDER", "Standard OVER", "Standard UNDER")


def rate(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def overall(df):
    n = int(len(df))
    h = int(df["hit"].sum()) if n else 0
    def m(col):
        s = pd.to_numeric(df[col], errors="coerce") if n else pd.Series(dtype=float)
        return None if n == 0 or s.notna().sum() == 0 else round(float(s.mean()), 2)
    return {"hits": h, "n": n, "rate": rate(h, n), "mean_l5_gap": m("g_l5"), "mean_min_gap": m("min_gap"), "mean_szn_gap": m("g_szn")}


def tabulate(df, field, specs):
    s = pd.to_numeric(df[field], errors="coerce")
    out = {}
    for name, fn in specs:
        mask = s.notna() & fn(s)
        n = int(mask.sum())
        if n == 0:
            continue
        h = int(df.loc[mask, "hit"].sum())
        out[name] = {"hits": h, "n": n, "rate": rate(h, n)}
    return out


def book_of(pick, direction):
    return f"{pick} {direction}"


def main():
    dates = sorted(p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json"))
    frames = []
    n_files = 0
    for date in dates:
        for sport in B.SPORTS:
            p = B.find_step8(date, sport)
            if not p:
                continue
            print(f"load {date} {sport}", flush=True)
            frames.append(B.load_step8(p, sport, date))
            n_files += 1
    step8 = pd.concat(frames, ignore_index=True)
    print("step8", len(step8), flush=True)

    rows = []
    for date in dates:
        raw = json.loads((GRADED / f"graded_props_{date}.json").read_text(encoding="utf-8"))
        for g in raw.get("props", []):
            sport = str(g.get("sport") or "")
            if sport not in B.SPORTS:
                continue
            result = str(g.get("result") or "").upper()
            if result not in ("HIT", "MISS"):
                continue
            pk = str(g.get("pick_type") or "").strip().lower()
            if "goblin" in pk:
                pick = "Goblin"
            elif "demon" in pk:
                continue
            else:
                pick = "Standard"
            d = str(g.get("direction") or g.get("over_under") or "").strip().upper()
            if d in ("O", "OVER"):
                d = "OVER"
            elif d in ("U", "UNDER"):
                d = "UNDER"
            else:
                continue
            try:
                line_v = float(g.get("line"))
            except (TypeError, ValueError):
                line_v = math.nan
            rows.append(
                {
                    "date": date,
                    "sport": sport,
                    "player_n": re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(g.get("player") or "").strip().lower())).strip(),
                    "prop_n": re.sub(r"\s+", " ", str(g.get("prop") or "").strip().lower()),
                    "line": line_v,
                    "direction": d,
                    "pick": pick,
                    "hit": 1 if result == "HIT" else 0,
                }
            )
    graded = pd.DataFrame(rows)
    graded["line_r"] = graded["line"].round(2)
    keys = ["date", "sport", "player_n", "prop_n", "line_r", "direction"]
    merged = graded.merge(step8, on=keys, how="inner")
    leftover = graded.merge(merged[keys].drop_duplicates().assign(_ok=1), on=keys, how="left")
    leftover = leftover[leftover["_ok"].isna()].drop(columns="_ok")
    loose = leftover.merge(
        step8.drop(columns=["line_r"]).drop_duplicates(["date", "sport", "player_n", "prop_n", "direction"], keep="last"),
        on=["date", "sport", "player_n", "prop_n", "direction"],
        how="inner",
    )
    if len(loose):
        merged = pd.concat([merged, loose], ignore_index=True)

    line = pd.to_numeric(merged["line"], errors="coerce")
    over = merged["direction"].eq("OVER")
    under = merged["direction"].eq("UNDER")
    for src, dest in (("l5", "g_l5"), ("l10", "g_l10"), ("szn", "g_szn")):
        avg = pd.to_numeric(merged[src], errors="coerce")
        gap = pd.Series(np.nan, index=merged.index)
        gap = gap.mask(over, avg - line)
        gap = gap.mask(under, line - avg)
        merged[dest] = gap
    merged["min_gap"] = merged[["g_l5", "g_l10", "g_szn"]].min(axis=1, skipna=True)
    merged["book"] = merged["pick"] + " " + merged["direction"]
    l5_dir = np.where(over, merged["l5_over"], merged["l5_under"])
    merged["l5_hits"] = pd.to_numeric(pd.Series(l5_dir, index=merged.index), errors="coerce")
    merged["list_gate"] = merged["l5_hits"] >= 4

    def pack(df):
        return {
            "all": overall(df),
            "list_l5ge4": overall(df[df["list_gate"]]),
            "min_gap": tabulate(df, "min_gap", B.WNBA_GAPS),
            "min_gap_small": tabulate(df, "min_gap", B.SMALL_GAPS),
            "l5_gap": tabulate(df, "g_l5", B.WNBA_GAPS),
            "l5_gap_small": tabulate(df, "g_l5", B.SMALL_GAPS),
            "list_min_gap": tabulate(df[df["list_gate"]], "min_gap", B.WNBA_GAPS),
            "list_min_gap_small": tabulate(df[df["list_gate"]], "min_gap", B.SMALL_GAPS),
        }

    combined = {bk: pack(merged[merged["book"].eq(bk)]) for bk in BOOKS}
    by_sport = {}
    for sp in B.SPORTS:
        by_sport[sp] = {}
        for bk in BOOKS:
            sub = merged[merged["sport"].eq(sp) & merged["book"].eq(bk)]
            specs = B.SPORT_GAPS[sp]
            by_sport[sp][bk] = {
                "all": overall(sub),
                "list_l5ge4": overall(sub[sub["list_gate"]]),
                "min_gap": tabulate(sub, "min_gap", specs),
                "list_min_gap": tabulate(sub[sub["list_gate"]], "min_gap", specs),
            }

    out = {
        "window": {"first": str(merged["date"].min()), "last": str(merged["date"].max()), "n": int(len(merged)), "n_files": n_files},
        "note": "Std+Goblin decided HIT/MISS joined to step8. list_l5ge4 = directional L5 >= 4 (live list gate). Goblin UNDER is shown for contrast, not a recommended list.",
        "combined": combined,
        "by_sport": by_sport,
        "counts": {k: int(v) for k, v in merged["book"].value_counts().items()},
    }
    dest = OPEN / "data" / "reports" / "avg_gap_by_book.json"
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    slim = {
        "counts": out["counts"],
        "combined": {bk: {"all": v["all"], "list_l5ge4": v["list_l5ge4"], "min_gap": v["min_gap"], "list_min_gap": v["list_min_gap"]} for bk, v in combined.items()},
        "by_sport": {
            sp: {bk: {"all": vv["all"], "list_l5ge4": vv["list_l5ge4"], "min_gap": vv["min_gap"], "list_min_gap": vv["list_min_gap"]} for bk, vv in books.items()}
            for sp, books in by_sport.items()
        },
    }
    print(json.dumps(slim, indent=2))
    print("wrote", dest)


if __name__ == "__main__":
    main()
