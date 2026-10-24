"""Backtest three-book list logic with soccer Standard Unders classified (not skipped).

Baseline: live list = L5>=4, Goblin OVER + Standard OVER + Standard UNDER, no Demons.
New Goblin: same + sport min-gap floors (WNBA>=2, MLB>=1, Tennis>=2, Soccer>=1).
Standard OVER: L5>=4, no cover floor (soccer included).
Standard UNDER: L5>=4, soccer included and tagged by minutes/starter/prop/cover.
"""
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
GOBLIN_FLOOR = {"WNBA": 2.0, "MLB": 1.0, "Tennis": 2.0, "Soccer": 1.0}


def rate(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def overall(df):
    n = int(len(df))
    h = int(df["hit"].sum()) if n else 0
    return {"hits": h, "n": n, "rate": rate(h, n)}


def soccer_prop_family(prop: str) -> str:
    p = (prop or "").lower()
    if "save" in p or "goalie" in p:
        return "goalie_saves"
    if "shot on" in p or p in {"sog", "shots on goal"}:
        return "sog"
    if "shot" in p:
        return "shots"
    if "goal" in p and "shot" not in p:
        return "goals"
    if "foul" in p:
        return "fouls"
    if "tackle" in p:
        return "tackles"
    if "pass" in p:
        return "passes"
    return "other"


def soccer_minutes_class(row) -> str:
    starter = str(row.get("starter_tier") or "").strip().upper()
    mt = str(row.get("min_tier") or "").strip().upper()
    amin = row.get("avg_min")
    try:
        amin = float(amin)
    except (TypeError, ValueError):
        amin = math.nan
    if starter == "SUB" or mt == "LOW" or (amin == amin and amin < 45):
        return "low_minutes"
    if starter == "ROTATION" or mt == "MEDIUM" or (amin == amin and amin < 70):
        return "rotation"
    if starter == "STARTER" or mt == "HIGH" or (amin == amin and amin >= 70):
        return "starter"
    if amin == amin:
        return "starter" if amin >= 60 else "rotation"
    return "unknown_minutes"


def cover_class(gap) -> str:
    if gap is None or (isinstance(gap, float) and math.isnan(gap)):
        return "no_avg"
    if gap < -1e-9:
        return "against"
    if gap < 0.5:
        return "thin"
    if gap < 1.0:
        return "covered_0.5"
    return "covered_1+"


def soccer_under_label(row) -> str:
    return f"{soccer_prop_family(str(row.get('prop_n') or ''))}|{soccer_minutes_class(row)}|{cover_class(row.get('min_gap'))}"


def goblin_cover_ok(sport, min_gap) -> bool:
    need = GOBLIN_FLOOR.get(sport, 1.0)
    return min_gap is not None and min_gap == min_gap and min_gap >= need


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
                    "player_n": re.sub(
                        r"\s+",
                        " ",
                        re.sub(r"[^a-z0-9 ]+", " ", str(g.get("player") or "").strip().lower()),
                    ).strip(),
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
        step8.drop(columns=["line_r"]).drop_duplicates(
            ["date", "sport", "player_n", "prop_n", "direction"], keep="last"
        ),
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
    merged["list_l5"] = merged["l5_hits"] >= 4
    l10_board = pd.to_numeric(merged["l10"], errors="coerce")
    l10_g = pd.to_numeric(merged.get("l10_rebuilt"), errors="coerce")
    szn = pd.to_numeric(merged["szn"], errors="coerce")
    merged["l10_distinct"] = l10_g.notna() & szn.notna() & ((l10_g - szn).abs() >= 0.05)
    merged["z"] = merged["min_gap"] / pd.to_numeric(merged.get("dist_std"), errors="coerce").replace(0, np.nan)

    gob = merged[merged["book"].eq("Goblin OVER")].copy()
    std_o = merged[merged["book"].eq("Standard OVER")].copy()
    std_u = merged[merged["book"].eq("Standard UNDER")].copy()

    gob["cover_ok"] = [
        goblin_cover_ok(sp, g) for sp, g in zip(gob["sport"], gob["min_gap"])
    ]
    baseline = merged[merged["list_l5"] & merged["book"].isin(["Goblin OVER", "Standard OVER", "Standard UNDER"])]
    new_gob = gob[gob["list_l5"] & gob["cover_ok"]]
    new_std_o = std_o[std_o["list_l5"]]
    new_std_u = std_u[std_u["list_l5"]]  # soccer included

    # Soccer under classification (all decided std unders, then listed)
    soc_u = std_u[std_u["sport"].eq("Soccer")].copy()
    if len(soc_u):
        soc_u["family"] = soc_u["prop_n"].map(soccer_prop_family)
        soc_u["minutes"] = [soccer_minutes_class(r) for r in soc_u.to_dict("records")]
        soc_u["cover"] = soc_u["min_gap"].map(cover_class)
        soc_u["class"] = soc_u["family"] + "|" + soc_u["minutes"] + "|" + soc_u["cover"]
        soc_u["d"] = soc_u["def_tier"].astype(str).str.strip()
        elite = soc_u["d"].str.lower().isin(["elite", "above avg", "above average", "hard"])
        soc_u["d_under_ok"] = elite
        # Keep class: L5>=4 AND (low/rotation minutes OR cover>=0.5) AND not against
        soc_u["keep"] = (
            soc_u["list_l5"]
            & ~soc_u["cover"].eq("against")
            & (
                soc_u["minutes"].isin(["low_minutes", "rotation"])
                | (soc_u["cover"].isin(["covered_0.5", "covered_1+"]) & soc_u["d_under_ok"])
                | (soc_u["cover"].eq("covered_1+"))
            )
        )
    else:
        soc_u["keep"] = False

    def class_table(df, col):
        out = {}
        for k, sub in df.groupby(col):
            out[str(k)] = overall(sub)
        return dict(sorted(out.items(), key=lambda kv: (-(kv[1]["n"] or 0), kv[0])))

    # Tennis / MLB under extra tags (classify, do not drop soccer)
    tu = std_u[std_u["sport"].eq("Tennis") & std_u["list_l5"]].copy()
    tu["cover"] = tu["min_gap"].map(cover_class)
    mu = std_u[std_u["sport"].eq("MLB") & std_u["list_l5"]].copy()
    mu["far"] = pd.to_numeric(mu["min_gap"], errors="coerce") >= 2
    wu = std_u[std_u["sport"].eq("WNBA") & std_u["list_l5"]].copy()

    new_std_u_keep = new_std_u.copy()
    if len(soc_u):
        keep_idx = set(soc_u.index[soc_u["keep"]])
        # listed non-soccer unders stay; soccer listed only if keep
        mask = []
        for i, r in new_std_u.iterrows():
            if r["sport"] != "Soccer":
                mask.append(True)
            else:
                mask.append(i in keep_idx)
        new_std_u_keep = new_std_u[mask]

    # MLB under far-cover fade as a class, still on "all listed unders"; optional tight list
    tight_u = new_std_u_keep.copy()
    if len(tight_u):
        far_mlb = tight_u["sport"].eq("MLB") & (pd.to_numeric(tight_u["min_gap"], errors="coerce") >= 2)
        far_te = tight_u["sport"].eq("Tennis") & (pd.to_numeric(tight_u["min_gap"], errors="coerce") >= 3)
        tight_u = tight_u[~(far_mlb | far_te)]

    new_list = pd.concat([new_gob, new_std_o, new_std_u_keep], ignore_index=True)
    new_tight = pd.concat([new_gob, new_std_o, tight_u], ignore_index=True)

    fill = {
        "soccer_avg_min": {
            "n": int((merged["sport"].eq("Soccer")).sum()),
            "filled": int((merged["sport"].eq("Soccer") & pd.to_numeric(merged["avg_min"], errors="coerce").notna()).sum()),
        },
        "soccer_starter_tier": {
            "n": int((merged["sport"].eq("Soccer")).sum()),
            "filled": int(
                (
                    merged["sport"].eq("Soccer")
                    & merged["starter_tier"].astype(str).str.upper().isin(["STARTER", "ROTATION", "SUB"])
                ).sum()
            ),
        },
        "tennis_def_rank": {
            "n": int((merged["sport"].eq("Tennis")).sum()),
            "filled": int((merged["sport"].eq("Tennis") & (pd.to_numeric(merged["def_rank"], errors="coerce") > 0)).sum()),
        },
        "l10_distinct_from_season": {
            "n": int(szn.notna().sum()),
            "filled": int(merged["l10_distinct"].sum()),
        },
        "mlb_bat_order": {
            "n": int((merged["sport"].eq("MLB")).sum()),
            "filled": int(
                (
                    merged["sport"].eq("MLB")
                    & merged["bat_order"].astype(str).str.len().gt(0)
                    & ~merged["bat_order"].astype(str).isin(["nan", "None", ""])
                ).sum()
            ),
        },
        "tennis_aces_l5": {
            "n": int((merged["sport"].eq("Tennis") & merged["prop_n"].str.contains("ace", na=False)).sum()),
            "filled": int(
                (
                    merged["sport"].eq("Tennis")
                    & merged["prop_n"].str.contains("ace", na=False)
                    & merged["l5_hits"].notna()
                ).sum()
            ),
        },
        "dist_std": {
            "n": int(len(merged)),
            "filled": int(pd.to_numeric(merged.get("dist_std"), errors="coerce").notna().sum()),
        },
    }
    for k, v in fill.items():
        v["pct"] = rate(v["filled"], v["n"])

    by_book_new = {
        "Goblin OVER": {
            "baseline_l5": overall(gob[gob["list_l5"]]),
            "new_cover_floor": overall(new_gob),
            "dropped": overall(gob[gob["list_l5"] & ~gob["cover_ok"]]),
        },
        "Standard OVER": {
            "baseline_l5": overall(new_std_o),
            "by_sport": {sp: overall(new_std_o[new_std_o["sport"].eq(sp)]) for sp in B.SPORTS},
        },
        "Standard UNDER": {
            "baseline_l5_including_soccer": overall(new_std_u),
            "classified_keep_including_soccer": overall(new_std_u_keep),
            "tight_fade_far_mlb_tennis": overall(tight_u),
            "by_sport_listed": {sp: overall(new_std_u[new_std_u["sport"].eq(sp)]) for sp in B.SPORTS},
            "by_sport_keep": {sp: overall(new_std_u_keep[new_std_u_keep["sport"].eq(sp)]) for sp in B.SPORTS},
        },
    }

    soc_report = {}
    if len(soc_u):
        soc_report = {
            "all_decided": overall(soc_u),
            "listed_l5": overall(soc_u[soc_u["list_l5"]]),
            "keep": overall(soc_u[soc_u["keep"]]),
            "drop": overall(soc_u[soc_u["list_l5"] & ~soc_u["keep"]]),
            "by_family": class_table(soc_u, "family"),
            "by_minutes": class_table(soc_u, "minutes"),
            "by_cover": class_table(soc_u, "cover"),
            "by_class_listed": class_table(soc_u[soc_u["list_l5"]], "class"),
            "by_minutes_listed": class_table(soc_u[soc_u["list_l5"]], "minutes"),
            "by_family_listed": class_table(soc_u[soc_u["list_l5"]], "family"),
            "d_under_ok_listed": {
                "pass": overall(soc_u[soc_u["list_l5"] & soc_u["d_under_ok"]]),
                "fail": overall(soc_u[soc_u["list_l5"] & ~soc_u["d_under_ok"]]),
            },
        }

    out = {
        "window": {
            "first": str(merged["date"].min()),
            "last": str(merged["date"].max()),
            "n_joined": int(len(merged)),
            "n_files": n_files,
        },
        "fill_rates_for_skip_data": fill,
        "baseline_live_list": overall(baseline),
        "new_list": overall(new_list),
        "new_list_tight_unders": overall(new_tight),
        "lift_pp": (
            None
            if len(baseline) == 0 or len(new_list) == 0
            else round(
                100.0
                * (
                    float(new_list["hit"].sum()) / len(new_list)
                    - float(baseline["hit"].sum()) / len(baseline)
                ),
                1,
            )
        ),
        "by_book": by_book_new,
        "soccer_standard_under": soc_report,
        "tennis_under_listed_by_cover": class_table(tu, "cover") if len(tu) else {},
        "mlb_under_listed_far_ge2": {
            "far": overall(mu[mu["far"]]) if len(mu) else overall(mu),
            "not_far": overall(mu[~mu["far"]]) if len(mu) else overall(mu),
        },
        "goblin_floor": GOBLIN_FLOOR,
        "soccer_under_keep_rule": "L5>=4 and not against the line and (low/rotation minutes OR cover>=1 OR (cover>=0.5 and D Elite/Above))",
    }
    dest = OPEN / "data" / "reports" / "three_book_gate_backtest.json"
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2)[:12000])
    print("wrote", dest)


if __name__ == "__main__":
    main()
