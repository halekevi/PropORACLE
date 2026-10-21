"""Saves keep: own-D fill + before/after hit rates 2026-09-01..21."""
from __future__ import annotations

import sys
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import soccer_keep_eligible  # noqa: E402

WEAK = {"Weak", "Below Avg"}


def pname(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold().replace(".", " ").replace("'", "").replace("’", "")
    return " ".join(t.split())


def pflt(v):
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def on_date(start_time, game_date, day: str) -> bool:
    if start_time not in (None, "", "nan"):
        ts = pd.to_datetime(start_time, errors="coerce", utc=True)
        if pd.notna(ts):
            return ts.tz_convert("America/New_York").strftime("%Y-%m-%d") == day
    gd = str(game_date or "")[:10]
    return gd == day


def grade(actual, line, side):
    if actual is None or line is None:
        return None
    if abs(actual - line) < 1e-9:
        return "PUSH"
    hit = actual > line if side == "OVER" else actual < line
    return "HIT" if hit else "MISS"


def load_actuals(day: str) -> dict:
    p = MAIN / "outputs" / day / f"actuals_soccer_{day}.csv"
    if not p.is_file():
        return {}
    df = pd.read_csv(p)
    out = {}
    for _, r in df.iterrows():
        act = pflt(r.get("actual"))
        if act is None:
            continue
        out[(pname(r.get("player")), canon_prop("Soccer", str(r.get("prop_type") or "")))] = act
    return out


def load_step8(day: str) -> pd.DataFrame:
    p = MAIN / "outputs" / day / "soccer" / "step8_soccer_direction.csv"
    if p.is_file():
        return pd.read_csv(p, low_memory=False)
    x = MAIN / "outputs" / day / "soccer" / "step8_soccer_direction_clean.xlsx"
    if x.is_file():
        return pd.read_excel(x)
    return pd.DataFrame()


def own_tier(r: dict) -> str:
    for k in ("own_def_tier", "OWN_DEF_TIER", "Own Def Tier"):
        lab = normalize_def_tier_label(r.get(k))
        if lab:
            return lab
    return ""


def opp_def(r: dict) -> str:
    return normalize_def_tier_label(r.get("def") or r.get("def_tier") or r.get("DEF_TIER")) or ""


def pick(r: dict) -> str:
    p = str(r.get("pick_type") or "").strip().lower()
    if "goblin" in p:
        return "Goblin"
    if "standard" in p:
        return "Standard"
    if "demon" in p:
        return "Demon"
    return ""


def side_of(r: dict) -> str:
    s = str(r.get("side") or r.get("final_bet_direction") or r.get("bet_direction") or r.get("model_dir") or "").strip().upper()
    return s if s in {"OVER", "UNDER"} else ""


def l5_of(r: dict, side: str) -> float | None:
    keys = ("l5_under", "last5_under") if side == "UNDER" else ("l5_over", "last5_over")
    for k in keys:
        v = pflt(r.get(k))
        if v is not None:
            return v
    return None


def tally(rows: list) -> str:
    hit = sum(1 for x in rows if x == "HIT")
    miss = sum(1 for x in rows if x == "MISS")
    d = hit + miss
    if not d:
        return f"— n={len(rows)}"
    return f"{100.0 * hit / d:.1f}% ({hit}/{d})"


def main() -> None:
    start = date(2026, 9, 1)
    end = date(2026, 9, 21)
    cells: dict[str, list] = defaultdict(list)
    own_n = own_filled = opp_only = 0
    goblin_l5 = []
    d = start
    while d <= end:
        day = d.isoformat()
        df = load_step8(day)
        acts = load_actuals(day)
        if df is None or df.empty:
            d += timedelta(days=1)
            continue
        seen = set()
        for _, row in df.iterrows():
            rec = {k: row.get(k) for k in df.columns}
            rec["sport"] = "SOCCER"
            rec["prop"] = rec.get("prop_type") or rec.get("prop")
            rec["side"] = side_of(rec)
            rec["pick_type"] = pick(rec)
            if canon_prop("Soccer", str(rec.get("prop") or "")) != "saves":
                continue
            if rec["pick_type"] not in {"Goblin", "Standard"}:
                continue
            if rec["pick_type"] == "Goblin" and rec["side"] != "OVER":
                continue
            if rec["side"] not in {"OVER", "UNDER"}:
                continue
            if not on_date(rec.get("start_time"), rec.get("game_date"), day):
                continue
            key = (
                pname(rec.get("player")),
                rec["pick_type"],
                rec["side"],
                pflt(rec.get("line")),
            )
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_of(rec, rec["side"])
            if l5 is None or l5 < 4:
                continue
            own = own_tier(rec)
            opp = opp_def(rec)
            own_n += 1
            if own:
                own_filled += 1
            elif opp:
                opp_only += 1
            g = grade(
                acts.get((pname(rec.get("player")), "saves")),
                pflt(rec.get("line")),
                rec["side"],
            )
            if g not in {"HIT", "MISS"}:
                continue
            own_ok = bool(own) and d_aligned("Soccer", rec["side"], own, "saves")
            opp_ok = bool(opp) and d_aligned("Soccer", rec["side"], opp, "saves")
            old_d = own_ok if own else opp_ok
            rec["l5_over"] = rec.get("l5_over") or rec.get("last5_over")
            rec["l5_under"] = rec.get("l5_under") or rec.get("last5_under")
            live_now = soccer_keep_eligible(rec)
            if rec["pick_type"] == "Goblin":
                goblin_l5.append(g)
                cells["goblin_l5ge4"].append(g)
                if old_d:
                    cells["old_live_goblin_wrong_fallback"].append(g)
                if own_ok:
                    cells["goblin_own_d_required"].append(g)
            cells["l5ge4_all_books"].append(g)
            if old_d:
                cells["old_live_all_wrong_fallback"].append(g)
            if own_ok:
                cells["own_d_required_all"].append(g)
            if live_now:
                cells["restored_own_d_keep"].append(g)
        d += timedelta(days=1)

    print(f"Saves L5>=4 rows (Goblin OVER + Standard O/U), ET same-day, 2026-09-01..21")
    print(f"OWN_DEF_TIER filled: {own_filled}/{own_n} ({0 if not own_n else 100*own_filled/own_n:.0f}%)")
    print(f"own missing, opp DEF_TIER present: {opp_only}")
    print("Goblin OVER L5>=4 (catalog shape):", tally(cells["goblin_l5ge4"]))
    print("Old live Goblin (own D else opp D):", tally(cells["old_live_goblin_wrong_fallback"]))
    print("Goblin L5>=4 + own D required (no fallback):", tally(cells["goblin_own_d_required"]))
    print("All-book L5>=4:", tally(cells["l5ge4_all_books"]))
    print("Old live all-book (own else opp D):", tally(cells["old_live_all_wrong_fallback"]))
    print("All-book L5>=4 + own D required:", tally(cells["own_d_required_all"]))
    print("Restored soccer_keep_eligible (own D, no opp-D fallback):", tally(cells["restored_own_d_keep"]))


if __name__ == "__main__":
    main()
