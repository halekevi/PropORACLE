"""Rebounds D polarity: overall vs CSV reb-allowed vs flipped, plus PR/RA bleed."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import graded_paths, l5_dir, l10_dir, num, pick_book  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

CSV = default_csv_path()
PROPS = {"rebounds": "reb", "reb+ast": "ra", "pts+reb": "pr"}


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def canon_team(raw):
    s = str(raw or "").strip().upper()
    if not s or "/" in s:
        return ""
    return TEAM_MAP.get(s, s)


def load_csv():
    df = pd.read_csv(CSV)
    df["team"] = df["team"].astype(str).str.strip().str.upper().map(lambda t: TEAM_MAP.get(t, t))
    by = {}
    for rec in df.to_dict("records"):
        by[rec["team"]] = rec
    return by


def collect(by_team):
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
            line = num(r.get("line"))
            player = str(r.get("player") or "").strip()
            key = (date, player, prop, line)
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_dir(r, "OVER")
            l10 = l10_dir(r, "OVER")
            overall = normalize_def_tier_label(r.get("def_tier") or r.get("DEF_TIER") or r.get("def")) or ""
            stat = normalize_def_tier_label(r.get("stat_def_tier") or "") or ""
            opp = canon_team(r.get("opp_team") or r.get("opp") or r.get("Opp"))
            csv = by_team.get(opp) or {}
            cat = PROPS[prop]
            csv_tier = normalize_def_tier_label(csv.get(f"{cat}_tier")) or ""
            csv_rank = csv.get(f"{cat}_rank")
            csv_opp = csv.get(f"opp_{cat}")
            hit = 1 if str(r.get("result")).strip().upper() == "HIT" else 0
            rows.append(
                {
                    "prop": prop,
                    "line": line,
                    "hit": hit,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "l10_ok": l10 is not None and l10 >= 8,
                    "overall": overall,
                    "stat": stat,
                    "csv": csv_tier,
                    "csv_rank": csv_rank,
                    "csv_opp": csv_opp,
                    "opp": opp,
                    "agree_overall_csv": overall == csv_tier and bool(overall) and bool(csv_tier),
                }
            )
    return rows


def split(rows, key):
    d_pass = [r for r in rows if d_aligned("WNBA", "OVER", r.get(key), "rebounds")]
    d_fail = [r for r in rows if r.get(key) and not d_aligned("WNBA", "OVER", r.get(key), "rebounds")]
    flipped_pass = [r for r in rows if d_aligned("WNBA", "UNDER", r.get(key), "rebounds")]
    flipped_fail = [r for r in rows if r.get(key) and not d_aligned("WNBA", "UNDER", r.get(key), "rebounds")]
    print(f"    graded/overall D-pass {hr(sum(x['hit'] for x in d_pass), len(d_pass))}   D-fail {hr(sum(x['hit'] for x in d_fail), len(d_fail))}" if key == "overall" else "")
    return d_pass, d_fail, flipped_pass, flipped_fail


def report_key(label, rows, key):
    d_pass = [r for r in rows if d_aligned("WNBA", "OVER", r.get(key), "rebounds")]
    d_fail = [r for r in rows if r.get(key) and not d_aligned("WNBA", "OVER", r.get(key), "rebounds")]
    flip_pass = [r for r in rows if d_aligned("WNBA", "UNDER", r.get(key), "rebounds")]
    flip_fail = [r for r in rows if r.get(key) and not d_aligned("WNBA", "UNDER", r.get(key), "rebounds")]
    print(
        f"    {label:22}  D-pass {hr(sum(r['hit'] for r in d_pass), len(d_pass)):18}  "
        f"D-fail {hr(sum(r['hit'] for r in d_fail), len(d_fail)):18}  "
        f"FLIP-pass {hr(sum(r['hit'] for r in flip_pass), len(flip_pass)):18}  "
        f"FLIP-fail {hr(sum(r['hit'] for r in flip_fail), len(flip_fail))}"
    )


def block(title, rows):
    print(f"\n{title}  n={len(rows)}  {hr(sum(r['hit'] for r in rows), len(rows))}")
    if not rows:
        return
    n_stat = sum(1 for r in rows if r["stat"])
    n_csv = sum(1 for r in rows if r["csv"])
    n_agree = sum(1 for r in rows if r["agree_overall_csv"])
    print(f"    overall filled {sum(1 for r in rows if r['overall'])}/{len(rows)}  stat_def on row {n_stat}/{len(rows)}  csv reb/pr/ra {n_csv}/{len(rows)}  overall==csv {n_agree}/{len(rows)}")
    report_key("overall def_tier", rows, "overall")
    report_key("csv stat (reb/pr/ra)", rows, "csv")
    if n_stat:
        report_key("row stat_def_tier", rows, "stat")


def main():
    by = load_csv()
    rows = collect(by)
    print(f"collected {len(rows)}")
    # CSV sanity: Elite should have lower opp_reb than Weak
    print("\nCSV polarity check (reb): Elite opp_reb vs Weak opp_reb")
    from collections import defaultdict
    buckets = defaultdict(list)
    for rec in by.values():
        t = normalize_def_tier_label(rec.get("reb_tier"))
        if t and rec.get("opp_reb") is not None:
            buckets[t].append(float(rec["opp_reb"]))
    for t in ("Elite", "Above Avg", "Avg", "Below Avg", "Weak"):
        xs = buckets.get(t) or []
        if xs:
            print(f"  {t:12} n_teams={len(xs)}  mean opp_reb={sum(xs)/len(xs):.2f}")

    for prop, lab in (("rebounds", "REBOUNDS"), ("reb+ast", "REB+AST"), ("pts+reb", "PTS+REB")):
        rs = [r for r in rows if r["prop"] == prop]
        print(f"\n========== {lab} ==========")
        block("all Goblin OVER", rs)
        l5 = [r for r in rs if r["l5eq5"]]
        block("L5=5", l5)
        if prop == "rebounds":
            block("L5=5 line 3.5", [r for r in l5 if r["line"] == 3.5])
            block("L5=5 line <=2.5", [r for r in l5 if r["line"] is not None and r["line"] <= 2.5])
            block("L5=5 line >=4.5", [r for r in l5 if r["line"] is not None and r["line"] >= 4.5])
            block("L5=5+L10 line 3.5", [r for r in l5 if r["l10_ok"] and r["line"] == 3.5])


if __name__ == "__main__":
    main()
