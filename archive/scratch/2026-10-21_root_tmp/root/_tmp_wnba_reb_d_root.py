"""Pin Rebounds D: label bug vs market inversion; PR/RA bleed on reb half vs own axis."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num, pick_book  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

PROPS = {"rebounds": "reb", "reb+ast": "ra", "pts+reb": "pr"}


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
            opp = canon_team(r.get("opp_team"))
            csv = by_team.get(opp) or {}
            cat = PROPS[prop]
            rows.append(
                {
                    "prop": prop,
                    "line": line,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "l5eq5": l5 is not None and l5 >= 5,
                    "l10_ok": l10 is not None and l10 >= 8,
                    "overall": normalize_def_tier_label(r.get("def_tier") or r.get("def")) or "",
                    "own": normalize_def_tier_label(csv.get(f"{cat}_tier")) or "",
                    "reb": normalize_def_tier_label(csv.get("reb_tier")) or "",
                    "pts": normalize_def_tier_label(csv.get("pts_tier")) or "",
                    "pr": normalize_def_tier_label(csv.get("pr_tier")) or "",
                    "ra": normalize_def_tier_label(csv.get("ra_tier")) or "",
                }
            )
    return rows


def dsplit(rows, key):
    p = [r for r in rows if d_aligned("WNBA", "OVER", r.get(key), r["prop"])]
    f = [r for r in rows if r.get(key) and not d_aligned("WNBA", "OVER", r.get(key), r["prop"])]
    fp = [r for r in rows if d_aligned("WNBA", "UNDER", r.get(key), r["prop"])]
    return p, f, fp


def show(label, rows, key):
    p, f, fp = dsplit(rows, key)
    print(
        f"    {label:28} D-pass {hr(sum(r['hit'] for r in p), len(p)):18}  "
        f"D-fail {hr(sum(r['hit'] for r in f), len(f)):18}  "
        f"FLIP-pass {hr(sum(r['hit'] for r in fp), len(fp))}"
    )


def block(title, rows):
    print(f"\n  {title}  n={len(rows)}  {hr(sum(r['hit'] for r in rows), len(rows))}")
    if len(rows) < 8:
        return
    show("csv OWN axis", rows, "own")
    show("csv reb_tier (shared half)", rows, "reb")
    show("csv pts_tier (scoring)", rows, "pts")
    show("graded overall def_tier", rows, "overall")


def polarity(by_team, cat, col):
    buckets = defaultdict(list)
    for rec in by_team.values():
        t = normalize_def_tier_label(rec.get(f"{cat}_tier"))
        v = rec.get(col)
        if t and v is not None:
            buckets[t].append(float(v))
    print(f"\nCSV polarity  {cat}  ({col}, rank 1 = stingiest)")
    for t in ("Elite", "Above Avg", "Avg", "Below Avg", "Weak"):
        xs = buckets.get(t) or []
        if xs:
            print(f"  {t:12} n={len(xs)}  mean {col}={sum(xs)/len(xs):.2f}")


def agree(by_team, a, b):
    n = same = 0
    for rec in by_team.values():
        ta = normalize_def_tier_label(rec.get(f"{a}_tier"))
        tb = normalize_def_tier_label(rec.get(f"{b}_tier"))
        if not ta or not tb:
            continue
        n += 1
        same += int(ta == tb)
    print(f"  {a}_tier == {b}_tier  {same}/{n}" + (f"  ({100.0*same/n:.0f}%)" if n else ""))


def main():
    by = load_csv()
    polarity(by, "reb", "opp_reb")
    polarity(by, "pr", "opp_pr")
    polarity(by, "ra", "opp_ra")
    polarity(by, "pts", "opp_pts")
    print("\nTeam-level tier agreement (WNBA CSV)")
    agree(by, "reb", "pr")
    agree(by, "reb", "ra")
    agree(by, "reb", "pts")
    agree(by, "pr", "pts")
    agree(by, "ra", "pts")

    rows = collect(by)
    print(f"\ncollected {len(rows)} Goblin OVER reb/PR/RA faces")
    slices = [
        ("REBOUNDS 3.5  L5=5", lambda r: r["prop"] == "rebounds" and r["l5eq5"] and r["line"] == 3.5),
        ("REBOUNDS 3.5  L5=5+L10", lambda r: r["prop"] == "rebounds" and r["l5eq5"] and r["l10_ok"] and r["line"] == 3.5),
        ("REBOUNDS 4.5+ L5=5", lambda r: r["prop"] == "rebounds" and r["l5eq5"] and r["line"] is not None and r["line"] >= 4.5),
        ("REBOUNDS 4.5+ L5=5+L10", lambda r: r["prop"] == "rebounds" and r["l5eq5"] and r["l10_ok"] and r["line"] is not None and r["line"] >= 4.5),
        ("REB+AST L5=5", lambda r: r["prop"] == "reb+ast" and r["l5eq5"]),
        ("REB+AST L5=5+L10", lambda r: r["prop"] == "reb+ast" and r["l5eq5"] and r["l10_ok"]),
        ("PTS+REB L5=5", lambda r: r["prop"] == "pts+reb" and r["l5eq5"]),
        ("PTS+REB L5=5+L10", lambda r: r["prop"] == "pts+reb" and r["l5eq5"] and r["l10_ok"]),
    ]
    for title, pred in slices:
        print(f"\n========== {title} ==========")
        block(title, [r for r in rows if pred(r)])


if __name__ == "__main__":
    main()
