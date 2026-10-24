import json, re, sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

_REPO = Path(r"H:\PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "Sports" / "Tennis" / "scripts"), str(_REPO / "scripts")]

from tennis_shared import (
    build_sackmann_player_index,
    ensure_sackmann_matches,
    norm_key,
    norm_tennis_prop,
    parse_sackmann_tourney_date,
)
from utils.tennis_keep_gates import (
    GAMES_WON_L5_SECOND_WON_MIN,
    tennis_goblin_keep_eligible,
    tennis_standard_games_won_over_eligible,
    tennis_standard_under_serve_eligible,
)

OUT = _REPO / "data" / "reports" / "tennis_board_category_hit_rates.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]


def finite(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def pct(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def book(row):
    pt = str(row.get("pick_type") or row.get("book") or "").strip().lower()
    d = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt:
        return f"Demon {d}" if d in ("OVER", "UNDER") else "Demon"
    if "goblin" in pt:
        return f"Goblin {d if d in ('OVER', 'UNDER') else 'OVER'}"
    if d in ("OVER", "UNDER"):
        return f"Standard {d}"
    return "Unknown"


def iter_props(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for k in ("props", "legs", "rows", "graded", "items"):
            v = data.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                if "player" in v[0] or "prop" in v[0] or "sport" in v[0]:
                    return v
    return []


def l5(row, side):
    keys = ("l5_over", "last5_over", "L5 Over", "line_hits_over_5") if side == "OVER" else ("l5_under", "last5_under", "L5 Under", "line_hits_under_5")
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def l10(row, side):
    keys = ("l10_over", "last10_over", "L10 Over", "line_hits_over_10") if side == "OVER" else ("l10_under", "last10_under", "L10 Under", "line_hits_under_10")
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


print("Loading Sackmann for keep-factor fields…")
matches = ensure_sackmann_matches()
index = build_sackmann_player_index(matches)
hand_by = {}
for _, rd in matches.iterrows():
    for nc, hc in (("winner_name", "winner_hand"), ("loser_name", "loser_hand")):
        nm = norm_key(str(rd.get(nc) or ""))
        hd = str(rd.get(hc) or "").strip().upper()
        if nm and hd in {"L", "R", "U"}:
            hand_by[nm] = hd

# Collect ALL tennis decided legs with provenance
seen_names = set()
paths = []
for d in GRADED_DIRS:
    if not d.exists():
        continue
    for p in sorted(d.glob("graded_props_20*.json")):
        if p.name in seen_names:
            continue
        seen_names.add(p.name)
        # Prefer MAIN over OPEN when same filename — already handled by seen_names order
        paths.append(p)

legs = []
seen = set()
date_min = date_max = None
source_files = 0
for path in paths:
    rows = iter_props(path)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    if not m:
        continue
    slate = date.fromisoformat(m.group(1))
    source_files += 1
    if date_min is None or slate < date_min:
        date_min = slate
    if date_max is None or slate > date_max:
        date_max = slate
    tree = "main_cp" if "PropORACLE_main_cp" in str(path) else "open"
    for row in rows:
        if str(row.get("sport") or "").strip().lower() != "tennis":
            continue
        result = str(row.get("result") or "").strip().upper()
        if result not in ("HIT", "MISS", "VOID"):
            continue
        prop_raw = str(row.get("prop") or row.get("prop_type") or "").strip()
        if not prop_raw:
            continue
        # keep raw label for 1st-set etc.
        pnorm = norm_tennis_prop(prop_raw)
        # fix 1st-set mis-map: if raw has 1st/first, use raw-based norm
        low = prop_raw.lower()
        if "1st" in low or "first set" in low:
            if "ace" in low:
                pnorm = "first_set_aces"
            elif "double" in low:
                pnorm = "first_set_double_faults"
            elif "won" in low and "game" in low:
                pnorm = "first_set_games_won"
            elif "game" in low:
                pnorm = "first_set_total_games"
        b = book(row)
        player = str(row.get("player") or "").strip()
        line = finite(row.get("line"))
        pk = norm_key(player)
        # Stable dedupe: slate+player+prop+book+line+result+projection id
        pid = str(row.get("pp_projection_id") or row.get("projection_id") or "")
        dkey = f"{slate}|{pk}|{pnorm}|{b}|{line}|{result}|{pid}"
        if dkey in seen:
            continue
        seen.add(dkey)

        side = "OVER" if "OVER" in b else "UNDER"
        prior = []
        for rec in index.get(pk) or []:
            dt = parse_sackmann_tourney_date(rec.get("date"))
            if dt and dt < slate:
                prior.append(rec)
        sw_vals = [finite(r.get("second_won_pct")) for r in prior[:5]]
        sw = None
        sw_vals = [v for v in sw_vals if v is not None]
        if sw_vals:
            sw = sum(sw_vals) / len(sw_vals)

        opp_name = norm_key(str(row.get("opp_team") or row.get("opponent") or row.get("opp") or ""))
        today_hand = hand_by.get(opp_name) if opp_name else None
        last_hand = str((prior[0].get("opp_hand") if prior else "") or "").strip().upper() or None
        if last_hand and last_hand not in {"L", "R", "U"}:
            last_hand = None
        opp_hand = today_hand or last_hand
        lefty = True if opp_hand == "L" else (False if opp_hand in {"R", "U"} else None)

        # Build gate row
        gate_row = {
            "sport": "TENNIS",
            "pick_type": "Goblin" if "Goblin" in b else ("Demon" if "Demon" in b else "Standard"),
            "side": side,
            "direction": side,
            "prop": prop_raw,
            "line": line,
            "standard_line": finite(row.get("standard_line") or row.get("Standard Line")),
            "l5_over": l5(row, "OVER"),
            "l5_under": l5(row, "UNDER"),
            "l10_over": l10(row, "OVER"),
            "l10_under": l10(row, "UNDER"),
            "dist_l5": finite(row.get("dist_l5") or row.get("Dist_L5")),
            "stat_last5_avg": finite(row.get("stat_last5_avg") or row.get("Last 5 Avg")),
            "opp_lefty": "Y" if lefty is True else ("N" if lefty is False else ""),
            "l5_second_won_pct": sw,
        }

        legs.append({
            "slate": slate.isoformat(),
            "tree": tree,
            "file": path.name,
            "player": player,
            "prop_raw": prop_raw,
            "prop": pnorm,
            "book": b,
            "line": line,
            "result": result,
            "hit": 1 if result == "HIT" else 0,
            "void": 1 if result == "VOID" else 0,
            "void_reason": str(row.get("void_reason") or ""),
            "actual": finite(row.get("actual_value") if row.get("actual_value") not in (None, "") else row.get("actual")),
            "actual_source": str(row.get("actual_source") or ""),
            "l5": l5(row, side),
            "l10": l10(row, side),
            "sw": sw,
            "lefty": lefty,
            "gate_row": gate_row,
        })

print(f"legs={len(legs)} files={source_files} window={date_min}..{date_max}")

# Compare Aces Std OVER under different filters to explain 29.6 vs 24.4
aces_so = [l for l in legs if l["prop"] == "aces" and l["book"] == "Standard OVER" and l["result"] in ("HIT", "MISS")]
print(f"\n=== Std OVER Aces diagnostics ===")
print(f"all decided: n={len(aces_so)} hit={pct(sum(l['hit'] for l in aces_so), len(aces_so))}%")

# by actual_source
by_src = defaultdict(lambda: {"n": 0, "h": 0})
for l in aces_so:
    src = l["actual_source"] or "(blank)"
    by_src[src]["n"] += 1
    by_src[src]["h"] += l["hit"]
print("by actual_source:")
for src, st in sorted(by_src.items(), key=lambda x: -x[1]["n"]):
    print(f"  {src:40} n={st['n']:4} hit={pct(st['h'], st['n'])}%")

# with actual present vs missing coerced
with_act = [l for l in aces_so if l["actual"] is not None]
no_act = [l for l in aces_so if l["actual"] is None]
print(f"with actual: n={len(with_act)} hit={pct(sum(l['hit'] for l in with_act), len(with_act))}%")
print(f"actual None but HIT/MISS: n={len(no_act)}")

# date split: before/after sackmann backfill era
for cutoff in ("2026-08-01", "2026-08-15", "2026-08-28"):
    pre = [l for l in aces_so if l["slate"] < cutoff]
    post = [l for l in aces_so if l["slate"] >= cutoff]
    print(f"slate <{cutoff}: n={len(pre)} hit={pct(sum(l['hit'] for l in pre), len(pre))}% | >={cutoff}: n={len(post)} hit={pct(sum(l['hit'] for l in post), len(post))}%")

# open-only vs main-preferred (current)
# Re-scan open-only for comparison
open_aces = [l for l in aces_so if l["tree"] == "open"]
main_aces = [l for l in aces_so if l["tree"] == "main_cp"]
print(f"from main_cp files: n={len(main_aces)} hit={pct(sum(l['hit'] for l in main_aces), len(main_aces))}%")
print(f"from open files (only when filename not in main): n={len(open_aces)} hit={pct(sum(l['hit'] for l in open_aces), len(open_aces))}%")

# Load all_ranked for comparison
ranked_path = _REPO / "data" / "reports" / "all_ranked_prop_hit_rates.json"
ranked_aces = None
if ranked_path.exists():
    ranked = json.loads(ranked_path.read_text(encoding="utf-8"))
    # find tennis standard over aces
    cells = ranked.get("cells") or ranked.get("rows") or ranked.get("sports") or []
    # structure varies — search recursively
    def walk(obj, path=""):
        if isinstance(obj, dict):
            sport = str(obj.get("sport") or obj.get("Sport") or "")
            book = str(obj.get("book") or obj.get("Book") or "")
            prop = str(obj.get("prop") or obj.get("Prop") or obj.get("prop_norm") or "")
            if "tennis" in sport.lower() and "standard" in book.lower() and "over" in book.lower() and "ace" in prop.lower():
                return obj
            for k, v in obj.items():
                found = walk(v, path + "/" + str(k))
                if found:
                    return found
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                found = walk(v, path + f"[{i}]")
                if found:
                    return found
        return None
    ranked_aces = walk(ranked)
    print(f"all_ranked_prop_hit_rates tennis Std OVER Aces cell: {ranked_aces}")

# Build board ledger
BOARD_ORDER = [
    ("Total Games", "match_total_games"),
    ("Total Games Won", "games_won"),
    ("Fantasy Score", "fantasy_score"),
    ("Total Sets", "total_sets"),
    ("Aces", "aces"),
    ("Break Points Won", "break_points_won"),
    ("Total Tie Breaks", "total_tie_breaks"),
    ("1st Set Total Games", "first_set_total_games"),
    ("1st Set Total Games Won", "first_set_games_won"),
    ("1st Set Aces", "first_set_aces"),
    ("Double Faults", "double_faults"),
]

BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER", "Demon OVER", "Demon UNDER")


def cell(subset):
    decided = [l for l in subset if l["result"] in ("HIT", "MISS")]
    voids = [l for l in subset if l["result"] == "VOID"]
    n = len(decided)
    h = sum(l["hit"] for l in decided)
    return {"n": n, "hits": h, "hit_pct": pct(h, n), "void": len(voids)}


props_out = []
for label, pnorm in BOARD_ORDER:
    subset = [l for l in legs if l["prop"] == pnorm]
    # also match fantasy by raw
    if pnorm == "fantasy_score":
        subset = [l for l in legs if l["prop"] == "fantasy_score" or "fantasy" in l["prop_raw"].lower()]
    if pnorm == "break_points_won":
        subset = [l for l in legs if l["prop"] == "break_points_won" or ("break" in l["prop_raw"].lower() and "point" in l["prop_raw"].lower())]

    entry = {"label": label, "norm": pnorm, "books": [], "keep_paths": []}
    for b in BOOKS:
        st = cell([l for l in subset if l["book"] == b])
        if st["n"] == 0 and st["void"] == 0:
            continue
        entry["books"].append({"book": b, **st})

    # Keep paths with explicit n
    if pnorm == "aces":
        su = [l for l in subset if l["book"] == "Standard UNDER" and l["result"] in ("HIT", "MISS")]
        entry["keep_paths"].append({"path": "Std UNDER ungated", **cell(su), "call": "KEEP"})
    if pnorm == "double_faults":
        su = [l for l in subset if l["book"] == "Standard UNDER" and l["result"] in ("HIT", "MISS")]
        entry["keep_paths"].append({"path": "Std UNDER ungated", **cell(su), "call": "KEEP"})
    if pnorm == "games_won":
        gob = [l for l in subset if l["book"] == "Goblin OVER" and l["result"] in ("HIT", "MISS")]
        # path A: std discount — often missing on graded
        gap = [l for l in gob if tennis_goblin_keep_eligible({**l["gate_row"], "standard_line": l["gate_row"].get("standard_line")}) and finite(l["gate_row"].get("standard_line")) is not None and (finite(l["gate_row"].get("standard_line")) - (l["line"] or 0)) >= 4]
        # Better compute discount manually
        gap = []
        for l in gob:
            std = l["gate_row"].get("standard_line")
            if std is None or l["line"] is None:
                continue
            if std - l["line"] >= 4:
                gap.append(l)
        lefty = [l for l in gob if l["l10"] is not None and l["l10"] >= 8 and l["lefty"] is True]
        sw = [l for l in gob if l["l10"] is not None and l["l10"] >= 8 and l["sw"] is not None and l["sw"] >= GAMES_WON_L5_SECOND_WON_MIN]
        union_factor = [l for l in gob if l["l10"] is not None and l["l10"] >= 8 and (l["lefty"] is True or (l["sw"] is not None and l["sw"] >= GAMES_WON_L5_SECOND_WON_MIN))]
        # full keep via function
        keep_fn = [l for l in gob if tennis_goblin_keep_eligible(l["gate_row"])]
        entry["keep_paths"].extend([
            {"path": "Goblin OVER Std−Goblin≥4", **cell(gap), "call": "KEEP path", "note": "standard_line often missing on graded_props"},
            {"path": "Goblin OVER L10≥8 + vs lefty", **cell(lefty), "call": "KEEP path"},
            {"path": f"Goblin OVER L10≥8 + L5 2nd-won%≥{GAMES_WON_L5_SECOND_WON_MIN}", **cell(sw), "call": "KEEP path"},
            {"path": "Goblin OVER L10≥8+(lefty|2nd-won) union", **cell(union_factor), "call": "KEEP"},
            {"path": "Goblin OVER tennis_goblin_keep_eligible()", **cell(keep_fn), "call": "KEEP live fn"},
        ])
        so = [l for l in subset if l["book"] == "Standard OVER" and l["result"] in ("HIT", "MISS")]
        so_keep = [l for l in so if tennis_standard_games_won_over_eligible(l["gate_row"])]
        so_sw = [l for l in so if l["l10"] is not None and l["l10"] >= 8 and l["sw"] is not None and l["sw"] >= GAMES_WON_L5_SECOND_WON_MIN]
        entry["keep_paths"].extend([
            {"path": "Std OVER L10≥8 + 2nd-won keep", **cell(so_sw), "call": "KEEP thin"},
            {"path": "Std OVER tennis_standard_games_won_over_eligible()", **cell(so_keep), "call": "KEEP thin"},
        ])
    if pnorm == "match_total_games":
        gob = [l for l in subset if l["book"] == "Goblin OVER" and l["result"] in ("HIT", "MISS")]
        l5l10 = [l for l in gob if l["l5"] is not None and l["l5"] >= 4 and l["l10"] is not None and l["l10"] >= 8]
        lefty = [l for l in gob if l["l10"] is not None and l["l10"] >= 8 and l["lefty"] is True]
        union = [l for l in gob if l["l10"] is not None and l["l10"] >= 8 and ((l["l5"] is not None and l["l5"] >= 4) or l["lefty"] is True)]
        keep_fn = [l for l in gob if tennis_goblin_keep_eligible(l["gate_row"])]
        entry["keep_paths"].extend([
            {"path": "Goblin OVER L5≥4 + L10≥8", **cell(l5l10), "call": "KEEP path"},
            {"path": "Goblin OVER L10≥8 + vs lefty", **cell(lefty), "call": "KEEP path"},
            {"path": "Goblin OVER (L5≥4+L10≥8)|lefty union", **cell(union), "call": "KEEP"},
            {"path": "Goblin OVER tennis_goblin_keep_eligible()", **cell(keep_fn), "call": "KEEP live fn"},
        ])

    props_out.append(entry)

# Consistency note for Aces
consistency = {
    "issue": "Std OVER Aces 29.6% n=541 vs 24.4% n=591",
    "resolution": "Not a single running ledger. 29.6%/541 came from an earlier all_ranked / canvas snapshot with a different date window and/or pre-backfill actuals. Canonical now is this script on deduped graded_props (main_cp first).",
    "canonical_std_over_aces": cell(aces_so),
    "by_actual_source": {k: {"n": v["n"], "hit_pct": pct(v["h"], v["n"])} for k, v in by_src.items()},
}

report = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "script": "scripts/tennis_board_category_hit_rates.py",
    "method": {
        "sources": [str(d) for d in GRADED_DIRS if d.exists()],
        "dedupe": "slate|player|prop|book|line|result|pp_projection_id; first file wins (main_cp before open)",
        "window": {"min": date_min.isoformat() if date_min else None, "max": date_max.isoformat() if date_max else None},
        "files": source_files,
        "legs": len(legs),
        "decided": sum(1 for l in legs if l["result"] in ("HIT", "MISS")),
        "voids": sum(1 for l in legs if l["result"] == "VOID"),
    },
    "consistency_notes": consistency,
    "props": props_out,
    "catalog_gap_note": "Std−Goblin≥4 keep rate 83.1% n=331 is from a separate Standard-Line catalog join; graded_props often lack standard_line so that path undercounts here.",
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(f"\nWrote {OUT}")

print("\n=== CANONICAL BOARD (decided) ===")
for p in props_out:
    print(f"\n## {p['label']}")
    for b in p["books"]:
        print(f"  {b['book']:16} hit={b['hit_pct']}% n={b['n']} void={b['void']}")
    for k in p["keep_paths"]:
        print(f"  KEEP {k['path']}: hit={k['hit_pct']}% n={k['n']} hits={k['hits']}")
