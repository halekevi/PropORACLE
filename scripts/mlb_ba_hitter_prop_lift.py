"""Goblin OVER hitter props sliced by season batting average and K-rate.

Fetches 2026 MLB Stats API season hitting (playerPool=all), joins graded
Goblin OVER Hits / TB / H+R+RBI / hitter Ks.

  py -3.14 scripts/mlb_ba_hitter_prop_lift.py
"""
from __future__ import annotations

import json
import re
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import all_ranked_prop_hit_rates as C  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
import mlb_counting_gate_grid as G  # noqa: E402
import mlb_l10_dual_gates as D  # noqa: E402
from utils.defense_tiers import d_aligned  # noqa: E402
from utils.mlb_keep_gates import opp_pitching_tier  # noqa: E402

OUT = _REPO / "data" / "reports" / "mlb_ba_hitter_prop_lift.json"
STATS_URL = (
    "https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting"
    "&season=2026&gameType=R&sportId=1&playerPool=all&limit=2000"
)
PROPS = ("hits", "total_bases", "hits+runs+rbis", "hitter_ks")
DISPLAY = {
    "hits": "Hits",
    "total_bases": "Total Bases",
    "hits+runs+rbis": "H+R+RBI",
    "hitter_ks": "Hitter Ks",
}
BA_BUCKETS = (
    (".300+", 0.300, 1.01),
    (".275-.299", 0.275, 0.300),
    (".250-.274", 0.250, 0.275),
    (".225-.249", 0.225, 0.250),
    ("under .225", 0.0, 0.225),
)
K_BUCKETS = (
    ("K% 28%+", 0.28, 1.01),
    ("K% 22-28%", 0.22, 0.28),
    ("K% 16-22%", 0.16, 0.22),
    ("K% under 16%", 0.0, 0.16),
)
MIN_AB = 80
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _norm(s: object) -> str:
    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _cell() -> dict:
    return {"n": 0, "h": 0}


def _add(c: dict, hit: int) -> None:
    c["n"] += 1
    c["h"] += int(hit)


def _rate(c: dict) -> dict:
    n = c["n"]
    h = c["h"]
    return {
        "n": n,
        "hits": h,
        "hr": None if n <= 0 else round(100.0 * h / n, 1),
    }


def _bucket(val: float | None, edges: tuple) -> str:
    if val is None:
        return "no_stat"
    for name, lo, hi in edges:
        if lo <= val < hi:
            return name
    return "no_stat"


def fetch_hitting() -> dict[str, dict]:
    req = urllib.request.Request(STATS_URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = json.loads(resp.read().decode())
    by_id: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for split in (raw.get("stats") or [{}])[0].get("splits") or []:
        st = split.get("stat") or {}
        player = split.get("player") or {}
        pid = str(player.get("id") or "").strip()
        try:
            ab = float(st.get("atBats") or 0)
        except (TypeError, ValueError):
            ab = 0.0
        try:
            avg = float(str(st.get("avg") or "").replace(".", "0.") if str(st.get("avg") or "").startswith(".") else st.get("avg") or "")
        except (TypeError, ValueError):
            avg = None
        if avg is None:
            try:
                avg = float(st.get("avg"))
            except (TypeError, ValueError):
                avg = None
        try:
            so = float(st.get("strikeOuts") or 0)
            pa = float(st.get("plateAppearances") or 0)
        except (TypeError, ValueError):
            so, pa = 0.0, 0.0
        rec = {
            "id": pid,
            "name": str(player.get("fullName") or ""),
            "avg": avg,
            "ab": ab,
            "k_rate": (so / pa) if pa else None,
            "so": so,
            "pa": pa,
        }
        if pid:
            by_id[pid] = rec
        nm = _norm(rec["name"])
        if nm:
            by_name[nm] = rec
    return {"by_id": by_id, "by_name": by_name}


def load_id_cache() -> dict[str, str]:
    import pandas as pd

    p = _REPO / "Sports" / "MLB" / "mlb_id_cache.csv"
    if not p.is_file():
        p = _REPO / "Sports" / "MLB" / "scripts" / "mlb_id_cache.csv"
    if not p.is_file():
        return {}
    d = pd.read_csv(p)
    out = {}
    for r in d.to_dict("records"):
        nm = _norm(r.get("player_norm") or r.get("player"))
        pid = str(r.get("mlb_player_id") or "").strip()
        if nm and pid:
            out[nm] = pid
    return out


def lookup(row: dict, hitting: dict, id_cache: dict[str, str]) -> dict | None:
    for k in ("mlb_player_id",):
        pid = str(row.get(k) or "").strip()
        if pid and pid in hitting["by_id"]:
            return hitting["by_id"][pid]
    name = _norm(row.get("player") or row.get("player_name"))
    if not name:
        return None
    pid = id_cache.get(name)
    if pid and pid in hitting["by_id"]:
        return hitting["by_id"][pid]
    return hitting["by_name"].get(name)


def fmt(c: dict) -> str:
    r = _rate(c)
    if not r["n"]:
        return "—"
    return f"{r['hr']:5.1f}%  {r['hits']}/{r['n']}"


def main() -> None:
    hitting = fetch_hitting()
    id_cache = load_id_cache()
    acc: dict[str, dict[str, dict[str, dict]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(_cell))
    )
    n_rows = 0
    n_matched = 0
    n_ab = 0
    seen: set[str] = set()
    step_cache: dict[str, dict] = {}
    for path in C.graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        if date in seen:
            continue
        seen.add(date)
        if date not in step_cache:
            sp = G.find_step8_csv(date)
            step_cache[date] = D.load_step8_over(sp) if sp else {}
        step_idx = step_cache[date]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in raw.get("props") or []:
            if T.norm_sport(str(r.get("sport") or "")) != "MLB":
                continue
            if str(r.get("result") or "").strip().upper() not in ("HIT", "MISS"):
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            else:
                continue
            if C.pick_book(str(r.get("pick_type") or ""), side) != "Goblin OVER":
                continue
            canon = T.canon_prop("MLB", str(r.get("prop") or r.get("prop_type") or ""))
            if canon not in PROPS:
                continue
            n_rows += 1
            rec = lookup(r, hitting, id_cache)
            if not rec:
                _add(acc[canon]["all"]["unmatched"], 1 if str(r.get("result")).upper() == "HIT" else 0)
                continue
            n_matched += 1
            if rec["ab"] < MIN_AB or rec["avg"] is None:
                _add(acc[canon]["all"]["low_ab"], 1 if str(r.get("result")).upper() == "HIT" else 0)
                continue
            n_ab += 1
            hit = 1 if str(r.get("result")).upper() == "HIT" else 0
            ba = _bucket(rec["avg"], BA_BUCKETS)
            kr = _bucket(rec["k_rate"], K_BUCKETS)
            l5 = C.l5_dir(r, "OVER")
            l10 = C.l10_dir(r, "OVER")
            if (l5 is None or l10 is None) and step_idx:
                s8 = step_idx.get(
                    (
                        _norm(r.get("player") or r.get("player_name")),
                        canon,
                        C.num(r.get("line")),
                    )
                )
                if s8:
                    if l5 is None:
                        l5 = s8[0]
                    if l10 is None:
                        l10 = s8[1]
            d_raw = r.get("def_tier") or r.get("def") or r.get("DEF_TIER")
            if not d_raw:
                d_raw = opp_pitching_tier(
                    {
                        "opp_team": r.get("opp_team") or r.get("opp") or r.get("Opp"),
                    }
                )
            d_ok = d_aligned("MLB", "OVER", d_raw, canon)
            rec_keys = ["all"]
            if l5 is not None and l5 >= 4:
                rec_keys.append("l5ge4")
            if l5 is not None and l5 >= 5:
                rec_keys.append("l5eq5")
            if l10 is not None and l10 >= 8:
                rec_keys.append("l10ge8")
            if d_ok:
                rec_keys.append("d")
            if l5 is not None and l5 >= 5 and d_ok:
                rec_keys.append("l5eq5_d")
            if l10 is not None and l10 >= 8 and d_ok:
                rec_keys.append("l10ge8_d")
            if l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8:
                rec_keys.append("l5eq5_l10ge8")
            if (
                l5 is not None
                and l5 >= 5
                and l10 is not None
                and l10 >= 8
                and d_ok
            ):
                rec_keys.append("l5eq5_l10ge8_d")
            ba275 = rec["avg"] is not None and rec["avg"] >= 0.275
            k28 = rec["k_rate"] is not None and rec["k_rate"] >= 0.28
            if ba275:
                rec_keys.append("ba275")
            if ba275 and l5 is not None and l5 >= 5:
                rec_keys.append("ba275_l5eq5")
            if ba275 and l5 is not None and l5 >= 5 and d_ok:
                rec_keys.append("ba275_l5eq5_d")
            if ba275 and l10 is not None and l10 >= 8:
                rec_keys.append("ba275_l10ge8")
            if ba275 and l10 is not None and l10 >= 8 and d_ok:
                rec_keys.append("ba275_l10ge8_d")
            if k28:
                rec_keys.append("k28")
            if k28 and l5 is not None and l5 >= 5:
                rec_keys.append("k28_l5eq5")
            if k28 and l5 is not None and l5 >= 5 and d_ok:
                rec_keys.append("k28_l5eq5_d")
            if k28 and l10 is not None and l10 >= 8:
                rec_keys.append("k28_l10ge8")
            if k28 and l10 is not None and l10 >= 8 and d_ok:
                rec_keys.append("k28_l10ge8_d")
            for gk in rec_keys:
                _add(acc[canon][gk]["all"], hit)
                _add(acc[canon][gk][ba], hit)
                _add(acc[canon][gk][kr], hit)

    report = {
        "source": "graded Goblin OVER + MLB Stats API 2026 season hitting (as-of now, not as-of-game)",
        "min_ab": MIN_AB,
        "n_dates": len(seen),
        "n_hitter_goblin_over": n_rows,
        "n_matched": n_matched,
        "n_min_ab": n_ab,
        "n_hitters_api": len(hitting["by_id"]),
        "props": {},
    }
    print(
        f"matched {n_matched}/{n_rows}  min_ab>={MIN_AB}: {n_ab}  "
        f"api hitters {len(hitting['by_id'])}  dates {len(seen)}"
    )
    ba_names = [b[0] for b in BA_BUCKETS]
    k_names = [b[0] for b in K_BUCKETS]
    gates = (
        "all",
        "d",
        "l5eq5",
        "l5eq5_d",
        "l10ge8",
        "l10ge8_d",
        "l5eq5_l10ge8",
        "l5eq5_l10ge8_d",
        "ba275",
        "ba275_l5eq5",
        "ba275_l5eq5_d",
        "ba275_l10ge8",
        "ba275_l10ge8_d",
        "k28",
        "k28_l5eq5",
        "k28_l5eq5_d",
        "k28_l10ge8",
        "k28_l10ge8_d",
    )
    pairs = (
        ("recency only", "l5eq5", "l10ge8"),
        ("+ D", "l5eq5_d", "l10ge8_d"),
        (".275+ BA", "ba275_l5eq5", "ba275_l10ge8"),
        (".275+ BA + D", "ba275_l5eq5_d", "ba275_l10ge8_d"),
        ("K% 28%+", "k28_l5eq5", "k28_l10ge8"),
        ("K% 28%+ + D", "k28_l5eq5_d", "k28_l10ge8_d"),
    )

    def _delta(a: dict, b: dict) -> str:
        if not a["n"] or not b["n"]:
            return "—"
        return f"{b['hr'] - a['hr']:+.1f}"

    for canon in PROPS:
        print(f"\n== {DISPLAY[canon]} Goblin OVER ==")
        print(f"{'gate':18} {'all':>14} " + " ".join(f"{n:>16}" for n in ba_names))
        prop_out = {}
        for gk in gates:
            buckets = acc[canon][gk]
            row = {name: _rate(buckets[name]) for name in ["all", *ba_names, *k_names]}
            prop_out[gk] = row
            print(
                f"{gk:18} {fmt(buckets['all']):>14} "
                + " ".join(f"{fmt(buckets[n]):>16}" for n in ba_names)
            )
        print(f"\n  L5=5 vs L10>=8  ({DISPLAY[canon]})")
        print(f"  {'combo':16} {'L5=5':>16} {'L10>=8':>16} {'L10-L5':>8}")
        for label, g5, g10 in pairs:
            c5 = acc[canon][g5]["all"]
            c10 = acc[canon][g10]["all"]
            print(
                f"  {label:16} {fmt(c5):>16} {fmt(c10):>16} "
                f"{_delta(_rate(c5), _rate(c10)):>8}"
            )
        print(f"  {'L5=5 AND L10':16} {fmt(acc[canon]['l5eq5_l10ge8']['all']):>16}")
        print(
            f"  {'L5=5 AND L10+D':16} {fmt(acc[canon]['l5eq5_l10ge8_d']['all']):>16}"
        )
        if canon == "hitter_ks":
            print(f"{'gate':18} " + " ".join(f"{n:>16}" for n in k_names))
            for gk in (
                "all",
                "l5eq5",
                "l5eq5_d",
                "l10ge8",
                "l10ge8_d",
                "k28_l5eq5",
                "k28_l5eq5_d",
                "k28_l10ge8",
                "k28_l10ge8_d",
            ):
                buckets = acc[canon][gk]
                print(
                    f"{gk:18} "
                    + " ".join(f"{fmt(buckets[n]):>16}" for n in k_names)
                )
        report["props"][canon] = prop_out
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("\nwrote", OUT)


if __name__ == "__main__":
    main()
