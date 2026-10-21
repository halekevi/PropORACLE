"""Rebounds 4.5+ skip-D ticket path vs current G70 (D required). Line mix too."""
from __future__ import annotations

import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num, pick_book  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402
from utils.wnba_prop_defense import TEAM_MAP, default_csv_path  # noqa: E402

CACHE = ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"


def _norm(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


def hr(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def ztwo(h1, n1, h2, n2):
    if n1 < 5 or n2 < 5:
        return None
    p1, p2 = h1 / n1, h2 / n2
    p = (h1 + h2) / (n1 + n2)
    den = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    if den == 0:
        return None
    return (p1 - p2) / den


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
    df["PLAYER_NORM"] = df["PLAYER_NORM"].fillna("").astype(str)
    empty = df["PLAYER_NORM"] == ""
    if empty.any():
        df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm)
    df["MIN"] = pd.to_numeric(df["MIN"], errors="coerce").fillna(0.0)
    df["REB"] = pd.to_numeric(df["REB"], errors="coerce").fillna(0.0)
    df = df.sort_values(["PLAYER_NORM", "game_date", "event_id"])
    prior = {}
    run: dict[str, list] = {}
    for rec in df.itertuples(index=False):
        pn = rec.PLAYER_NORM
        d = rec.game_date
        games = run.setdefault(pn, [])
        last5 = games[-5:]
        n = len(games)
        prior[(pn, d)] = {
            "l5": (sum(last5) / len(last5)) if last5 else None,
            "avg": (sum(games) / n) if n else None,
        }
        if float(rec.MIN) >= 1:
            games.append(float(rec.REB))
    return prior


def collect(prior, by_team):
    rows = []
    seen = set()
    unit = T.cover_need("WNBA", "rebounds")
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
            if prop != "rebounds":
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, line)
            if key in seen:
                continue
            seen.add(key)
            l5 = l5_dir(r, "OVER")
            l10 = l10_dir(r, "OVER")
            st = prior.get((_norm(player), date)) or {}
            last5 = st.get("l5")
            savg = st.get("avg")
            cover = (last5 - line) if last5 is not None and line is not None else None
            opp = canon_team(r.get("opp_team"))
            csv = by_team.get(opp) or {}
            d_raw = normalize_def_tier_label(csv.get("reb_tier")) or ""
            cover_ok = T.cover_clears_floor("WNBA", cover, "OVER", "rebounds")
            avg_ok = savg is None or (line is not None and savg >= line + unit)
            d_ok = d_aligned("WNBA", "OVER", d_raw, "rebounds")
            l5eq5 = l5 is not None and l5 >= 5
            l10_ok = l10 is not None and l10 >= 8
            rows.append(
                {
                    "line": line,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "l5eq5": l5eq5,
                    "stack": l5eq5 and l10_ok,
                    "d_ok": d_ok,
                    "now": l5eq5 and l10_ok and d_ok and cover_ok and avg_ok,
                    "skip": l5eq5 and l10_ok and cover_ok and avg_ok,
                }
            )
    return rows


def band(line):
    if line is None:
        return "none"
    if abs(line - 3.5) < 1e-9:
        return "3.5"
    if line >= 4.5:
        return f"{line}"
    if line <= 2.5:
        return "<=2.5"
    return f"{line}"


def show(label, sub):
    print(f"  {label:42} {hr(sum(r['hit'] for r in sub), len(sub))}")


def main():
    rows = collect(load_prior(), load_csv())
    print(f"Goblin OVER Rebounds faces {len(rows)}")
    print("line mix L5=5")
    c = Counter(band(r["line"]) for r in rows if r["l5eq5"])
    for k, n in sorted(c.items(), key=lambda kv: str(kv[0])):
        print(f"  {k:8} n={n}")

    groups = [
        ("3.5", lambda r: abs((r["line"] or -1) - 3.5) < 1e-9),
        ("4.5+", lambda r: r["line"] is not None and r["line"] >= 4.5),
        ("<=2.5", lambda r: r["line"] is not None and r["line"] <= 2.5),
        ("ALL lines", lambda r: True),
    ]
    # also split 4.5 / 5.5 / 6.5+
    extra_lines = sorted({r["line"] for r in rows if r["line"] is not None and r["line"] >= 4.5})
    for ln in extra_lines:
        groups.append((f"{ln}", lambda r, ln=ln: r["line"] == ln))

    for title, pred in groups:
        rs = [r for r in rows if pred(r)]
        if not rs:
            continue
        l5l10 = [r for r in rs if r["stack"]]
        dpass = [r for r in l5l10 if r["d_ok"]]
        dfail = [r for r in l5l10 if not r["d_ok"]]
        z = ztwo(sum(r["hit"] for r in dpass), len(dpass), sum(r["hit"] for r in dfail), len(dfail))
        print(f"\n========== {title} ==========")
        show("L5=5+L10", l5l10)
        show("L5=5+L10 D-pass", dpass)
        show("L5=5+L10 D-fail", dfail)
        print(f"  two-prop z (pass-fail)                     {z:+.2f}" if z is not None else "  two-prop z                                 —")
        show("NOW G70 (L5+L10+D+cover+avg)", [r for r in rs if r["now"]])
        show("SKIP D (L5+L10+cover+avg)", [r for r in rs if r["skip"]])
        show("added by skip D", [r for r in rs if r["skip"] and not r["now"]])


if __name__ == "__main__":
    main()
