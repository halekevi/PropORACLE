"""Hand-verify Games Won against the board opponent (not any ESPN match)."""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.leg_start_window import usable_start_date  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
LEGS = json.loads((ROOT / "data" / "reports" / "gw_hand_branch_20261007.json").read_text())["legs"]


def _p(*a):
    print(*a, flush=True)


def _espn(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode())


def _iter_comps(payload):
    for ev in payload.get("events") or []:
        groupings = ev.get("groupings") or []
        if groupings:
            for grp in groupings:
                for comp in grp.get("competitions") or []:
                    yield ev, comp
        else:
            for comp in ev.get("competitions") or []:
                yield ev, comp


def _norm(s: str) -> str:
    return " ".join(str(s or "").casefold().split())


def _last(s: str) -> str:
    parts = _norm(s).split()
    return parts[-1] if parts else ""


def _board_opp(day: str, player: str) -> str:
    root = MAIN / "outputs" / day
    files = (
        list(root.glob("**/step8*tennis*clean*"))
        + list(root.glob("tennis/step1_tennis_props.csv"))
        + list(root.glob("step1_tennis_props.csv"))
    )
    want = _norm(player)
    for path in files:
        try:
            if path.suffix == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix in {".xlsx", ".xls"}:
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        if not pc:
            continue
        for _, r in df.iterrows():
            if _norm(str(r.get(pc) or "")) != want:
                continue
            for k in ("opp", "opponent", "opp_name", "matchup"):
                c = cols.get(k)
                if not c:
                    continue
                val = str(r.get(c) or "").strip()
                if not val or val.lower() == "nan":
                    continue
                if k == "matchup":
                    # "A vs B" / "A @ B"
                    for sep in (" vs ", " VS ", " @ ", " v "):
                        if sep in val:
                            a, b = val.split(sep, 1)
                            if _norm(a) == want:
                                return b.strip()
                            if _norm(b) == want:
                                return a.strip()
                    return val
                return val
    return ""


def _hand(player: str, opp: str, day: str) -> dict | None:
    want = _norm(player)
    want_opp = _norm(opp)
    want_opp_last = _last(opp)
    hits = []
    for tour in ("atp", "wta"):
        for ymd in (
            day,
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
        ):
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
                f"?dates={ymd.replace('-', '')}"
            )
            try:
                payload = _espn(url)
            except Exception:
                continue
            for ev, comp in _iter_comps(payload):
                comps = list(comp.get("competitors") or [])
                names = [
                    str(((c.get("athlete") or {}).get("displayName") or ""))
                    for c in comps
                ]
                if not any(want == _norm(n) or want in _norm(n) for n in names):
                    continue
                opp_names = [_norm(n) for n in names if want != _norm(n) and want not in _norm(n)]
                if want_opp:
                    if not any(
                        want_opp == o or want_opp_last == _last(o) or want_opp_last in o
                        for o in opp_names
                    ):
                        continue
                status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
                detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
                mine = next(
                    (
                        c
                        for c in comps
                        if want == _norm(str(((c.get("athlete") or {}).get("displayName") or "")))
                        or want
                        in _norm(str(((c.get("athlete") or {}).get("displayName") or "")))
                    ),
                    None,
                )
                if mine is None:
                    continue
                lines = mine.get("linescores") or []
                games = sum(float((x or {}).get("value") or 0) for x in lines)
                set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                other = next((c for c in comps if c is not mine), None)
                other_bits = []
                other_games = 0.0
                if other is not None:
                    ols = other.get("linescores") or []
                    other_games = sum(float((x or {}).get("value") or 0) for x in ols)
                    other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                score_line = " ".join(
                    f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                )
                hits.append(
                    {
                        "tour": tour,
                        "ymd": ymd,
                        "opp": str(((other or {}).get("athlete") or {}).get("displayName") or ""),
                        "games_won_hand": games,
                        "opp_games": other_games,
                        "match_total": games + other_games,
                        "score_line": score_line,
                        "status": status,
                        "detail": detail,
                        "retired": any(
                            tok in (status + " " + detail).upper()
                            for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
                        ),
                    }
                )
    return hits[0] if hits else None


def main() -> None:
    # Prefer the phase2 sample mix + retirement Cina + a few std_discount
    picks = []
    by_key = {(r["date"], r["player"], r["line"]): r for r in LEGS}
    wanted = [
        ("2026-08-30", "Tamara Zidansek"),
        ("2026-08-31", "Darja Vidmanova"),
        ("2026-08-31", "Janice Tjen"),
        ("2026-08-31", "Mary Stoiana"),
        ("2026-08-31", "Kaitlin Quevedo"),
        ("2026-09-06", "Iva Jovic"),
        ("2026-09-06", "Naomi Osaka"),
        ("2026-09-07", "Emma Navarro"),
        ("2026-09-07", "Linda Noskova"),
        ("2026-08-28", "Federico Cina"),
    ]
    for day, player in wanted:
        for r in LEGS:
            if r["date"] == day and r["player"] == player:
                picks.append(r)
                break

    match_n = found_n = flip_n = 0
    for rec in picks:
        opp = _board_opp(rec["date"], rec["player"])
        day = usable_start_date(rec["start"]) or rec["date"]
        if str(rec["source"]).startswith("fill:"):
            day = rec["source"].split(":", 1)[1]
        hand = _hand(rec["player"], opp, day)
        stored = rec["actual"]
        line = rec["line"]
        if hand is None:
            _p(
                f"  {rec['date']} {rec['player']} opp={opp!r} stored={stored} "
                f"ESPN=NOT_FOUND day={day}"
            )
            continue
        found_n += 1
        hand_g = hand["games_won_hand"]
        hand_grade = "HIT" if line is not None and hand_g > line else "MISS"
        ok = stored is not None and abs(float(stored) - hand_g) < 0.01
        grade_ok = hand_grade == rec["grade"]
        if ok and grade_ok:
            match_n += 1
        if not grade_ok:
            flip_n += 1
        _p(
            f"  {rec['date']} {rec['player']} board_opp={opp!r} espn_opp={hand['opp']!r} "
            f"line={line} stored={stored} hand={hand_g} mtg_hand={hand['match_total']} "
            f"score={hand['score_line']} status={hand['status']} retired={hand['retired']} "
            f"grade={rec['grade']} hand_grade={hand_grade} ACTUAL_MATCH={ok} GRADE_MATCH={grade_ok}"
        )
    _p(f"SUMMARY found={found_n}/{len(picks)} actual_match={match_n} grade_flips={flip_n}")


if __name__ == "__main__":
    main()
