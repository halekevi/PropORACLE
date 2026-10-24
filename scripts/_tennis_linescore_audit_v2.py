"""Linescore audit: only score rows whose match_total agrees (same match)."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from utils.prop_norm import canon_prop  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _p(*a):
    print(*a, flush=True)


def _norm(s: object) -> str:
    return " ".join(str(s or "").casefold().split())


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


def _board_map(ymd: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for tour in ("atp", "wta"):
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
            if len(comps) < 2:
                continue
            status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
            detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
            retired = any(
                tok in (status + " " + detail).upper()
                for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
            )
            for i, c in enumerate(comps):
                name = str(((c.get("athlete") or {}).get("displayName") or "")).strip()
                if not name:
                    continue
                lines = c.get("linescores") or []
                games = sum(float((x or {}).get("value") or 0) for x in lines)
                other = comps[1 - i]
                ols = other.get("linescores") or []
                opp_games = sum(float((x or {}).get("value") or 0) for x in ols)
                set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                score = " ".join(
                    f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                )
                out[_norm(name)].append(
                    {
                        "games": games,
                        "opp_games": opp_games,
                        "match_total": games + opp_games,
                        "status": status,
                        "score": score,
                        "opp": str(((other.get("athlete") or {}).get("displayName") or "")),
                        "retired": retired,
                        "ymd": ymd,
                        "tour": tour,
                    }
                )
    return out


def main() -> None:
    # Join games_won + match_total_games per player-day
    pairs: dict[tuple[str, str], dict] = {}
    seen = set()
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            cols = {str(c).lower(): c for c in df.columns}
            pc, prop_c, act_c = (
                cols.get("player"),
                cols.get("prop_type") or cols.get("prop"),
                cols.get("actual"),
            )
            if not (pc and prop_c and act_c):
                continue
            for _, r in df.iterrows():
                prop = canon_prop("TENNIS", str(r.get(prop_c) or ""))
                if prop not in {"games_won", "match_total_games"}:
                    continue
                try:
                    actual = float(r.get(act_c))
                except (TypeError, ValueError):
                    continue
                player = str(r.get(pc) or "").strip()
                key = (_norm(player), day)
                rec = pairs.setdefault(key, {"player": player, "day": day})
                rec[prop] = actual

    with_both = [r for r in pairs.values() if "games_won" in r and "match_total_games" in r]
    gw_only = [r for r in pairs.values() if "games_won" in r and "match_total_games" not in r]
    _p(f"player-days with games_won: {sum(1 for r in pairs.values() if 'games_won' in r)}")
    _p(f"with both gw+mtg: {len(with_both)}; gw only: {len(gw_only)}")

    days = sorted({r["day"] for r in with_both})
    pull = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards: dict[str, dict[str, list]] = {}
    for i, d in enumerate(sorted(pull)):
        boards[d] = _board_map(d)
        if (i + 1) % 15 == 0 or i == len(pull) - 1:
            _p(f"  espn {i+1}/{len(pull)}")

    same_match_ok = same_match_bad = wrong_match = no_espn = 0
    ret_ok = ret_bad = ret_wrong = 0
    samples_bad = []
    samples_ret = []
    for r in with_both:
        key = _norm(r["player"])
        cands = []
        for d in (
            r["day"],
            (date.fromisoformat(r["day"]) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(r["day"]) - timedelta(days=1)).isoformat(),
        ):
            cands.extend(boards.get(d, {}).get(key) or [])
        if not cands:
            no_espn += 1
            continue
        # Prefer candidate whose match_total matches stored mtg
        mtg = float(r["match_total_games"])
        gw = float(r["games_won"])
        same = [c for c in cands if abs(c["match_total"] - mtg) < 0.01]
        if not same:
            wrong_match += 1
            continue
        # pick first same-total (if multiple, prefer non-retired then matching gw)
        same.sort(key=lambda c: (c["retired"], abs(c["games"] - gw)))
        hand = same[0]
        ok = abs(hand["games"] - gw) < 0.01
        if hand["retired"]:
            if ok:
                ret_ok += 1
            else:
                ret_bad += 1
                if len(samples_ret) < 12:
                    samples_ret.append({**r, "hand": hand})
        else:
            if ok:
                same_match_ok += 1
            else:
                same_match_bad += 1
                if len(samples_bad) < 15:
                    samples_bad.append({**r, "hand": hand})

    verifiable = same_match_ok + same_match_bad + ret_ok + ret_bad
    _p(f"\nVERIFIABLE (mtg matched ESPN): {verifiable}")
    _p(f"  completed match={same_match_ok} mismatch={same_match_bad}")
    _p(
        f"  completed mismatch rate="
        f"{same_match_bad / (same_match_ok + same_match_bad) if (same_match_ok + same_match_bad) else None}"
    )
    _p(f"  retired match={ret_ok} mismatch={ret_bad}")
    _p(
        f"  retired mismatch rate="
        f"{ret_bad / (ret_ok + ret_bad) if (ret_ok + ret_bad) else None}"
    )
    _p(f"wrong_match (espn found, mtg disagree): {wrong_match}")
    _p(f"no_espn: {no_espn}")
    _p(f"uncheckable bound: wrong_match+no_espn+gw_only = {wrong_match + no_espn + len(gw_only)}")
    _p("completed mismatches (same mtg, different gw):")
    for s in samples_bad:
        h = s["hand"]
        _p(
            f"  {s['day']} {s['player']} stored_gw={s['games_won']} hand={h['games']} "
            f"mtg={s['match_total_games']} score={h['score']} opp={h['opp']}"
        )
    _p("retirement mismatches (same mtg):")
    for s in samples_ret:
        h = s["hand"]
        _p(
            f"  {s['day']} {s['player']} stored_gw={s['games_won']} hand={h['games']} "
            f"mtg={s['match_total_games']} score={h['score']} status={h['status']} opp={h['opp']}"
        )
    out = {
        "with_both": len(with_both),
        "verifiable": verifiable,
        "completed_match": same_match_ok,
        "completed_mismatch": same_match_bad,
        "retired_match": ret_ok,
        "retired_mismatch": ret_bad,
        "wrong_match": wrong_match,
        "no_espn": no_espn,
        "gw_only": len(gw_only),
    }
    path = ROOT / "data" / "reports" / "tennis_linescore_audit_v2_20261007.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    _p(f"WROTE {path}")


if __name__ == "__main__":
    main()
