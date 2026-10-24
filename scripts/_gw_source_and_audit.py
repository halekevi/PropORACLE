"""GW keep split by standard_line_source; non-keep build dates; tennis linescore audit."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.leg_start_window import usable_start_date  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_STD_DISCOUNT_MIN,
    tennis_goblin_keep_eligible,
    tennis_std_discount,
)

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
ET = ZoneInfo("America/New_York")
LEGS = json.loads(
    (ROOT / "data" / "reports" / "gw_hand_branch_20261007.json").read_text(encoding="utf-8")
)["legs"]
GATE_FILE_ADDED = date(2026, 9, 15)  # utils/tennis_keep_gates.py created
# User cited Sep 24-25; report both anchors.
GATE_USER_ANCHOR = date(2026, 9, 24)


def _p(*a):
    print(*a, flush=True)


def _norm(s: object) -> str:
    return " ".join(str(s or "").casefold().split())


def _step8_source(day: str, player: str, line: float | None) -> str:
    root = MAIN / "outputs" / day
    files = (
        list(root.glob("**/step8*tennis*clean*.parquet"))
        + list(root.glob("**/step8*tennis*clean*.xlsx"))
        + list(root.glob("**/step8*tennis*clean*.csv"))
    )
    want = _norm(player)
    for path in files:
        try:
            if path.suffix == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix in {".xlsx", ".xls"}:
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, dtype=str)
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        src_c = cols.get("standard_line_source") or cols.get("standard line source")
        line_c = cols.get("line")
        prop_c = cols.get("prop_type") or cols.get("prop")
        std_c = cols.get("standard_line") or cols.get("standard line")
        if not pc:
            continue
        best = ""
        for _, r in df.iterrows():
            if _norm(r.get(pc)) != want:
                continue
            prop = canon_prop("TENNIS", str(r.get(prop_c) or "")) if prop_c else ""
            if prop and prop != "games_won":
                continue
            if line is not None and line_c is not None:
                try:
                    if abs(float(r.get(line_c)) - float(line)) > 0.01:
                        continue
                except (TypeError, ValueError):
                    pass
            src = str(r.get(src_c) or "").strip() if src_c else ""
            if not src or src.lower() == "nan":
                # blank often means sibling fill before source was stamped
                src = "blank"
            std = r.get(std_c) if std_c else None
            return f"{src}|std={std}"
        if best:
            return best
    return "no_step8_row"


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


def _board_map_for_day(ymd: str) -> dict[str, dict]:
    """player_norm -> {games, opp_games, status, score, opp, retired, event}."""
    out: dict[str, dict] = {}
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
                other = comps[1 - i] if len(comps) == 2 else None
                opp_games = 0.0
                opp_name = ""
                other_bits = []
                if other is not None:
                    ols = other.get("linescores") or []
                    opp_games = sum(float((x or {}).get("value") or 0) for x in ols)
                    other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                    opp_name = str(((other.get("athlete") or {}).get("displayName") or ""))
                set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                score = " ".join(
                    f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                )
                key = _norm(name)
                # Prefer final completed over in-progress if duplicate
                prev = out.get(key)
                if prev and not retired and prev.get("retired"):
                    continue
                out[key] = {
                    "games": games,
                    "opp_games": opp_games,
                    "match_total": games + opp_games,
                    "status": status,
                    "score": score,
                    "opp": opp_name,
                    "retired": retired,
                    "event": ev.get("id"),
                    "tour": tour,
                    "ymd": ymd,
                }
    return out


def main() -> None:
    # ---- keep / non-keep from archive-field branch report ----
    keep_legs = [dict(r) for r in LEGS if r.get("keep")]
    nonkeep = [dict(r) for r in LEGS if not r.get("keep")]

    _p(f"KEEP n={len(keep_legs)} hits={sum(1 for r in keep_legs if r['grade']=='HIT')}")
    _p(f"NONKEEP n={len(nonkeep)} hits={sum(1 for r in nonkeep if r['grade']=='HIT')}")

    # ---- split keep by standard_line_source ----
    _p("\n=== KEEP BY standard_line_source ===")
    by_src: dict[str, Counter] = defaultdict(Counter)
    detail = []
    for rec in keep_legs:
        raw = _step8_source(rec["date"], rec["player"], rec.get("line"))
        src = raw.split("|", 1)[0] if raw else "unknown"
        by_src[src][rec["grade"]] += 1
        detail.append({**rec, "standard_line_source": src, "source_raw": raw})
        _p(
            f"  {rec['date']} {rec['player']} line={rec['line']} std={rec.get('standard_line')} "
            f"disc={rec.get('disc')} grade={rec['grade']} src={src}"
        )
    _p("SOURCE_SUMMARY")
    for src, c in sorted(by_src.items()):
        n = c["HIT"] + c["MISS"]
        _p(f"  {src}: {c['HIT']}/{n} ({c['HIT']/n if n else 0:.1%})")

    # ---- non-keep build dates vs gate ----
    _p("\n=== NON-KEEP BUILD DATES ===")
    _p(f"tennis_keep_gates.py added: {GATE_FILE_ADDED}")
    _p(f"user anchor: {GATE_USER_ANCHOR}")
    before_file = after_file = before_user = after_user = 0
    for rec in nonkeep:
        d = date.fromisoformat(rec["date"])
        flag_file = "before_gate_file" if d < GATE_FILE_ADDED else "on_or_after_gate_file"
        flag_user = "before_user_anchor" if d < GATE_USER_ANCHOR else "on_or_after_user_anchor"
        if d < GATE_FILE_ADDED:
            before_file += 1
        else:
            after_file += 1
        if d < GATE_USER_ANCHOR:
            before_user += 1
        else:
            after_user += 1
        _p(
            f"  {rec['date']} {rec['player']} line={rec['line']} disc={rec.get('disc')} "
            f"grade={rec['grade']} {flag_file} {flag_user}"
        )
    _p(
        f"vs gate file Sep 15: before={before_file} on/after={after_file}; "
        f"vs user Sep 24: before={before_user} on/after={after_user}"
    )

    # ---- linescore audit all verifiable tennis games_won actuals ----
    _p("\n=== LINESCORE AUDIT (games_won rows with actuals) ===")
    actual_roots = [MAIN / "outputs", ROOT / "outputs"]
    rows = []
    seen_files = set()
    for root in actual_roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            key = path.name
            if key in seen_files:
                continue
            seen_files.add(key)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            cols = {str(c).lower(): c for c in df.columns}
            pc = cols.get("player")
            prop_c = cols.get("prop_type") or cols.get("prop")
            act_c = cols.get("actual")
            if not (pc and prop_c and act_c):
                continue
            for _, r in df.iterrows():
                prop = canon_prop("TENNIS", str(r.get(prop_c) or ""))
                if prop != "games_won":
                    continue
                try:
                    actual = float(r.get(act_c))
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "day": day,
                        "player": str(r.get(pc) or "").strip(),
                        "actual": actual,
                        "path": str(path),
                    }
                )
    _p(f"games_won actual rows: {len(rows)} files={len(seen_files)}")
    days = sorted({r["day"] for r in rows})
    _p(f"distinct actual dates: {len(days)} {days[0] if days else '?'}..{days[-1] if days else '?'}")

    board_cache: dict[str, dict[str, dict]] = {}
    # Pull day and ±1 for each actual day (matches can cross midnight ET)
    pull_days = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull_days.add((dd + timedelta(days=off)).isoformat())
    _p(f"ESPN scoreboard days to pull: {len(pull_days)}")
    for i, d in enumerate(sorted(pull_days)):
        board_cache[d] = _board_map_for_day(d)
        if (i + 1) % 10 == 0 or i == len(pull_days) - 1:
            _p(f"  pulled {i+1}/{len(pull_days)} ({d})")

    match = mismatch = not_found = retired_match = retired_mismatch = retired_nf = 0
    mismatch_samples = []
    ret_samples = []
    for r in rows:
        key = _norm(r["player"])
        hand = None
        # prefer same day, then ±1
        for d in (r["day"],
                  (date.fromisoformat(r["day"]) + timedelta(days=1)).isoformat(),
                  (date.fromisoformat(r["day"]) - timedelta(days=1)).isoformat()):
            hand = (board_cache.get(d) or {}).get(key)
            if hand:
                break
        if hand is None:
            not_found += 1
            continue
        ok = abs(float(hand["games"]) - float(r["actual"])) < 0.01
        if hand["retired"]:
            if ok:
                retired_match += 1
            else:
                retired_mismatch += 1
                if len(ret_samples) < 15:
                    ret_samples.append(
                        {
                            **r,
                            "hand": hand["games"],
                            "score": hand["score"],
                            "status": hand["status"],
                            "opp": hand["opp"],
                            "mtg": hand["match_total"],
                        }
                    )
        else:
            if ok:
                match += 1
            else:
                mismatch += 1
                if len(mismatch_samples) < 20:
                    mismatch_samples.append(
                        {
                            **r,
                            "hand": hand["games"],
                            "score": hand["score"],
                            "status": hand["status"],
                            "opp": hand["opp"],
                            "mtg": hand["match_total"],
                        }
                    )

    found = match + mismatch + retired_match + retired_mismatch
    _p(f"FOUND {found}/{len(rows)} NOT_FOUND {not_found}")
    _p(
        f"completed: match={match} mismatch={mismatch} "
        f"rate_mismatch={mismatch/(match+mismatch) if (match+mismatch) else None}"
    )
    _p(
        f"retired: match={retired_match} mismatch={retired_mismatch} "
        f"rate_mismatch={retired_mismatch/(retired_match+retired_mismatch) if (retired_match+retired_mismatch) else None}"
    )
    _p("mismatch samples (completed):")
    for s in mismatch_samples[:12]:
        _p(
            f"  {s['day']} {s['player']} stored={s['actual']} hand={s['hand']} "
            f"score={s['score']} opp={s['opp']} status={s['status']}"
        )
    _p("retirement mismatch samples:")
    for s in ret_samples:
        _p(
            f"  {s['day']} {s['player']} stored={s['actual']} hand={s['hand']} "
            f"mtg_hand={s['mtg']} score={s['score']} opp={s['opp']} status={s['status']}"
        )

    out = {
        "keep_by_source": {k: dict(v) for k, v in by_src.items()},
        "keep_detail": detail,
        "nonkeep": nonkeep,
        "nonkeep_vs_gate_file": {"before": before_file, "on_or_after": after_file},
        "nonkeep_vs_user_anchor": {"before": before_user, "on_or_after": after_user},
        "linescore": {
            "rows": len(rows),
            "found": found,
            "not_found": not_found,
            "completed_match": match,
            "completed_mismatch": mismatch,
            "retired_match": retired_match,
            "retired_mismatch": retired_mismatch,
            "mismatch_samples": mismatch_samples,
            "retired_mismatch_samples": ret_samples,
        },
    }
    path = ROOT / "data" / "reports" / "gw_source_audit_20261007.json"
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    _p(f"\nWROTE {path}")


if __name__ == "__main__":
    main()
