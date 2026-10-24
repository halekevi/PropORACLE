"""How many Sep 1–27 Goblin Saves legs the settlement gate would pass."""
from __future__ import annotations

import sys
from collections import Counter
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import utils.soccer_board_coverage as sbc  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_board_coverage import (  # noqa: E402
    competition_has_box_history,
    keeper_lineup_status,
)
from utils.soccer_board_coverage import find_covered_event  # noqa: E402
from utils.soccer_keep_gates import (  # noqa: E402
    _l5,
    _saves_opp_off_ok,
    _saves_own_d_ok,
    soccer_ticket_gate_passes,
)

_orig_summary = sbc._summary_for_event


@lru_cache(maxsize=512)
def _cached_summary(eid: str, league: str):
    return _orig_summary({"id": eid, "_league_id": league})


def _wrapped(event: dict):
    return _cached_summary(str(event.get("id") or ""), str(event.get("_league_id") or ""))


sbc._summary_for_event = _wrapped


def _p(*a):
    print(*a, flush=True)


def main() -> None:
    start = date(2026, 9, 1)
    end = date(2026, 9, 27)
    day = start
    rows = 0
    passed = 0
    settle_ok = 0
    by_week_settle = Counter()
    by_week_ticket = Counter()
    by_block = Counter()
    settle_leagues = Counter()
    history_leagues = Counter()
    nohist_leagues = Counter()
    l5_on_settle = Counter()
    while day <= end:
        stamp = day.isoformat()
        path = MAIN / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        if not path.is_file():
            path = ROOT / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        day += timedelta(days=1)
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        cols = {str(c).lower(): c for c in df.columns}
        seen = set()
        for _, r in df.iterrows():
            prop = canon_prop(
                "SOCCER", str(r.get(cols.get("prop_type") or cols.get("prop") or "") or "")
            )
            if prop != "saves":
                continue
            pick = str(r.get(cols.get("pick_type") or "") or "")
            side = str(r.get(cols.get("side") or cols.get("direction") or "") or "").upper()
            if "goblin" not in pick.lower() or side not in {"", "OVER"}:
                continue
            player = str(r.get(cols.get("player") or "") or "").strip()
            if not player:
                continue
            key = (stamp, player.lower())
            if key in seen:
                continue
            seen.add(key)
            row = {str(c): r.get(c) for c in df.columns}
            row["sport"] = "SOCCER"
            row["player"] = player
            row["prop"] = "Goalie Saves"
            row["side"] = "OVER"
            row["pick_type"] = "Goblin"
            rows += 1
            week = f"{date.fromisoformat(stamp).isocalendar().year}-W{date.fromisoformat(stamp).isocalendar().week:02d}"
            event = find_covered_event(row)
            league = str((event or {}).get("_league_id") or "unmatched")
            hist = competition_has_box_history(row)
            lineup = keeper_lineup_status(row) if hist else "no_history"
            ok_settle = hist and lineup != "not_starter"
            l5 = _l5(row)
            if ok_settle:
                settle_ok += 1
                by_week_settle[week] += 1
                settle_leagues[league] += 1
                l5_on_settle[str(l5)] += 1
                if not _saves_own_d_ok(row):
                    by_block["settle_but_own_d"] += 1
                elif l5 is None or l5 < 5:
                    by_block["settle_but_l5_under_5"] += 1
                elif not _saves_opp_off_ok(row):
                    by_block["settle_but_opp_off"] += 1
                else:
                    by_block["settle_l5_d_opp_ok"] += 1
                    from utils.soccer_board_coverage import game_on_covered_board
                    from utils.soccer_keep_gates import soccer_keep_eligible

                    _p(
                        f"  NEAR {stamp} {player} {row.get('team')} vs {row.get('opp_team')} "
                        f"league={league} l5={l5} ownD={row.get('OWN_DEF_TIER')} "
                        f"board={game_on_covered_board(row)} "
                        f"keep={soccer_keep_eligible(row)} "
                        f"ticket={soccer_ticket_gate_passes(row)}"
                    )
            elif not hist:
                by_block["no_past_box"] += 1
                nohist_leagues[league] += 1
            elif lineup == "not_starter":
                by_block["not_starter"] += 1
                history_leagues[league] += 1
            ticket = soccer_ticket_gate_passes(row)
            if ticket:
                passed += 1
                by_week_ticket[week] += 1
                _p(f"  PASS {stamp} {player} league={league} l5={l5} lineup={lineup}")
        _p(f"{stamp} goblin saves players {len(seen)}")
    _p(f"\nSep 1-27 unique Goblin Saves player-days: {rows}")
    _p(f"settlement pass (history + not a posted non-starter): {settle_ok}")
    _p(f"full ticket gate pass: {passed}")
    _p(f"settlement passes by iso week: {dict(sorted(by_week_settle.items()))}")
    _p(f"ticket passes by iso week: {dict(sorted(by_week_ticket.items()))}")
    _p(f"blocks: {dict(by_block)}")
    _p(f"L5 among settlement passes: {dict(l5_on_settle)}")
    _p(f"leagues with a settlement pass: {dict(settle_leagues.most_common(12))}")
    _p(f"leagues with no past box: {dict(nohist_leagues.most_common(12))}")
    _p(f"leagues of posted non-starters: {dict(history_leagues.most_common(8))}")


if __name__ == "__main__":
    main()
