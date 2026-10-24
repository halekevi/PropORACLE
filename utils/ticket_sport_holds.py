"""Ticket holds that are policy, not missing keep-files.

NBA1Q: 1Q Goblin Points can show a full-game projection/edge (Gafford 1.5 vs ~16)
and a false L5=5 if period history is missing or mixed with full-game logs.
Period fetch exists; hold tickets until 1Q L5/projection are period-only.
Re-enable with PROPORACLE_NBA1Q_TICKETS=1 after that audit.
"""

from __future__ import annotations

import os

NBA1Q_HOLD_REASON = (
    "NBA1Q tickets held: 1Q lines can inherit full-game averages/L5 "
    "(Gafford Goblin Points 1.5 vs ~16 projection). Period history must "
    "drive L5 and projection before this sport is ticketable. Early-October "
    "cards were a May Finals leftover (not leak cost); 2026-10-20 is the "
    "first real 1Q slate and is two slips — not enough to price the leak. "
    "Judge on forward grades after the period-L5 path is live."
)


def nba1q_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    override = str(src.get("PROPORACLE_NBA1Q_TICKETS") or "").strip().lower()
    if override in {"1", "true", "yes", "on"}:
        return False
    if override in {"0", "false", "no", "off"}:
        return True
    return True


NBA_G70_HOLD_REASON = (
    "NBA Goblin-70 held: the generic L5=5+L10>=8+D stack is 0.668 vs ~0.737 "
    "breakeven at 2.5x (~-0.07 EV/leg). Live Goblin quotes (2-leg ~1.4x, "
    "3-leg ~1.65x) raise that bar. Shadow/observe until a +EV gate "
    "(proj_edge scout) is chosen. Re-enable with PROPORACLE_NBA_G70_TICKETS=1."
)

NBA_MIXER_HOLD_REASON = (
    "NBA mixer/Standard held: ml_prob is the step7 default (~0.49), not a 7b "
    "score, so rank-by-ml_prob is arbitrary. Hold until step7b actually runs. "
    "Re-enable with PROPORACLE_NBA_MIXER_TICKETS=1."
)


def _hold_flag(src: dict[str, str], key: str, *, default: bool = True) -> bool:
    override = str(src.get(key) or "").strip().lower()
    if override in {"1", "true", "yes", "on"}:
        return False
    if override in {"0", "false", "no", "off"}:
        return True
    return default


def nba_g70_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    return _hold_flag(src, "PROPORACLE_NBA_G70_TICKETS")


def nba_mixer_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    return _hold_flag(src, "PROPORACLE_NBA_MIXER_TICKETS")


# Face audit 2026-10-24: NHL graded rows carry default labels (zero
# Goblin/Demon) on 45 dates 2026-02-25..06-15 and laddered Standard on
# 03-06..03-08 (17,437 decided legs) — most of the regular-season sample. The
# L10>=8 Standard UNDER 70.9% n=1266 cell was measured there: on those dates
# Standard UNDER L5=5+L10>=8 reads points 130/162 = 80.2%, assists 133/162 =
# 82.1%, goals 107/126 = 84.9%, while real-face dates give points 5/6,
# assists 1/2, goals 0/0 — the "Standard UNDER" rows were Demon rungs, the same
# shape as NFL sacks. That explains the 53.4% forward read. Goblin OVER cells
# (sog 2472/3959 = 62.4%, points 845/1398 = 60.4% on verified dates) were
# Goblin-labeled rows and stand. Keep the hold; do not re-price NHL Standard
# UNDER from the Feb–Jun sample.
NHL_STANDARD_UNDER_CELL_FACES_VERIFIED = False
NHL_UNVERIFIED_FACE_WINDOW = ("2026-02-25", "2026-06-15")

NHL_MIXER_HOLD_REASON = (
    "NHL mixer held: NHL has no keep gate and the generic L5=5+L10>=8+D stack "
    "does not lift it (Goblin OVER 61.9% n=21 vs 61.3% ungated; Standard UNDER "
    "51.2% n=285 vs 58.6% ungated, below the 57.7% 3x breakeven). The mixer was "
    "still seeding 3-20 NHL Goblin OVER legs a day. L10>=8 Standard UNDER "
    "(70.9% n=1266) is regular-season-only and 53.4% once judged forward, and "
    "the 2026-10-24 face audit shows that cell was Demon rungs graded as "
    "Standard on default-label Feb-Jun dates, so it is not a replacement gate "
    "either. Re-enable with PROPORACLE_NHL_MIXER_TICKETS=1."
)


TENNIS_TOTAL_GAMES_HOLD_REASON = (
    "Tennis Goblin OVER Total Games held from tickets (2026-10-24): on "
    "face-verified dates the keep cell L5>=4+L10>=8 reads 35/62 = 56.5% "
    "(keep_gate_face_split.json) against the 70.2% price, and a 2-leg at the "
    "1.6x quoted today needs ~79% per leg. Hold until the cell is re-measured on "
    "a face-verified step8 join. Games Won is unaffected. Re-enable with "
    "PROPORACLE_TENNIS_TOTAL_GAMES_TICKETS=1."
)


def tennis_total_games_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    return _hold_flag(src, "PROPORACLE_TENNIS_TOTAL_GAMES_TICKETS")


def nhl_mixer_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    return _hold_flag(src, "PROPORACLE_NHL_MIXER_TICKETS")
