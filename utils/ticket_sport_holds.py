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
    "drive L5 and projection before this sport is ticketable."
)


def nba1q_tickets_held(*, env: dict[str, str] | None = None) -> bool:
    src = env if env is not None else os.environ
    override = str(src.get("PROPORACLE_NBA1Q_TICKETS") or "").strip().lower()
    if override in {"1", "true", "yes", "on"}:
        return False
    if override in {"0", "false", "no", "off"}:
        return True
    return True
