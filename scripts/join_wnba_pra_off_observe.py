#!/usr/bin/env python3
"""Join graded_props HIT/MISS onto the WNBA PRA Off-without-S/A observe ledger."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.wnba_pra_off_observe import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
