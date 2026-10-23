#!/usr/bin/env python3
"""Import smoke test for scheduled-run entrypoints.

Fails fast when a live-tree script imports a util that only exists in another
worktree (cfb_l8_bonus / role_from_snap class of bugs).

  py -3.14 scripts/smoke_import_scheduled.py
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

# Modules scheduled runs import early. Keep this list short and critical-path.
MODULES = [
    "utils.cfb_keep_gates",
    "utils.nfl_prop_defense",
    "utils.mlb_keep_gates",
    "utils.soccer_keep_gates",
    "utils.tennis_keep_gates",
    "utils.ticket_70_pool",
    "utils.ui_live_json",
    "rank_best_props_today",
    "build_goblin70_tickets",
    "combined_slate_tickets",
]

# Symbols that must exist on the live tree (caught today's two crashes).
SYMBOLS = [
    ("utils.cfb_keep_gates", "cfb_l8_bonus"),
    ("utils.nfl_prop_defense", "role_from_snap"),
]


def main() -> int:
    errors: list[str] = []
    for name in MODULES:
        try:
            importlib.import_module(name)
            print(f"  OK  import {name}")
        except Exception as exc:  # noqa: BLE001 — surface any import failure
            errors.append(f"import {name}: {type(exc).__name__}: {exc}")
            print(f"  FAIL import {name}: {type(exc).__name__}: {exc}")
    for mod_name, attr in SYMBOLS:
        try:
            mod = importlib.import_module(mod_name)
            if not hasattr(mod, attr):
                errors.append(f"missing {mod_name}.{attr}")
                print(f"  FAIL missing {mod_name}.{attr}")
            else:
                print(f"  OK  symbol {mod_name}.{attr}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"symbol {mod_name}.{attr}: {exc}")
            print(f"  FAIL symbol {mod_name}.{attr}: {exc}")
    if errors:
        print("\n[smoke-import] FAILED — fix live-tree imports before scheduled fetch:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\n[smoke-import] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
