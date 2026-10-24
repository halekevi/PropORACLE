#!/usr/bin/env python3
"""Shim: NBA Goblin OVER quote gates now live in ml.discover_gates."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO)]

from ml.discover_gates import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(["--sport", "NBA", "--book", "Goblin OVER", *sys.argv[1:]]))
