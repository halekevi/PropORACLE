#!/usr/bin/env python3
"""Import smoke test for scheduled-run entrypoints.

1) Import critical modules (fast fail on missing packages).
2) AST-scan every .py under scripts/, utils/, Sports/*/scripts for
   ``from utils.X import a, b`` — including imports inside function bodies —
   and verify each name exists on the live-tree module. Catches the
   role_from_snap class of bugs (import buried in a rarely-hit function).

  py -3.14 scripts/smoke_import_scheduled.py
"""
from __future__ import annotations

import ast
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
    "utils.step4_asof",
    "utils.proporacle_ref_db",
    "utils.stat_attach_guard",
    "utils.ticket_sport_holds",
    "utils.ui_live_json",
    "rank_best_props_today",
    "build_goblin70_tickets",
    "combined_slate_tickets",
]

# Symbols that must exist on the live tree (caught today's two crashes).
SYMBOLS = [
    ("utils.cfb_keep_gates", "cfb_l8_bonus"),
    ("utils.nfl_prop_defense", "role_from_snap"),
    ("utils.step4_asof", "game_date_before_slate_sql"),
    ("utils.proporacle_ref_db", "resolve_ref_db"),
    ("utils.stat_attach_guard", "l5_eq5_ok"),
    ("utils.ticket_sport_holds", "nba_mixer_sport_hold_unlock_ok"),
]

SCAN_ROOTS = [
    ROOT / "scripts",
    ROOT / "utils",
    ROOT / "Sports",
]


def _iter_py_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if not root.is_dir():
            continue
        for p in root.rglob("*.py"):
            if any(part in {"__pycache__", ".venv", "node_modules", "archive"} for part in p.parts):
                continue
            files.append(p)
    return files


def _utils_import_names(tree: ast.AST) -> list[tuple[str, list[str], int]]:
    """Return (module, [names], lineno) for ``from utils… import …`` anywhere in the tree."""
    out: list[tuple[str, list[str], int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        mod = node.module or ""
        if not mod.startswith("utils"):
            continue
        names = [a.name for a in node.names if a.name != "*"]
        if not names:
            continue
        out.append((mod, names, int(getattr(node, "lineno", 0) or 0)))
    return out


def _module_defines(mod_name: str, attr: str, cache: dict[str, set[str] | None]) -> bool | None:
    """True/False if module loads; None if module itself is missing."""
    if mod_name not in cache:
        try:
            mod = importlib.import_module(mod_name)
            cache[mod_name] = set(dir(mod))
        except Exception:  # noqa: BLE001
            cache[mod_name] = None
    names = cache[mod_name]
    if names is None:
        return None
    if attr in names:
        return True
    # `from utils import graded_props_store` is a submodule, not a package attr.
    try:
        importlib.import_module(f"{mod_name}.{attr}")
        names.add(attr)
        return True
    except ModuleNotFoundError:
        return False
    except Exception:  # noqa: BLE001 — submodule file exists but failed to load
        return None


def ast_scan_utils_imports() -> list[str]:
    errors: list[str] = []
    cache: dict[str, set[str] | None] = {}
    checked = 0
    for path in _iter_py_files():
        try:
            src = path.read_text(encoding="utf-8")
            tree = ast.parse(src, filename=str(path))
        except SyntaxError as exc:
            errors.append(f"syntax {path.relative_to(ROOT)}: {exc}")
            continue
        except OSError as exc:
            errors.append(f"read {path.relative_to(ROOT)}: {exc}")
            continue
        for mod_name, names, lineno in _utils_import_names(tree):
            for attr in names:
                checked += 1
                ok = _module_defines(mod_name, attr, cache)
                if ok is None:
                    errors.append(
                        f"{path.relative_to(ROOT)}:{lineno}: "
                        f"cannot import module {mod_name} (needed for {attr})"
                    )
                elif not ok:
                    errors.append(
                        f"{path.relative_to(ROOT)}:{lineno}: "
                        f"from {mod_name} import {attr} — name missing on live tree"
                    )
    n_files = len(_iter_py_files())
    print(f"  OK  ast-scan checked {checked} utils import name(s) across {n_files} files")
    return errors


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

    print("  … AST scan (includes imports inside function bodies)")
    errors.extend(ast_scan_utils_imports())

    if errors:
        print("\n[smoke-import] FAILED — fix live-tree imports before scheduled fetch:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\n[smoke-import] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
