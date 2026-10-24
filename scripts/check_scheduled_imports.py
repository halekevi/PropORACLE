#!/usr/bin/env python3
"""Static import check for every script the schedulers run.

py_compile only proves syntax. This walks the AST of each scheduled script
(and, transitively, the first-party modules it imports) and resolves every
``import`` / ``from ... import`` against the tree on disk:

  missing_module  the target file / package does not exist in this tree
                  (e.g. utils.l5_ranked_by_cell on the live tree)
  missing_name    the module exists but has no top-level def / class /
                  assignment / import alias / submodule with that name

Local (function-level) imports are included, since that is where most of the
"works in one tree, not the other" breaks hide. Third-party modules are
checked with importlib.util.find_spec only.

  py -3.14 scripts/check_scheduled_imports.py
  py -3.14 scripts/check_scheduled_imports.py --root H:\\PropORACLE_main_cp
  py -3.14 scripts/check_scheduled_imports.py --script scripts/report_track_comparison.py

Exit 1 when anything is missing.
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import re
import sys
from functools import lru_cache
from pathlib import Path

FIRST_PARTY = ("utils", "scripts", "proporacle", "ui_runner", "ml", "Sports")
_PS1_REF = re.compile(
    r"(?:(?:scripts|Sports)[\\/][A-Za-z0-9_\\/.-]+?\.py)|(?:-m\s+(scripts\.[A-Za-z0-9_.]+))",
)
ALWAYS = (
    "scripts/build_goblin70_tickets.py",
    "scripts/combined_slate_tickets.py",
    "scripts/report_track_comparison.py",
    "scripts/check_leg_ledger.py",
    "scripts/rank_best_props_today.py",
)


def discover_scripts(root: Path) -> list[Path]:
    found: set[Path] = set()
    for rel in ALWAYS:
        p = root / rel
        if p.is_file():
            found.add(p)
    for ps1 in list(root.glob("*.ps1")) + list((root / "scripts").glob("*.ps1")):
        try:
            text = ps1.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for m in _PS1_REF.finditer(text):
            mod = m.group(1)
            if mod:
                p = root / (mod.replace(".", "/") + ".py")
            else:
                p = root / m.group(0).replace("\\", "/")
            if p.is_file():
                found.add(p.resolve())
    return sorted(found)


_SKIP_DIRS = {"node_modules", ".venv", "venv", "__pycache__", ".git", "site-packages", "_archive", "archive"}


class Resolver:
    def __init__(self, root: Path):
        self.root = root
        self._bare: dict[str, list[Path]] | None = None

    def _bare_index(self) -> dict[str, list[Path]]:
        """basename -> module files anywhere in the tree (sys.path.insert targets)."""
        if self._bare is None:
            idx: dict[str, list[Path]] = {}
            for p in self.root.rglob("*.py"):
                if any(part in _SKIP_DIRS for part in p.parts):
                    continue
                idx.setdefault(p.stem, []).append(p)
            self._bare = idx
        return self._bare

    @lru_cache(maxsize=None)
    def module_path(self, dotted: str, script_dir: Path | None = None) -> Path | None:
        parts = dotted.split(".")
        bases = [self.root, self.root / "scripts"]
        if script_dir is not None:
            bases.insert(0, script_dir)
            # Sports/<X>/scripts siblings add their own dir and Sports/<X> to sys.path.
            for up in (script_dir.parent, script_dir.parent.parent):
                bases.append(up)
        for base in bases:
            cand = base.joinpath(*parts)
            if cand.with_suffix(".py").is_file():
                return cand.with_suffix(".py")
            if (cand / "__init__.py").is_file():
                return cand / "__init__.py"
        if len(parts) == 1 and parts[0] not in FIRST_PARTY:
            hits = self._bare_index().get(parts[0]) or []
            if script_dir is not None:
                hits = sorted(hits, key=lambda p: len(set(p.parts) ^ set(script_dir.parts)))
            if hits:
                return hits[0]
        return None

    def is_first_party(self, dotted: str, script_dir: Path | None) -> bool:
        head = dotted.split(".")[0]
        if head in FIRST_PARTY:
            return True
        # Bare sibling modules (prizepicks_league_ids, ticket_leg_index, ...)
        return self.module_path(dotted, script_dir) is not None

    @staticmethod
    def third_party_exists(dotted: str) -> bool:
        head = dotted.split(".")[0]
        try:
            return importlib.util.find_spec(head) is not None
        except (ImportError, ValueError, AttributeError):
            return True  # cannot judge; do not flag


@lru_cache(maxsize=None)
def exported_names(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except SyntaxError:
        return {"*"}
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                for n in ast.walk(t):
                    if isinstance(n, ast.Name):
                        names.add(n.id)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for a in node.names:
                names.add((a.asname or a.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name == "*":
                    names.add("*")
                else:
                    names.add(a.asname or a.name)
        elif isinstance(node, (ast.If, ast.Try)):
            # try/except ImportError fallbacks and version guards
            for sub in ast.walk(node):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    names.add(sub.name)
                elif isinstance(sub, ast.Assign):
                    for t in sub.targets:
                        if isinstance(t, ast.Name):
                            names.add(t.id)
                elif isinstance(sub, ast.ImportFrom):
                    for a in sub.names:
                        names.add(a.asname or a.name)
                elif isinstance(sub, ast.Import):
                    for a in sub.names:
                        names.add((a.asname or a.name).split(".")[0])
    return names


def check_file(path: Path, res: Resolver, *, seen: set[Path], problems: list[dict], depth: int, max_depth: int) -> None:
    path = path.resolve()
    if path in seen:
        return
    seen.add(path)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except SyntaxError as exc:
        problems.append({"file": str(path), "kind": "syntax_error", "detail": str(exc)[:120]})
        return
    script_dir = path.parent
    optional_lines = _optional_import_lines(tree)
    for node in ast.walk(tree):
        if getattr(node, "lineno", None) in optional_lines:
            continue  # inside try/except ImportError: a fallback by design
        if isinstance(node, ast.Import):
            for a in node.names:
                _check_module(a.name, None, path, node.lineno, script_dir, res, seen, problems, depth, max_depth)
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import; resolve against the package dir
                base = script_dir
                for _ in range(node.level - 1):
                    base = base.parent
                dotted = node.module or ""
                target = base.joinpath(*dotted.split(".")) if dotted else base
                mod_path = target.with_suffix(".py") if target.with_suffix(".py").is_file() else (target / "__init__.py")
                if not mod_path.is_file():
                    problems.append({"file": str(path), "line": node.lineno, "kind": "missing_module", "module": f"{'.' * node.level}{dotted}"})
                    continue
                _check_names(mod_path, node, path, res, problems)
                if depth < max_depth:
                    check_file(mod_path, res, seen=seen, problems=problems, depth=depth + 1, max_depth=max_depth)
                continue
            _check_module(node.module or "", node, path, node.lineno, script_dir, res, seen, problems, depth, max_depth)


def _optional_import_lines(tree: ast.AST) -> set[int]:
    """Line numbers of import statements guarded by except ImportError / Exception."""
    out: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        catches = False
        for h in node.handlers:
            if h.type is None:
                catches = True
            else:
                for n in ast.walk(h.type):
                    if isinstance(n, ast.Name) and n.id in {"ImportError", "ModuleNotFoundError", "Exception", "BaseException"}:
                        catches = True
        if not catches:
            continue
        for stmt in node.body:
            for sub in ast.walk(stmt):
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    out.add(sub.lineno)
    return out


def _check_module(dotted, from_node, path, lineno, script_dir, res, seen, problems, depth, max_depth):
    if not dotted:
        return
    if not res.is_first_party(dotted, script_dir):
        if not res.third_party_exists(dotted):
            problems.append({"file": str(path), "line": lineno, "kind": "missing_third_party", "module": dotted})
        return
    mod_path = res.module_path(dotted, script_dir)
    if mod_path is None:
        problems.append({"file": str(path), "line": lineno, "kind": "missing_module", "module": dotted})
        return
    if from_node is not None:
        _check_names(mod_path, from_node, path, res, problems)
    if depth < max_depth:
        check_file(mod_path, res, seen=seen, problems=problems, depth=depth + 1, max_depth=max_depth)


def _check_names(mod_path: Path, node: ast.ImportFrom, path: Path, res: Resolver, problems: list[dict]) -> None:
    names = exported_names(mod_path)
    if "*" in names:
        return
    pkg_dir = mod_path.parent if mod_path.name == "__init__.py" else None
    for a in node.names:
        if a.name == "*" or a.name in names:
            continue
        if pkg_dir is not None and ((pkg_dir / f"{a.name}.py").is_file() or (pkg_dir / a.name / "__init__.py").is_file()):
            continue
        problems.append(
            {"file": str(path), "line": node.lineno, "kind": "missing_name", "module": node.module, "name": a.name}
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path(__file__).resolve().parent.parent))
    ap.add_argument("--script", action="append", default=[], help="Check only these (repeatable)")
    ap.add_argument("--max-depth", type=int, default=2, help="How far to follow first-party imports")
    ap.add_argument("--list", action="store_true", help="Print the discovered scheduled set and exit")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    scripts = [Path(s) if Path(s).is_absolute() else root / s for s in args.script] or discover_scripts(root)
    if args.list:
        for s in scripts:
            print(s.relative_to(root))
        return 0
    res = Resolver(root)
    problems: list[dict] = []
    for s in scripts:
        seen: set[Path] = set()
        check_file(s, res, seen=seen, problems=problems, depth=0, max_depth=args.max_depth)
    # de-dupe
    uniq = {tuple(sorted(p.items())): p for p in problems}
    problems = sorted(uniq.values(), key=lambda p: (p.get("kind", ""), p.get("file", ""), p.get("line", 0)))
    print(f"root {root}: {len(scripts)} scheduled scripts, {len(problems)} problem(s)")
    for p in problems:
        rel = Path(p["file"])
        try:
            rel = rel.relative_to(root)
        except ValueError:
            pass
        extra = p.get("module", "")
        if p.get("name"):
            extra += f" -> {p['name']}"
        print(f"  {p['kind']:20} {rel}:{p.get('line', '')}  {extra}{p.get('detail', '')}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
