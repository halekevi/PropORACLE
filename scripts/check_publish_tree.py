#!/usr/bin/env python3
"""Pre-publish tree check for the live checkout.

Why: on 2026-10-20 a half-landed edit to ``scripts/build_goblin70_tickets.py`` imported
``utils.postseason_slate`` (never committed). Every Goblin-70 build then died with
ModuleNotFoundError and seven publishes failed with only a line in a log nobody read.

The live checkout is *always* dirty with data files and unrelated WIP, so a blanket
"git status must be clean" would block every publish. This checks only the code the
publish path actually runs: the transitive local-import closure of the Goblin-70 builder
and the live-publish guard. It fails when any file in that closure is

  * modified / staged / untracked (code that differs from what main will run), or
  * a module-level local import that does not resolve to a file (missing dependency).

Exit 0 = ok, 1 = problem (details on stdout). ``--json`` prints a machine-readable report.
"""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path

ENTRY_POINTS = (
    "scripts/build_goblin70_tickets.py",
    "scripts/assert_live_publish.py",
)
LOCAL_PREFIXES = ("utils", "scripts")


def _resolve(root: Path, dotted: str, importer_dir: Path) -> Path | None:
    parts = dotted.split(".")
    # Bare `import prop_hit_tiers` works because callers sys.path.insert(scripts/).
    for base in (root, importer_dir, root / "scripts"):
        p = base.joinpath(*parts)
        if p.with_suffix(".py").is_file():
            return p.with_suffix(".py")
        if (p / "__init__.py").is_file():
            return p / "__init__.py"
    return None


def _is_local(root: Path, dotted: str, importer_dir: Path) -> bool:
    head = dotted.split(".")[0]
    if head in LOCAL_PREFIXES:
        return True
    return (importer_dir / f"{head}.py").is_file() or (root / "scripts" / f"{head}.py").is_file()


def _imports(tree: ast.AST) -> list[tuple[str, bool]]:
    """(dotted module, module_level) for every import, including `from pkg import submodule`."""
    out: list[tuple[str, bool]] = []
    top = set(id(n) for n in getattr(tree, "body", []))
    for node in ast.walk(tree):
        level = id(node) in top
        if isinstance(node, ast.Import):
            for a in node.names:
                out.append((a.name, level))
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.append((node.module, level))
            for a in node.names:
                out.append((f"{node.module}.{a.name}", False))  # submodule guess, never "missing"
    return out


def closure(root: Path) -> tuple[set[Path], list[str]]:
    seen: set[Path] = set()
    missing: list[str] = []
    stack = [root / e for e in ENTRY_POINTS if (root / e).is_file()]
    while stack:
        f = stack.pop()
        if f in seen:
            continue
        seen.add(f)
        try:
            tree = ast.parse(f.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError as exc:
            missing.append(f"{f.relative_to(root).as_posix()}: syntax error line {exc.lineno}")
            continue
        for dotted, module_level in _imports(tree):
            if not _is_local(root, dotted, f.parent):
                continue
            target = _resolve(root, dotted, f.parent)
            if target is None:
                # `from pkg import name` where name is an attribute is not a file; only the
                # module part (module_level=True entries) can be reported as missing.
                if module_level:
                    missing.append(f"{f.relative_to(root).as_posix()} imports {dotted} (no such file)")
                continue
            if target not in seen:
                stack.append(target)
    return seen, missing


def git_dirty(root: Path, files: set[Path]) -> list[str]:
    rels = sorted(f.relative_to(root).as_posix() for f in files)
    if not rels:
        return []
    res = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all", "--", *rels],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    return [ln.rstrip() for ln in res.stdout.splitlines() if ln.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    files, missing = closure(root)
    dirty = git_dirty(root, files)
    # A dependency that exists on disk but is untracked also lives outside main.
    problems = [f"DIRTY  {d}" for d in dirty] + [f"MISSING {m}" for m in missing]
    report = {"root": str(root), "files_checked": len(files), "dirty": dirty, "missing": missing, "ok": not problems}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"[publish-tree] {len(files)} publish-path code files checked under {root}")
        for p in problems:
            print(f"[publish-tree] {p}")
        if not problems:
            print("[publish-tree] OK - publish-path code matches git, no missing local imports")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
