"""Pull graded_props_<date>.json from the graded-data branch into the app's graded dir, then ingest.

Run in the background by scripts/railway_serve.py at boot (gunicorn starts immediately; /grades
for a date not yet synced is fetched on demand by utils.graded_props_store.path_for_date).

    py scripts/sync_graded_props_from_branch.py            # sync + ingest --all --purge-demo
    py scripts/sync_graded_props_from_branch.py --no-ingest
    py scripts/sync_graded_props_from_branch.py --dest /app/data/graded_props

Env: PROPORACLE_GRADED_TARBALL_URL (default codeload tar.gz of graded-data),
     PROPORACLE_GRADED_PROPS_DIR / PROPORACLE_PERSISTENT_DATA_DIR (destination resolution).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils import graded_props_store as store  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dest", default="", help="override destination directory")
    ap.add_argument("--no-ingest", action="store_true")
    ap.add_argument("--newest-first", action="store_true", help="per-file newest-first sync (default on Railway)")
    ap.add_argument("--timeout", type=int, default=600)
    a = ap.parse_args()

    templates = _REPO / "ui_runner" / "templates"
    dest = Path(a.dest) if a.dest else store.graded_props_dir(_REPO, templates)
    if dest.resolve() == templates.resolve():
        # Local pipeline tree: the exports are written here by the grader; do not overwrite them
        # with the branch copy (which may lag a regrade that has not been published yet).
        print(f"[graded-sync] dest is the templates dir ({dest}); skipping tarball sync", flush=True)
        rep = {"ok": True, "written": 0, "skipped": 0, "seconds": 0.0}
    elif store.disk_budget_bytes() is not None or a.newest_first:
        # Railway: the writable layer stopped at ~450 MB on the first full sync. Newest dates
        # first, stop at the budget; older dates are fetched on demand with eviction.
        print(f"[graded-sync] newest-first {store.raw_base()} -> {dest} (budget {store.disk_budget_bytes()})", flush=True)
        rep = store.sync_newest_first(dest, timeout=a.timeout)
        print(
            f"[graded-sync] ok={rep['ok']} written={rep['written']} skipped={rep['skipped']} "
            f"stopped_at={rep.get('stopped_at') or '-'} {rep.get('seconds')}s {rep.get('error') or ''}",
            flush=True,
        )
    else:
        print(f"[graded-sync] {store.tarball_url()} -> {dest}", flush=True)
        rep = store.sync_from_branch(dest, timeout=a.timeout)
        print(
            f"[graded-sync] ok={rep['ok']} written={rep['written']} skipped={rep['skipped']} "
            f"{rep.get('seconds')}s {rep.get('error') or ''}",
            flush=True,
        )
    if a.no_ingest:
        return 0 if rep["ok"] else 1
    ingest = _REPO / "scripts" / "ingest_graded_to_income_db.py"
    if not ingest.is_file():
        return 0
    n_files = len(store.list_files(_REPO, templates))
    if not n_files:
        print("[graded-sync] no graded_props files to ingest", flush=True)
        return 0 if rep["ok"] else 1
    print(f"[graded-sync] ingesting {n_files} graded_props files -> proporacle_income.db", flush=True)
    rc = subprocess.call([sys.executable, str(ingest), "--all", "--purge-demo"], cwd=str(_REPO))
    return rc if rc else (0 if rep["ok"] else 1)


if __name__ == "__main__":
    raise SystemExit(main())
