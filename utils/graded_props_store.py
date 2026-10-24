"""Where graded_props_<date>.json live, and how the live site gets them without git.

Until 2026-10-24 the daily pipeline force-added ``ui_runner/templates/graded_props_<date>.json`` to
``main`` and the Flask app read them from the checkout. 108 tracked files / 1.1 GB that are
regraded nightly made ``git status`` on the live tree take ~6 s and tripped publishes. They now
live on the data-only ``graded-data`` branch (``graded_props/graded_props_<date>.json`` +
``index.json``), written by ``scripts/Publish-GradedData.ps1``.

Resolution order for a date (first hit wins):

1. ``PROPORACLE_GRADED_PROPS_DIR`` (explicit)
2. ``<DATA_ROOT>/graded_props`` — on Railway that is the volume / ``/app/data``; filled by
   :func:`sync_from_branch` at boot (tarball of the branch) and by :func:`path_for_date`
   on demand (raw file fetch) for misses
3. ``ui_runner/templates`` (local pipeline tree; files are untracked there)
4. ``ui_runner/templates/archive``

Nothing here imports Flask. Pure paths + urllib so the pipeline, the app, and scripts share it.
"""
from __future__ import annotations

import io
import json
import os
import re
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable

GRADED_PROPS_RE = re.compile(r"^graded_props_(\d{4}-\d{2}-\d{2})\.json$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

RAW_BASE_DEFAULT = "https://raw.githubusercontent.com/halekevi/PropORACLE/graded-data/graded_props"
TARBALL_DEFAULT = "https://codeload.github.com/halekevi/PropORACLE/tar.gz/refs/heads/graded-data"

_NEG_CACHE: dict[str, float] = {}
_NEG_TTL_SEC = 600.0
_NEG_TTL_TRANSIENT_SEC = 45.0  # disk / network error, not a 404
_FETCH_LOCK = threading.Lock()

# Railway's writable layer stopped accepting files ~450 MB into the first full sync
# (2026-10-24 15:02 ET deploy: 07-01..08-16 landed, 08-20 onward failed). The store therefore
# keeps the newest dates within a byte budget and evicts the oldest dates to make room. A mounted
# volume (PROPORACLE_PERSISTENT_DATA_DIR) can raise the budget to hold everything.
DISK_BUDGET_MB_DEFAULT = 300
DISK_FREE_MARGIN_BYTES = 150 * 1024 * 1024


def _on_railway() -> bool:
    return bool((os.environ.get("RAILWAY_ENVIRONMENT") or "").strip())


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "on"}


def raw_base() -> str:
    return (os.environ.get("PROPORACLE_GRADED_RAW_BASE") or RAW_BASE_DEFAULT).strip().rstrip("/")


def tarball_url() -> str:
    return (os.environ.get("PROPORACLE_GRADED_TARBALL_URL") or TARBALL_DEFAULT).strip()


def fetch_enabled() -> bool:
    """On-demand raw fetch of a missing date. Default on for Railway, off locally."""
    raw = os.environ.get("PROPORACLE_GRADED_FETCH")
    if raw is None or not str(raw).strip():
        return _on_railway()
    return _truthy(raw)


def graded_props_dir(repo_root: Path, templates_dir: Path, data_root: Path | None = None) -> Path:
    """Primary (writable) directory for graded props."""
    explicit = (os.environ.get("PROPORACLE_GRADED_PROPS_DIR") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    if data_root is None:
        try:
            from utils.proporacle_data_root import persistent_data_dir

            data_root = persistent_data_dir(repo_root)
        except Exception:  # noqa: BLE001
            data_root = repo_root / "data"
    if _on_railway():
        d = Path(data_root) / "graded_props"
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError:
            return templates_dir
        return d
    # Local: the pipeline writes the exports into templates; keep reading them there unless a
    # synced data dir already exists and has files.
    d = Path(data_root) / "graded_props"
    if d.is_dir() and next(d.glob("graded_props_*.json"), None) is not None:
        return d
    return templates_dir


def search_dirs(repo_root: Path, templates_dir: Path, data_root: Path | None = None) -> list[Path]:
    out: list[Path] = []
    for d in (graded_props_dir(repo_root, templates_dir, data_root), templates_dir, templates_dir / "archive"):
        try:
            rd = d.resolve()
        except OSError:
            rd = d
        if rd not in out:
            out.append(rd)
    return out


def list_files(repo_root: Path, templates_dir: Path, data_root: Path | None = None) -> list[Path]:
    """All graded_props_*.json across the search dirs, one per date (earlier dir wins), sorted."""
    seen: dict[str, Path] = {}
    for d in search_dirs(repo_root, templates_dir, data_root):
        if not d.is_dir():
            continue
        for p in d.glob("graded_props_*.json"):
            if GRADED_PROPS_RE.match(p.name) and p.name not in seen:
                seen[p.name] = p
    return [seen[k] for k in sorted(seen)]


def signature(files: Iterable[Path]) -> str:
    files = list(files)
    if not files:
        return "empty"
    mt = max(f.stat().st_mtime_ns for f in files)
    return f"n={len(files)}:mt={mt}"


def _ssl_context():
    import ssl

    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()


def disk_budget_bytes() -> int | None:
    """Byte budget for graded files in the synced dir; None = unlimited (local trees)."""
    raw = (os.environ.get("PROPORACLE_GRADED_DISK_BUDGET_MB") or "").strip()
    if raw:
        try:
            mb = float(raw)
        except ValueError:
            mb = DISK_BUDGET_MB_DEFAULT
        return None if mb <= 0 else int(mb * 1024 * 1024)
    return DISK_BUDGET_MB_DEFAULT * 1024 * 1024 if _on_railway() else None


def _graded_files_in(d: Path) -> list[Path]:
    try:
        return sorted(p for p in d.glob("graded_props_*.json") if GRADED_PROPS_RE.match(p.name))
    except OSError:
        return []


def ensure_room(dest_dir: Path, need_bytes: int, *, protect: str = "") -> bool:
    """Evict the oldest dates in ``dest_dir`` until ``need_bytes`` fits the budget and the disk.

    Returns False when it still does not fit (so the caller skips the write instead of
    filling the disk). ``protect`` is a filename never evicted.
    """
    budget = disk_budget_bytes()
    files = _graded_files_in(dest_dir)
    total = sum((p.stat().st_size for p in files), 0)

    def _free() -> int:
        try:
            import shutil

            return shutil.disk_usage(str(dest_dir)).free
        except OSError:
            return 1 << 62

    for p in files:  # ascending date = oldest first
        fits_budget = budget is None or total + need_bytes <= budget
        fits_disk = _free() - need_bytes >= DISK_FREE_MARGIN_BYTES
        if fits_budget and fits_disk:
            return True
        if p.name == protect:
            continue
        try:
            sz = p.stat().st_size
            p.unlink()
            total -= sz
        except OSError:
            continue
    fits_budget = budget is None or total + need_bytes <= budget
    return fits_budget and (_free() - need_bytes >= DISK_FREE_MARGIN_BYTES)


def _download_to(url: str, dest: Path, *, timeout: int = 120, make_room: bool = True) -> str:
    """Stream ``url`` into ``dest`` atomically.

    Returns "ok", "notfound" (HTTP 404 → cache the miss), "noroom" (budget / disk) or
    "error" (network / other OSError → short retry window).
    """
    req = urllib.request.Request(url, headers={"User-Agent": "proporacle-graded-store/1"})
    tmp = dest.with_suffix(dest.suffix + f".part{os.getpid()}")
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context()) as resp:
            length = resp.headers.get("Content-Length")
            need = int(length) if length and length.isdigit() else 40 * 1024 * 1024
            if make_room and not ensure_room(dest.parent, need, protect=dest.name):
                return "noroom"
            with tmp.open("wb") as fh:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    fh.write(chunk)
        # Must be a JSON object before we accept it.
        with tmp.open("rb") as fh:
            head = fh.read(2)
        if head.lstrip()[:1] != b"{":
            tmp.unlink(missing_ok=True)
            return "error"
        os.replace(tmp, dest)
        return "ok"
    except urllib.error.HTTPError as exc:
        _cleanup(tmp)
        return "notfound" if exc.code == 404 else "error"
    except OSError as exc:  # includes URLError, disk full (ENOSPC)
        _cleanup(tmp)
        import errno

        if getattr(exc, "errno", None) in (errno.ENOSPC, errno.EDQUOT):
            return "noroom"
        return "error"
    except (TimeoutError, ValueError):
        _cleanup(tmp)
        return "error"


def _cleanup(tmp: Path) -> None:
    try:
        tmp.unlink(missing_ok=True)
    except OSError:
        pass


def path_for_date(
    date_str: str,
    repo_root: Path,
    templates_dir: Path,
    data_root: Path | None = None,
    *,
    fetch: bool | None = None,
) -> Path | None:
    """Local path for ``graded_props_<date>.json``; fetch from the data branch on a miss."""
    date_str = str(date_str or "").strip()[:10]
    if not DATE_RE.match(date_str):
        return None
    fname = f"graded_props_{date_str}.json"
    for d in search_dirs(repo_root, templates_dir, data_root):
        cand = d / fname
        if cand.is_file():
            return cand
    if fetch is None:
        fetch = fetch_enabled()
    if not fetch:
        return None
    now = time.monotonic()
    neg = _NEG_CACHE.get(fname)
    if neg is not None and now < neg:
        return None
    dest_dir = graded_props_dir(repo_root, templates_dir, data_root)
    dest = dest_dir / fname
    with _FETCH_LOCK:
        if dest.is_file():
            return dest
        status = _download_to(f"{raw_base()}/{fname}", dest)
    if status == "ok":
        return dest
    _NEG_CACHE[fname] = now + (_NEG_TTL_SEC if status == "notfound" else _NEG_TTL_TRANSIENT_SEC)
    return None


def sync_newest_first(dest_dir: Path, *, timeout: int = 120, budget: int | None = None) -> dict:
    """Fetch graded_props files one by one, newest date first, until the byte budget is used.

    This is the Railway boot path: the writable layer cannot hold the whole branch, and the
    grades pages people open are the recent ones. Older dates are still served on demand (with
    eviction of the oldest cached date). Returns a report dict.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    if budget is None:
        budget = disk_budget_bytes()
    report = {
        "mode": "newest_first",
        "dest": str(dest_dir),
        "budget_mb": None if budget is None else round(budget / 1048576),
        "written": 0,
        "skipped": 0,
        "stopped_at": "",
        "ok": False,
        "error": "",
    }
    t0 = time.monotonic()
    idx = read_index(timeout=30)
    dates = sorted((idx or {}).get("dates") or [], reverse=True)
    if not dates:
        report["error"] = "index.json unavailable"
        report["seconds"] = round(time.monotonic() - t0, 1)
        return report
    used = sum((p.stat().st_size for p in _graded_files_in(dest_dir)), 0)
    for d in dates:
        fname = f"graded_props_{d}.json"
        dest = dest_dir / fname
        if dest.is_file():
            report["skipped"] += 1
            continue
        # Do not evict newer files to make room for an older one: stop at the budget instead.
        status = _download_to(f"{raw_base()}/{fname}", dest, timeout=timeout, make_room=False)
        if status == "ok":
            report["written"] += 1
            used += dest.stat().st_size
            if budget is not None and used >= budget:
                report["stopped_at"] = d
                break
        elif status == "noroom":
            report["stopped_at"] = d
            break
        elif status == "error":
            report["error"] = f"fetch error at {d}"
    report["ok"] = not report["error"]
    report["seconds"] = round(time.monotonic() - t0, 1)
    try:
        (dest_dir / ".last_sync.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    except OSError:
        pass
    return report


def sync_from_branch(dest_dir: Path, *, timeout: int = 600, overwrite: bool = True) -> dict:
    """Download the graded-data branch tarball and extract graded_props_*.json into ``dest_dir``.

    Streams the gzip tar (no full download into memory). Returns a small report dict. Use only
    where the whole set fits (local trees, a mounted volume); Railway boot uses
    :func:`sync_newest_first`.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = tarball_url()
    report = {"url": url, "dest": str(dest_dir), "written": 0, "skipped": 0, "ok": False, "error": ""}
    req = urllib.request.Request(url, headers={"User-Agent": "proporacle-graded-store/1"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context()) as resp:
            with tarfile.open(fileobj=resp, mode="r|gz") as tar:
                for member in tar:
                    name = Path(member.name).name
                    if not member.isfile() or not GRADED_PROPS_RE.match(name):
                        continue
                    if "/graded_props/" not in member.name.replace("\\", "/"):
                        continue
                    out = dest_dir / name
                    if out.is_file() and not overwrite and out.stat().st_size == member.size:
                        report["skipped"] += 1
                        continue
                    src = tar.extractfile(member)
                    if src is None:
                        continue
                    tmp = out.with_suffix(out.suffix + f".part{os.getpid()}")
                    with tmp.open("wb") as fh:
                        while True:
                            chunk = src.read(1 << 20)
                            if not chunk:
                                break
                            fh.write(chunk)
                    os.replace(tmp, out)
                    report["written"] += 1
        report["ok"] = True
    except (urllib.error.URLError, TimeoutError, OSError, tarfile.TarError, ValueError) as exc:
        report["error"] = str(exc)
    report["seconds"] = round(time.monotonic() - t0, 1)
    try:
        (dest_dir / ".last_sync.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    except OSError:
        pass
    return report


def read_index(*, timeout: int = 30) -> dict | None:
    """index.json from the data branch (dates list), or None."""
    url = raw_base().rsplit("/", 1)[0] + "/index.json"
    req = urllib.request.Request(url, headers={"User-Agent": "proporacle-graded-store/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context()) as resp:
            return json.loads(io.TextIOWrapper(resp, encoding="utf-8-sig").read())
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None


__all__ = [
    "GRADED_PROPS_RE",
    "graded_props_dir",
    "search_dirs",
    "list_files",
    "signature",
    "path_for_date",
    "sync_from_branch",
    "sync_newest_first",
    "ensure_room",
    "disk_budget_bytes",
    "read_index",
    "fetch_enabled",
    "raw_base",
    "tarball_url",
]
