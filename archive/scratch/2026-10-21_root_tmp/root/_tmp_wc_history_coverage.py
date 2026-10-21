"""Verify competition_history coverage of the real 2026 WC window vs cache."""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    cache = json.loads((ROOT / "data/cache/competition_history.json").read_text(encoding="utf-8"))
    wc = (cache.get("competitions") or {}).get("SOCCER|WORLDCUP")
    print("=== CACHE SOCCER|WORLDCUP ===")
    print(wc)

    hits: dict[str, Counter] = defaultdict(Counter)
    files_with_wc: list[tuple[str, int, str]] = []
    patterns = [
        "**/step1*soccer*.csv",
        "**/step8*soccer*.csv",
        "**/s1_soccer*.csv",
    ]
    seen: set[str] = set()
    files: list[Path] = []
    for pat in patterns:
        for fp in ROOT.glob(pat):
            try:
                rp = str(fp.resolve())
            except OSError:
                rp = str(fp)
            if rp in seen:
                continue
            seen.add(rp)
            files.append(fp)
    print(f"scanned candidate files: {len(files)}")

    for fp in files:
        day = ""
        for part in fp.parts:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", part):
                day = part
                break
        try:
            hdr = pd.read_csv(fp, nrows=0)
        except Exception:
            continue
        cols = {c.lower(): c for c in hdr.columns}
        if "league" not in cols:
            continue
        want = [cols["league"]]
        for k in ("start_time", "game_time", "date"):
            if k in cols:
                want.append(cols[k])
        try:
            df = pd.read_csv(fp, low_memory=False, usecols=want)
        except Exception:
            continue
        lg = df[cols["league"]].astype(str).str.upper().str.replace(" ", "", regex=False)
        mask = lg.str.startswith("WORLDCUP")
        if not mask.any():
            continue
        files_with_wc.append((str(fp.relative_to(ROOT)), int(mask.sum()), day))
        sub = df.loc[mask]
        date_c = next((cols[k] for k in ("start_time", "game_time", "date") if k in cols), None)
        for r in sub.itertuples(index=False):
            lgv = str(getattr(r, cols["league"])).upper().replace(" ", "")
            if lgv.startswith("WORLDCUP"):
                lgv = "WORLDCUP"
            row_day = day
            if date_c:
                raw = str(getattr(r, date_c) or "")
                if len(raw) >= 10 and raw[4] == "-":
                    row_day = raw[:10]
            if row_day:
                hits[row_day][lgv] += 1

    print("\n=== Files containing WORLDCUP* rows ===")
    for path, n, day in sorted(files_with_wc, key=lambda x: (x[2] or "", x[0])):
        print(f"  {(day or '?'):10} n={n:5d}  {path}")
    print(f"total files with WC: {len(files_with_wc)}")

    print("\n=== WORLDCUP props by date (Jun-Jul) ===")
    for d in sorted(hits):
        if d.startswith("2026-06") or d.startswith("2026-07"):
            print(d, dict(hits[d]), "total", sum(hits[d].values()))
    print("--- all WC days from step archives ---")
    print(
        f"days={len(hits)} props={sum(sum(c.values()) for c in hits.values())} "
        f"range={min(hits) if hits else None} -> {max(hits) if hits else None}"
    )

    print("\n=== outputs dirs spanning WC (2026-06-11 .. 2026-07-19) ===")
    out = ROOT / "outputs"
    present = missing = 0
    for p in sorted(out.glob("2026-0[67]*")) if out.is_dir() else []:
        name = p.name
        if name < "2026-06-11" or name > "2026-07-19":
            continue
        soc = p / "soccer"
        step1 = list(soc.glob("step1*.csv")) if soc.is_dir() else []
        step8 = list(soc.glob("step8*.csv")) if soc.is_dir() else []
        has = bool(step1 or step8)
        present += int(has)
        missing += int(not has)
        flag = "HAS" if has else "MISS"
        print(f"  {flag} {name} step1={len(step1)} step8={len(step8)}")
    print(f"summary HAS={present} MISS={missing}")

    # graded_props league tags in WC window
    print("\n=== graded_props soccer league tags Jun-Jul ===")
    gp_days = Counter()
    gp_lg = Counter()
    tpl = ROOT / "ui_runner" / "templates"
    for fp in sorted(tpl.glob("graded_props_2026-0[67]*.json")):
        m = re.match(r"graded_props_(\d{4}-\d{2}-\d{2})\.json$", fp.name)
        day = m.group(1) if m else ""
        if not day or day < "2026-06-11" or day > "2026-07-19":
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        props = data if isinstance(data, list) else (data.get("props") or [])
        n_soc = 0
        for p in props:
            if not isinstance(p, dict):
                continue
            if str(p.get("sport") or "").upper() != "SOCCER":
                continue
            n_soc += 1
            lg = str(p.get("league") or "").strip() or "(empty)"
            gp_lg[lg] += 1
        if n_soc:
            gp_days[day] = n_soc
    print(f"graded soccer days in window: {len(gp_days)} props={sum(gp_days.values())}")
    print("league field values:", gp_lg.most_common(10))
    print("sample days:", sorted(gp_days.items())[:5], "...", sorted(gp_days.items())[-3:])


if __name__ == "__main__":
    main()
