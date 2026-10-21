"""Lead 2 (ticket_eval upstream) + Lead 3 (tickets_latest backups) vs grade_history."""
from __future__ import annotations

import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:/PropORACLE")
DATA = ROOT / "ui_runner" / "data"
TEMPLATES = ROOT / "ui_runner" / "templates"


def load_grade_history() -> dict[str, dict]:
    from utils.proporacle_data_root import load_best_grade_history_runs

    runs = load_best_grade_history_runs(ROOT, templates_dir=TEMPLATES)
    by: dict[str, dict] = {}
    for r in runs or []:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date") or "")[:10]
        if d.startswith("2026-05") or d.startswith("2026-06"):
            # keep MAIN / default track if present
            track = str(r.get("track") or r.get("product") or "main").lower()
            if track in ("main", "", "default", "combined", "graded-main", "mixer"):
                by[d] = r
            elif d not in by:
                by[d] = r
    return by


def gh_nw(rec: dict) -> tuple[int | None, int | None]:
    if not rec:
        return None, None
    n = rec.get("decided") or rec.get("n_decided") or rec.get("ticket_decided")
    w = rec.get("wins") or rec.get("money_wins") or rec.get("ticket_wins")
    if n is None:
        n = rec.get("n_tickets") or rec.get("tickets") or rec.get("n")
    if w is None and n is not None:
        # sometimes winrate + n
        wr = rec.get("winrate") or rec.get("ticket_pct") or rec.get("wr")
        if wr is not None:
            try:
                wrf = float(wr)
                if wrf > 1:
                    wrf /= 100.0
                w = int(round(wrf * int(n)))
            except Exception:
                pass
    try:
        n = int(n) if n is not None else None
    except Exception:
        n = None
    try:
        w = int(w) if w is not None else None
    except Exception:
        w = None
    return n, w


def count_payload_tickets(payload: dict) -> tuple[int, int, int]:
    """Return (n_slips, n_with_outcome_win, n_decided_outcome)."""
    n = wins = decided = 0
    for g in payload.get("groups") or []:
        for t in g.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            n += 1
            o = str(
                t.get("outcome")
                or t.get("result")
                or t.get("ticket_result")
                or t.get("grade")
                or ""
            ).upper()
            if o in ("WIN", "HIT", "WON", "CASH"):
                wins += 1
                decided += 1
            elif o in ("LOSS", "MISS", "LOST", "NO_CASH"):
                decided += 1
    return n, wins, decided


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    gh = load_grade_history()
    print(f"grade_history May-June days: {len(gh)}")
    if gh:
        d0 = sorted(gh)[0]
        print(f"sample {d0} keys: {sorted(gh[d0].keys())[:50]}")
        print(f"sample {d0}: n/w={gh_nw(gh[d0])} subset={ {k: gh[d0].get(k) for k in list(gh[d0])[:12]} }")

    # --- Lead 2: combined_slate (ticket_eval upstream) ---
    print("\n=== Lead 2: combined_slate_tickets_*.json vs grade_history ===")
    files = sorted(DATA.glob("combined_slate_tickets_2026-05*.json")) + sorted(
        DATA.glob("combined_slate_tickets_2026-06*.json")
    )
    print(f"files on disk: {len(files)}")
    diffs = []
    for f in files:
        m = re.search(r"(2026-0[56]-\d{2})", f.name)
        d = m.group(1) if m else "?"
        try:
            n, w, dec = count_payload_tickets(load_json(f))
        except Exception as e:
            print(f"  FAIL {f.name}: {e}")
            continue
        ghn, ghw = gh_nw(gh.get(d) or {})
        dn = (n - ghn) if ghn is not None else None
        print(
            f"{d}  cs_n={n:5d} cs_dec={dec:4d}  gh_n={str(ghn):>5} gh_w={str(ghw):>4}  dn={dn}  {f.stat().st_size/1e6:.1f}MB"
        )
        if ghn is not None:
            diffs.append(abs(n - ghn))
    if diffs:
        print(f"Lead2 overlap={len(diffs)} mean_|cs_n-gh_n|={sum(diffs)/len(diffs):.1f}")
        hit = sum(1 for x in diffs if x <= max(5, 0.15 * (sum(diffs) / len(diffs) + 1))) 
        # clearer: fraction within 20% of gh
        close = 0
        for f in files:
            m = re.search(r"(2026-0[56]-\d{2})", f.name)
            d = m.group(1) if m else None
            if not d or d not in gh:
                continue
            n, _, _ = count_payload_tickets(load_json(f))
            ghn, _ = gh_nw(gh[d])
            if ghn and abs(n - ghn) / max(ghn, 1) <= 0.20:
                close += 1
        print(f"Lead2 days within 20% of gh_n: {close}/{len(diffs)}")
        if diffs and (sum(diffs) / len(diffs)) < 30:
            print("HIT: combined_slate daily n close to grade_history — build on this.")
        else:
            print("MISS as published-card proxy (expected if grade-pool >> published); still is ticket_eval upstream.")

    # --- Lead 3a: uniform_tickets dated ---
    print("\n=== Lead 3a: uniform_tickets_2026-05/06*.json ===")
    ufiles = sorted(TEMPLATES.glob("uniform_tickets_2026-05*.json")) + sorted(
        TEMPLATES.glob("uniform_tickets_2026-06*.json")
    )
    print(f"uniform files: {len(ufiles)}")
    udiffs = []
    for f in ufiles[:40]:
        m = re.search(r"(2026-0[56]-\d{2})", f.name)
        d = m.group(1) if m else "?"
        try:
            payload = load_json(f)
            n, w, dec = count_payload_tickets(payload)
        except Exception as e:
            print(f"  FAIL {f.name}: {e}")
            continue
        ghn, ghw = gh_nw(gh.get(d) or {})
        dn = (n - ghn) if ghn is not None else None
        print(
            f"{d}  uni_n={n:4d}  gh_n={str(ghn):>5} gh_w={str(ghw):>4}  dn={dn}  {f.stat().st_size/1e3:.0f}KB"
        )
        if ghn is not None:
            udiffs.append(abs(n - ghn))
    if udiffs:
        print(f"uniform overlap={len(udiffs)} mean_|n|={sum(udiffs)/len(udiffs):.1f}")

    # --- Lead 3b: git history of tickets_latest for May/June dates ---
    print("\n=== Lead 3b: git tickets_latest.json commits May-June (sample by slate date in payload) ===")
    # list commits touching the file in May-June window
    log = subprocess.check_output(
        [
            "git",
            "-C",
            str(ROOT),
            "log",
            "--format=%H %ad",
            "--date=short",
            "--after=2026-04-30",
            "--before=2026-07-02",
            "--",
            "ui_runner/templates/tickets_latest.json",
        ],
        text=True,
    ).strip().splitlines()
    print(f"commits May-June touching tickets_latest: {len(log)}")

    # Sample ~1 commit per calendar day (first of that commit-date)
    by_commit_day: dict[str, str] = {}
    for line in log:
        parts = line.split()
        if len(parts) < 2:
            continue
        sha, cday = parts[0], parts[1]
        by_commit_day.setdefault(cday, sha)  # keep latest first? log is newest-first; setdefault keeps newest

    # Actually we want representative: for each commit-day, check payload date + slip count
    checked = 0
    git_rows = []
    for cday in sorted(by_commit_day):
        sha = by_commit_day[cday]
        try:
            blob = subprocess.check_output(
                ["git", "-C", str(ROOT), "show", f"{sha}:ui_runner/templates/tickets_latest.json"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            payload = json.loads(blob)
        except Exception as e:
            print(f"  skip {cday} {sha[:8]}: {e}")
            continue
        slate = str(payload.get("date") or payload.get("slate_date") or "")[:10]
        n, w, dec = count_payload_tickets(payload)
        ghn, ghw = gh_nw(gh.get(slate) or {})
        dn = (n - ghn) if ghn is not None else None
        git_rows.append((cday, slate, n, ghn, ghw, dn, sha[:8]))
        checked += 1
        if checked <= 35:
            print(
                f"commit={cday} slate={slate or '?':10} n={n:4d} gh_n={str(ghn):>5} dn={dn} sha={sha[:8]}"
            )

    # Aggregate: for each unique slate date, pick the commit with n closest to gh_n (or last of day)
    best: dict[str, tuple] = {}
    for cday, slate, n, ghn, ghw, dn, sha in git_rows:
        if not slate:
            continue
        prev = best.get(slate)
        if prev is None:
            best[slate] = (n, ghn, dn, sha, cday)
        else:
            # prefer closer to gh
            if ghn is not None:
                if prev[1] is None or abs(n - ghn) < abs(prev[0] - ghn):
                    best[slate] = (n, ghn, (n - ghn), sha, cday)

    print(f"\nunique slate dates recovered from git tickets_latest: {len(best)}")
    gdiffs = []
    for slate in sorted(best):
        n, ghn, dn, sha, cday = best[slate]
        if ghn is not None:
            gdiffs.append(abs(n - ghn))
        print(f"  slate={slate} git_n={n:4d} gh_n={str(ghn):>5} |dn|={abs(dn) if dn is not None else '-'} via {sha}")
    if gdiffs:
        mean = sum(gdiffs) / len(gdiffs)
        close = sum(1 for x, (n, ghn, *_r) in zip(gdiffs, [best[s] for s in sorted(best) if best[s][1] is not None]) if False)
        close = 0
        for slate, (n, ghn, dn, sha, cday) in best.items():
            if ghn and abs(n - ghn) / max(ghn, 1) <= 0.25:
                close += 1
        print(f"Lead3 git overlap={len(gdiffs)} mean_|n|={mean:.1f} within25%={close}/{len(gdiffs)}")
        if mean < 40 or close >= max(3, len(gdiffs) // 2):
            print("HIT: git tickets_latest daily n near grade_history — use as published reconstruction.")
        else:
            print("MISS: git tickets_latest still far from grade_history daily n/wins.")
    else:
        print("No overlapping slate dates between git tickets_latest and grade_history.")

    # Verdict
    print("\n=== VERDICT ===")
    print("Lead1 ticket_id-only: MISS (already run, mean_|n|/gh ~75%)")
    print("Lead2 upstream = combined_slate_tickets_{date}.json (build_ticket_eval.find_ticket_json)")
    print("Lead3: git history of tickets_latest exists through May-June; ticket_runs starts 2026-07-12 only")


if __name__ == "__main__":
    main()
