"""Parent L10 cells, tier lift, and whether Oct 7–8 MLB boards are postseason."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from all_ranked_prop_hit_rates import graded_paths, l10_dir, pick_book  # noqa: E402
from utils.mlb_keep_gates import STINGY, _opp_bats_strong, _own_pitch  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

PROPS = ("pitcher_ks", "hits_allowed", "pitching_outs")
# Published as-of qualified cells (L10>=8 + tier), from the forward check.
QUAL = {
    "pitcher_ks": (83, 114, "L10>=8 + own pitch Elite|Above"),
    "hits_allowed": (49, 68, "L10>=8 + opp bats Strong|Above"),
    "pitching_outs": (31, 36, "L10>=8 + own pitch Elite|Above"),
}
RS_END = "2026-09-27"


def _p(*a):
    print(*a, flush=True)


def _band(h: int, n: int) -> str:
    if not n:
        return "n=0"
    p = h / n
    se = (p * (1 - p) / n) ** 0.5
    return f"{h}/{n} ({p:.1%}, band {p - 1.96 * se:.1%}–{p + 1.96 * se:.1%})"


def _lift(qh: int, qn: int, ph: int, pn: int) -> str:
    """Qualified slice vs the rest of the parent, if the slice fits inside."""
    if qn > pn or qh > ph:
        qp, pp = qh / qn, ph / pn
        se = ((qp * (1 - qp) / qn) + (pp * (1 - pp) / pn)) ** 0.5
        d = qp - pp
        return (
            f"lift {d:+.1%} vs parent (cells are not a nested pair); "
            f"band {d - 1.96 * se:+.1%}–{d + 1.96 * se:+.1%}"
        )
    rh, rn = ph - qh, pn - qn
    qp, rp = qh / qn, (rh / rn if rn else 0.0)
    se = ((qp * (1 - qp) / qn) + (rp * (1 - rp) / rn if rn else 0.0)) ** 0.5
    d = qp - rp
    return (
        f"remainder {rh}/{rn} ({rp:.1%}); lift {d:+.1%} "
        f"(band {d - 1.96 * se:+.1%}–{d + 1.96 * se:+.1%}, SE {se:.1%})"
    )


def recount() -> None:
    cells = {p: defaultdict(lambda: [0, 0]) for p in PROPS}
    # bucket: all / pre / post, and qualified using the current defense file
    for path in graded_paths():
        day = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        bucket = "post" if day > RS_END else "pre"
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            if str(r.get("sport") or "").upper() != "MLB":
                continue
            res = str(r.get("result") or "").upper()
            if res not in ("HIT", "MISS"):
                continue
            if pick_book(str(r.get("pick_type") or ""), str(r.get("direction") or "")) != "Goblin OVER":
                continue
            prop = canon_prop("MLB", str(r.get("prop") or ""))
            if prop not in PROPS:
                continue
            l10 = l10_dir(r, "OVER")
            if l10 is None or l10 < 8:
                continue
            hit = 1 if res == "HIT" else 0
            for b in ("all", bucket):
                cells[prop][b][0] += hit
                cells[prop][b][1] += 1
            qual = False
            if prop in ("pitcher_ks", "pitching_outs"):
                qual = _own_pitch(r) in STINGY
            elif prop == "hits_allowed":
                qual = _opp_bats_strong(r)
            if qual:
                for b in ("q_all", "q_" + bucket):
                    cells[prop][b][0] += hit
                    cells[prop][b][1] += 1
    _p("=== graded Goblin OVER L10>=8 (current defense file for the tier) ===")
    for prop in PROPS:
        h, n = cells[prop]["all"]
        hp, np_ = cells[prop]["pre"]
        ho, no = cells[prop]["post"]
        qh, qn = cells[prop]["q_all"]
        _p(f"{prop} parent {_band(h, n)}  pre {hp}/{np_}  post {ho}/{no} ({(no / n if n else 0):.1%} of the cell)")
        _p(f"  tier on these rows {_band(qh, qn)}")
        if qn and n and qn <= n:
            _p(f"  {_lift(qh, qn, h, n)}")


def schedule() -> None:
    _p("\n=== MLB schedule 2026-09-27 through 2026-10-08 ===")
    url = (
        "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
        "&startDate=2026-09-27&endDate=2026-10-08&gameType=R,F,D,L,W,A"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    for day in data.get("dates") or []:
        games = day.get("games") or []
        kinds = {}
        for g in games:
            gt = str(g.get("gameType") or "?")
            kinds[gt] = kinds.get(gt, 0) + 1
        _p(f"  {day.get('date')} games={len(games)} types={kinds}")


def main() -> None:
    catalog = json.loads(
        (ROOT / "data/reports/mlb_l10_dual_gates.json").read_text(encoding="utf-8")
    )["catalog"]
    _p("catalog parents as of 2026-09-03, tier cells from the as-of forward check")
    parents = {}
    for prop in PROPS:
        block = catalog[prop]["l10ge8"]
        n = int(block["n"])
        hits = int(round(float(block["hr"]) / 100.0 * n))
        parents[prop] = (hits, n)
    for prop in PROPS:
        ph, pn = parents[prop]
        qh, qn, label = QUAL[prop]
        _p(f"{prop} parent {_band(ph, pn)}")
        _p(f"  tier {label} {_band(qh, qn)}")
        _p(f"  {_lift(qh, qn, ph, pn)}")
    recount()
    schedule()


if __name__ == "__main__":
    main()
