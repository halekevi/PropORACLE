"""Refined WC vs domestic split + pp_projection_id league join attempt."""
from __future__ import annotations

import html as html_lib
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import date as _date
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TE_ARTICLE = re.compile(
    r'<article class="ticket-card([^"]*)">(.*?)</article>',
    re.S | re.I,
)

EXTRA_NATIONALS = {
    "curacao", "cabo verde", "cape verde", "cote d ivoire", "ivory coast",
    "korea rep", "korea republic", "south korea", "north korea",
    "usa", "usmnt", "uswnt", "mexico", "canada", "brazil", "argentina",
    "france", "germany", "spain", "england", "portugal", "netherlands",
    "italy", "belgium", "croatia", "uruguay", "colombia", "chile", "ecuador",
    "peru", "paraguay", "japan", "australia", "morocco", "senegal", "nigeria",
    "ghana", "cameroon", "egypt", "algeria", "tunisia", "saudi arabia", "iran",
    "qatar", "poland", "switzerland", "austria", "serbia", "denmark", "sweden",
    "norway", "scotland", "wales", "ireland", "northern ireland", "czechia",
    "czech republic", "slovakia", "hungary", "romania", "ukraine", "turkey",
    "turkiye", "greece", "china", "new zealand", "costa rica", "panama",
    "jamaica", "honduras", "haiti", "venezuela", "bolivia", "south africa",
    "uzbekistan", "bosnia", "bosnia and herzegovina", "slovenia", "albania",
    "iceland", "finland", "georgia", "israel", "wales", "canada",
}


def _fold(s: object) -> str:
    s = html_lib.unescape(str(s or ""))
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def money_hit(cls: str, body: str) -> bool | None:
    if re.search(r"grade-ticket-result\s+won", body, re.I):
        return True
    if re.search(r"grade-ticket-result\s+lost", body, re.I):
        return False
    low = (cls or "").lower()
    if "all-hit" in low:
        return True
    if "card-missed" in low:
        return False
    return None


def parse_soccer_legs(body: str) -> list[dict]:
    legs = []
    for lm in re.finditer(
        r'<div class="legrow([^"]*)">(.*?)(?=<div class="legrow|<(?:div class="ticket-grade|/article))',
        body,
        re.S | re.I,
    ):
        row, b = lm.group(1), lm.group(2)
        if "leg-pend" in row.lower() or "leg-void" in row.lower():
            continue
        sp = re.search(r'<span class="pill[^"]*">([^<]+)</span>', b, re.I)
        if (sp.group(1) if sp else "").strip().upper() != "SOCCER":
            continue
        prop_m = re.search(
            r'<div class="leg-prop-col[^"]*">\s*<div>(.*?)</div>\s*<div class="meta-muted">(.*?)</div>',
            b,
            re.S | re.I,
        )
        matchup = _fold(re.sub(r"<[^>]+>", " ", prop_m.group(2) if prop_m else ""))
        # keep display forms too
        matchup_raw = html_lib.unescape(
            re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", prop_m.group(2) if prop_m else "")).strip()
        )
        parts = re.split(r"\s+vs\.?\s+|\s+@\s+", matchup_raw, maxsplit=1, flags=re.I)
        team = parts[0].strip() if parts else ""
        opp = parts[1].strip() if len(parts) > 1 else ""
        legs.append({"team": team, "opp": opp})
    return legs


CLUB_HINTS = (
    "united", "city", "fc", "cf", "sc", "juventus", "bayern", "dortmund",
    "arsenal", "chelsea", "liverpool", "madrid", "barcelona", "milan",
    "inter", "napoli", "roma", "ajax", "porto", "benfica", "galaxy",
    "sounders", "whitecaps", "lafc", "dallas", "cincinnati", "charlotte",
    "orlando", "vancouver", "seattle", "houston", "chicago", "new england",
    "earthquakes", "aston villa", "crystal palace", "freiburg", "genoa",
    "alaves", "getafe", "fiorentina", "lille", "everton", "stuttgart",
    "atalanta", "hoffenheim", "leverkusen", "union berlin", "monaco",
    "strasbourg", "parma", "nantes", "lecce", "lorient", "villarreal",
    "real sociedad", "real betis",
)


def is_national(name: str) -> bool:
    f = _fold(name)
    if not f or f in {"unknown opp", "unknown"}:
        return False
    if f in EXTRA_NATIONALS:
        return True
    # single-token country-ish already in set
    return False


def is_club(name: str) -> bool:
    f = _fold(name)
    if not f or is_national(name):
        return False
    return any(h in f for h in CLUB_HINTS)


def ticket_bucket(legs: list[dict]) -> str:
    labs = []
    for leg in legs:
        for side in (leg["team"], leg["opp"]):
            if is_national(side):
                labs.append("national")
            elif is_club(side):
                labs.append("club")
            elif _fold(side) in {"unknown opp", "unknown", ""}:
                labs.append("missing")
            else:
                labs.append("unknown")
    if "national" in labs:
        return "intl_national"
    if "club" in labs and "national" not in labs:
        return "domestic_club"
    return "unclassified"


def load_ppid_league() -> dict[str, str]:
    """pp_projection_id -> league from any step1/step8 with both columns."""
    out: dict[str, str] = {}
    files = list(ROOT.glob("**/step1*soccer*.csv")) + list(ROOT.glob("**/step8*soccer*.csv"))
    for p in files:
        try:
            cols = {c.lower(): c for c in pd.read_csv(p, nrows=0).columns}
        except Exception:
            continue
        if "league" not in cols:
            continue
        pid_c = cols.get("pp_projection_id") or cols.get("projection_id")
        if not pid_c:
            continue
        try:
            df = pd.read_csv(p, low_memory=False, usecols=[pid_c, cols["league"]])
        except Exception:
            continue
        for r in df.itertuples(index=False):
            pid = str(getattr(r, pid_c) or "").strip()
            lg = str(getattr(r, cols["league"]) or "").strip().upper()
            if pid and lg and lg not in {"NAN", "NONE"}:
                out[pid] = lg
    print(f"[ppid] league map size={len(out)}")
    return out


def main() -> None:
    ppid_league = load_ppid_league()

    # Does graded_props carry pp_projection_id for soccer June?
    gp_june = ROOT / "ui_runner/templates/graded_props_2026-06-15.json"
    if gp_june.exists():
        import json

        data = json.loads(gp_june.read_text(encoding="utf-8"))
        props = data if isinstance(data, list) else data.get("props") or []
        soccer = [p for p in props if str(p.get("sport") or "").upper() == "SOCCER"]
        with_pid = sum(1 for p in soccer if p.get("pp_projection_id"))
        joined = 0
        lg_ct = Counter()
        for p in soccer:
            pid = str(p.get("pp_projection_id") or "").strip()
            if pid and pid in ppid_league:
                joined += 1
                lg_ct[ppid_league[pid]] += 1
        print(
            f"[graded 06-15] soccer props={len(soccer)} with_ppid={with_pid} "
            f"joined_league={joined} leagues={lg_ct.most_common(10)}"
        )

    by = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0}))
    unknown_sides = Counter()
    weekly = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0}))

    for fp in sorted((ROOT / "ui_runner/templates").glob("ticket_eval_2026-0[56]-*.html")):
        if not re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", fp.name):
            continue
        d = fp.name[len("ticket_eval_") : -5]
        month = d[:7]
        text = fp.read_text(encoding="utf-8", errors="replace")
        cut = text.find('class="ticket-bucket sb-default manual-tb')
        if cut > 0:
            text = text[:cut]
        for am in TE_ARTICLE.finditer(text):
            mh = money_hit(am.group(1), am.group(2))
            if mh is None:
                continue
            legs = parse_soccer_legs(am.group(2))
            if len(legs) < 2:
                continue
            bucket = ticket_bucket(legs)
            by[month][bucket]["n"] += 1
            if mh:
                by[month][bucket]["w"] += 1
            iso = _date.fromisoformat(d).isocalendar()
            wk = f"{iso.year}-W{iso.week:02d}"
            weekly[wk][bucket]["n"] += 1
            if mh:
                weekly[wk][bucket]["w"] += 1
            for leg in legs:
                for side in (leg["team"], leg["opp"]):
                    if not is_national(side) and not is_club(side) and _fold(side) not in {
                        "",
                        "unknown opp",
                        "unknown",
                    }:
                        unknown_sides[side] += 1

    print("\n=== Refined national/club split ===")
    print(f"{'month':8} {'bucket':16} {'n':>5} {'WR%':>6}")
    for month in sorted(by):
        tot_n = sum(v["n"] for v in by[month].values())
        tot_w = sum(v["w"] for v in by[month].values())
        for bucket in ("domestic_club", "intl_national", "unclassified"):
            s = by[month][bucket]
            if not s["n"]:
                continue
            print(f"{month:8} {bucket:16} {s['n']:5d} {100*s['w']/s['n']:6.1f}")
        print(f"{month:8} {'ALL':16} {tot_n:5d} {100*tot_w/tot_n:6.1f}")

    print("\n=== Weekly x bucket ===")
    for wk in sorted(weekly):
        parts = []
        for bucket, s in sorted(weekly[wk].items()):
            parts.append(f"{bucket}={s['n']}@{100*s['w']/s['n']:.0f}%")
        print(f"{wk}: " + ", ".join(parts))

    print("\nRemaining unclassified sides:", unknown_sides.most_common(20))


if __name__ == "__main__":
    from collections import Counter

    main()
