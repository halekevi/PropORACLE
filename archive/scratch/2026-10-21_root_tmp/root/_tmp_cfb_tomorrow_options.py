"""Quick live L5=5 CFB ticket options for 2026-09-26."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.cfb_keep_gates import (
    cfb_ticket_fade,
    cfb_pass_yards_goblin_skip_d,
    cfb_list_eligible,
)
from utils.pp_manual_slip import prop_ok, _norm
from utils.prop_norm import canon_prop

STEP8_CANDIDATES = [
    Path(r"H:/PropORACLE/outputs/2026-09-26/cfb/step8_cfb_direction_clean_2026-09-26.xlsx"),
    Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.parquet"),
    Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx"),
]


def load_step8():
    for p in STEP8_CANDIDATES:
        if not p.exists():
            continue
        if p.suffix == ".parquet":
            df = pd.read_parquet(p)
        else:
            df = pd.read_excel(p)
        if "Last 5 Avg" in df.columns or "last5_avg" in {c.lower() for c in df.columns}:
            print(f"using {p} rows={len(df)}", flush=True)
            return df
        print(f"skip thin {p} cols={len(df.columns)}", flush=True)
    raise FileNotFoundError("no full step8 with Last 5 Avg")


CDP = "http://127.0.0.1:9222"
# Already on user's open upcoming slips per screenshot
USED = {
    ("jaylenraynor", "pass"),
    ("katinhouser", "pass"),
    ("camedwards", "rush"),
    ("emmettmosley", "rec"),  # E. V Mosley
    ("terrancecarter", "rec"),
    ("archmanning", "pass"),
    ("juliansayin", "pass"),
}


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    return ("elite" in o) or ("above" in o)


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    for a in ("pass", "rush", "rec"):
        if a in w and "yard" in w and a in g and "yard" in g:
            return True
    return False


def col(df, *names):
    for n in names:
        if n in df.columns:
            return n
    lower = {str(c).lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    raise KeyError(names)


def load_clears():
    df = load_step8()
    c_dir = col(df, "Direction", "direction")
    c_l5o = col(df, "L5 Over", "l5_over", "Last5 Over")
    c_l5u = col(df, "L5 Under", "l5_under")
    c_l10o = col(df, "L10 Over", "l10_over")
    c_l10u = col(df, "L10 Under", "l10_under")
    c_pt = col(df, "Pick Type", "pick_type")
    c_prop = col(df, "Prop", "prop")
    c_player = col(df, "Player", "player")
    c_team = col(df, "Team", "team")
    c_line = col(df, "Line", "line")
    c_od = col(df, "Def Tier", "Opp Def", "def_tier")
    c_avg = col(df, "Last 5 Avg", "last5_avg", "L5 Avg")

    d = df[c_dir].astype(str).str.upper()
    l5 = pd.to_numeric(df[c_l5o], errors="coerce").where(
        d == "OVER", pd.to_numeric(df[c_l5u], errors="coerce")
    )
    l10 = pd.to_numeric(df[c_l10o], errors="coerce").where(
        d == "OVER", pd.to_numeric(df[c_l10u], errors="coerce")
    )
    l5avg = pd.to_numeric(df[c_avg], errors="coerce")
    line = pd.to_numeric(df[c_line], errors="coerce")

    rows = []
    for i, r in df.iterrows():
        side = str(r[c_dir]).upper()
        if float(l5.loc[i] or 0) < 5 or float(l10.loc[i] or 0) < 8:
            continue
        pt = str(r[c_pt])
        low = pt.lower()
        if "demon" in low:
            continue
        if "goblin" in low:
            if side != "OVER":
                continue
            pick = "Goblin"
        elif "standard" in low:
            pick = "Standard"
        else:
            continue
        prop = canon_prop("CFB", str(r[c_prop]))
        if prop not in {"pass_yds", "rush_yds", "rec_yds"}:
            continue
        rec = {
            "sport": "CFB",
            "Direction": side,
            "side": side,
            "pick_type": pt,
            "Pick Type": pt,
            "Prop": r[c_prop],
            "prop": r[c_prop],
        }
        if cfb_ticket_fade(rec) or not cfb_list_eligible(rec):
            continue
        skip = cfb_pass_yards_goblin_skip_d(rec)
        if not skip and not d_ok(side, str(r[c_od] if c_od else "")):
            continue
        avg, ln = l5avg.loc[i], line.loc[i]
        if pd.isna(avg) or pd.isna(ln):
            continue
        cover = (float(avg) - float(ln)) if side == "OVER" else (float(ln) - float(avg))
        need = 15 if prop == "pass_yds" else 8
        if cover < need:
            continue
        label = {
            "pass_yds": "Pass Yards",
            "rush_yds": "Rush Yards",
            "rec_yds": "Rec Yards",
        }[prop]
        rows.append(
            {
                "player": str(r[c_player]),
                "team": str(r[c_team]).upper(),
                "prop": label,
                "line": float(r[c_line]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "pick": pick,
                "side": side,
                "cover": cover,
            }
        )
    best = {}
    for r in rows:
        k = (r["player"], r["prop"], r["pick"], r["side"])
        cur = best.get(k)
        if cur is None:
            best[k] = r
            continue
        if r["side"] == "OVER" and r["line"] < cur["line"]:
            best[k] = r
        elif r["side"] == "UNDER" and r["line"] > cur["line"]:
            best[k] = r
    return list(best.values())


def hidden_search(page, player: str):
    page.evaluate(
        """(q) => {
          const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
          if (!inp) return false;
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
          setter.call(inp, '');
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          setter.call(inp, q);
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          return true;
        }""",
        player,
    )
    page.wait_for_timeout(1100)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def used_key(player: str, prop: str):
    pn = _norm(player)
    for a in ("pass", "rush", "rec"):
        if a in _norm(prop):
            return (pn[:12] if False else pn, a)  # noqa — simplify below
    return (pn, _norm(prop)[:8])


def is_used(player: str, prop: str) -> bool:
    pn = _norm(player)
    for a in ("pass", "rush", "rec"):
        if a in _norm(prop):
            for up, ua in USED:
                if ua == a and (up in pn or pn in up or pn.startswith(up[:8])):
                    return True
    return False


def main():
    clears = load_clears()
    print(
        f"clears G={sum(1 for c in clears if c['pick']=='Goblin')} "
        f"S={sum(1 for c in clears if c['pick']=='Standard')}",
        flush=True,
    )
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        page = next(
            pg
            for pg in b.contexts[0].pages
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
        )
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=2000)
        live = []
        for rec in sorted(clears, key=lambda z: (-z["l10"], z["player"])):
            hits = hidden_search(page, rec["player"])
            if not hits:
                continue
            # try cycle lightly for goblin exact
            exact = False
            for c in hits:
                if not yard_ok(rec["prop"], str(c.get("prop_type") or "")):
                    continue
                if str(c.get("pick_type") or "").lower() != rec["pick"].lower():
                    continue
                if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                    exact = True
                    break
            if not exact and rec["pick"] == "Goblin":
                start = next(
                    (
                        c
                        for c in hits
                        if yard_ok(rec["prop"], str(c.get("prop_type") or ""))
                    ),
                    None,
                )
                if start is None:
                    continue
                frame = P.find_prizepicks_frame(page)
                P.cycle_card_to_pick_type(
                    frame,
                    start.get("more_btn"),
                    player=rec["player"],
                    prop=start.get("prop_type") or rec["prop"],
                    want_pick="goblin",
                    want_line=rec["line"],
                    require_line=True,
                    more_index=start.get("more_index"),
                )
                page.wait_for_timeout(300)
                hits = hidden_search(page, rec["player"])
                for c in hits:
                    if not yard_ok(rec["prop"], str(c.get("prop_type") or "")):
                        continue
                    if str(c.get("pick_type") or "").lower() != "goblin":
                        continue
                    if P._line_key(c.get("line")) == P._line_key(rec["line"]):
                        exact = True
                        break
            if not exact:
                continue
            flag = "USED" if is_used(rec["player"], rec["prop"]) else "FREE"
            live.append({**rec, "flag": flag})
            print(
                f"LIVE {flag} {rec['pick'][0]} {rec['side'][0]} "
                f"{rec['player']:22} {rec['prop']:12} {rec['line']} "
                f"L10={rec['l10']:.0f} ({rec['team']})",
                flush=True,
            )

    free = [x for x in live if x["flag"] == "FREE"]
    used = [x for x in live if x["flag"] == "USED"]
    print(f"\nlive={len(live)} used_on_open={len(used)} still_free={len(free)}", flush=True)
    print("--- FREE for new tickets ---", flush=True)
    for x in free:
        print(
            f"  {x['pick'][0]} {x['side'][0]} {x['player']} ({x['team']}) "
            f"{x['prop']} {x['line']}",
            flush=True,
        )
    # Suggest packs
    if len(free) >= 6:
        print("\nSUGGEST F6:", ", ".join(f"{x['player']} {x['line']}" for x in free[:6]))
    if len(free) >= 4:
        print("SUGGEST F4:", ", ".join(f"{x['player']} {x['line']}" for x in free[:4]))
    if len(free) >= 5:
        print("SUGGEST F5:", ", ".join(f"{x['player']} {x['line']}" for x in free[:5]))
    if len(free) < 4:
        print("\nNot enough FREE L5=5 live faces for a new Flex 4 (most already on your open slips).")


if __name__ == "__main__":
    main()
