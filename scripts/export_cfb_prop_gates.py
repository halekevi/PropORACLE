#!/usr/bin/env python3
"""Export CFB chip × gate tables to Excel (Week 1 live + today's pool)."""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.cfb_prop_defense import prop_def_axis
from utils.defense_tiers import normalize_def_tier_label

TODAY = Path(r"H:\PropORACLE_main_cp\outputs\2026-09-03\cfb\step5b_cfb.csv")
W1 = ROOT / "outputs/2026-08-29/cfb/step5b_cfb.csv"
ACT = ROOT / "outputs/2026-08-29/actuals_cfb_2026-08-29.csv"
OUT = ROOT / "outputs/2026-09-03/cfb/cfb_prop_gates.xlsx"

WEAK = {"Weak", "Below Avg"}
ELITE = {"Elite", "Above Avg"}

CHIPS = [
    ("pass_yds", "Pass Yards"),
    ("rec_yds", "Receiving Yards"),
    ("rush_yds", "Rush Yards"),
    ("player_td", "Player Touchdowns"),
    ("pass_td", "Pass TDs"),
    ("rec", "Receptions"),
    ("rush_td", "Rush TDs"),
    ("rush_rec_yds", "Rush+Rec Yds"),
    ("fg_made", "FG Made"),
    ("pass_att", "Pass Attempts"),
    ("pass_cmp", "Pass Completions"),
    ("rec_td", "Rec TDs"),
    ("int", "INT"),
    ("pass_rush_yds", "Pass+Rush Yds"),
    ("rush_att", "Rush Attempts"),
    ("kick_pts", "Kicking Points"),
    ("pat_made", "PAT Made"),
    ("pass_long", "Longest Completion"),
    ("rush_long", "Longest Rush"),
    ("rec_long", "Longest Reception"),
]

ACT_PROP = {
    "passing_yards": "pass_yds",
    "receiving_yards": "rec_yds",
    "rushing_yards": "rush_yds",
    "player_touchdowns": "player_td",
    "anytime_td": "player_td",
    "passing_tds": "pass_td",
    "receptions": "rec",
    "rushing_tds": "rush_td",
    "fg_made": "fg_made",
    "completions": "pass_cmp",
    "receiving_tds": "rec_td",
    "interceptions": "int",
    "rushing_attempts": "rush_att",
    "kicking_points": "kick_pts",
    "pat_made": "pat_made",
}

READ = {
    "pass_yds": "Goblin OVER: drop D (74% ungated; D-fail 90%). Still L5=5+L10>=8+cover.",
    "rec_yds": "Goblin OVER keeps L5=5+L10>=8+D. Standard UNDER is off the cover-stack (L5=5 0/5 vs 41% base).",
    "rush_yds": "Standard UNDER keeps the full stack (71%, 2/2). Do not lump with Rec Yards UNDER.",
    "player_td": "Fade from the L5=5 ticket stack. 0.5-line; scored-5-straight, not yards (0/2, 0/1). List L5>=4 unchanged.",
    "pass_td": "Thin. 58% ungated Goblin n=12. Gate barely fires.",
    "rec": "Ungated Goblin 61% already fine. Gated n is tiny.",
    "rush_td": "Thin / Demon-heavy. Do not copy the yards gate.",
    "rush_rec_yds": "Gate does not fire (0 at L5=5 today).",
    "fg_made": "40% Goblin n=5. D zeros it today.",
    "pass_att": "Standard only. No Goblins.",
    "pass_cmp": "Standard OVER 70% (7/10). No Goblins.",
    "rec_td": "Almost all Demons. Week 1 Demon OVER 10%.",
    "int": "0/2 Goblin. Gate n=0.",
    "pass_rush_yds": "Gate fires (7/3). No Week 1 actuals.",
    "rush_att": "Goblin OVER off the list (33% ungated). Standard UNDER 56% (10/18).",
    "kick_pts": "Goblin OVER off the list (20% ungated).",
    "pat_made": "n too small.",
    "pass_long": "Standard-only coin flip. Goblin n=0.",
    "rush_long": "Standard-only. Goblin n=0 at the gate.",
    "rec_long": "Standard-only coin flip. Goblin n=0.",
}

BUCKET = {
    "pass_yds": "Goblin OVER yards — no D; L5=5+L10>=8+cover",
    "rec_yds": "Goblin OVER yards — full stack; Standard UNDER off tickets",
    "rush_yds": "Standard UNDER keeps full stack; Goblin OVER still needs D",
    "player_td": "Fade from L5=5 tickets / Demons",
    "pass_td": "Thin Goblin",
    "rec": "Goblin OVER — ungated already OK",
    "rush_td": "Fade / Demons",
    "rush_rec_yds": "Gate never fires",
    "fg_made": "Fade / thin",
    "pass_att": "Standard only",
    "pass_cmp": "Standard OVER (different book)",
    "rec_td": "Fade / Demons",
    "int": "Fade",
    "pass_rush_yds": "Goblin OVER yards — no W1 grade",
    "rush_att": "Goblin OVER off list",
    "kick_pts": "Goblin OVER off list",
    "pat_made": "Thin",
    "pass_long": "Standard only — skip Goblin gate",
    "rush_long": "Standard only — skip Goblin gate",
    "rec_long": "Standard only — skip Goblin gate",
}

USE = [
    "player",
    "prop_norm",
    "pick_type",
    "line",
    "stat_status",
    "last5_over",
    "last5_under",
    "line_hits_over_10",
    "line_hits_under_10",
    "stat_last5_avg",
    "def_tier",
    "cfb_def_axis",
    "pass_att_share",
    "rush_att_share",
    "rec_tgt_share",
    "stat_season_avg",
]
OFF_AX = {
    "pass": ("team_pass_off_tier_nat", "team_pass_off_tier_conf"),
    "rush": ("team_rush_off_tier_nat", "team_rush_off_tier_conf"),
    "pass_td": ("team_pass_td_off_tier_nat", "team_pass_td_off_tier_conf"),
    "rush_td": ("team_rush_td_off_tier_nat", "team_rush_td_off_tier_conf"),
    "rec_td": ("team_rec_td_off_tier_nat", "team_rec_td_off_tier_conf"),
    "td": ("team_td_off_tier_nat", "team_td_off_tier_conf"),
    "fg": ("team_fg_off_tier_nat", "team_fg_off_tier_conf"),
    "pat": ("team_pat_off_tier_nat", "team_pat_off_tier_conf"),
    "kick": ("team_kick_pts_off_tier_nat", "team_kick_pts_off_tier_conf"),
    "int": ("team_int_off_tier_nat", "team_int_off_tier_conf"),
    "points": ("team_overall_off_tier_nat", "team_overall_off_tier_conf"),
}
CONF_D = {
    "pass": "opp_pass_def_tier_conf",
    "rush": "opp_rush_def_tier_conf",
    "pass_td": "opp_pass_td_def_tier_conf",
    "rush_td": "opp_rush_td_def_tier_conf",
    "rec_td": "opp_rec_td_def_tier_conf",
    "td": "opp_td_def_tier_conf",
    "fg": "opp_fg_def_tier_conf",
    "pat": "opp_pat_def_tier_conf",
    "kick": "opp_kick_pts_def_tier_conf",
    "int": "opp_int_def_tier_conf",
    "points": "opp_overall_def_tier_conf",
}


def norm_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = t.encode("ascii", "ignore").decode("ascii").lower().strip()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def load_slate(path: Path) -> pd.DataFrame:
    have = list(pd.read_csv(path, nrows=0).columns)
    extra = []
    for a, b in OFF_AX.values():
        extra.extend([a, b])
    extra.extend(CONF_D.values())
    cols = [c for c in USE + extra if c in have]
    df = pd.read_csv(path, usecols=cols, low_memory=False).copy()
    df["prop"] = df["prop_norm"].astype(str).str.strip()
    df["pick"] = df["pick_type"].astype(str).str.strip()
    df["l5"] = pd.to_numeric(df.get("last5_over"), errors="coerce")
    df["l10"] = pd.to_numeric(df.get("line_hits_over_10"), errors="coerce")
    df["l5u"] = pd.to_numeric(df.get("last5_under"), errors="coerce")
    df["l10u"] = pd.to_numeric(df.get("line_hits_under_10"), errors="coerce")
    df["line_n"] = pd.to_numeric(df.get("line"), errors="coerce")
    df["ok"] = df.get("stat_status", pd.Series("", index=df.index)).astype(str).str.upper().eq("OK")
    df["d"] = df.get("def_tier", pd.Series("", index=df.index)).map(normalize_def_tier_label)
    axis = df.get("cfb_def_axis")
    if axis is None:
        df["axis"] = df["prop"].map(prop_def_axis)
    else:
        df["axis"] = axis.astype(str).where(axis.astype(str).str.strip().ne(""), df["prop"].map(prop_def_axis))
    off_nat, off_conf, d_conf = [], [], []
    for r in df.itertuples(index=False):
        ax = str(getattr(r, "axis", "") or "")
        nat_c, conf_c = OFF_AX.get(ax, OFF_AX["points"])
        dcc = CONF_D.get(ax, "opp_overall_def_tier_conf")
        off_nat.append(normalize_def_tier_label(getattr(r, nat_c, "")) if nat_c in df.columns else "")
        off_conf.append(normalize_def_tier_label(getattr(r, conf_c, "")) if conf_c in df.columns else "")
        d_conf.append(normalize_def_tier_label(getattr(r, dcc, "")) if dcc in df.columns else "")
    df["off_nat"] = off_nat
    df["off_conf"] = off_conf
    df["d_conf"] = d_conf
    df["pn"] = df["player"].map(norm_name)
    return df


def load_actuals() -> dict[tuple[str, str], float]:
    act = pd.read_csv(ACT)
    act["prop"] = act["prop_type"].astype(str).str.strip().str.lower().map(ACT_PROP)
    act = act[act["prop"].notna()].copy()
    act["pn"] = act["player"].map(norm_name)
    act["actual_num"] = pd.to_numeric(act["actual"], errors="coerce")
    act["_pri"] = (act["prop_type"].astype(str).str.lower() != "anytime_td").astype(int)
    act = act.sort_values("_pri", ascending=False).drop_duplicates(["pn", "prop"], keep="first")
    return {
        (r.pn, r.prop): float(r.actual_num)
        for r in act.itertuples()
        if pd.notna(r.actual_num)
    }


def grade(sub: pd.DataFrame, actuals: dict, *, over: bool) -> tuple[int, int]:
    hits = n = 0
    for r in sub.itertuples():
        actual = actuals.get((r.pn, r.prop))
        if actual is None or pd.isna(r.line_n):
            continue
        if actual == r.line_n:
            continue
        n += 1
        hits += int(actual > r.line_n) if over else int(actual < r.line_n)
    return hits, n


def hr_row(hits: int, n: int) -> dict:
    return {
        "hits": hits,
        "n": n,
        "hit_rate": (hits / n) if n else None,
        "record": "" if n == 0 else f"{hits}/{n}",
    }


def goblin_masks(g: pd.DataFrame) -> dict[str, pd.Series]:
    ok = g["ok"]
    gob = g["pick"].eq("Goblin")
    return {
        "Ungated Goblin": gob,
        "Attached L5": gob & ok,
        "L5=5": gob & ok & (g["l5"] == 5),
        "L10>=8": gob & ok & (g["l10"] >= 8),
        "L5=5 and L10>=8": gob & ok & (g["l5"] == 5) & (g["l10"] >= 8),
        "Base + D Weak|Below": gob & ok & (g["l5"] == 5) & (g["l10"] >= 8) & g["d"].isin(WEAK),
        "L5>=4": gob & ok & (g["l5"] >= 4),
    }


def style(ws, df: pd.DataFrame, pct_cols: list[str]) -> None:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True, name="Calibri", size=10)
    thin = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"),
    )
    alt = PatternFill("solid", fgColor="F2F7FB")
    pct_idx = {i for i, c in enumerate(df.columns, start=1) if c in pct_cols}
    wrap_idx = {i for i, c in enumerate(df.columns, start=1) if c in ("read", "bucket", "notes", "gate")}
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    ws.row_dimensions[1].height = 36
    for r_i, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column), start=2):
        for c_i, cell in enumerate(row, start=1):
            cell.border = thin
            cell.alignment = Alignment(vertical="center", wrap_text=c_i in wrap_idx)
            if r_i % 2 == 0:
                cell.fill = alt
            if c_i in pct_idx and isinstance(cell.value, (int, float)):
                cell.number_format = "0.0%"
    for i, col in enumerate(df.columns, start=1):
        letter = get_column_letter(i)
        sample = [str(col)] + [str(v)[:60] for v in df[col].head(30).tolist()]
        ws.column_dimensions[letter].width = min(48, max(12, max(len(x) for x in sample) + 2))


def write_df(writer: pd.ExcelWriter, name: str, df: pd.DataFrame, pct_cols: list[str]) -> None:
    df.to_excel(writer, sheet_name=name, index=False)
    style(writer.sheets[name], df, pct_cols)


def main() -> None:
    w1 = load_slate(W1)
    today = load_slate(TODAY)
    actuals = load_actuals()

    gob_rows = []
    std_rows = []
    board_rows = []
    for key, label in CHIPS:
        w = w1[w1["prop"] == key]
        t = today[today["prop"] == key]
        gm_w, gm_t = goblin_masks(w), goblin_masks(t)
        rec = {"chip": label, "prop": key, "bucket": BUCKET[key], "read": READ[key]}
        for name in gm_w:
            h, n = grade(w.loc[gm_w[name]], actuals, over=True)
            rec[f"{name} W1 hits"] = h
            rec[f"{name} W1 n"] = n
            rec[f"{name} W1 hit rate"] = (h / n) if n else None
            rec[f"{name} W1 record"] = "" if n == 0 else f"{h}/{n}"
            rec[f"{name} today n"] = int(gm_t[name].sum())
        gob_rows.append(rec)

        tp = t["pick"].value_counts().to_dict()
        board_rows.append(
            {
                "chip": label,
                "prop": key,
                "today Goblin": int(tp.get("Goblin", 0)),
                "today Standard": int(tp.get("Standard", 0)),
                "today Demon": int(tp.get("Demon", 0)),
                "Goblin L5=5": int(gm_t["L5=5"].sum()),
                "Goblin L5=5 and L10>=8": int(gm_t["L5=5 and L10>=8"].sum()),
                "Goblin base + D": int(gm_t["Base + D Weak|Below"].sum()),
                "bucket": BUCKET[key],
            }
        )

        for side, over, l5c, l10c, dset in (
            ("OVER", True, "l5", "l10", WEAK),
            ("UNDER", False, "l5u", "l10u", ELITE),
        ):
            ws = w[w["pick"].eq("Standard")]
            ts = t[t["pick"].eq("Standard")]
            ok_w = ws["ok"]
            ok_t = ts["ok"]
            masks_w = {
                "Ungated": ws["pick"].eq("Standard") if False else pd.Series(True, index=ws.index),
                "L5=5": ok_w & (ws[l5c] == 5),
                "L10>=8": ok_w & (ws[l10c] >= 8),
                "L5=5 and L10>=8": ok_w & (ws[l5c] == 5) & (ws[l10c] >= 8),
                "Base + D": ok_w & (ws[l5c] == 5) & (ws[l10c] >= 8) & ws["d"].isin(dset),
            }
            masks_t = {
                "Ungated": pd.Series(True, index=ts.index),
                "L5=5": ok_t & (ts[l5c] == 5),
                "L10>=8": ok_t & (ts[l10c] >= 8),
                "L5=5 and L10>=8": ok_t & (ts[l5c] == 5) & (ts[l10c] >= 8),
                "Base + D": ok_t & (ts[l5c] == 5) & (ts[l10c] >= 8) & ts["d"].isin(dset),
            }
            row = {"chip": label, "prop": key, "side": side, "D band": "Weak|Below" if over else "Elite|Above"}
            for name in masks_w:
                h, n = grade(ws.loc[masks_w[name]], actuals, over=over)
                row[f"{name} W1 hits"] = h
                row[f"{name} W1 n"] = n
                row[f"{name} W1 hit rate"] = (h / n) if n else None
                row[f"{name} W1 record"] = "" if n == 0 else f"{h}/{n}"
                row[f"{name} today n"] = int(masks_t[name].sum())
            std_rows.append(row)

    gob_df = pd.DataFrame(gob_rows)
    # Compact view for the first sheet
    compact = pd.DataFrame(
        [
            {
                "chip": r["chip"],
                "bucket": r["bucket"],
                "ungated W1": r["Ungated Goblin W1 record"],
                "ungated W1 hit rate": r["Ungated Goblin W1 hit rate"],
                "L5=5 W1": r["L5=5 W1 record"],
                "L5=5 W1 hit rate": r["L5=5 W1 hit rate"],
                "L10>=8 W1": r["L10>=8 W1 record"],
                "L10>=8 W1 hit rate": r["L10>=8 W1 hit rate"],
                "L5=5 and L10>=8 W1": r["L5=5 and L10>=8 W1 record"],
                "L5=5 and L10>=8 W1 hit rate": r["L5=5 and L10>=8 W1 hit rate"],
                "base + D W1": r["Base + D Weak|Below W1 record"],
                "base + D W1 hit rate": r["Base + D Weak|Below W1 hit rate"],
                "today Goblin": r["Ungated Goblin today n"],
                "today L5=5": r["L5=5 today n"],
                "today L5=5 and L10>=8": r["L5=5 and L10>=8 today n"],
                "today base + D": r["Base + D Weak|Below today n"],
                "read": r["read"],
            }
            for r in gob_rows
        ]
    )

    # Rank stacks overall Goblin
    gob_t = today[today["pick"].eq("Goblin")]
    gob_w = w1[w1["pick"].eq("Goblin")]
    gates = [
        ("Goblin attached", lambda d: d["ok"]),
        ("L5=5", lambda d: d["ok"] & (d["l5"] == 5)),
        ("L10>=8", lambda d: d["ok"] & (d["l10"] >= 8)),
        ("L5=5 and L10>=8", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8)),
        ("base + D Weak|Below", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["d"].isin(WEAK)),
        ("base + own OFF Elite|Above", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["off_nat"].isin(ELITE)),
        ("base + own OFF Weak|Below", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["off_nat"].isin(WEAK)),
        ("base + conf D Weak|Below", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["d_conf"].isin(WEAK)),
        ("base + conf OFF Elite|Above", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["off_conf"].isin(ELITE)),
        ("base + D + own OFF Elite", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["d"].isin(WEAK) & d["off_nat"].isin(ELITE)),
        ("full stack D+OFF Elite", lambda d: d["ok"] & (d["l5"] == 5) & (d["l10"] >= 8) & d["d"].isin(WEAK) & d["off_nat"].isin(ELITE)),
    ]
    stack_rows = []
    for name, fn in gates:
        tw, tt = gob_w.loc[fn(gob_w)], gob_t.loc[fn(gob_t)]
        h, n = grade(tw, actuals, over=True)
        cover = pd.to_numeric(tt.get("stat_last5_avg"), errors="coerce") - pd.to_numeric(tt.get("line_n"), errors="coerce")
        stack_rows.append(
            {
                "gate": name,
                "today n": int(len(tt)),
                "mean cover (L5 avg - line)": None if tt.empty or cover.dropna().empty else round(float(cover.mean()), 2),
                "W1 hits": h,
                "W1 n": n,
                "W1 hit rate": (h / n) if n else None,
                "W1 record": "" if n == 0 else f"{h}/{n}",
            }
        )
    stack_df = pd.DataFrame(stack_rows)

    # Week 1 decided at base
    base_w = gob_w[gob_w["ok"] & (gob_w["l5"] == 5) & (gob_w["l10"] >= 8)].copy()
    decided = []
    for r in base_w.itertuples():
        actual = actuals.get((r.pn, r.prop))
        if actual is None or pd.isna(r.line_n):
            result = "no actual"
        elif actual == r.line_n:
            result = "PUSH"
        elif actual > r.line_n:
            result = "HIT"
        else:
            result = "MISS"
        decided.append(
            {
                "result": result,
                "player": r.player,
                "prop": r.prop,
                "line": r.line_n,
                "actual": actual,
                "D": r.d,
                "own OFF": r.off_nat,
            }
        )
    decided_df = pd.DataFrame(decided)

    notes = pd.DataFrame(
        [
            ["Sport", "CFB full game (not CFB1H)"],
            ["Goblin gate", "L5=5 and L10>=8 vs today's line (2025 boxes), Goblin OVER only"],
            ["D", "OVER Weak|Below Avg on the prop axis. Avg never passes. UNDER Elite|Above Avg"],
            ["Week 1 live", "2026-08-29 board vs actuals_cfb_2026-08-29.csv. Pushes dropped. Many later games have no Saturday actual."],
            ["Today pool", str(TODAY)],
            ["Fantasy / combos", "Fantasy dropped in step2. Yards (Combo) never mapped — not in this file."],
            ["Do not use 1st place multipliers", "PrizePicks payout is N-correct only"],
        ],
        columns=["item", "detail"],
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    gob_pct = [c for c in compact.columns if "hit rate" in c]
    std_df = pd.DataFrame(std_rows)
    std_pct = [c for c in std_df.columns if "hit rate" in c]
    board_df = pd.DataFrame(board_rows)

    with pd.ExcelWriter(OUT, engine="openpyxl") as writer:
        write_df(writer, "Goblin OVER by chip", compact, gob_pct)
        write_df(writer, "Standard O-U by chip", std_df, std_pct)
        write_df(writer, "Today board mix", board_df, [])
        write_df(writer, "Rank stack gates", stack_df, ["W1 hit rate"])
        write_df(writer, "W1 decided at base", decided_df, [])
        write_df(writer, "Notes", notes, [])

    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
