"""Rewrite NFL O/D canvas with all 32 teams in every ranking table."""
from __future__ import annotations

import json
from pathlib import Path

rows = json.loads(Path(r"H:/PropORACLE/_tmp_od_canvas_rows.json").read_text(encoding="utf-8"))
OUT = Path(
    r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/nfl-od-rankings-by-prop.canvas.tsx"
)

# matchup leans from prior build
cross = __import__("pandas").read_excel(
    r"H:/PropORACLE/outputs/2026-09-20/nfl/nfl_od_rankings_by_prop_week2.xlsx",
    sheet_name="matchup_cross",
)
pass_over = (
    cross[cross["rec_pass_lean"] == "OVER"]
    .sort_values("pass_edge", ascending=False)
    .head(12)
)
rush_over = (
    cross[cross["rush_lean"] == "OVER"]
    .sort_values("rush_edge", ascending=False)
    .head(12)
)
pass_under = (
    cross[cross["rec_pass_lean"] == "UNDER"]
    .sort_values("pass_edge", ascending=True)
    .head(12)
)
rush_under = (
    cross[cross["rush_lean"] == "UNDER"]
    .sort_values("rush_edge", ascending=True)
    .head(12)
)


def ts_rows(items, keys):
    lines = []
    for r in items:
        cells = ", ".join(f"'{r.get(k, '')}'" for k in keys)
        lines.append(f"          [{cells}],")
    return "\n".join(lines)


def cross_rows(frame, edge_col):
    lines = []
    for _, r in frame.iterrows():
        edge = r[edge_col]
        es = f"{edge:+.0f}" if __import__("pandas").notna(edge) else ""
        lines.append(
            f"          ['{r.team}', '{r.opp}', '{int(r.pass_off) if __import__('pandas').notna(r.pass_off) and edge_col.startswith('pass') else (int(r.rush_off) if __import__('pandas').notna(r.rush_off) else '')}', "
            f"'{int(r.opp_pass_def) if edge_col.startswith('pass') and __import__('pandas').notna(r.opp_pass_def) else (int(r.opp_rush_def) if __import__('pandas').notna(r.opp_rush_def) else '')}', '{es}'],"
        )
    return "\n".join(lines)


# cleaner cross row builder
def cross_rows2(frame, off_col, def_col, edge_col):
    lines = []
    for _, r in frame.iterrows():
        o = "" if __import__("pandas").isna(r[off_col]) else str(int(r[off_col]))
        d = "" if __import__("pandas").isna(r[def_col]) else str(int(r[def_col]))
        e = "" if __import__("pandas").isna(r[edge_col]) else f"{float(r[edge_col]):+.0f}"
        lines.append(f"          ['{r.team}', '{r.opp}', '{o}', '{d}', '{e}'],")
    return "\n".join(lines)


tsx = f"""import {{
  Callout,
  Divider,
  Grid,
  H1,
  H2,
  H3,
  Stack,
  Stat,
  Table,
  Text,
}} from "cursor/canvas";

/** NFL O/D rankings — all 32 teams. From step8 + unit ranks. */
export default function NflOdRankingsByProp() {{
  return (
    <Stack gap={{24}}>
      <Stack gap={{8}}>
        <H1>NFL O/D rankings by prop</H1>
        <Text tone="secondary">
          All 32 teams. Rank 1 = best unit. Tier: 1–8 Elite, 9–14 Above, 15–20
          Avg, 21–26 Below, 27–32 Weak. Edge = opp def rank − own off rank
          (≥8 OVER lean, ≤−8 UNDER lean). Source: step8 Week-2 board +
          ol/dl/secondary/box CSVs.
        </Text>
      </Stack>

      <Grid columns={{4}} gap={{12}}>
        <Stat value="32" label="Teams (full boards)" />
        <Stat value="DAL@WSH" label="Top pass OVER lean" tone="success" />
        <Stat value="LAR@NYG" label="Top rush OVER lean" tone="success" />
        <Stat value="Excel" label="Full workbook on disk" />
      </Grid>

      <Callout tone="info" title="Workbook">
        outputs/2026-09-20/nfl/nfl_od_rankings_by_prop_week2.xlsx — sheets:
        offense_by_team, defense_by_team, keep_*, matchup_cross.
      </Callout>

      <H2>Offense — all 32</H2>
      <Grid columns={{2}} gap={{16}}>
        <Stack gap={{8}}>
          <H3>Pass offense (rec / pass yards)</H3>
          <Table
            headers={{["Rk", "Team", "Tier", "YPG", "Top Rec%"]}}
            columnAlign={{["right", "left", "left", "right", "right"]}}
            rows={{[
{ts_rows(rows["pass_off"], ["rk", "team", "tier", "ypg", "rec"])}
            ]}}
          />
        </Stack>
        <Stack gap={{8}}>
          <H3>Rush offense</H3>
          <Table
            headers={{["Rk", "Team", "Tier", "YPG", "Top Rush%"]}}
            columnAlign={{["right", "left", "left", "right", "right"]}}
            rows={{[
{ts_rows(rows["rush_off"], ["rk", "team", "tier", "ypg", "share"])}
            ]}}
          />
        </Stack>
      </Grid>

      <Divider />

      <H2>Defense — all 32</H2>
      <Text tone="secondary">
        OVER chips → Weak/Below (bottom of board). UNDER chips → Elite/Above
        (top of board).
      </Text>
      <Grid columns={{2}} gap={{16}}>
        <Stack gap={{8}}>
          <H3>Pass defense (1 = stingiest)</H3>
          <Table
            headers={{["Rk", "Team", "Tier", "Yds/G", "Sec Rk"]}}
            columnAlign={{["right", "left", "left", "right", "right"]}}
            rows={{[
{ts_rows(rows["pass_def"], ["rk", "team", "tier", "ypg", "sec"])}
            ]}}
          />
        </Stack>
        <Stack gap={{8}}>
          <H3>Rush defense (1 = stingiest)</H3>
          <Table
            headers={{["Rk", "Team", "Tier", "Yds/G", "Box Rk"]}}
            columnAlign={{["right", "left", "left", "right", "right"]}}
            rows={{[
{ts_rows(rows["rush_def"], ["rk", "team", "tier", "ypg", "box"])}
            ]}}
          />
        </Stack>
      </Grid>

      <Grid columns={{2}} gap={{16}}>
        <Stack gap={{8}}>
          <H3>Pass rush / sacks (1 = best)</H3>
          <Table
            headers={{["Rk", "Team", "DL Tier", "Sacks/G"]}}
            columnAlign={{["right", "left", "left", "right"]}}
            rows={{[
{ts_rows(rows["sack"], ["rk", "team", "tier", "spg"])}
            ]}}
          />
        </Stack>
        <Stack gap={{8}}>
          <H3>Secondary (1 = best)</H3>
          <Table
            headers={{["Rk", "Team", "Tier"]}}
            columnAlign={{["right", "left", "left"]}}
            rows={{[
{ts_rows(rows["secondary"], ["rk", "team", "tier"])}
            ]}}
          />
        </Stack>
      </Grid>

      <H3>Box / run fit (1 = best)</H3>
      <Table
        headers={{["Rk", "Team", "Tier"]}}
        columnAlign={{["right", "left", "left"]}}
        rows={{[
{ts_rows(rows["box"], ["rk", "team", "tier"])}
        ]}}
      />

      <Divider />

      <H2>Week-2 matchup leans</H2>
      <Grid columns={{2}} gap={{16}}>
        <Stack gap={{8}}>
          <H3>Pass / rec OVER</H3>
          <Table
            headers={{["Team", "Opp", "Pass Off", "Opp Pass D", "Edge"]}}
            columnAlign={{["left", "left", "right", "right", "right"]}}
            rows={{[
{cross_rows2(pass_over, "pass_off", "opp_pass_def", "pass_edge")}
            ]}}
          />
        </Stack>
        <Stack gap={{8}}>
          <H3>Pass / rec UNDER</H3>
          <Table
            headers={{["Team", "Opp", "Pass Off", "Opp Pass D", "Edge"]}}
            columnAlign={{["left", "left", "right", "right", "right"]}}
            rows={{[
{cross_rows2(pass_under, "pass_off", "opp_pass_def", "pass_edge")}
            ]}}
          />
        </Stack>
      </Grid>
      <Grid columns={{2}} gap={{16}}>
        <Stack gap={{8}}>
          <H3>Rush OVER</H3>
          <Table
            headers={{["Team", "Opp", "Rush Off", "Opp Rush D", "Edge"]}}
            columnAlign={{["left", "left", "right", "right", "right"]}}
            rows={{[
{cross_rows2(rush_over, "rush_off", "opp_rush_def", "rush_edge")}
            ]}}
          />
        </Stack>
        <Stack gap={{8}}>
          <H3>Rush UNDER</H3>
          <Table
            headers={{["Team", "Opp", "Rush Off", "Opp Rush D", "Edge"]}}
            columnAlign={{["left", "left", "right", "right", "right"]}}
            rows={{[
{cross_rows2(rush_under, "rush_off", "opp_rush_def", "rush_edge")}
            ]}}
          />
        </Stack>
      </Grid>

      <Callout tone="warning" title="Week 3">
        Team O/D ranks above are season unit ranks (all 32). Matchup leans use
        Week-2 opponents from this step8 — swap pairings for Week 3.
      </Callout>
    </Stack>
  );
}}
"""

OUT.write_text(tsx, encoding="utf-8")
print("wrote", OUT, "bytes", OUT.stat().st_size)
