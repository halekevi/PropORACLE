"""Emit canvas TSX for NFL gap ranking 2026-09-20."""
from __future__ import annotations

import json
from pathlib import Path

d = json.load(open(r"H:/PropORACLE/_tmp_nfl_gap_rank_920.json", encoding="utf-8"))
OUT = Path(r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/nfl-l5-gap-rank-2026-09-20.canvas.tsx")


def esc(s: object) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def abs_row(r: dict) -> str:
    g = f"{r['gap']:+.1f}"
    n = int(r["games"]) if r.get("games") else "?"
    return (
        f"    [{r['rank']}, '{esc(r['player'])}', '{esc(r['pick'][0])}', "
        f"'{esc(r['prop'])}', '{r['line']}', '{r['l5_avg']}', '{g}', "
        f"'{esc(r['matchup'])}', '{esc(r['kick'])}', 'n={n}'],"
    )


seen: set[tuple[str, ...]] = set()
games: list[dict] = []
for r in d["by_matchup_top"]:
    key = tuple(sorted(r["matchup"].replace(" vs ", "/").split("/")))
    if key in seen:
        continue
    seen.add(key)
    games.append(r)


def game_row(r: dict) -> str:
    return (
        f"    ['{esc(r['matchup'])}', '{esc(r['kick'])}', '{esc(r['player'])}', "
        f"'{esc(r['pick'][0])}', '{esc(r['prop'])}', '{r['line']}', "
        f"'{r['l5_avg']}', '{r['gap']:+.1f}'],"
    )


rows_abs = "\n".join(abs_row(r) for r in d["top_abs"][:40])
rows_g = "\n".join(game_row(r) for r in games)

tsx = f"""import {{
  Callout,
  Divider,
  Grid,
  H1,
  H2,
  Stack,
  Stat,
  Table,
  Text,
}} from "cursor/canvas";

/** NFL Sun 9/20 — ranked by |season-cut L5 avg − line|. Source: step8 Projection. */
export default function NflL5GapRank20260920() {{
  return (
    <Stack gap={{24}}>
      <Stack gap={{8}}>
        <H1>NFL gap rank — Sun 9/20</H1>
        <Text tone="secondary">
          All 14 Sunday games. Standard + Goblin only (Demons/combos/longest
          dropped). One face per player × prop × pick type (deepest |gap|).
          Gap = Projection − Line (season-cut L5; Week 2 → almost all n=1).
        </Text>
      </Stack>

      <Grid columns={{4}} gap={{12}}>
        <Stat value="{d['n_faces_deduped']}" label="Std/Gob faces" />
        <Stat value="14" label="Sunday games" />
        <Stat value="{int(round(d['n1_share'] * 100))}%" label="n=1 (THIN)" tone="warning" />
        <Stat value="+248.5" label="Max gap (Shough G)" />
      </Grid>

      <Callout tone="warning" title="Honest L5">
        88% of faces are Games Used = 1 (Week-1 actual as the \"avg\"). Large
        gaps are n=1 clearance, not a true 5-game mean. Shough +248 is the OT
        blowup — treat as outlier, not a ticket seed.
      </Callout>

      <H2>Top 40 by |L5 avg − line|</H2>
      <Text tone="secondary">
        Pick column: G = Goblin, S = Standard. Positive gap = avg above line.
      </Text>
      <Table
        headers={{[
          "#",
          "Player",
          "P",
          "Prop",
          "Line",
          "Avg",
          "Gap",
          "Game",
          "ET",
          "n",
        ]}}
        columnAlign={{[
          "right",
          "left",
          "left",
          "left",
          "right",
          "right",
          "right",
          "left",
          "left",
          "left",
        ]}}
        rows={{[
{rows_abs}
        ]}}
      />

      <Divider />

      <H2>Best gap per game</H2>
      <Table
        headers={{["Game", "ET", "Player", "P", "Prop", "Line", "Avg", "Gap"]}}
        columnAlign={{[
          "left",
          "left",
          "left",
          "left",
          "left",
          "right",
          "right",
          "right",
        ]}}
        rows={{[
{rows_g}
        ]}}
      />
    </Stack>
  );
}}
"""
OUT.write_text(tsx, encoding="utf-8")
print("wrote", OUT, "games", len(games))
