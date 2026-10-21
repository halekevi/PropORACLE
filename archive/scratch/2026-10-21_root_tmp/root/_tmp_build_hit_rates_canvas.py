import json
from pathlib import Path

d = json.loads(Path(r"H:/PropORACLE/data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8"))
LIVE = {"Goblin OVER", "Standard OVER", "Standard UNDER"}
cells = []
for r in d["rows"]:
    if r.get("book") not in LIVE:
        continue
    cells.append(
        {
            "s": r["sport"],
            "b": r["book"],
            "p": r.get("prop_display") or r.get("prop"),
            "t": r.get("prop_tier") or "",
            "n": int(r.get("n") or 0),
            "h": int(r.get("hits") or 0),
            "a4n": int(r.get("listed_n") or 0),
            "a4h": int(r.get("listed_hits") or 0),
            "e5n": int(r.get("l5eq5_n") or 0),
            "e5h": int(r.get("l5eq5_hits") or 0),
            "t8n": int(r.get("l10ge8_n") or 0),
            "t8h": int(r.get("l10ge8_hits") or 0),
            "j4n": int(r.get("l5ge4_l10ge8_n") or 0),
            "j4h": int(r.get("l5ge4_l10ge8_hits") or 0),
            "j5n": int(r.get("l5eq5_l10ge8_n") or 0),
            "j5h": int(r.get("l5eq5_l10ge8_hits") or 0),
            "src": "graded",
        }
    )

nfl = json.loads(
    Path(r"H:/PropORACLE/Sports/NFL/data/nfl_prop_gate_ledger_week1.json").read_text(encoding="utf-8")
)


def to_hits(nn: int, hr) -> int:
    if not nn or hr is None:
        return 0
    x = float(hr)
    if x > 1.0:
        x /= 100.0
    return int(round(nn * x))


for r in nfl.get("rows") or []:
    pt = str(r.get("pick_type") or "Standard").strip().title()
    side = str(r.get("side") or "").strip().upper()
    if pt == "Goblin" and side == "OVER":
        book = "Goblin OVER"
    elif pt == "Standard" and side == "OVER":
        book = "Standard OVER"
    elif pt == "Standard" and side == "UNDER":
        book = "Standard UNDER"
    else:
        continue
    n = int(r.get("n") or 0)
    h = int(r.get("hits") or 0)
    a4n = int(r.get("l5_ge4_n") or 0)
    e5n = int(r.get("l5_eq5_n") or 0)
    g70n = int(r.get("gate70_n") or 0)
    cells.append(
        {
            "s": "NFL",
            "b": book,
            "p": str(r.get("prop") or "").replace("_", " ").title(),
            "t": "",
            "n": n,
            "h": h,
            "a4n": a4n,
            "a4h": to_hits(a4n, r.get("l5_ge4_hr")),
            "e5n": e5n,
            "e5h": to_hits(e5n, r.get("l5_eq5_hr")),
            "t8n": 0,
            "t8h": 0,
            "j4n": 0,
            "j4h": 0,
            "j5n": g70n,
            "j5h": to_hits(g70n, r.get("gate70_hr")),
            "src": "nfl_w1",
        }
    )

cfb = json.loads(
    Path(r"H:/PropORACLE/data/reports/cfb_prop_gate_lift_2026-09-03_05.json").read_text(encoding="utf-8")
)
for book, rows in cfb.items():
    if book not in LIVE or not isinstance(rows, list):
        continue
    for r in rows:
        allv = r.get("all") or {}
        n = int(allv.get("n") or 0)
        h = int(allv.get("h") or 0)
        gates = r.get("gates") or {}
        g4 = gates.get("L5>=4") or {}
        g5 = gates.get("L5=5") or {}
        cells.append(
            {
                "s": "CFB",
                "b": book,
                "p": str(r.get("prop") or "").replace("_", " ").title(),
                "t": "",
                "n": n,
                "h": h,
                "a4n": int(g4.get("n") or 0),
                "a4h": int(g4.get("h") or 0),
                "e5n": int(g5.get("n") or 0),
                "e5h": int(g5.get("h") or 0),
                "t8n": 0,
                "t8h": 0,
                "j4n": 0,
                "j4h": 0,
                "j5n": 0,
                "j5h": 0,
                "src": "cfb_w1",
            }
        )

order = {"Goblin OVER": 0, "Standard OVER": 1, "Standard UNDER": 2}
cells.sort(key=lambda x: (x["s"], order.get(x["b"], 9), -x["n"], x["p"]))
sports = sorted({c["s"] for c in cells})
meta = {
    "from": d["from"],
    "to": d["to"],
    "days": d["n_days"],
    "decided": d["n_decided"],
    "cats": len(cells),
    "sports": sports,
}

sports_js = json.dumps(sports)
cells_js = json.dumps(cells, separators=(",", ":"))
meta_js = json.dumps(meta, separators=(",", ":"))

tsx = f"""import {{
  Callout,
  H1,
  H2,
  Pill,
  Row,
  Select,
  Stack,
  Stat,
  Table,
  Text,
  useCanvasState,
}} from "cursor/canvas";

type Cell = {{
  s: string;
  b: string;
  p: string;
  t: string;
  n: number;
  h: number;
  a4n: number;
  a4h: number;
  e5n: number;
  e5h: number;
  t8n: number;
  t8h: number;
  j4n: number;
  j4h: number;
  j5n: number;
  j5h: number;
  src: string;
}};

const META = {meta_js} as const;
const SPORTS: string[] = {sports_js};
const BOOKS = ["Goblin OVER", "Standard OVER", "Standard UNDER"] as const;
const MIN_NS = ["1", "12", "20", "40"] as const;
const CELLS: Cell[] = {cells_js};

function pct(h: number, n: number): string {{
  if (!n) return "—";
  return (100 * h / n).toFixed(1) + "% (" + h + "/" + n + ")";
}}

export default function TrackedPropHitRates() {{
  const [sport, setSport] = useCanvasState("sport", "All");
  const [book, setBook] = useCanvasState("book", "Goblin OVER");
  const [minN, setMinN] = useCanvasState("minN", "20");
  const floor = Number(minN) || 20;

  const rows = CELLS.filter((c) => {{
    if (sport !== "All" && c.s !== sport) return false;
    if (c.b !== book) return false;
    return c.n >= floor;
  }});

  const decided = rows.reduce((a, c) => a + c.n, 0);
  const hits = rows.reduce((a, c) => a + c.h, 0);

  return (
    <Stack gap={{16}}>
      <H1>Tracked prop hit rates</H1>
      <Text tone="secondary">
        Sport × Goblin OVER / Standard OVER / Standard UNDER. Ungated HR plus L5≥4, L5=5, L10≥8, and joint windows when available.
      </Text>
      <Callout tone="info" title="Sources">
        Master graded catalog {{META.from}} → {{META.to}} ({{META.decided.toLocaleString()}} decided). NFL = Week 1 unique-game ledger. CFB = Sep 3–5 gate lift. Golf not in this dump.
      </Callout>
      <Row gap={{12}} wrap>
        <Stat value={{String(META.cats)}} label="Prop × book cells" />
        <Stat value={{String(rows.length)}} label="Rows in view" />
        <Stat value={{decided ? (100 * hits / decided).toFixed(1) + "%" : "—"}} label="View ungated HR" />
        <Stat value={{String(SPORTS.length)}} label="Sports" />
      </Row>
      <Row gap={{12}} wrap>
        <Select
          value={{sport}}
          onChange={{setSport}}
          options={{[{{ label: "All sports", value: "All" }}, ...SPORTS.map((s) => ({{ label: s, value: s }}))]}}
        />
        <Select
          value={{book}}
          onChange={{setBook}}
          options={{BOOKS.map((b) => ({{ label: b, value: b }}))}}
        />
        <Select
          value={{minN}}
          onChange={{setMinN}}
          options={{MIN_NS.map((n) => ({{ label: "min n=" + n, value: n }}))}}
        />
        <Pill tone="neutral">{{book}}</Pill>
      </Row>
      <H2>{{sport === "All" ? "All sports" : sport}} · {{book}}</H2>
      <Table
        columns={{[
          {{ id: "s", header: "Sport", accessor: (r: Cell) => r.s, width: 80 }},
          {{ id: "p", header: "Prop", accessor: (r: Cell) => r.p }},
          {{ id: "t", header: "Tier", accessor: (r: Cell) => r.t || "—", width: 48 }},
          {{ id: "all", header: "Ungated", accessor: (r: Cell) => pct(r.h, r.n), align: "right" }},
          {{ id: "a4", header: "L5≥4", accessor: (r: Cell) => pct(r.a4h, r.a4n), align: "right" }},
          {{ id: "e5", header: "L5=5", accessor: (r: Cell) => pct(r.e5h, r.e5n), align: "right" }},
          {{ id: "t8", header: "L10≥8", accessor: (r: Cell) => pct(r.t8h, r.t8n), align: "right" }},
          {{ id: "j5", header: "L5=5+L10≥8", accessor: (r: Cell) => pct(r.j5h, r.j5n), align: "right" }},
          {{ id: "n", header: "n", accessor: (r: Cell) => String(r.n), align: "right", width: 72 }},
        ]}}
        rows={{rows}}
      />
      <Text tone="secondary" size="small">
        Filter by sport and book. Sort is n descending within sport. Source: data/reports/all_ranked_prop_hit_rates.json (+ NFL/CFB appends).
      </Text>
    </Stack>
  );
}}
"""

out = Path(r"C:/Users/halek/.cursor/projects/h-PropORACLE/canvases/tracked-prop-hit-rates.canvas.tsx")
out.write_text(tsx, encoding="utf-8")
print("wrote", out)
print("cells", len(cells), "sports", sports)
