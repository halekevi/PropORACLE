"""Surgical payout-rate sort patch onto main's tickets_render.py."""
from __future__ import annotations

from pathlib import Path

p = Path(r"H:\PropORACLE_main_cp\utils\tickets_render.py")
text = p.read_text(encoding="utf-8")

# 1) Add _group_payout_rate_score after _group_payout_confidence_score
anchor = "def _group_payout_confidence_score(tickets: list) -> float:"
if "def _group_payout_rate_score" not in text:
    # find end of that function — next def after it
    i = text.find(anchor)
    if i < 0:
        raise SystemExit("confidence score fn missing")
    j = text.find("\ndef ", i + 10)
    insert = '''

def _group_payout_rate_score(group: dict) -> float:
    """N-correct board payout x for /tickets sort (never 1st-place sweep)."""
    tickets = group.get("tickets") or []
    best = 0.0
    for t in tickets:
        if not isinstance(t, dict):
            continue
        pay = t.get("payout") if isinstance(t.get("payout"), dict) else None
        x = _resolve_ticket_display_min_x(pay, t)
        if x is None:
            continue
        try:
            v = float(x)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > best:
            best = v
    if best > 0:
        return best
    for k in ("power_payout", "flex_payout", "display_min_x"):
        raw = group.get(k)
        if raw is None:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > best:
            best = v
    return best

'''
    text = text[:j] + insert + text[j:]
    print("inserted _group_payout_rate_score")
else:
    print("score fn already present")

# 2) Sort options
old_opts = """        '<option value="ev_desc" selected>EV ↓</option>'
        '<option value="ev_asc">EV ↑</option>'
        '<option value="pwin_desc">P(WIN) ↓</option>'
"""
new_opts = """        '<option value="ev_desc" selected>EV ↓</option>'
        '<option value="ev_asc">EV ↑</option>'
        '<option value="payout_desc">Payout rate ↓</option>'
        '<option value="payout_asc">Payout rate ↑</option>'
        '<option value="pwin_desc">P(WIN) ↓</option>'
"""
if "payout_desc" not in text:
    if old_opts not in text:
        raise SystemExit("options block not found")
    text = text.replace(old_opts, new_opts, 1)
    print("added sort options")
else:
    print("options already present")

# 3) Title on label
text = text.replace(
    '<label class="ticket-filter-sort-wrap" for="ticket-sort-select">',
    '<label class="ticket-filter-sort-wrap" for="ticket-sort-select" title="Sort groups. Payout uses N-correct board rate, not 1st-place.">',
    1,
)
text = text.replace(
    '<select id="ticket-sort-select" class="ticket-filter-sort">',
    '<select id="ticket-sort-select" class="ticket-filter-sort" aria-label="Sort ticket groups">',
    1,
)

# 4) Wire pay_rate into prepared groups — find pc_max = _group_payout_confidence_score
needle = "pc_max = _group_payout_confidence_score(tickets)"
if "pay_rate = _group_payout_rate_score" not in text:
    if needle not in text:
        raise SystemExit("pc_max line missing")
    text = text.replace(
        needle,
        needle + "\n        pay_rate = _group_payout_rate_score(group)",
        1,
    )
    text = text.replace(
        '"payout_confidence": pc_max,',
        '"payout_confidence": pc_max,\n                "payout_rate": pay_rate,',
        1,
    )
    print("wired pay_rate into prepared")

# 5) data attributes
old_div = 'data-payout-confidence="{_fmt(d_pc, 2)}" data-n-legs="{d_n_legs}"'
new_div = 'data-payout-confidence="{_fmt(d_pc, 2)}" data-payout-rate="{_fmt(d_pay_rate, 4)}" data-n-legs="{d_n_legs}"'
if "data-payout-rate=" not in text:
    if old_div not in text:
        raise SystemExit("group div attrs missing")
    text = text.replace(old_div, new_div, 1)
    # add d_pay_rate extract near d_pc
    text = text.replace(
        "d_pc = float(ent.get(\"payout_confidence\") or 0.0)",
        "d_pc = float(ent.get(\"payout_confidence\") or 0.0)\n        d_pay_rate = float(ent.get(\"payout_rate\") or 0.0)",
        1,
    )
    print("added data-payout-rate on groups")

# 6) ticket div
old_t = 'data-fp="{_h(fp)}">'
new_t = 'data-fp="{_h(fp)}" data-payout-rate="{_fmt(_safe_positive_float(kpi_payout) or 0.0, 4)}">'
# more specific
old_t2 = '<div class="ticket" style="border-left:4px solid {accent};" data-group-name="{_h(group_name)}" data-ticket-no="{_h(ticket_no)}" data-fp="{_h(fp)}">'
new_t2 = '<div class="ticket" style="border-left:4px solid {accent};" data-group-name="{_h(group_name)}" data-ticket-no="{_h(ticket_no)}" data-fp="{_h(fp)}" data-payout-rate="{_fmt(_safe_positive_float(kpi_payout) or 0.0, 4)}">'
if old_t2 in text and "data-payout-rate=\"{_fmt(_safe_positive_float(kpi_payout)" not in text:
    text = text.replace(old_t2, new_t2, 1)
    print("added data-payout-rate on tickets")
elif "kpi_payout" in text and "data-payout-rate" in text:
    print("ticket payout-rate maybe already set")
else:
    # try looser
    if 'data-fp="{_h(fp)}">' in text and 'data-payout-rate="{_fmt(_safe_positive_float(kpi_payout)' not in text:
        text = text.replace(
            '<div class="ticket" style="border-left:4px solid {accent};" data-group-name="{_h(group_name)}" data-ticket-no="{_h(ticket_no)}" data-fp="{_h(fp)}">',
            '<div class="ticket" style="border-left:4px solid {accent};" data-group-name="{_h(group_name)}" data-ticket-no="{_h(ticket_no)}" data-fp="{_h(fp)}" data-payout-rate="{_fmt(_safe_positive_float(kpi_payout) or 0.0, 4)}">',
            1,
        )
        print("added ticket attr (fallback)")
    else:
        print("WARN ticket attr")

# 7) JS sortGroups payout branch + sortTicketsInGroup
js_anchor = "  function sortGroups(groups){"
js_insert = """  function sortGroups(groups){
    if(sortMode === 'payout_desc' || sortMode === 'payout_asc'){
      groups.sort(function(a,b){
        var d = parseNum(a, 'data-payout-rate') - parseNum(b, 'data-payout-rate');
        if(d === 0) d = parseNum(a, 'data-ev-score') - parseNum(b, 'data-ev-score');
        return sortMode === 'payout_asc' ? d : -d;
      });
      return;
    }
"""
if "sortMode === 'payout_desc'" not in text:
    if js_anchor not in text:
        raise SystemExit("sortGroups missing")
    text = text.replace(js_anchor, js_insert, 1)
    print("added JS payout sortGroups")

if "function sortTicketsInGroup" not in text:
    # insert before matchesFilter
    mf = "  function matchesFilter(group, filter){"
    block = """  function sortTicketsInGroup(group){
    if(sortMode !== 'payout_desc' && sortMode !== 'payout_asc') return;
    var body = group.querySelector('.ticket-group-body');
    if(!body) return;
    var tickets = Array.from(body.querySelectorAll(':scope > .ticket'));
    if(tickets.length < 2) return;
    tickets.sort(function(a,b){
      var d = parseNum(a, 'data-payout-rate') - parseNum(b, 'data-payout-rate');
      return sortMode === 'payout_asc' ? d : -d;
    });
    tickets.forEach(function(t){ body.appendChild(t); });
  }

  function matchesFilter(group, filter){"""
    if mf not in text:
        raise SystemExit("matchesFilter missing")
    text = text.replace(mf, block, 1)
    print("added sortTicketsInGroup")

# call sortTicketsInGroup after sortGroups(visible)
old_apply = "      sortGroups(visible);\n    }"
# look for pattern after filters
if "sortTicketsInGroup(g)" not in text:
    # find: sortGroups(visible);
    if "sortGroups(visible);" not in text:
        raise SystemExit("sortGroups(visible) missing")
    text = text.replace(
        "sortGroups(visible);",
        "sortGroups(visible);\n    visible.forEach(function(g){ sortTicketsInGroup(g); });",
        1,
    )
    print("wired sortTicketsInGroup call")

p.write_text(text, encoding="utf-8")
print("OK", p)
print("has payout_desc", "payout_desc" in text)
print("has mlb_prop_matchup", "mlb_prop_matchup" in text)
