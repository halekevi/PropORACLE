import json
from collections import Counter, defaultdict
from pathlib import Path

gh = json.loads(Path("ui_runner/templates/grade_history.json").read_text(encoding="utf-8"))
print("grade_history entries", len(gh))
sports = Counter()
props = Counter()
nfl_props = Counter()
nfl_results = Counter()
by_prop = defaultdict(lambda: Counter())
for t in gh:
    if not isinstance(t, dict):
        continue
    for leg in t.get("legs") or t.get("picks") or []:
        if not isinstance(leg, dict):
            continue
        sp = str(leg.get("sport") or t.get("sport") or "").upper()
        sports[sp] += 1
        prop = str(leg.get("prop") or leg.get("prop_type") or "").lower()
        res = str(leg.get("result") or leg.get("grade") or "").upper()
        if "NFL" in sp:
            nfl_props[prop] += 1
            nfl_results[res] += 1
            by_prop[prop][res] += 1
print("sports top", sports.most_common(15))
print("nfl results", nfl_results)
print("nfl props", nfl_props.most_common(40))
for prop, c in sorted(by_prop.items(), key=lambda x: -sum(x[1].values())):
    h = c.get("HIT", 0) + c.get("WON", 0) + c.get("WIN", 0)
    m = c.get("MISS", 0) + c.get("LOST", 0) + c.get("LOSS", 0)
    n = h + m
    if n:
        print(f"  {prop:30} {h}/{n} = {h/n:.1%}")
