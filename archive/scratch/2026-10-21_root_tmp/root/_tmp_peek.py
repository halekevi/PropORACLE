import json
from pathlib import Path
from collections import Counter
# peek graded tennis dates for keep-ish volume
import re
MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
dates = Counter()
for d in [MAIN/"ui_runner"/"templates", REPO/"ui_runner"/"templates"]:
    if not d.exists(): continue
    for p in d.glob("graded_props_20*.json"):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        if m: dates[m.group(1)[:7]] += 1
print("graded month files", dates.most_common(12))
