from pathlib import Path
from tennis_shared import ensure_sackmann_matches
import sys
sys.path[:0]=[r"H:\PropORACLE\Sports\Tennis\scripts", r"H:\PropORACLE"]
# Does TA have Fearnley Aug 28?
from pathlib import Path
ta = Path(r"H:\PropORACLE\Sports\Tennis\data")
for p in ta.rglob("*"):
    if p.is_file() and "ta" in p.name.lower() or "matchlog" in p.name.lower() or "fearn" in p.name.lower():
        print(p, p.stat().st_size)
