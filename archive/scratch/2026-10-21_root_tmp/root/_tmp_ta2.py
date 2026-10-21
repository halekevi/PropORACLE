import json
from pathlib import Path
p=Path(r"H:\PropORACLE\Sports\Tennis\data\cache\tennis_abstract_matchlog.json")
d=json.loads(p.read_text(encoding="utf-8"))
print(type(d), list(d)[:10] if isinstance(d,dict) else len(d))
if isinstance(d,dict):
    for k in ("players","by_player","cache","updated","generated"):
        if k in d: print(k, type(d[k]), (len(d[k]) if hasattr(d[k],"__len__") else d[k]))
    # find structure
    for k,v in list(d.items())[:5]:
        print("key", k, type(v))
        if isinstance(v, dict):
            print("  subkeys", list(v.keys())[:8])
            for sk,sv in list(v.items())[:1]:
                print("  sample", sk, str(sv)[:300])
        elif isinstance(v, list) and v:
            print("  list0", str(v[0])[:300])
