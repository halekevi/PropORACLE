# 2026-11-13: Kevin wants all numbers in my claude_* outputs (Excel and CSV, every sport) limited to 2 decimals.
import pandas as pd
def _wrap(orig):
    def f(self,*a,**k):
        try:
            fc=self.select_dtypes(include='float').columns
            if len(fc): self=self.copy(); self[fc]=self[fc].round(2)
        except Exception: pass
        return orig(self,*a,**k)
    return f
if not getattr(pd.DataFrame,'_r2',False):
    pd.DataFrame.to_excel=_wrap(pd.DataFrame.to_excel); pd.DataFrame.to_csv=_wrap(pd.DataFrame.to_csv); pd.DataFrame._r2=True
