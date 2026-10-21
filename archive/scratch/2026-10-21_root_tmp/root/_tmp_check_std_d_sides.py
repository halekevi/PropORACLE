from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=60_000)
    boards = [pg for pg in b.contexts[0].pages if "prizepicks.com" in (pg.url or "")]
    page = boards[3] if len(boards) >= 4 else boards[-1]
    page.bring_to_front()
    info = page.evaluate(
        """() => {
      const root = document.body;
      const btns = [...root.querySelectorAll('button')].filter(b =>
        ['More','Less'].includes((b.innerText||'').trim())
      );
      const out = [];
      for (const b of btns) {
        let p = b.parentElement;
        let blob = '';
        for (let i=0;i<6 && p;i++) {
          const t = (p.innerText||'').replace(/\\s+/g,' ');
          if (t.includes('Goalie Saves') && t.length < 350) { blob = t; break; }
          p = p.parentElement;
        }
        if (!blob) continue;
        if (!/Corvi|Diouf|Lange/.test(blob)) continue;
        const pressed = b.getAttribute('aria-pressed')
          || b.getAttribute('aria-selected')
          || (b.className||'').match(/active|selected|pressed|chosen/i)?.[0]
          || '';
        const style = getComputedStyle(b);
        out.push({
          action: (b.innerText||'').trim(),
          pressed,
          bg: style.backgroundColor,
          fg: style.color,
          font: style.fontWeight,
          blob: blob.slice(0, 140),
        });
      }
      return out;
    }"""
    )
    for row in info:
        print(row)
