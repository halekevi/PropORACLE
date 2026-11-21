/* Props Board — config/props_board.json + /api/slate-sport; missing cells = n/a */
(function initPropsBoard() {
  const NA = "n/a";
  const PP_BOARD = "https://app.prizepicks.com/board";
  const PP_ENTRY = "https://app.prizepicks.com/";
  const state = {
    cfg: null,
    sport: "nba",
    tab: "Goblin",
    side: "", // "" | OVER | UNDER
    groupIdx: 0,
    rows: [],
    sortKey: "gap",
    sortDir: "desc",
    selected: new Set(),
    rowByKey: {},
  };

  const SORT_LABELS = {
    side: "Side",
    player: "Player",
    team: "Team",
    prop: "Prop",
    line: "Line",
    l5_avg: "L5 avg",
    gap: "Gap",
    sd: "SD",
    l10: "L10",
    l20: "L20",
    games_played: "GP",
  };

  function activeSportKeys() {
    const fromHome = window.HOME_ACTIVE_SPORTS;
    if (fromHome && typeof fromHome.forEach === "function") {
      return [...fromHome].map((s) => String(s).toLowerCase());
    }
    return Object.keys((state.cfg && state.cfg.sports) || {});
  }

  function num(v) {
    if (v == null || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function fmt2(v) {
    const n = num(v);
    return n == null ? NA : n.toFixed(2);
  }

  function sideOf(r) {
    return String(r.dir || r.direction || r.side || "").trim().toUpperCase() || NA;
  }

  function pickTypeOf(r) {
    return String(r.pick_type || "").trim().toLowerCase();
  }

  function rowKey(r) {
    return [
      String(r.player || "").trim().toLowerCase(),
      String(r.prop || r.prop_type || "").trim().toLowerCase(),
      String(r.line ?? ""),
      sideOf(r),
      pickTypeOf(r),
      String(r.sport || state.sport || "").toLowerCase(),
    ].join("|");
  }

  function hitsFrac(r, window) {
    // CFB seasons are ~16 games — L20 is permanently n/a (use prior-year for averages).
    const neverL20 = new Set((state.cfg && state.cfg.l20_never) || ["cfb", "cfb1h"]);
    if (window === "l20" && neverL20.has(String(state.sport || "").toLowerCase())) {
      return null;
    }
    const side = sideOf(r);
    const over = num(r[window + "_over"] != null ? r[window + "_over"] : r["last" + window.slice(1) + "_over"]);
    const under = num(r[window + "_under"] != null ? r[window + "_under"] : r["last" + window.slice(1) + "_under"]);
    const gp = num(r[window + "_games_played"]) || (over != null && under != null ? over + under : null);
    let hits = null;
    if (side === "OVER") hits = over;
    else if (side === "UNDER") hits = under;
    else hits = over;
    if (hits == null || gp == null) return null;
    return { hits, gp, text: Math.round(hits) + "/" + Math.round(gp), rate: gp > 0 ? hits / gp : null };
  }

  function l5Hits(r) {
    const side = sideOf(r);
    const over = num(r.l5_over != null ? r.l5_over : r.last5_over);
    const under = num(r.l5_under != null ? r.l5_under : r.last5_under);
    if (side === "UNDER") return under;
    return over;
  }

  function gamesPlayed(r) {
    return num(r.games_played) || num(r.l10_games_played) || num(r.distribution_n) || num(r.l5_games_played);
  }

  /** Play-side cover: OVER = L5−line, UNDER = line−L5. Positive = clearing the play. */
  function gapOf(r) {
    const line = num(r.line);
    const l5 = num(r.l5_avg);
    if (line == null || l5 == null) return num(r.avg_vs_line);
    const side = sideOf(r);
    if (side === "UNDER") return line - l5;
    if (side === "OVER") return l5 - line;
    return l5 - line;
  }

  function propAllowed(prop, group) {
    if (!group || !Array.isArray(group.props) || !group.props.length) return true;
    const want = new Set(group.props.map((p) => String(p).toLowerCase()));
    return want.has(String(prop || "").toLowerCase());
  }

  function sortValue(r, key) {
    switch (key) {
      case "side":
        return sideOf(r);
      case "player":
        return String(r.player || "").toLowerCase();
      case "team":
        return String(r.team || "").toLowerCase();
      case "prop":
        return String(r.prop || r.prop_type || "").toLowerCase();
      case "line":
        return num(r.line);
      case "l5_avg":
        return num(r.l5_avg);
      case "gap":
        return gapOf(r);
      case "sd":
        return num(r.sd);
      case "l10": {
        const h = hitsFrac(r, "l10");
        return h ? h.rate : null;
      }
      case "l20": {
        const h = hitsFrac(r, "l20");
        return h ? h.rate : null;
      }
      case "games_played":
        return gamesPlayed(r);
      default:
        return null;
    }
  }

  function cmpSort(a, b) {
    const key = state.sortKey || "gap";
    const va = sortValue(a, key);
    const vb = sortValue(b, key);
    const aMiss = va == null || va === "";
    const bMiss = vb == null || vb === "";
    if (aMiss !== bMiss) return aMiss ? 1 : -1;
    let primary = 0;
    if (typeof va === "number" && typeof vb === "number") {
      primary = va - vb;
    } else {
      primary = String(va).localeCompare(String(vb), undefined, { sensitivity: "base" });
    }
    if (primary !== 0) {
      return state.sortDir === "asc" ? primary : -primary;
    }
    // Tie-break: Gap desc magnitude, then SD asc, then player.
    if (key !== "gap") {
      const ga = gapOf(a);
      const gb = gapOf(b);
      if (ga != null && gb != null && ga !== gb) return gb - ga;
    }
    if (key !== "sd") {
      const sa = num(a.sd);
      const sb = num(b.sd);
      if (sa != null && sb != null && sa !== sb) return sa - sb;
    }
    return String(a.player || "").localeCompare(String(b.player || ""));
  }

  function fillSportSelect() {
    const sel = document.getElementById("pb-sport");
    if (!sel || !state.cfg) return;
    const keys = activeSportKeys().filter((k) => state.cfg.sports[k]);
    const order = Object.keys(state.cfg.sports);
    const sorted = order.filter((k) => keys.includes(k));
    for (const k of keys) if (!sorted.includes(k)) sorted.push(k);
    sel.innerHTML = sorted.map((k) => `<option value="${k}">${k.toUpperCase()}</option>`).join("");
    if (!sorted.includes(state.sport)) state.sport = sorted[0] || "nba";
    sel.value = state.sport;
  }

  function fillGroups() {
    const host = document.getElementById("pb-groups");
    if (!host || !state.cfg) return;
    const sportCfg = state.cfg.sports[state.sport] || {};
    const groups = sportCfg.groups || [{ name: "All props", props: sportCfg.props_catalog || [] }];
    if (state.groupIdx >= groups.length) state.groupIdx = 0;
    host.innerHTML = groups
      .map(
        (g, i) =>
          `<button type="button" class="pb-group${i === state.groupIdx ? " active" : ""}" data-gi="${i}">${g.name}</button>`
      )
      .join("");
    host.querySelectorAll(".pb-group").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.groupIdx = Number(btn.getAttribute("data-gi") || 0);
        fillGroups();
        render();
      });
    });
  }

  function syncSideButtons() {
    document.querySelectorAll("#props-board .pb-side").forEach((btn) => {
      const side = String(btn.getAttribute("data-side") || "").toUpperCase();
      btn.classList.toggle("active", state.side === side);
    });
  }

  function updateSortHeaders() {
    document.querySelectorAll("#props-board th[data-sort]").forEach((th) => {
      const key = th.getAttribute("data-sort");
      const label = SORT_LABELS[key] || key;
      const active = key === state.sortKey;
      const arrow = active ? (state.sortDir === "asc" ? " ▲" : " ▼") : "";
      th.textContent = label + arrow;
      th.setAttribute("aria-sort", active ? (state.sortDir === "asc" ? "ascending" : "descending") : "none");
      th.title = active
        ? `Sorted ${state.sortDir === "asc" ? "ascending" : "descending"} — click to reverse`
        : `Sort by ${label}`;
      th.classList.toggle("pb-sort-active", active);
    });
  }

  async function loadRows() {
    const sport = state.sport;
    try {
      if (typeof ensureSlateSportLoaded === "function") {
        await ensureSlateSportLoaded(sport);
      } else if (typeof loadSlateSport === "function") {
        await Promise.resolve(loadSlateSport(sport));
      }
    } catch (_e) {}
    let rows = (window.SLATE_DATA && window.SLATE_DATA[sport]) || [];
    if (!rows.length) {
      try {
        const res = await fetch("/api/slate-sport/" + encodeURIComponent(sport), { cache: "no-store" });
        if (res.ok) {
          const payload = await res.json();
          const sports = payload.sports || payload;
          rows = sports[sport] || payload.rows || [];
          if (!Array.isArray(rows) && sports && typeof sports === "object") {
            rows = sports[sport] || [];
          }
        }
      } catch (_e) {}
    }
    state.rows = Array.isArray(rows) ? rows : [];
  }

  function filtered() {
    const sportCfg = (state.cfg && state.cfg.sports[state.sport]) || {};
    const groups = sportCfg.groups || [{ name: "All", props: sportCfg.props_catalog || [] }];
    const group = groups[state.groupIdx] || groups[0];
    const tab = String(state.tab || "").toLowerCase();
    const want5 = document.getElementById("pb-l5-5")?.checked;
    const want4 = document.getElementById("pb-l5-4")?.checked;
    const out = [];
    for (const r of state.rows) {
      if (pickTypeOf(r) !== tab) continue;
      if (!propAllowed(r.prop || r.prop_type, group)) continue;
      if (state.side && sideOf(r) !== state.side) continue;
      const l5 = l5Hits(r);
      if (want5 || want4) {
        let ok = false;
        if (want5 && l5 != null && Math.round(l5) === 5) ok = true;
        if (want4 && l5 != null && Math.round(l5) === 4) ok = true;
        if (!ok) continue;
      }
      const keyed = { ...r, sport: r.sport || state.sport };
      out.push(keyed);
    }
    const multi = new Set();
    const byPlayer = {};
    for (const r of out) {
      const p = String(r.player || "");
      byPlayer[p] = (byPlayer[p] || 0) + 1;
    }
    for (const [p, n] of Object.entries(byPlayer)) if (n > 1) multi.add(p);

    out.sort(cmpSort);
    return { rows: out, multi };
  }

  function selectedRows() {
    const out = [];
    for (const k of state.selected) {
      const r = state.rowByKey[k];
      if (r) out.push(r);
    }
    return out;
  }

  function updateActionBar(visibleRows) {
    const n = state.selected.size;
    const countEl = document.getElementById("pb-sel-count");
    if (countEl) countEl.textContent = n ? `${n} selected` : "";
    const addBtn = document.getElementById("pb-add-slip");
    const copyBtn = document.getElementById("pb-copy-pp");
    const openBtn = document.getElementById("pb-open-pp");
    if (addBtn) addBtn.disabled = n === 0;
    if (copyBtn) copyBtn.disabled = n === 0;
    if (openBtn) openBtn.disabled = n === 0;
    const master = document.getElementById("pb-check-all");
    if (master) {
      const keys = visibleRows.map(rowKey);
      const allOn = keys.length > 0 && keys.every((k) => state.selected.has(k));
      const someOn = keys.some((k) => state.selected.has(k));
      master.checked = allOn;
      master.indeterminate = someOn && !allOn;
    }
  }

  function formatLegLine(r) {
    const side = sideOf(r);
    const line = r.line == null ? "" : r.line;
    const prop = r.prop || r.prop_type || "";
    const pick = r.pick_type || state.tab || "";
    return `${r.player} · ${side} ${line} ${prop}${pick ? " · " + pick : ""}`;
  }

  function projectionIdOf(r) {
    const raw = r.pp_projection_id != null ? r.pp_projection_id : r.projection_id;
    if (raw == null || raw === "") return null;
    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) return String(Math.trunc(n));
    const s = String(raw).trim();
    return s && s.toLowerCase() !== "nan" ? s : null;
  }

  function lineToken(v) {
    const n = num(v);
    if (n == null) return "";
    return Number.isInteger(n) ? String(n) : String(n);
  }

  /** PrizePicks share/entry URL: ?projections={id}-o|{u}-{line}, comma-joined. */
  function buildPrizePicksSlipUrl(rows) {
    const parts = [];
    const missing = [];
    for (const r of rows) {
      const pid = projectionIdOf(r);
      const line = lineToken(r.line);
      const side = sideOf(r);
      if (!pid || !line || (side !== "OVER" && side !== "UNDER")) {
        missing.push(formatLegLine(r));
        continue;
      }
      const ou = side === "UNDER" ? "u" : "o";
      parts.push(`${pid}-${ou}-${line}`);
    }
    if (!parts.length) {
      return { url: null, missing, parts: [] };
    }
    return {
      url: PP_ENTRY + "?projections=" + parts.join(","),
      missing,
      parts,
    };
  }

  function setActionMsg(msg, ok) {
    const el = document.getElementById("pb-action-msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("ok", !!ok);
    el.classList.toggle("err", msg && !ok);
  }

  function addSelectedToSlip() {
    const rows = selectedRows();
    if (!rows.length) {
      setActionMsg("Select at least one prop first.", false);
      return;
    }
    if (!window.MySlip || typeof MySlip.addPick !== "function") {
      setActionMsg("My slip is not loaded on this page.", false);
      return;
    }
    for (const r of rows) {
      MySlip.addPick({
        sport: r.sport || state.sport,
        player: r.player,
        prop: r.prop || r.prop_type,
        prop_type: r.prop || r.prop_type,
        dir: sideOf(r),
        direction: sideOf(r),
        line: r.line,
        pick: r.pick_type || state.tab,
        pick_type: r.pick_type || state.tab,
        standard_line: r.standard_line,
      });
    }
    setActionMsg(
      `Sent ${rows.length} leg${rows.length === 1 ? "" : "s"} to My slip. Enter them on PrizePicks yourself — PP has no public auto-fill link.`,
      true
    );
    if (typeof MySlip.render === "function") MySlip.render();
  }

  async function copySelectedForPp() {
    const rows = selectedRows();
    if (!rows.length) {
      setActionMsg("Select at least one prop first.", false);
      return;
    }
    const text = rows.map(formatLegLine).join("\n");
    try {
      await navigator.clipboard.writeText(text);
      setActionMsg(`Copied ${rows.length} leg${rows.length === 1 ? "" : "s"} — paste into notes while you build on PrizePicks.`, true);
    } catch (_e) {
      setActionMsg("Clipboard blocked — use Add to My slip instead.", false);
    }
  }

  function openPrizePicksSlip() {
    const rows = selectedRows();
    if (!rows.length) {
      setActionMsg("Select at least one prop first.", false);
      return;
    }
    if (rows.length > 6) {
      setActionMsg("PrizePicks max is 6 legs — deselect some first.", false);
      return;
    }
    const built = buildPrizePicksSlipUrl(rows);
    if (!built.url) {
      setActionMsg(
        "Missing PrizePicks projection IDs on these rows (slate needs a refresh with IDs). Use Copy / My slip for now.",
        false
      );
      return;
    }
    window.open(built.url, "_blank", "noopener,noreferrer");
    const missNote = built.missing.length
      ? ` Skipped ${built.missing.length} without an ID.`
      : "";
    setActionMsg(
      `Opened PrizePicks with ${built.parts.length} leg${built.parts.length === 1 ? "" : "s"} on the slip — confirm and submit in the app.${missNote}`,
      true
    );
  }

  function openPrizePicksBoard() {
    window.open(PP_BOARD, "_blank", "noopener,noreferrer");
    setActionMsg("Opened PrizePicks board (empty).", true);
  }

  function render() {
    const tbody = document.getElementById("pb-tbody");
    const countEl = document.getElementById("pb-count");
    if (!tbody) return;
    const flagThin = document.getElementById("pb-flag-thin")?.checked;
    const flagMulti = document.getElementById("pb-flag-multi")?.checked;
    const { rows, multi } = filtered();
    state.rowByKey = {};
    for (const r of rows) state.rowByKey[rowKey(r)] = r;

    const sideLbl = state.side ? ` · ${state.side}` : "";
    if (countEl) {
      countEl.textContent =
        `${rows.length} rows · ${state.tab}${sideLbl} · ${state.sport.toUpperCase()}` +
        ` · sort ${SORT_LABELS[state.sortKey] || state.sortKey} ${state.sortDir === "asc" ? "↑" : "↓"}`;
    }
    syncSideButtons();
    updateSortHeaders();
    tbody.innerHTML = rows
      .map((r) => {
        const key = rowKey(r);
        const gp = gamesPlayed(r);
        const thin = flagThin && gp != null && gp < 15;
        const isMulti = flagMulti && multi.has(String(r.player || ""));
        const l10 = hitsFrac(r, "l10");
        const l20 = hitsFrac(r, "l20");
        const cls = [thin ? "pb-flag-thin" : "", isMulti ? "pb-flag-multi" : "", state.selected.has(key) ? "pb-selected" : ""]
          .filter(Boolean)
          .join(" ");
        const cell = (txt, missing) => (missing ? `<span class="pb-na">${NA}</span>` : txt);
        const checked = state.selected.has(key) ? " checked" : "";
        return `<tr class="${cls}" data-pb-key="${key.replace(/"/g, "&quot;")}">
        <td class="pb-check-cell"><input type="checkbox" class="pb-row-check" data-key="${key.replace(/"/g, "&quot;")}"${checked} aria-label="Select ${String(r.player || "prop").replace(/"/g, "&quot;")}"></td>
        <td>${sideOf(r)}</td>
        <td class="pb-player">${r.player || NA}</td>
        <td>${r.team || NA}</td>
        <td>${r.prop || r.prop_type || NA}</td>
        <td>${fmt2(r.line)}</td>
        <td>${cell(fmt2(r.l5_avg), num(r.l5_avg) == null)}</td>
        <td>${cell(fmt2(gapOf(r)), gapOf(r) == null)}</td>
        <td>${cell(fmt2(r.sd), num(r.sd) == null)}</td>
        <td>${l10 ? l10.text : cell(NA, true)}</td>
        <td>${l20 ? l20.text : cell(NA, true)}</td>
        <td>${gp != null ? fmt2(gp) : cell(NA, true)}</td>
      </tr>`;
      })
      .join("");

    tbody.querySelectorAll(".pb-row-check").forEach((box) => {
      box.addEventListener("change", () => {
        const k = box.getAttribute("data-key") || "";
        if (!k) return;
        if (box.checked) state.selected.add(k);
        else state.selected.delete(k);
        const tr = box.closest("tr");
        if (tr) tr.classList.toggle("pb-selected", box.checked);
        updateActionBar(rows);
      });
    });
    updateActionBar(rows);
  }

  function exportCsv() {
    const { rows } = filtered();
    const headers = ["side", "player", "team", "prop", "line", "l5_avg", "gap", "sd", "l10", "l20", "games_played"];
    const lines = [headers.join(",")];
    for (const r of rows) {
      const l10 = hitsFrac(r, "l10");
      const l20 = hitsFrac(r, "l20");
      const vals = [
        sideOf(r),
        r.player || "",
        r.team || "",
        r.prop || r.prop_type || "",
        fmt2(r.line),
        fmt2(r.l5_avg),
        fmt2(gapOf(r)),
        fmt2(r.sd),
        l10 ? l10.text : NA,
        l20 ? l20.text : NA,
        (() => {
          const g = gamesPlayed(r);
          return g == null ? NA : fmt2(g);
        })(),
      ].map((v) => `"${String(v).replace(/"/g, '""')}"`);
      lines.push(vals.join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `props_board_${state.sport}_${state.tab.toLowerCase()}_${(state.cfg && state.cfg.as_of) || "slate"}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function bindSortHeaders() {
    document.querySelectorAll("#props-board th[data-sort]").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.getAttribute("data-sort");
        if (!key) return;
        if (state.sortKey === key) {
          state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
          state.sortKey = key;
          // Text / rate columns default largest-first; names A→Z.
          const textKeys = new Set(["side", "player", "team", "prop"]);
          state.sortDir = textKeys.has(key) ? "asc" : "desc";
        }
        render();
      });
    });
  }

  async function boot() {
    try {
      const res = await fetch("/api/props-board/config", { cache: "no-store" });
      if (!res.ok) throw new Error("config " + res.status);
      state.cfg = await res.json();
    } catch (e) {
      const countEl = document.getElementById("pb-count");
      if (countEl) countEl.textContent = "Props Board config missing";
      return;
    }
    fillSportSelect();
    fillGroups();
    bindSortHeaders();
    syncSideButtons();
    document.getElementById("pb-sport")?.addEventListener("change", async (ev) => {
      state.sport = String(ev.target.value || "").toLowerCase();
      state.groupIdx = 0;
      state.selected.clear();
      fillGroups();
      await loadRows();
      render();
    });
    document.querySelectorAll("#props-board .pb-tab").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll("#props-board .pb-tab").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        state.tab = btn.getAttribute("data-tab") || "Goblin";
        state.selected.clear();
        render();
      });
    });
    document.querySelectorAll("#props-board .pb-side").forEach((btn) => {
      btn.addEventListener("click", () => {
        const side = String(btn.getAttribute("data-side") || "").toUpperCase();
        state.side = state.side === side ? "" : side;
        render();
      });
    });
    ["pb-l5-5", "pb-l5-4", "pb-flag-thin", "pb-flag-multi"].forEach((id) => {
      document.getElementById(id)?.addEventListener("change", render);
    });
    document.getElementById("pb-export")?.addEventListener("click", exportCsv);
    document.getElementById("pb-add-slip")?.addEventListener("click", addSelectedToSlip);
    document.getElementById("pb-copy-pp")?.addEventListener("click", () => {
      copySelectedForPp();
    });
    document.getElementById("pb-open-pp")?.addEventListener("click", openPrizePicksSlip);
    document.getElementById("pb-open-pp-board")?.addEventListener("click", openPrizePicksBoard);
    document.getElementById("pb-check-all")?.addEventListener("change", (ev) => {
      const on = !!ev.target.checked;
      const { rows } = filtered();
      for (const r of rows) {
        const k = rowKey(r);
        if (on) state.selected.add(k);
        else state.selected.delete(k);
      }
      render();
    });
    await loadRows();
    render();
  }

  function start() {
    // Wait briefly for HOME_ACTIVE_SPORTS from pipeline status.
    setTimeout(boot, 600);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
