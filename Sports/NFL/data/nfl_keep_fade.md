# NFL keep / fade (do not mix the two tapes)

Unique-game ledger: Weeks 1–2 2026 (`nfl_prop_gate_ledger_week1_2.csv`).
Engine: `utils/nfl_keep_gates.py`. Axis: `utils/nfl_prop_defense.py`.

**Goblin OVER tickets are live** (`NFL_GOBLIN_TICKETS_ENABLED = True`) for
wave-1 only (fg_made / receiving_yards / longest_reception / rushing_yards).
Pass-side Goblin stays staged in `NFL_GOBLIN_OVER_KEEP_PASS_LATER`.
Wave-1 ticket gate skips directional D (early-season Def noise); L5=5 +
L10≥8 + cover still required.

D bands (default): OVER vs Weak|Below; UNDER vs Elite|Above.
Exception: **tackles** use **opp rush offense** as the unit (Elite = strong
run attack). Same bands, different tape — do not read as opp pass D.

---

## Keep / fade (live Standard + staged Goblin)

| Chip | Live rule | Soft / note | Ungated unique W1+W2 |
|------|-----------|-------------|----------------------|
| Receptions Standard UNDER | **keep** (skip L5=5) | Option C pack bump: WOPR low + Pass D Elite\|Above | 75.0% 66/88; L5=5 15/18=83% but skip L5 path |
| Longest Reception Standard UNDER | **keep** full stack | Elite Pass D | 66.3% 65/98; G70 16/20=80% |
| Sacks Standard UNDER | **keep** full stack | — | 94.2% 49/52; G70 26/27=96% |
| Tackles+Ast Standard UNDER | **keep** full stack | **Axis = opp rush OFF** (not pass D) | 70.4% 19/27; L5=5 5/6=83%; G70 3/3 |
| Kicking Points Standard UNDER | **keep** full stack | kick D | 80.0% 12/15 |
| Receiving Yards Standard UNDER | **keep** (skip L5=5) | Elite Pass D; NOT Elite Pass Off stack | 62.5% 75/120; L5=5 hurts 4/10=40% |
| Pass Yards / Att / Comp / Comp% / Pass TDs Standard UNDER | **keep** + soft unit | Fail-closed if Pass D missing. Soft = Pass D Elite (+ NOT Elite Pass Off on yards; + DL pass on Comp%) | L5=5 n=1–3 thin — soft does the work |
| Punts / YPC / Longest Completion Standard OVER | **keep** + soft | Elite Rush D / Weak Rush D+NOT Elite Rush Off / Mobile QB | mixed |
| Rushing Yards Standard OVER | **watch** | Elite OL + Weak Rush D — reopen after W3 gated volume | 40.5% 34/84; L5=5 **2/3** |
| Pass Yards / Pass TDs Standard OVER | **watch** | reopen at L5=5 **n≥10** | L5=5 **1/1** each |
| Rushing Yards Standard UNDER | **watch** | NOT Elite Rush Off + NOT Elite RB + Elite Rush D | 67.3% 35/52; L5=5 5/6 |
| Rush Attempts Standard UNDER | **watch** | — | 80.0% 16/20; L5=5 3/3 |
| Fantasy Score | **off** (all books) | — | Goblin OVER 61.6% 101/164 — not a reopen |
| Player TDs / Anytime TDs Standard OVER | **fade** | RZ soft only | 13.8% 16/116 |
| Goblin OVER | **wave-1 live** | fg / rec yds / longest rec / rush yds; pass-side later | — |

Do not reopen fades from fuller ungated n when the original allowlist cell is thin.

### Soft-gate fail-closed (pass UNDER)

`nfl_soft_unit_ticket_ok` requires the soft stack. Missing `opp_pass_d_tier`
**and** missing `opp_pass_def_rank` → **False** (ticket suppressed). Present
but Avg / wrong band also fails. Soft never passes a chip through on empty
context.

---

## O / D axis (who the badge talks about)

| Prop family | Def Axis | Rank column | OVER lean | UNDER lean |
|-------------|----------|-------------|-----------|------------|
| Pass yards / att / comp / Comp% / Pass TDs / longest completion | `pass` | `opp_pass_def_rank` | Weak Pass D (± Weak own Pass Off for line inflation) | Elite Pass D (± NOT Elite own Pass Off) |
| Rec yards / receptions / longest reception | `pass` (coverage) | `opp_pass_def_rank` | Weak Pass D | Elite Pass D (± WOPR low / NOT Elite Pass Off) |
| Rush yards / att / YPC / longest rush | `rush` | `opp_rush_def_rank` | Weak Rush D (± Elite OL) | Elite Rush D (± NOT Elite Rush Off / RB) — watch |
| FG / kick pts | `kick` | `opp_fg_def_rank` | Weak kick D | Elite kick D |
| **Tackles / Tackles+Ast / solo** | **`opp_rush_off`** | **`opp_rush_off_rank`** | **Weak opp rush OFF** (short stops → more LB tackles) | **Elite opp rush OFF** (chunk plays past LB) ± soft own D |
| Sacks (defender) | `pass` proxy today | pass D / soft `opp_dl_pass_tier` | Weak Pass D / Elite DL pass | Elite Pass D |
| Sacks taken (QB) | soft OL / DL pass | `own_ol` + `opp_dl_pass_tier` | Weak pass block | Elite pass block + Weak DL pass |

Skill props: Def Tier = **opponent defense**.
Tackle props: Def Tier = **opponent rush offense** (same Elite→Weak labels).

### Soft W1+W2 polarity check (unique Standard UNDER n=27, base 70.4%)

Axis is live; **do not harden** a soft unit gate yet — sample flips the story:

| Cell | HR |
|------|-----|
| Elite opp rush OFF | 7/10 = 70% (flat) |
| Weak\|Below opp rush OFF | 5/6 = 83% (thin anti vs user UNDER lean) |
| Elite/Above rush OFF + Weak/Below own rush D | 4/6 = 67% |
| Weak\|Below own rush D alone | 4/8 = 50% |

User UNDER lean (strong opp offense / weak own D) is the **axis rationale**, not a W1+W2 lift. OVER vs Weak rush OFF is also flat-to-hurt (Goblin+Std mix). Revisit after Week 3+.

**TNF Franklin UNDER 7.5 (GB vs ATL):** ATL `rush_off_rank=15` → **Avg** → D fails under the new axis. Own GB rush D Above Avg. Old Pass-D Above clear was wrong; do not ticket on D.

---

## Passing (ungated unique W1+W2)

| Chip | Goblin OVER | Standard OVER | Standard UNDER |
|------|-------------|---------------|----------------|
| Pass Yards | 73.3% 22/30 | 45.0% 9/20 | 50.0% 10/20 |
| Pass Completions | 60.7% 17/28 | 38.5% 5/13 | 47.1% 8/17 |
| Pass Attempts | 50.0% 15/30 | 25.0% 6/24 | **69.2% 9/13** keep+soft |
| Pass TDs | 66.7% 18/27 | 66.7% 8/12 | 66.7% 6/9 keep+soft |
| Longest Completion | 63.3% 19/30 | 53.6% 15/28 | 50.0% 3/6 |
| Completion % | — | — | 45.2% 14/31 soft only |
| Pass+Rush Yards | 64.3% 18/28 | 37.5% 9/24 | 50.0% 6/12 |

Soft ticket stacks (not ungated reopen): Pass D Elite ± NOT Elite Pass Off; Comp% also Elite DL pass.

---

## Rushing

| Chip | Goblin OVER | Standard OVER | Standard UNDER |
|------|-------------|---------------|----------------|
| Rush Yards | 58.8% 47/80 | 40.5% 34/84 **keep+soft OVER** | 67.3% 35/52 **watch** |
| Rush Attempts | 60.3% 35/58 watch Goblin | 50.9% 29/57 | 80.0% 16/20 **watch** |
| Longest Rush | 50.0% 21/42 | 33.8% 22/65 | 62.1% 18/29 watch (Elite Rush D 11/14) |
| Rush YPC | — | 42.9% 15/35 keep+soft | — |
| Rush+Rec Yards | 50.0% 17/34 | 41.5% 17/41 | 61.1% 11/18 |

Rush Attempts UNDER watch ≠ auto-allowlist. Rush Yards UNDER soft cell (Elite Rush D + not-elite off/RB) is the reopen candidate, not base 67%.

---

## Receiving

| Chip | Goblin OVER | Standard OVER | Standard UNDER |
|------|-------------|---------------|----------------|
| Rec Yards | 66.7% 104/156 staged | 41.4% 72/174 fade | **62.5% 75/120 keep** (skip L5=5) |
| Receptions | 65.4% 106/162 watch | 42.3% 41/97 fade | **75.0% 66/88 keep** (skip L5=5) |
| Longest Reception | 63.6% 68/107 staged | 40.2% 51/127 fade | **66.3% 65/98 keep** |

L5=5 on Rec Yards UNDER (40%) is why that chip skips L5=5 on tickets. Receptions L5=5 is fine (83%) but still on the skip path for consistency with the allowlist decision.

---

## Defense (tackles / sacks)

| Chip | Goblin OVER | Standard OVER | Standard UNDER |
|------|-------------|---------------|----------------|
| Tackles+Ast | 63.1% 41/65 | 45.0% 18/40 | **70.4% 19/27 keep** — axis **opp rush OFF** |
| Sacks | 1/2 | 14.3% 4/28 fade | **94.2% 49/52 keep** |
| Sacks Taken | 2/5 | — | 52.6% 10/19 soft (mobility / DL pass / OL) |

**Tackles polarity (live):** defender counting. UNDER = fewer tackles → Elite\|Above **opp rush offense** (and soft weak own D). OVER = Weak\|Below opp rush offense. Do not badge against opp pass D.

---

## TDs / kicking / fantasy

| Chip | Goblin OVER | Standard OVER | Standard UNDER |
|------|-------------|---------------|----------------|
| Player TDs | — | **13.8% 16/116 fade** | — |
| Anytime TDs | 4/10 | 1/5 | — |
| Kicking Points | 63.3% 19/30 staged off tickets | 59.1% 13/22 | **80.0% 12/15 keep** |
| FG Made | 76.7% 23/30 staged | 25.0% 3/12 fade | 2/4 |
| Punts | — | 46.4% 13/28 keep+soft | — |
| Fantasy | 61.6% 101/164 **off** | 41.0% 55/134 **off** | 67.0% 71/106 **off** |

---

## Goblin (staged — tickets still off)

`NFL_GOBLIN_TICKETS_ENABLED = True` — wave-1 Goblin OVER live (fg_made /
receiving_yards / longest_reception / rushing_yards). Pass-side still staged.

### Flag flip trigger (locked — not a judgment call)

**GO** only when **each** of the four wave-1 chips has unique-game Goblin OVER
HR **≥ 75%** with **n ≥ 5** on the Week-3 ledger (`nfl_goblin_wave1_flip_go`).

**NO-GO** if any wave-1 chip is below 75%, or n < 5 (insufficient), or missing.
Do **not** use overall Goblin HR (pass-side / faded noise). Pass-side stays in
`NFL_GOBLIN_OVER_KEEP_PASS_LATER` even after a GO flip.

Constants: `NFL_GOBLIN_WAVE1_FLIP_MIN_HR = 0.75`, `NFL_GOBLIN_WAVE1_FLIP_MIN_N = 5`.

### Wave-1 (enable on GO flip) — `NFL_GOBLIN_OVER_KEEP_FLIP`

| Order | Prop | L5=5 unique (W1+W2) | Ungated |
|-------|------|---------------------|---------|
| 1 | fg_made | **6/6=100%** | 23/30=77% |
| 2 | receiving_yards | 17/23=74% (largest n) | 104/156=67% |
| 3 | longest_reception | 9/12=75% | 68/107=64% |
| 4 | rushing_yards | 10/14=71% | 47/80=59% |

### Wave-2 later (do **not** enable on flip) — `NFL_GOBLIN_OVER_KEEP_PASS_LATER`

Pass-side L5=5 degrades vs ungated — separate validation after Week 3+:

| Prop | L5=5 | Ungated |
|------|------|---------|
| passing_yards | 3/5=60% | 22/30=73% |
| pass_completions | 2/4=50% | 17/28=61% |
| pass_attempts | 1/3=33% | 15/30=50% |
| longest_completion | — | 19/30=63% |
| passing_tds | 4/6=67% | 18/27=67% |

Watch (not keep): `receptions`, `rush_attempts`.
Fantasy / Player TDs / Sacks OVER stay off.

---

## Week 3 review flags (before / after step1 posts)

1. Soft-gated pass UNDER — fail-closed on missing Pass D (tests); volume-check L5=5 after W3.
2. `rushing_yards` Standard OVER — gated Elite OL + Weak Rush D; reopen only with usable gated n.
3. `passing_yards` / `passing_tds` Standard OVER — reopen at L5=5 **n≥10**.
4. Goblin: run `nfl_goblin_wave1_flip_go` on Week-3 unique-game rates → binary flip or hold.
5. Ping when PrizePicks posts Week 3 and step1 has real rows.
