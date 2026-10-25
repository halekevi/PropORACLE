# NBA Standard OVER — generic gate vs proj_edge scout at scraped quotes

Window 2026-02-19 → 2026-10-20 (89 slates). Walk-forward box + cell priors; hold = thin player or thin cell. Scout candidate pool = unheld legs only. `slate_minutes` rescales `proj_mu` only on a pre-tip snapshot (0 joined / 96 stored keys). Post-tip confirmed is never an input.

Breakevens: power2_3.00x 57.7% · power3_6.00x 55.0%.

| sleeve | HR | n | Wilson lo | EV@3.0x | EV@6.0x | Wilson clears power3_6.00x |
|---|---|---|---|---|---|---|
| ungated Standard OVER | 49.2% | 23509 | 0.4853 | -0.2746 | -0.2866 | no |
| GATE L5=5 + L10>=8 + D | 40.0% | 10 | 0.1682 | -0.52 | -0.616 | no |
| scout top-N proj_edge (matched gate volume) | 80.0% | 10 | 0.4902 | 0.92 | 2.072 | no |
| scout top-N proj_ev_edge (matched) | 60.0% | 10 | 0.3127 | 0.08 | 0.296 | no |
| scout top-3/day proj_edge | 52.0% | 252 | 0.4583 | -0.1893 | -0.1571 | no |
| gate AND scout | — | 0 | — | — | — | no |
| scout minus gate | 80.0% | 10 | 0.4902 | 0.92 | 2.072 | no |
| GATE high proj_edge half | 80.0% | 5 | 0.3755 | 0.92 | 2.072 | no |
| GATE low proj_edge half | 0.0% | 5 | 0.0 | -1.0 | -1.0 | no |
| pre-tip slate_minutes (joined only) | — | 0 | — | — | — | no |
| GATE + pre-tip slate_minutes | — | 0 | — | — | — | no |
| scout top-N + pre-tip slate_minutes | — | 0 | — | — | — | no |

## GATE by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|

## Scout top-N by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_3.00x | clears power3_6.00x |
|---|---|---|---|---|---|

NBA Standard OVER stays shadow until a sleeve's Wilson lower bound clears the scraped quote.
