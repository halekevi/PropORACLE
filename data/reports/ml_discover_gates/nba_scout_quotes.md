# NBA Goblin OVER — generic gate vs proj_edge scout at scraped quotes

Window 2026-02-19 → 2026-10-20 (89 slates). Walk-forward box + cell priors; hold = thin player or thin cell. Scout candidate pool = unheld legs only. `slate_minutes` rescales `proj_mu` only on a pre-tip snapshot (0 joined / 96 stored keys). Post-tip confirmed is never an input.

Breakevens: power2_1.40x 84.5% · power3_1.65x 84.6% · power3_2.00x 79.4% · power3_2.50x 73.7%.

| sleeve | HR | n | Wilson lo | EV@1.4x | EV@1.65x | EV@2.0x | EV@2.5x | Wilson clears power3_1.65x |
|---|---|---|---|---|---|---|---|---|
| ungated Goblin OVER | 64.9% | 67952 | 0.6451 | -0.411 | -0.5497 | -0.4542 | -0.3177 | no |
| GATE L5=5 + L10>=8 + D | 66.9% | 519 | 0.627 | -0.3742 | -0.5069 | -0.4023 | -0.2528 | no |
| scout top-N proj_edge (matched gate volume) | 76.4% | 509 | 0.7255 | -0.1823 | -0.2635 | -0.1073 | 0.1159 | no |
| scout top-N proj_ev_edge (matched) | 78.6% | 509 | 0.7481 | -0.1354 | -0.1992 | -0.0294 | 0.2133 | no |
| scout top-3/day proj_edge | 68.6% | 264 | 0.6273 | -0.3419 | -0.4682 | -0.3555 | -0.1943 | no |
| gate AND scout | 78.6% | 28 | 0.6046 | -0.1357 | -0.1997 | -0.0299 | 0.2126 | no |
| scout minus gate | 76.3% | 481 | 0.723 | -0.185 | -0.2671 | -0.1116 | 0.1105 | no |
| GATE high proj_edge half | 68.8% | 260 | 0.6298 | -0.3364 | -0.4616 | -0.3474 | -0.1842 | no |
| GATE low proj_edge half | 64.9% | 259 | 0.5887 | -0.411 | -0.5497 | -0.4542 | -0.3177 | no |
| pre-tip slate_minutes (joined only) | — | 0 | — | — | — | — | — | no |
| GATE + pre-tip slate_minutes | — | 0 | — | — | — | — | — | no |
| scout top-N + pre-tip slate_minutes | — | 0 | — | — | — | — | — | no |

## GATE by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_1.40x | clears power3_1.65x |
|---|---|---|---|---|---|
| Rebounds | 69.9% | 133 | 0.6166 | no | no |
| Assists | 70.7% | 92 | 0.6067 | no | no |
| 3-PT Made | 69.7% | 66 | 0.5778 | no | no |
| Points | 62.7% | 161 | 0.5505 | no | no |
| Steals | 65.5% | 58 | 0.5267 | no | no |

## Scout top-N by prop (n>=20)

| prop | HR | n | Wilson lo | clears power2_1.40x | clears power3_1.65x |
|---|---|---|---|---|---|
| Rebounds | 80.7% | 166 | 0.7405 | no | no |
| Points | 77.5% | 120 | 0.6924 | no | no |
| Assists | 77.4% | 106 | 0.6852 | no | no |
| 3-PT Made | 71.8% | 85 | 0.6142 | no | no |
| Turnovers | 57.7% | 26 | 0.3895 | no | no |

NBA Goblin OVER stays shadow until a sleeve's Wilson lower bound clears the scraped quote.
