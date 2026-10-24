# NBA Goblin OVER — generic gate vs proj_edge scout at scraped quotes

Window 2026-02-19 → 2026-10-20 (89 slates). Walk-forward box + cell priors; hold = thin player or thin cell. Scout candidate pool = unheld legs only.

Breakevens: Power-2 1.40x 84.5% · Power-3 1.65x 84.6% · 2.00x 79.4% · 2.50x 73.7%.

| sleeve | HR | n | Wilson lo | EV@1.40x | EV@1.65x | EV@2.00x | EV@2.50x | Wilson clears 1.65x |
|---|---|---|---|---|---|---|---|---|
| ungated Goblin OVER | 64.9% | 67952 | 0.6451 | -0.411 | -0.5497 | -0.4542 | -0.3177 | no |
| GATE L5=5 + L10>=8 + D | 67.0% | 533 | 0.6288 | -0.3719 | -0.5042 | -0.399 | -0.2488 | no |
| scout top-N proj_edge (matched gate volume) | 76.3% | 523 | 0.7246 | -0.1852 | -0.2673 | -0.1119 | 0.1101 | no |
| scout top-N proj_ev_edge (matched) | 78.0% | 523 | 0.7426 | -0.148 | -0.2166 | -0.0505 | 0.1869 | no |
| scout top-3/day proj_edge | 69.7% | 264 | 0.639 | -0.3199 | -0.4414 | -0.3229 | -0.1536 | no |
| gate AND scout | 78.1% | 32 | 0.6124 | -0.1455 | -0.2132 | -0.0463 | 0.1921 | no |
| scout minus gate | 76.2% | 491 | 0.7221 | -0.1877 | -0.2708 | -0.1161 | 0.1049 | no |
| GATE high proj_edge half | 68.5% | 267 | 0.6274 | -0.3423 | -0.4687 | -0.3561 | -0.1951 | no |
| GATE low proj_edge half | 65.4% | 266 | 0.5951 | -0.4009 | -0.5382 | -0.4402 | -0.3003 | no |

## GATE by prop (n>=20)

| prop | HR | n | Wilson lo | clears 1.65x | clears 2.00x |
|---|---|---|---|---|---|
| Rebounds | 69.8% | 136 | 0.6168 | no | no |
| 3-PT Made | 70.0% | 70 | 0.5846 | no | no |
| Assists | 68.6% | 86 | 0.5818 | no | no |
| Points | 64.7% | 173 | 0.5737 | no | no |
| Steals | 64.4% | 59 | 0.5166 | no | no |

## Scout top-N by prop (n>=20)

| prop | HR | n | Wilson lo | clears 1.65x | clears 2.00x |
|---|---|---|---|---|---|
| Rebounds | 81.1% | 169 | 0.7449 | no | no |
| Points | 78.1% | 128 | 0.702 | no | no |
| Assists | 76.6% | 107 | 0.6778 | no | no |
| 3-PT Made | 70.1% | 87 | 0.5981 | no | no |
| Turnovers | 57.7% | 26 | 0.3895 | no | no |

NBA Goblin stays shadow until a sleeve's Wilson lower bound clears the scraped quote.
