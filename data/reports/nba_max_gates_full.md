# NBA max gates — all factors

Train slates <= 2026-11-12; holdout = later slates. Break-even: Standard 57.7%, Goblin 75.0%. Observe-only.

## Goblin

| Prop | Side | Gate | Train | Holdout | Base | Holds |
|---|---|---|---:|---:|---:|---|
| Pts+Rebs | OVER | L10 hits >=8 · L10 avg right side · opp rank favorable · minutes not down 2+ · pace favorable | 82.9% (n=152) | 80.8% (n=26) | 63.1% | YES |
| Rebounds | OVER | L5 hits >=4 · L10 hits >=7 · L5 avg right side · opp rank favorable · minutes not down 2+ · pace not unfavorable | 77.7% (n=291) | 76.2% (n=206) | 68.0% | YES |
| Assists | OVER | L10 hits >=9 · season avg right side · team rank not unfavorable · opp rank favorable · minutes not down 2+ · pace not unfavorable | 95.1% (n=41) | 75.9% (n=29) | 66.6% | YES |
| Two Pointers Attempted | OVER | season avg >=5% · minutes not down 2+ · pace favorable | 66.7% (n=42) | 100.0% (n=1) | 53.0% |  |
| Rebs+Asts | OVER | L5 hits =5 · L10 avg >=10% · opp rank not unfavorable · pace favorable | 87.7% (n=57) | 80.0% (n=10) | 63.4% |  |
| Pts+Rebs+Asts | OVER | L10 hits >=9 · opp rank not unfavorable · minutes not down 2+ · pace favorable | 88.6% (n=70) | 75.0% (n=8) | 61.1% |  |
| Two Pointers Made | OVER | L10 hits >=7 · opp rank not unfavorable | 65.9% (n=41) | 73.1% (n=26) | 49.8% |  |
| Fantasy Score | OVER | opp rank not unfavorable | 71.4% (n=70) | 72.8% (n=173) | 62.3% |  |
| 3-PT Attempted | OVER | L10 hits >=7 · minutes not down 2+ · pace not unfavorable | 75.6% (n=41) | 72.7% (n=11) | 58.5% |  |
| Steals | OVER | L10 hits >=8 · season avg right side · team rank not unfavorable · opp rank favorable · minutes not down 2+ · pace not unfavorable | 84.6% (n=52) | 70.0% (n=20) | 62.5% |  |
| Points | OVER | L5 hits =5 · L10 hits >=9 · team rank favorable · opp rank favorable | 87.9% (n=91) | 69.2% (n=39) | 64.7% |  |
| Pts+Asts | OVER | L5 hits =5 · L10 hits >=7 · opp rank not unfavorable · pace favorable | 87.7% (n=81) | 66.7% (n=12) | 61.9% |  |
| Free Throws Attempted | OVER | season avg >=10% · team rank not unfavorable · minutes not down 2+ · pace not unfavorable | 62.3% (n=61) | 65.2% (n=23) | 49.2% |  |
| 3-PT Made | OVER | L5 hits =5 · team rank favorable · opp rank not unfavorable · pace not unfavorable | 90.2% (n=41) | 62.5% (n=24) | 67.4% |  |
| Turnovers | OVER | L10 avg right side · team rank not unfavorable · minutes not down 2+ · pace not unfavorable | 67.2% (n=128) | 62.5% (n=96) | 58.5% |  |
| FG Made | OVER | L5 avg >=10% · L10 avg right side · season avg right side | 58.3% (n=84) | 60.8% (n=51) | 52.2% |  |
| Blocked Shots | OVER | L5 avg >=10% · season avg right side · team rank favorable | 62.2% (n=172) | 60.3% (n=116) | 57.1% |  |
| Offensive Rebounds | OVER | L5 avg >=10% · L10 avg right side · season avg >=10% | 58.3% (n=48) | 60.0% (n=40) | 54.2% |  |
| Free Throws Made | OVER | L10 avg >=10% · team rank favorable · opp rank not unfavorable | 67.5% (n=40) | 52.6% (n=19) | 51.6% |  |
| FG Attempted | OVER | L5 avg right side · season avg >=5% · pace not unfavorable | 65.2% (n=46) | 50.0% (n=8) | 50.7% |  |
| Personal Fouls | OVER | opp rank favorable · pace not unfavorable | 63.5% (n=63) | 43.6% (n=39) | 53.2% |  |
| Blks+Stls | OVER | L10 hits >=8 · L5 avg right side · season avg right side · opp rank not unfavorable · minutes not down 2+ | 73.9% (n=46) | 41.2% (n=17) | 53.4% |  |

## Standard

| Prop | Side | Gate | Train | Holdout | Base | Holds |
|---|---|---|---:|---:|---:|---|
| Two Pointers Made | UNDER | L5 avg right side · season avg >=10% · team rank not unfavorable · minutes not down 2+ | 76.3% (n=38) | 69.2% (n=26) | 52.6% | YES |
| Turnovers | UNDER | L5 avg right side · opp rank not unfavorable | 78.1% (n=64) | 67.4% (n=43) | 55.9% | YES |
| Defensive Rebounds | UNDER | L5 avg right side | 76.0% (n=25) | 64.7% (n=17) | 67.4% | YES |
| Personal Fouls | UNDER | L5 avg right side · opp rank favorable | 73.3% (n=30) | 64.0% (n=25) | 54.8% | YES |
| 3-PT Attempted | OVER | L10 hits >=7 · season avg >=10% · minutes not down 2+ | 81.5% (n=27) | 62.5% (n=24) | 52.4% | YES |
| Pts+Rebs+Asts | UNDER | L10 hits >=8 · team rank not unfavorable · minutes not down 2+ · pace not unfavorable | 76.2% (n=42) | 62.5% (n=24) | 54.8% | YES |
| 3-PT Made | OVER | L5 hits >=4 · team rank favorable | 74.1% (n=27) | 60.7% (n=28) | 50.4% | YES |
| Blks+Stls | UNDER | season avg >=5% · opp rank not unfavorable · minutes not down 2+ | 69.8% (n=43) | 59.1% (n=22) | 57.2% | YES |
| Offensive Rebounds | OVER | opp rank not unfavorable · minutes not down 2+ | 65.5% (n=29) | 58.3% (n=60) | 62.2% | YES |
| 3-PT Attempted | UNDER | L5 hits >=4 · L5 avg >=10% · L10 avg right side | 80.8% (n=26) | 57.9% (n=19) | 47.6% | YES |
| Blocked Shots | UNDER | L10 hits >=7 | 80.0% (n=30) | 62.5% (n=8) | 54.6% |  |
| Steals | OVER | L5 avg right side · L10 avg >=10% · season avg right side · minutes not down 2+ · pace not unfavorable | 54.4% (n=90) | 58.8% (n=34) | 47.3% |  |
| Fantasy Score | OVER | opp rank not unfavorable | 54.3% (n=70) | 56.6% (n=173) | 46.5% |  |
| Pts+Asts | OVER | L10 hits >=7 · L5 avg right side · L10 avg >=10% · season avg right side · team rank not unfavorable · minutes not down 2+ · pace not unfavorable | 81.5% (n=27) | 54.5% (n=11) | 45.8% |  |
| Pts+Rebs | UNDER | L5 avg right side · L10 avg >=10% · season avg >=5% · team rank favorable · opp rank not unfavorable · pace not unfavorable | 90.0% (n=30) | 52.2% (n=23) | 55.4% |  |
| Free Throws Attempted | UNDER | minutes not down 2+ · pace favorable | 69.8% (n=106) | 51.6% (n=186) | 53.8% |  |
| Assists | OVER | L10 avg right side · season avg >=5% · team rank favorable | 62.1% (n=66) | 50.9% (n=53) | 43.8% |  |
| Blocked Shots | OVER | L5 avg >=10% · L10 avg right side | 54.4% (n=79) | 50.6% (n=85) | 45.4% |  |
| Points | UNDER | opp rank favorable · minutes not down 2+ · pace favorable | 61.6% (n=232) | 50.6% (n=259) | 54.7% |  |
| Defensive Rebounds | OVER | no filter | 32.6% (n=43) | 50.0% (n=40) | 32.6% |  |
| FG Attempted | UNDER | L10 hits >=8 · L5 avg right side · season avg right side · pace not unfavorable | 74.1% (n=27) | 50.0% (n=24) | 51.9% |  |
| Free Throws Made | UNDER | pace favorable | 61.7% (n=94) | 50.0% (n=176) | 52.8% |  |
| Pts+Rebs | OVER | L10 avg >=10% · opp rank favorable · minutes not down 2+ · pace favorable | 65.8% (n=38) | 50.0% (n=8) | 44.6% |  |
| Rebs+Asts | OVER | L10 hits >=7 · L10 avg >=10% · pace favorable | 75.0% (n=28) | 50.0% (n=4) | 47.1% |  |
| Rebounds | OVER | season avg >=5% · minutes down 2+ · pace not unfavorable | 70.4% (n=27) | 50.0% (n=4) | 48.6% |  |
| Rebounds | UNDER | L10 hits >=7 · L10 avg >=10% · season avg right side | 65.5% (n=87) | 50.0% (n=72) | 51.4% |  |
| Two Pointers Attempted | OVER | L5 hits >=4 · L10 avg >=10% · team rank not unfavorable | 72.0% (n=25) | 50.0% (n=20) | 43.8% |  |
| FG Made | UNDER | season avg right side · opp rank favorable · minutes not down 2+ · pace not unfavorable | 72.7% (n=33) | 47.8% (n=46) | 52.8% |  |
| Turnovers | OVER | L5 avg >=10% · season avg >=10% · minutes not down 2+ | 54.7% (n=95) | 47.6% (n=63) | 44.1% |  |
| Points | OVER | L10 hits >=8 · team rank favorable | 70.4% (n=27) | 47.4% (n=19) | 45.3% |  |
| Two Pointers Attempted | UNDER | opp rank not unfavorable | 58.5% (n=234) | 46.9% (n=194) | 56.2% |  |
| FG Made | OVER | L10 hits >=7 · L10 avg right side · season avg right side · pace not unfavorable | 69.2% (n=26) | 46.2% (n=13) | 47.2% |  |
| Offensive Rebounds | UNDER | no filter | 37.8% (n=37) | 45.9% (n=122) | 37.8% |  |
| Steals | UNDER | L5 hits >=4 · opp rank not unfavorable | 68.0% (n=25) | 45.5% (n=33) | 52.7% |  |
| Two Pointers Made | OVER | team rank favorable · opp rank favorable | 65.5% (n=55) | 45.5% (n=33) | 47.4% |  |
| Free Throws Attempted | OVER | opp rank not unfavorable · minutes not down 2+ · pace not unfavorable | 57.4% (n=183) | 43.2% (n=74) | 46.2% |  |
| Blks+Stls | OVER | L5 avg >=10% · L10 avg >=10% · pace not unfavorable | 52.3% (n=65) | 42.9% (n=35) | 42.8% |  |
| Pts+Asts | UNDER | team rank favorable · opp rank favorable · minutes not down 2+ · pace not unfavorable | 68.7% (n=99) | 41.1% (n=124) | 54.2% |  |
| Fantasy Score | UNDER | team rank not unfavorable · opp rank favorable · minutes not down 2+ · pace not unfavorable | 80.0% (n=25) | 40.2% (n=82) | 53.5% |  |
| 3-PT Made | UNDER | L10 hits >=7 · L10 avg >=10% · team rank not unfavorable | 76.0% (n=25) | 38.9% (n=18) | 49.6% |  |
| Assists | UNDER | L5 avg >=10% · L10 avg >=10% · season avg >=10% · pace not unfavorable | 75.7% (n=37) | 35.3% (n=17) | 56.2% |  |
| FG Attempted | OVER | L5 hits >=4 · L5 avg >=10% · team rank not unfavorable · pace not unfavorable | 77.5% (n=40) | 33.3% (n=9) | 48.1% |  |
| Personal Fouls | OVER | team rank favorable · opp rank not unfavorable · pace favorable | 76.9% (n=26) | 33.3% (n=9) | 45.2% |  |
| Pts+Rebs+Asts | OVER | L10 hits >=7 · L5 avg right side · L10 avg right side · minutes not down 2+ · pace not unfavorable | 56.3% (n=103) | 31.0% (n=29) | 45.2% |  |
| Rebs+Asts | UNDER | L5 avg >=10% · L10 avg >=10% · opp rank favorable · pace not unfavorable | 75.9% (n=29) | 25.0% (n=8) | 52.9% |  |
| Free Throws Made | OVER | L10 avg right side · team rank not unfavorable · opp rank favorable · minutes not down 2+ · pace favorable | 67.6% (n=34) | 0.0% (n=6) | 47.2% |  |
