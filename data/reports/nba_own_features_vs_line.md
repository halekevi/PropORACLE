# NBA: do our own features (ratings, per-stat opponent defense, pace) help beyond the line?

Games: 1563 (train 1282 before 2026-07-01, holdout 281). Features use each team's last 20 games before the game date (min 5); box-score match rate 100.0%.

## 1. Our model alone vs the closing line (mean absolute error, lower is better)

| Split | Our margin | Line margin | Our total | Line total |
|---|---|---|---|---|
| train | 11.61 | 10.987 | 15.037 | 14.476 |
| holdout | 11.682 | 10.746 | 15.698 | 14.45 |

## 2. Do our features predict the line's miss?

R² of predicting (actual − line). Above 0 on holdout means real information the line missed.

- Margin: train 0.0097, holdout -0.0126
- Total: train 0.014, holdout -0.0044

Feature correlation with the line's miss (a real signal keeps its sign on holdout):

| Feature | Margin miss train | Margin miss holdout | Total miss train | Total miss holdout |
|---|---|---|---|---|
| away_ast_matchup | -0.032 | 0.065 | 0.05 | 0.031 |
| away_fg3m_matchup | -0.012 | -0.02 | 0.019 | -0.093 |
| away_pts_matchup | -0.012 | 0.061 | 0.023 | 0.016 |
| away_reb_matchup | -0.043 | 0.046 | 0.011 | -0.013 |
| exp_margin_ours | 0.037 | -0.016 | -0.068 | -0.054 |
| exp_pace | 0.027 | 0.016 | -0.024 | -0.022 |
| exp_total_ours | 0.019 | 0.061 | -0.028 | -0.018 |
| home_ast_matchup | 0.044 | 0.098 | -0.023 | -0.068 |
| home_fg3m_matchup | -0.05 | 0.079 | -0.07 | -0.056 |
| home_pts_matchup | 0.04 | 0.038 | -0.066 | -0.044 |
| home_reb_matchup | 0.037 | -0.037 | -0.001 | -0.073 |
| net_rtg_diff | 0.038 | -0.015 | -0.068 | -0.053 |

## Betting spread on the predicted miss

Chosen on train: threshold 2 → holdout 20/37 = 54.0% (95% 38.4%–69.0%), ROI +3.6% (train 107/189 = 56.6% (95% 49.5%–63.5%), ROI +7.8%)

| Threshold | Train | Holdout |
|---|---|---|
| 0 | 654/1282 = 51.0% (95% 48.3%–53.7%), ROI -2.6% | 130/281 = 46.3% (95% 40.5%–52.1%), ROI -11.6% |
| 0.5 | 479/920 = 52.1% (95% 48.8%–55.3%), ROI -0.5% | 95/198 = 48.0% (95% 41.1%–54.9%), ROI -8.5% |
| 1 | 326/594 = 54.9% (95% 50.9%–58.8%), ROI +4.9% | 62/130 = 47.7% (95% 39.3%–56.2%), ROI -8.8% |
| 1.5 | 194/347 = 55.9% (95% 50.6%–61.0%), ROI +6.9% | 33/67 = 49.2% (95% 37.6%–60.9%), ROI -5.8% |
| 2 | 107/189 = 56.6% (95% 49.5%–63.5%), ROI +7.8% | 20/37 = 54.0% (95% 38.4%–69.0%), ROI +3.6% |
| 3 | 17/27 = 63.0% (95% 44.2%–78.5%), ROI +20.0% | 1/2 = 50.0% (95% 9.4%–90.5%), ROI -5.4% |

## Betting total on the predicted miss

Chosen on train: threshold 2 → holdout 45/83 = 54.2% (95% 43.5%–64.5%), ROI +3.3% (train 260/452 = 57.5% (95% 52.9%–62.0%), ROI +9.8%)

| Threshold | Train | Holdout |
|---|---|---|
| 0 | 685/1282 = 53.4% (95% 50.7%–56.1%), ROI +2.0% | 133/281 = 47.3% (95% 41.6%–53.2%), ROI -9.7% |
| 0.5 | 580/1056 = 54.9% (95% 51.9%–57.9%), ROI +4.9% | 114/228 = 50.0% (95% 43.6%–56.4%), ROI -4.7% |
| 1 | 461/829 = 55.6% (95% 52.2%–59.0%), ROI +6.2% | 88/175 = 50.3% (95% 43.0%–57.6%), ROI -4.2% |
| 1.5 | 354/623 = 56.8% (95% 52.9%–60.7%), ROI +8.5% | 64/123 = 52.0% (95% 43.3%–60.7%), ROI -0.9% |
| 2 | 260/452 = 57.5% (95% 52.9%–62.0%), ROI +9.8% | 45/83 = 54.2% (95% 43.5%–64.5%), ROI +3.3% |
| 3 | 118/214 = 55.1% (95% 48.4%–61.7%), ROI +5.3% | 22/38 = 57.9% (95% 42.2%–72.2%), ROI +10.0% |

## Betting moneyline on the predicted miss

Chosen on train: threshold 0.04 → holdout 29/75 = 38.7% (95% 28.5%–50.0%), ROI -24.3% (train 178/358 = 49.7% (95% 44.6%–54.9%), ROI +3.6%)

| Threshold | Train | Holdout |
|---|---|---|
| 0.0 | 353/700 = 50.4% (95% 46.7%–54.1%), ROI -3.3% | 79/161 = 49.1% (95% 41.5%–56.7%), ROI -13.1% |
| 0.02 | 254/503 = 50.5% (95% 46.1%–54.9%), ROI +3.0% | 46/107 = 43.0% (95% 34.0%–52.4%), ROI -22.4% |
| 0.04 | 178/358 = 49.7% (95% 44.6%–54.9%), ROI +3.6% | 29/75 = 38.7% (95% 28.5%–50.0%), ROI -24.3% |
| 0.06 | 122/259 = 47.1% (95% 41.1%–53.2%), ROI -3.6% | 15/52 = 28.8% (95% 18.3%–42.3%), ROI -38.6% |
| 0.08 | 74/175 = 42.3% (95% 35.2%–49.7%), ROI -7.4% | 11/34 = 32.4% (95% 19.1%–49.2%), ROI -27.6% |
| 0.1 | 43/108 = 39.8% (95% 31.1%–49.2%), ROI -5.9% | 7/22 = 31.8% (95% 16.4%–52.7%), ROI -39.8% |
