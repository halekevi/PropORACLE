import pandas as pd
import numpy as np
import argparse
import os

def analyze_performance(file_path):
    # 1. Load data (CSV or Excel)
    if file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Ensure numeric types
    cols_to_fix = ['Edge', 'Line', 'Actual', 'Margin', 'L5 Avg', 'Season Avg', 'Rank Score']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Standardize column names (handle your pipeline variations)
    df.rename(columns={
        'L5 Avg': 'L5_Avg',
        'Season Avg': 'Season_Avg',
        'Rank Score': 'Rank_Score'
    }, inplace=True)

    # 2. Binary Outcome
    df['is_hit'] = df['Result'].astype(str).str.upper().apply(lambda x: 1 if x == 'HIT' else 0)

    print("\n--- 📊 SLATEIQ PERFORMANCE AUDIT ---")

    # 3. Feature Correlations
    features = ['Edge', 'L5_Avg', 'Season_Avg', 'Rank_Score', 'OVERALL_DEF_RANK']
    available_features = [f for f in features if f in df.columns]

    if 'Margin' in df.columns:
        correlations = df[available_features + ['Margin']].corr()['Margin'].sort_values(ascending=False)
        print("\n[+] Feature Correlation with Margin:")
        print(correlations)
    else:
        correlations = pd.Series()

    # 4. Defense Tier Analysis
    if 'DEF_TIER' in df.columns:
        print("\n[+] Win Rate by Defense Tier:")
        def_analysis = df.groupby('DEF_TIER')['is_hit'].agg(['mean', 'count'])
        def_analysis.rename(columns={'mean': 'Win_Rate'}, inplace=True)
        print(def_analysis)

    # 5. Edge Buckets
    if 'Edge' in df.columns:
        df['edge_bins'] = pd.cut(df['Edge'], bins=6)
        edge_analysis = df.groupby('edge_bins')['is_hit'].mean()
        print("\n[+] Win Rate by Edge Size:")
        print(edge_analysis)

    # 6. Rank Score Buckets
    if 'Rank_Score' in df.columns:
        df['rank_bins'] = pd.qcut(df['Rank_Score'], q=5, duplicates='drop')
        rank_analysis = df.groupby('rank_bins')['is_hit'].mean()
        print("\n[+] Win Rate by Rank Score:")
        print(rank_analysis)

    # 7. L5 vs Line
    if 'L5_Avg' in df.columns and 'Line' in df.columns:
        df['L5_vs_Line'] = df['L5_Avg'] - df['Line']
        df['l5_bins'] = pd.cut(df['L5_vs_Line'], bins=5)
        l5_analysis = df.groupby('l5_bins')['is_hit'].mean()
        print("\n[+] Win Rate by L5 vs Line:")
        print(l5_analysis)

    # 8. Season vs Line
    if 'Season_Avg' in df.columns and 'Line' in df.columns:
        df['Season_vs_Line'] = df['Season_Avg'] - df['Line']
        df['season_bins'] = pd.cut(df['Season_vs_Line'], bins=5)
        season_analysis = df.groupby('season_bins')['is_hit'].mean()
        print("\n[+] Win Rate by Season Avg vs Line:")
        print(season_analysis)

    # 9. Insight
    if len(correlations) > 1:
        top_feature = correlations.index[1]
        print(f"\n💡 MOST PREDICTIVE FEATURE: {top_feature}")
    else:
        print("\n💡 Not enough data for correlation insight yet.")

    print("\n✅ Audit Complete\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", default="nba", help="nba, cbb, nhl, soccer")
    parser.add_argument("--date", help="YYYY-MM-DD (optional)")
    parser.add_argument("--file", help="Direct file path (optional override)")

    args = parser.parse_args()

    base_path = "outputs"

    if args.file:
        file_to_analyze = args.file
    elif args.date:
        file_to_analyze = os.path.join(base_path, args.date, f"graded_{args.sport}_{args.date}.xlsx")
    else:
        print("❌ Provide --date or --file")
        exit()

    print(f"\nAnalyzing: {file_to_analyze}")
    analyze_performance(file_to_analyze)