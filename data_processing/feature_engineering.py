import argparse
import sys
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"
PLOTS_DIR    = PROJECT_ROOT / "analysis" / "plots" / "features"

INPUT_CSV  = PROCESSED / "merged_dataset.csv"
OUTPUT_CSV = PROCESSED / "features_dataset.csv"
REPORT_TXT = PROCESSED / "feature_engineering_report.txt"

PAL = {
    'navy':   '#003f5c', 'blue':   '#2f4b7c', 'coral': '#f95d6a',
    'orange': '#ff7c43', 'green':  '#2ecc71', 'gray':  '#95a5a6',
    'yellow': '#ffa600', 'purple': '#665191',
}

VISIBILITY_THR = 5.0
WINDSPEED_THR  = 15.0
FREEZING_THR   = 0.0

COLS_TO_DROP = {
    'word_count':          'collinear with isw_report_length (r≈0.99)',
    'sentence_count':      'ISW structural artifact, no predictive value',
    'paragraph_count':     'ISW structural artifact, no predictive value',
    'avg_sentence_length': 'ISW structural artifact, no predictive value',
    'sources_resolved':    'sub-component of isw_sources_count — redundant',
    'sources_dead':        'sub-component of isw_sources_count — redundant',
    'sources_blocked':     'sub-component of isw_sources_count — redundant',
}

def load_merged() -> pd.DataFrame:
    print("=" * 65)
    print("STEP 1/4: Load merged dataset")
    print("=" * 65)
    if not INPUT_CSV.exists():
        print(f"ERROR: {INPUT_CSV} not found")
        print("Run: python data_processing/merge_datasets.py --merge")
        sys.exit(1)
    df = pd.read_csv(INPUT_CSV)
    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'])
    df = df.sort_values(['region', 'datetime_hour']).reset_index(drop=True)
    print(f"  shape:      {df.shape}")
    print(f"  regions:    {df.region.nunique()}")
    print(f"  range:      {df.datetime_hour.min()} -> {df.datetime_hour.max()}")
    print(f"  alarm rate: {df.alarm.mean() * 100:.2f}%")
    drop_existing = [c for c in COLS_TO_DROP if c in df.columns]
    if drop_existing:
        df = df.drop(columns=drop_existing)
        print(f"\n  Dropped {len(drop_existing)} redundant columns:")
        for c in drop_existing:
            print(f"    {c:30s} — {COLS_TO_DROP[c]}")
    return df

def _check_time_gaps(df: pd.DataFrame) -> None:
    gaps_found = 0
    for region, grp in df.groupby('region'):
        expected = pd.date_range(
            start=grp['datetime_hour'].min(),
            end=grp['datetime_hour'].max(),
            freq='h',
        )
        actual  = set(grp['datetime_hour'])
        missing = [h for h in expected if h not in actual]
        if missing:
            gaps_found += len(missing)
            print(f"  WARNING: {region} — {len(missing)} missing hours (first: {missing[0]})")
    if gaps_found == 0:
        print("  time gap check: no missing hours — shift() lags are exact")
    else:
        print(f"  WARNING: {gaps_found} total missing hours across all regions")
        print("  lag features may be slightly off for rows adjacent to gaps")

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 65)
    print("  STEP 2/4: Feature engineering")
    print("=" * 65)
    _check_time_gaps(df)
    if 'hour_temp' in df.columns and 'hour_feelslike' in df.columns:
        df['temp_diff_feels'] = (df['hour_temp'] - df['hour_feelslike']).astype(np.float32)
        df = df.drop(columns=['hour_feelslike'])
        print("  temp_diff_feels created (hour_temp - hour_feelslike)")
    elif 'hour_temp' in df.columns:
        print("  temp_diff_feels SKIP (hour_feelslike not in data)")
    for lag in [1, 3, 6, 24]:
        df[f'alarm_lag_{lag}h'] = df.groupby('region')['alarm'].shift(lag)
    print("  alarm_lag_1h/3h/6h/24h created")
    df['alarms_last_24h'] = (
        df.groupby('region')['alarm']
        .transform(lambda x: x.shift(1).rolling(window=24, min_periods=1).sum())
    )
    print("  alarms_last_24h created")
    if 'n_regions_alarm' in df.columns:
        df['n_regions_alarm_lag_1h'] = df.groupby('region')['n_regions_alarm'].shift(1).fillna(0).astype(np.int8)
        df['n_regions_alarm_lag_2h'] = df.groupby('region')['n_regions_alarm'].shift(2).fillna(0).astype(np.int8)
        print("  n_regions_alarm_lag_1h/2h created")
    if 'n_regions_alarm' in df.columns and 'n_regions_alarm_lag_1h' in df.columns:
        df['n_regions_alarm_momentum'] = (
            df['n_regions_alarm_lag_1h'].astype(np.int16) -
            df['n_regions_alarm_lag_2h'].astype(np.int16)
        ).astype(np.int8)
        print("  n_regions_alarm_momentum created")
    lag_cols   = [c for c in df.columns if 'alarm_lag' in c]
    last24_col = ['alarms_last_24h'] if 'alarms_last_24h' in df.columns else []
    nan_before = df[lag_cols + last24_col].isnull().sum().sum()
    for c in lag_cols:
        df[c] = df[c].fillna(0).astype(np.int8)
    for c in last24_col:
        df[c] = df[c].fillna(0).astype(np.int16)
    print(f"  NaN in lags filled: {nan_before:,} -> 0")
    hour  = df['datetime_hour'].dt.hour
    month = df['datetime_hour'].dt.month
    df['hour_sin']  = np.sin(2 * np.pi * hour  / 24).astype(np.float32)
    df['hour_cos']  = np.cos(2 * np.pi * hour  / 24).astype(np.float32)
    df['month_sin'] = np.sin(2 * np.pi * month / 12).astype(np.float32)
    df['month_cos'] = np.cos(2 * np.pi * month / 12).astype(np.float32)
    print("  hour_sin/cos, month_sin/cos created")
    df['is_weekend'] = (df['datetime_hour'].dt.dayofweek >= 5).astype(np.int8)
    print(f"  is_weekend: {df['is_weekend'].mean() * 100:.1f}% of hours")
    if 'hour_visibility' in df.columns:
        df['hour_visibility'] = df['hour_visibility'].fillna(df['hour_visibility'].median())
        df['low_visibility'] = (df['hour_visibility'] < VISIBILITY_THR).astype(np.int8)
        print(f"  low_visibility (<{VISIBILITY_THR}km): {df['low_visibility'].mean() * 100:.1f}%")
        df = df.drop(columns=['hour_visibility'])
    if 'hour_windspeed' in df.columns:
        df['strong_wind'] = (df['hour_windspeed'] > WINDSPEED_THR).astype(np.int8)
        print(f"  strong_wind (>{WINDSPEED_THR}m/s): {df['strong_wind'].mean() * 100:.1f}%")
    if 'hour_temp' in df.columns:
        df['freezing'] = (df['hour_temp'] < FREEZING_THR).astype(np.int8)
        print(f"  freezing (<{FREEZING_THR}C): {df['freezing'].mean() * 100:.1f}%")
    bad_idx = pd.Series(0, index=df.index, dtype=np.int8)
    if 'hour_precip' in df.columns:
        bad_idx = bad_idx + (df['hour_precip'].fillna(0) > 0).astype(np.int8)
    if 'low_visibility' in df.columns:
        bad_idx = bad_idx + df['low_visibility']
    if 'strong_wind' in df.columns:
        bad_idx = bad_idx + df['strong_wind']
    df['bad_weather_index'] = bad_idx.astype(np.int8)
    print(f"  bad_weather_index (0-3): mean={df['bad_weather_index'].mean():.2f}")
    if 'freezing' in df.columns and 'is_night' in df.columns:
        df['energy_infra_stress'] = (df['freezing'] & df['is_night']).astype(np.int8)
        print(f"  energy_infra_stress: {df['energy_infra_stress'].mean() * 100:.1f}%")
    if 'hour_temp' in df.columns:
        temp_72h_ago = df.groupby('region')['hour_temp'].shift(72)
        df['temp_drop_last_3d'] = (df['hour_temp'] - temp_72h_ago).astype(np.float32)
        nan_t = df['temp_drop_last_3d'].isna().sum()
        df['temp_drop_last_3d'] = df['temp_drop_last_3d'].fillna(0)
        print(f"  temp_drop_last_3d: {nan_t:,} NaN -> 0")
    if 'total_intensity' in df.columns:
        intensity_24h_ago = df.groupby('region')['total_intensity'].shift(24)
        df['isw_intensity_growth'] = (df['total_intensity'] - intensity_24h_ago).astype(np.float32)
        nan_i = df['isw_intensity_growth'].isna().sum()
        df['isw_intensity_growth'] = df['isw_intensity_growth'].fillna(0)
        print(f"  isw_intensity_growth: {nan_i:,} NaN -> 0")
    if 'n_regions_alarm' in df.columns:
        over25 = (df['n_regions_alarm'] > 25).sum()
        if over25:
            print(f"  WARNING: {over25} rows n_regions_alarm > 25 — capping at 25")
            df['n_regions_alarm'] = df['n_regions_alarm'].clip(upper=25).astype(np.int8)
    region_dummies = pd.get_dummies(df['region'], prefix='region', drop_first=True, dtype=np.int8)
    df = pd.concat([df, region_dummies], axis=1)
    print(f"  region OHE: {region_dummies.shape[1]} binary columns added")
    print(f"\n  Total columns after FE: {len(df.columns)}")
    return df

def plot_fe1(df: pd.DataFrame) -> None:
    print("\n" + "=" * 65)
    print("STEP 3/4: Figure FE1 — Feature Predictive Power")
    print("=" * 65)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    df_plot   = df.copy()
    base_rate = df_plot['alarm'].mean()
    binary_features = [
        ('alarm_lag_1h',        'Alarm Lag 1h'),
        ('alarm_lag_3h',        'Alarm Lag 3h'),
        ('alarm_lag_6h',        'Alarm Lag 6h'),
        ('alarm_lag_24h',       'Alarm Lag 24h'),
        ('is_night',            'Night Time (22:00-05:00)'),
        ('is_weekend',          'Weekend (Sat/Sun)'),
        ('low_visibility',      'Low Visibility (<5km)'),
        ('strong_wind',         'Strong Wind (>15 m/s)'),
        ('freezing',            'Freezing (<0C)'),
        ('energy_infra_stress', 'Energy Stress (Freeze x Night)'),
        ('is_rain',             'Rain Condition'),
        ('is_snow',             'Snow Condition'),
    ]
    available = [(f, lbl) for f, lbl in binary_features if f in df_plot.columns]
    n_plots = min(len(available), 12)
    n_cols  = 4
    n_rows  = (n_plots + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 9.5),
                             gridspec_kw={'hspace': 0.45, 'wspace': 0.25}, facecolor='white')
    axes = np.array(axes).flatten()
    lift_rows = []
    for idx, (feat, lbl) in enumerate(available[:n_plots]):
        ax     = axes[idx]
        values = df_plot[feat].dropna().unique()
        p0   = df_plot[df_plot[feat] == 0]['alarm'].mean() if 0 in values else 0.0
        p1   = df_plot[df_plot[feat] == 1]['alarm'].mean() if 1 in values else 0.0
        lift = p1 / base_rate if base_rate > 0 else 0.0
        bar_color = PAL['coral'] if lift > 1.3 else (PAL['blue'] if lift < 0.85 else '#5c7b9c')
        ax.bar([0, 1], [p0 * 100, p1 * 100],
               color=[PAL['gray'], bar_color], alpha=0.9, width=0.45, edgecolor='none')
        ax.axhline(y=base_rate * 100, color=PAL['orange'], linestyle='--', linewidth=1.5, zorder=0)
        lift_clr = PAL['coral'] if lift > 1.3 else (PAL['gray'] if lift < 0.85 else PAL['navy'])
        ax.text(0.95, 0.92, f'Lift: {lift:.2f}x',
                transform=ax.transAxes, ha='right', va='top',
                fontsize=10, fontweight='bold', color=lift_clr,
                bbox=dict(boxstyle='square,pad=0.3', facecolor='white',
                          edgecolor=lift_clr, alpha=0.9, linewidth=1))
        ax.set_title(lbl, fontsize=11, fontweight='bold', pad=10, color='#333333')
        ax.set_xticks([0, 1])
        ax.set_xticklabels(['False (=0)', 'True (=1)'], fontsize=9.5)
        if idx % n_cols == 0:
            ax.set_ylabel('P(Alarm) %', fontsize=9.5, fontweight='bold', color='#555555')
            ax.spines['left'].set_color('#cccccc')
        else:
            ax.set_ylabel('')
            ax.spines['left'].set_visible(False)
            ax.tick_params(axis='y', left=False, labelleft=False)
        ax.set_ylim(0, 70)
        ax.grid(axis='y', alpha=0.15, linestyle='--')
        ax.spines[['top', 'right']].set_visible(False)
        ax.spines['bottom'].set_color('#cccccc')
        lift_rows.append({'Feature': feat, 'P(=1)%': round(p1 * 100, 2),
                          'P(=0)%': round(p0 * 100, 2), 'Lift': round(lift, 3)})
    for idx in range(n_plots, len(axes)):
        axes[idx].set_visible(False)
    plt.suptitle('Figure FE1. Predictive Power of Engineered Binary Features',
                 fontsize=18, fontweight='bold', y=1.06, color='#111111')
    fig.text(0.5, 0.99,
             f"Bars show P(alarm) when feature is False (grey) vs True (coloured).\n"
             f"Dashed orange line = global baseline alarm rate ({base_rate * 100:.1f}%).",
             ha='center', va='top', fontsize=12, color='dimgrey')
    fig.subplots_adjust(top=0.85)
    out = PLOTS_DIR / 'FE1_feature_power.png'
    fig.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  Saved: {out}")
    lift_df = pd.DataFrame(lift_rows).sort_values('Lift', ascending=False)
    print(f"\n  Lift table (base rate = {base_rate * 100:.2f}%):")
    print(f"  {'Feature':30s}  {'P(=1)%':>8s}  {'P(=0)%':>8s}  {'Lift':>6s}")
    print(f"  {'-' * 58}")
    for _, row in lift_df.iterrows():
        flag = '  <- STRONG' if row['Lift'] > 1.5 else ('  confound' if 'visibility' in row['Feature'] else '')
        print(f"  {row['Feature']:30s}  {row['P(=1)%']:>8.2f}  {row['P(=0)%']:>8.2f}  {row['Lift']:>6.3f}x{flag}")

def validate_and_save(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> None:
    print("\n" + "=" * 65)
    print("STEP 4/4: Validate & Save")
    print("=" * 65)
    issues = []
    expected_features = [
        'alarm_lag_1h', 'alarm_lag_3h', 'alarm_lag_6h', 'alarm_lag_24h',
        'alarms_last_24h', 'n_regions_alarm', 'n_regions_alarm_lag_1h',
        'n_regions_alarm_momentum', 'is_night', 'is_weekend',
        'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
        'low_visibility', 'strong_wind', 'freezing',
        'bad_weather_index', 'energy_infra_stress',
        'temp_drop_last_3d', 'isw_intensity_growth',
        'isw_report_length', 'isw_sources_count', 'intensity_per_1000',
        'temp_diff_feels',
        'real_dead_ratio', 'blackout_score', 'ru_ua_balance', 'ru_official_ratio',
    ]
    for f in expected_features:
        if f not in df.columns:
            issues.append(f"MISSING feature: {f}")
    for c in COLS_TO_DROP:
        if c in df.columns:
            issues.append(f"Column '{c}' should be dropped but is still present")
    region_ohe_cols = [c for c in df.columns if c.startswith('region_')]
    if len(region_ohe_cols) < 20:
        issues.append(f"Only {len(region_ohe_cols)} region OHE columns (expected ~23)")
    else:
        print(f"  region OHE cols: {len(region_ohe_cols)}")
    for col in ['region', 'datetime_hour', 'alarm', 'n_regions_alarm']:
        if col in df.columns:
            n = df[col].isna().sum()
            if n:
                issues.append(f"NaN in critical column '{col}': {n}")
    tfidf_cols = [c for c in df.columns if c.startswith('tfidf_')]
    df_scalar  = df.drop(columns=tfidf_cols + region_ohe_cols, errors='ignore')
    scalar_nan = df_scalar.isnull().sum()
    scalar_nan = scalar_nan[scalar_nan > 0]
    if len(scalar_nan):
        print("  NaN in scalar columns (excl TF-IDF/OHE):")
        print(scalar_nan.to_string())
        for col, cnt in scalar_nan.items():
            issues.append(f"NaN in '{col}': {cnt}")
    else:
        print("  NaN in scalar columns: none")
    for lag in [1, 3, 6, 24]:
        col = f'alarm_lag_{lag}h'
        if col in df.columns and df[col].dtype != np.int8:
            issues.append(f"{col} dtype is {df[col].dtype}, expected int8")
    for col in ('real_dead_ratio', 'blackout_score', 'ru_ua_balance', 'ru_official_ratio'):
        if col in df.columns and df[col].sum() == 0:
            issues.append(f"All-zero in '{col}' — check isw_nlp_pipeline.py output")
    train      = df[df["datetime_hour"] < train_cutoff]
    test       = df[df["datetime_hour"] >= train_cutoff]
    alarm_rate = df['alarm'].mean() * 100
    print(f"  shape:       {df.shape}")
    print(f"  TF-IDF cols: {len(tfidf_cols)}")
    print(f"  scalar cols: {len(df.columns) - len(tfidf_cols) - len(region_ohe_cols)}")
    print(f"  alarm rate:  {alarm_rate:.2f}%")
    print(f"  train:       {len(train):,} rows (<  {train_cutoff.date()})")
    print(f"  test:        {len(test):,} rows (>= {train_cutoff.date()})")
    if len(test) == 0:
        issues.append("TEST set is empty — check data date range vs TRAIN_CUTOFF")
    if not (5 <= alarm_rate <= 50):
        issues.append(f"Alarm rate {alarm_rate:.2f}% outside expected 5-50%")
    if 'n_regions_alarm' in df.columns:
        max_n  = df['n_regions_alarm'].max()
        mass_h = (df['n_regions_alarm'] > 15).sum()
        if max_n > 25:
            issues.append(f"n_regions_alarm max={max_n} > 25")
        print(f"  n_regions_alarm max={max_n}  mass_attack_hours(>15)={mass_h:,}")
    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for iss in issues:
            print(f"    {iss}")
    else:
        print("\n  All validation checks passed.")
    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n  saved: {OUTPUT_CSV}  {df.shape}")
    added_features = [
        'alarm_lag_1h', 'alarm_lag_3h', 'alarm_lag_6h', 'alarm_lag_24h',
        'alarms_last_24h', 'n_regions_alarm_lag_1h', 'n_regions_alarm_momentum',
        'is_weekend', 'isw_intensity_growth', 'bad_weather_index',
        'energy_infra_stress', 'temp_diff_feels',
        'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
        'low_visibility', 'strong_wind', 'freezing', 'temp_drop_last_3d',
    ]
    model_cols = [c for c in df.columns if c not in ('region', 'datetime_hour', 'alarm')]
    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write("FEATURE ENGINEERING REPORT\n" + "=" * 60 + "\n\n")
        f.write(f"Input:          {INPUT_CSV}\n")
        f.write(f"Output:         {OUTPUT_CSV}\n")
        f.write(f"Shape:          {df.shape}\n")
        f.write(f"Train rows:     {len(train):,}\n")
        f.write(f"Test rows:      {len(test):,}\n")
        f.write(f"Train cutoff:   {train_cutoff.date()}\n")
        f.write(f"Alarm rate:     {alarm_rate:.2f}%\n")
        f.write(f"TF-IDF cols:    {len(tfidf_cols)}\n")
        f.write(f"Region OHE:     {len(region_ohe_cols)}\n")
        f.write(f"Total features: {len(model_cols)}\n")
        f.write(f"Issues:         {len(issues)}\n\n")
        f.write("Dropped columns (reason):\n")
        for c, reason in COLS_TO_DROP.items():
            f.write(f"  {c:30s} — {reason}\n")
        f.write("\nAdded features:\n")
        for c in added_features:
            status = "OK" if c in df.columns else "MISSING"
            f.write(f"  {c:30s} {status}\n")
        f.write("\nAll columns (dtype, NaN count):\n")
        for i, c in enumerate(df.columns, 1):
            nans = df[c].isna().sum()
            f.write(f"  {i:>3}. {c:40s} {str(df[c].dtype):10s} NaN:{nans:>8,}\n")
        if issues:
            f.write("\nIssues:\n")
            for iss in issues:
                f.write(f"  {iss}\n")
    print(f"  saved: {REPORT_TXT}")

def build() -> None:
    df = load_merged()
    df = add_features(df)
    real_data_end = df[df['n_regions_alarm'] > 0]['datetime_hour'].max()
    if pd.isna(real_data_end):
        real_data_end = df["datetime_hour"].max()
    train_cutoff = real_data_end.floor("D") - pd.Timedelta(days=30)
    plot_fe1(df)
    validate_and_save(df, train_cutoff)
    print("\n" + "=" * 65)
    print("FEATURE ENGINEERING COMPLETE")
    print("=" * 65)
    print(f"  Output:       {OUTPUT_CSV}")
    print(f"  TRAIN_CUTOFF: {train_cutoff.date()} (must match train_models.py)")
    print()
    print("  NOTE: 'region' column kept as metadata.")
    print("  train_models.py must exclude ['region','datetime_hour','alarm'] from X.")
    print()
    print("  Next: python models/train_models.py --train")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--build", action="store_true", help="Run feature engineering pipeline")
    args = parser.parse_args()
    if args.build:
        build()
    else:
        parser.print_help()