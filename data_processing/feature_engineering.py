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
    'navy': '#003f5c', 'blue': '#2f4b7c', 'coral': '#f95d6a',
    'orange': '#ff7c43', 'green': '#2ecc71', 'gray': '#95a5a6',
}

VISIBILITY_THR = 5.0
WINDSPEED_THR  = 15.0
FREEZING_THR   = 0.0

N_REGIONS_EXPECTED = 23

COLS_TO_DROP = {
    'word_count':          'collinear with isw_report_length (r≈0.99)',
    'sentence_count':      'ISW structural artifact, no predictive value',
    'paragraph_count':     'ISW structural artifact, no predictive value',
    'avg_sentence_length': 'ISW structural artifact, no predictive value',
    'sources_resolved':    'sub-component of isw_sources_count',
    'sources_dead':        'sub-component of isw_sources_count',
    'sources_blocked':     'sub-component of isw_sources_count',
}

def load_merged() -> pd.DataFrame:
    print("=" * 75)
    print("STEP 1/5: Load merged dataset")
    print("=" * 75)

    if not INPUT_CSV.exists():
        print(f"   FATAL: {INPUT_CSV} not found")
        print("   Run: python data_processing/merge_datasets.py --merge")
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, low_memory=False)
    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'], utc=False, errors='coerce')

    bad_dt = df['datetime_hour'].isna().sum()
    if bad_dt:
        print(f"     {bad_dt} bad datetime rows -> dropped")
        df = df.dropna(subset=['datetime_hour'])

    df = df.drop_duplicates(subset=['region', 'datetime_hour'], keep='last')
    df = df.sort_values(['region', 'datetime_hour']).reset_index(drop=True)

    print(f"     Loaded:   {df.shape}")
    print(f"     Regions: {df.region.nunique()}")
    print(f"     Range:   {df.datetime_hour.min().date()}  -> {df.datetime_hour.max().date()}")
    print(f"     Alarm:   {df.alarm.mean()*100:.2f}%")

    drop_existing = [c for c in COLS_TO_DROP if c in df.columns]
    if drop_existing:
        df = df.drop(columns=drop_existing)
        print(f"\n  [−] Dropped {len(drop_existing)} redundant cols:")
        for c in drop_existing:
            print(f"      {c}: {COLS_TO_DROP[c]}")

    return df

def _check_time_gaps(df: pd.DataFrame) -> None:
    gaps = 0
    for region, grp in df.groupby('region'):
        expected = pd.date_range(
            grp['datetime_hour'].min(),
            grp['datetime_hour'].max(),
            freq='h',
        )
        actual  = set(grp['datetime_hour'])
        missing = [h for h in expected if h not in actual]
        if missing:
            gaps += len(missing)
            if gaps <= 5:
                print(f"     {region}: {len(missing)} missing hours")
    if gaps == 0:
        print("     No time gaps — lag features are exact")
    else:
        print(f"     Total {gaps} gaps — lag features may be slightly off near gaps")

def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 2/5: Add lag & rolling features")
    print("=" * 75)
    _check_time_gaps(df)

    for lag in [1, 3, 6, 24]:
        df[f'alarm_lag_{lag}h'] = (
            df.groupby('region')['alarm']
            .shift(lag)
            .fillna(0)
            .astype(np.int8)
        )
    print("  [+] alarm_lag_1h / 3h / 6h / 24h")

    df['alarms_last_24h'] = (
        df.groupby('region')['alarm']
        .transform(lambda x: x.shift(1).rolling(window=24, min_periods=1).sum())
        .fillna(0)
        .astype(np.int16)
    )
    print("  [+] alarms_last_24h  (rolling 24-h sum, shift-1 applied first)")

    if 'n_regions_alarm' in df.columns:
        df['n_regions_lag_1h'] = (
            df.groupby('region')['n_regions_alarm']
            .shift(1)
            .fillna(0)
            .astype(np.int8)
        )
        df['n_regions_lag_3h'] = (
            df.groupby('region')['n_regions_alarm']
            .shift(3)
            .fillna(0)
            .astype(np.int8)
        )
        df['n_regions_momentum'] = (
            df['n_regions_lag_1h'].astype(np.int16)
            - df['n_regions_lag_3h'].astype(np.int16)
        ).astype(np.int8)
        print("  [+] n_regions_lag_1h / 3h + momentum")

        df = df.drop(columns=['n_regions_alarm'])
        print("  [−] n_regions_alarm DROPPED (target leakage — "
              "= sum of alarm across all regions at prediction hour)")
        print("      Model sees only n_regions_lag_1h / 3h / momentum (past data)")
    else:
        if 'n_regions_lag_1h' not in df.columns:
            print("     n_regions_alarm missing and no lag cols found — "
                  "check merge_datasets.py")
        else:
            if 'n_regions_momentum' not in df.columns and 'n_regions_lag_3h' in df.columns:
                df['n_regions_momentum'] = (
                    df['n_regions_lag_1h'].astype(np.int16)
                    - df['n_regions_lag_3h'].astype(np.int16)
                ).astype(np.int8)
            print("  [+] n_regions lags already present from merge step")

    if 'total_intensity' in df.columns:
        df['isw_intensity_growth_1d'] = (
            df.groupby('region')['total_intensity']
            .transform(lambda x: x - x.shift(24))
            .fillna(0)
            .astype(np.float32)
        )
        df['isw_intensity_growth_7d'] = (
            df.groupby('region')['total_intensity']
            .transform(lambda x: x - x.shift(168))
            .fillna(0)
            .astype(np.float32)
        )
        print("  [+] isw_intensity_growth_1d / 7d  (daily diff, no bogus rolling)")

    lag_count = len([c for c in df.columns if 'lag' in c or 'last_24h' in c
                     or 'momentum' in c or 'growth' in c])
    print(f"\n     {lag_count} lag/rolling columns total")
    return df

def add_temporal_features(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 3/5: Add temporal & weather features")
    print("=" * 75)

    train_mask = df['datetime_hour'] < train_cutoff

    hour  = df['datetime_hour'].dt.hour
    month = df['datetime_hour'].dt.month
    dow   = df['datetime_hour'].dt.dayofweek

    df['hour_sin']  = np.sin(2 * np.pi * hour  / 24).astype(np.float32)
    df['hour_cos']  = np.cos(2 * np.pi * hour  / 24).astype(np.float32)
    df['month_sin'] = np.sin(2 * np.pi * month / 12).astype(np.float32)
    df['month_cos'] = np.cos(2 * np.pi * month / 12).astype(np.float32)
    df['dow_sin']   = np.sin(2 * np.pi * dow   /  7).astype(np.float32)
    df['dow_cos']   = np.cos(2 * np.pi * dow   /  7).astype(np.float32)
    print("  [+] Circular: hour / month / dayofweek sin-cos")

    df['is_weekend']   = (dow >= 5).astype(np.int8)
    df['is_morning']   = ((hour >= 6)  & (hour < 12)).astype(np.int8)
    df['is_afternoon'] = ((hour >= 12) & (hour < 18)).astype(np.int8)
    df['is_evening']   = ((hour >= 18) & (hour < 24)).astype(np.int8)
    df['is_night']     = (hour < 6).astype(np.int8)
    print("  [+] Binary temporal: is_weekend, is_night/morning/afternoon/evening")

    if 'hour_visibility' in df.columns:
        vis_median_train = df.loc[train_mask, 'hour_visibility'].median()
        df['hour_visibility'] = df['hour_visibility'].fillna(vis_median_train)
        print(f"  [i] visibility median (train-only): {vis_median_train:.2f} km")
        df['low_visibility'] = (df['hour_visibility'] < VISIBILITY_THR).astype(np.int8)
        print(f"  [+] low_visibility (<{VISIBILITY_THR} km): "
              f"{df['low_visibility'].mean()*100:.1f}%")

    if 'hour_windspeed' in df.columns:
        df['strong_wind'] = (df['hour_windspeed'] > WINDSPEED_THR).astype(np.int8)
        print(f"  [+] strong_wind (>{WINDSPEED_THR} m/s): "
              f"{df['strong_wind'].mean()*100:.1f}%")

    if 'hour_temp' in df.columns:
        df['freezing'] = (df['hour_temp'] < FREEZING_THR).astype(np.int8)
        print(f"  [+] freezing (<{FREEZING_THR}°C): "
              f"{df['freezing'].mean()*100:.1f}%")

    bwi = pd.Series(0, index=df.index, dtype=np.int16)
    if 'hour_precip'    in df.columns:
        bwi += (df['hour_precip'].fillna(0) > 0).astype(np.int16)
    if 'low_visibility' in df.columns:
        bwi += df['low_visibility'].astype(np.int16)
    if 'strong_wind'    in df.columns:
        bwi += df['strong_wind'].astype(np.int16)
    df['bad_weather_index'] = bwi.clip(0, 3).astype(np.int8)
    print(f"  [+] bad_weather_index (0-3): mean={df['bad_weather_index'].mean():.2f}")

    if 'freezing' in df.columns and 'is_night' in df.columns:
        df['energy_stress'] = (
            df['freezing'].astype(bool) & df['is_night'].astype(bool)
        ).astype(np.int8)
        print(f"  [+] energy_stress (freezing & night): "
              f"{df['energy_stress'].mean()*100:.1f}%")

    if 'hour_temp' in df.columns:
        df['temp_72h_change'] = (
            df.groupby('region')['hour_temp']
            .transform(lambda x: x - x.shift(72))
            .fillna(0)
            .astype(np.float32)
        )
        print("  [+] temp_72h_change (3-day temp trend, grouped)")

    if 'hour_temp' in df.columns and 'hour_feelslike' in df.columns:
        df['temp_feels_diff'] = (
            df['hour_temp'] - df['hour_feelslike']
        ).astype(np.float32)
        print("  [+] temp_feels_diff (temp - feelslike)")

    print(f"\n     Temporal & weather features added")
    return df

def add_interaction_features(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 4/5: Add interaction features")
    print("=" * 75)

    train_mask = df['datetime_hour'] < train_cutoff

    added = []

    def _inter(name: str, a: pd.Series, b: pd.Series) -> None:
        df[name] = (a.astype(bool) & b.astype(bool)).astype(np.int8)
        added.append(name)

    if 'bad_weather_index' in df.columns and 'is_night' in df.columns:
        _inter('inter_bad_weather_night',
               df['bad_weather_index'] > 0, df['is_night'].astype(bool))
    if 'alarm_lag_1h' in df.columns and 'n_regions_lag_1h' in df.columns:
        _inter('inter_alarm_spreading',
               df['alarm_lag_1h'].astype(bool), df['n_regions_lag_1h'] > 0)
    elif 'alarm_lag_1h' in df.columns and 'n_regions_alarm' in df.columns:
        print("     LEAKAGE WARNING: n_regions_lag_1h missing, "
              "inter_alarm_spreading skipped to prevent target leakage")
    if 'intensity_per_1000' in df.columns and 'is_frontline' in df.columns:
        q75_intensity = df.loc[train_mask, 'intensity_per_1000'].quantile(0.75)
        print(f"  [i] intensity_per_1000 q75 (train-only): {q75_intensity:.4f}")
        high_isw = df['intensity_per_1000'] > q75_intensity
        _inter('inter_high_intensity_frontline', high_isw, df['is_frontline'].astype(bool))

    if 'alarms_last_24h' in df.columns and 'low_visibility' in df.columns:
        _inter('inter_recent_alarm_low_vis',
               df['alarms_last_24h'] > 2, df['low_visibility'].astype(bool))

    print(f"     {len(added)} interaction features:")
    for name in added:
        n = int((df[name] > 0).sum())
        print(f"     {name:<45s} {n:>8,} activations")

    return df

def ohe_regions(df: pd.DataFrame) -> pd.DataFrame:
    if 'region' not in df.columns:
        print("     'region' column not found — OHE skipped")
        return df

    region_dummies = pd.get_dummies(
        df['region'], prefix='region', drop_first=False, dtype=np.int8
    )
    df = pd.concat([df, region_dummies], axis=1)
    df = df.drop(columns=['region'])

    print(f"  [+] region OHE: {region_dummies.shape[1]} binary columns added")
    print("  [−] 'region' text column dropped (models require numeric input)")
    return df

def validate_and_save(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> None:
    print("\n" + "=" * 75)
    print("STEP 5/5: Validate & Save")
    print("=" * 75)

    issues = []

    if 'region' in df.columns:
        issues.append(
            "FATAL: 'region' text column still present after OHE — "
            "model will crash (string dtype)"
        )

    if 'n_regions_alarm' in df.columns:
        issues.append(
            "FATAL: 'n_regions_alarm' still present — TARGET LEAKAGE. "
            "Drop it in add_lag_features() after computing n_regions_lag_*"
        )

    for col in ['datetime_hour', 'alarm', 'n_regions_lag_1h', 'is_frontline']:
        if col not in df.columns:
            issues.append(f"MISSING critical: {col}")
        else:
            nans = df[col].isna().sum()
            if nans:
                issues.append(f"NaN in '{col}': {nans}")

    region_ohe_cols = [c for c in df.columns if c.startswith('region_')]
    if len(region_ohe_cols) < N_REGIONS_EXPECTED:
        issues.append(
            f"Only {len(region_ohe_cols)} region OHE cols "
            f"(expected ≥ {N_REGIONS_EXPECTED} with drop_first=True)"
        )
    else:
        print(f"  region OHE cols: {len(region_ohe_cols)}   ")

    for c in COLS_TO_DROP:
        if c in df.columns:
            issues.append(f"Redundant col still present: '{c}'")

    obj_cols = [c for c in df.columns
                if df[c].dtype == object and c != 'datetime_hour']
    if obj_cols:
        issues.append(f"String dtype cols (model crash): {obj_cols}")

    lag_cols = [c for c in df.columns if 'alarm_lag' in c]
    for col in lag_cols:
        if df[col].dtype not in [np.int8, np.int16]:
            issues.append(f"{col} dtype={df[col].dtype}, expected int8/int16")

    alarm_rate = df['alarm'].mean() * 100
    if not (2 <= alarm_rate <= 50):
        issues.append(f"alarm_rate {alarm_rate:.2f}% outside [2%, 50%]")

    if region_ohe_cols:
        dupe_key = ['datetime_hour'] + region_ohe_cols
    else:
        dupe_key = ['datetime_hour']
    dupes = df.duplicated(subset=dupe_key).sum()
    if dupes:
        issues.append(f"DUPLICATES (datetime_hour + region OHE): {dupes}")

    for col in ('real_dead_ratio', 'blackout_score', 'ru_ua_balance'):
        if col in df.columns and df[col].sum() == 0:
            issues.append(f"All-zero in '{col}' — check isw_nlp_pipeline.py")

    train = df[df['datetime_hour'] < train_cutoff]
    test  = df[df['datetime_hour'] >= train_cutoff]
    if len(test) == 0:
        issues.append("TEST set is empty")
    if len(train) < 500:
        issues.append(f"TRAIN too small: {len(train)}")

    tfidf_cols   = [c for c in df.columns if c.startswith('tfidf_')]
    synergy_cols = [c for c in df.columns if c.startswith('syn_')]
    lag_all      = [c for c in df.columns if 'lag' in c or 'last_24h' in c
                    or 'momentum' in c or 'growth' in c or 'roll' in c]
    ext_cols     = [c for c in df.columns if c.startswith(('gur_', 'tg_'))]
    zero_syn     = [s for s in synergy_cols if df[s].sum() == 0]

    print(f"  Shape:         {df.shape}")
    print(f"  TF-IDF:        {len(tfidf_cols)}")
    print(f"  Synergy:       {len(synergy_cols)} total | "
          f"with activations: {len(synergy_cols) - len(zero_syn)} | "
          f"zero: {len(zero_syn)}")
    print(f"  Lag/Roll:      {len(lag_all)}")
    print(f"  External:      {len(ext_cols)}")
    print(f"  Region OHE:    {len(region_ohe_cols)}")
    print(f"  Alarm rate:    {alarm_rate:.2f}%")
    print(f"  Train:         {len(train):,} | Test: {len(test):,}")

    if zero_syn:
        issues.append(
            f"Zero-activation synergies ({len(zero_syn)}): {zero_syn[:5]}"
            f" — check merge_datasets.py step order"
        )

    if issues:
        print(f"\n     Issues ({len(issues)}):")
        for iss in issues:
            print(f"       {iss}")
    else:
        print(f"\n     ALL CHECKS PASSED")

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n     Saved: {OUTPUT_CSV}  {df.shape}")

    model_cols = [c for c in df.columns if c != 'datetime_hour']
    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write("FEATURE ENGINEERING REPORT\n" + "=" * 70 + "\n\n")
        f.write(f"Input:        {INPUT_CSV}\n")
        f.write(f"Output:       {OUTPUT_CSV}\n")
        f.write(f"Generated:    {pd.Timestamp.now()}\n\n")
        f.write(f"Shape:        {df.shape}\n")
        f.write(f"Train rows:   {len(train):,}\n")
        f.write(f"Test rows:    {len(test):,}\n")
        f.write(f"Train cutoff: {train_cutoff.date()}\n")
        f.write(f"Alarm rate:   {alarm_rate:.2f}%\n\n")
        f.write("FEATURE GROUPS:\n")
        f.write(f"  TF-IDF:    {len(tfidf_cols)}\n")
        f.write(f"  Synergy:   {len(synergy_cols)} total, "
                f"{len(synergy_cols) - len(zero_syn)} with activations\n")
        f.write(f"  Lag/Roll:  {len(lag_all)}\n")
        f.write(f"  External:  {len(ext_cols)}\n")
        f.write(f"  RegionOHE: {len(region_ohe_cols)}\n")
        f.write(f"  Total model cols: {len(model_cols)}\n\n")
        f.write("BUG FIXES APPLIED:\n")
        f.write("     FIX 1: n_regions lags: groupby('region').shift() — no cross-region bleed\n")
        f.write("     FIX 2: ISW rolling: replaced with daily diff (growth_1d/7d)\n")
        f.write("     FIX 3: OHE: 'region' text col dropped after encoding\n")
        f.write("     FIX 4: duplicate check uses datetime_hour + region_OHE cols\n")
        f.write("            (plain datetime check gave N_regions-1 false dupes/hour)\n")
        f.write("     FIX 5: energy_stress uses .astype(bool) for & operator\n")
        f.write(f"    FIX 6: OHE threshold unified at N_REGIONS_EXPECTED={N_REGIONS_EXPECTED}\n")
        f.write("     FIX 7: [CRITICAL] n_regions_alarm TARGET LEAKAGE removed:\n")
        f.write("            - Dropped from features_dataset.csv after lag computation\n")
        f.write("            - inter_alarm_spreading now uses n_regions_lag_1h\n")
        f.write("            - Model never sees the current-hour alarm sum\n\n")
        if issues:
            f.write(f"ISSUES ({len(issues)}):\n")
            for iss in issues:
                f.write(f"  {iss}\n")
            f.write("\n")
        f.write(f"COLUMNS ({len(df.columns)}):\n")
        for i, col in enumerate(df.columns, 1):
            nans = df[col].isna().sum()
            f.write(f"  {i:>3}. {col:<45s} {str(df[col].dtype):<10s} NaN:{nans:>8,}\n")
    print(f"     Report: {REPORT_TXT}")

def build() -> None:
    print("\n")
    print("=" * 73)
    print("FEATURE ENGINEERING PIPELINE".center(73))
    print("=" * 73)

    df = load_merged()

    n_regions_col = 'n_regions_alarm' if 'n_regions_alarm' in df.columns \
                    else 'n_regions_lag_1h'
    real_data_end = df[df[n_regions_col] > 0]['datetime_hour'].max()
    if pd.isna(real_data_end):
        real_data_end = df['datetime_hour'].max()
    train_cutoff = real_data_end.floor('D') - pd.Timedelta(days=30)

    df = add_lag_features(df)
    df = add_temporal_features(df, train_cutoff)
    df = add_interaction_features(df, train_cutoff)
    df = ohe_regions(df)
    validate_and_save(df, train_cutoff)

    print("\n" + "=" * 75)
    print("   FEATURE ENGINEERING COMPLETE")
    print("=" * 75)
    print(f"  Output:       {OUTPUT_CSV}")
    print(f"  Shape:        {df.shape}")
    print(f"  Train cutoff: {train_cutoff.date()}")
    print("\n     CRITICAL: use the same TRAIN_CUTOFF in train_models.py")
    print("     Exclude 'datetime_hour' and 'alarm' from X")
    print("       ('region' is OHE'd — use region_* binary cols)")
    print("     'n_regions_alarm' was DROPPED — use n_regions_lag_1h/3h/momentum")
    print("\n  Next: python models/train_models.py --train")
    print("=" * 75)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--build", action="store_true", help="Run FE pipeline")
    args = parser.parse_args()
    if args.build:
        build()
    else:
        parser.print_help()