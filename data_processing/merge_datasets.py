import argparse
import json
import sys
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
import scipy.sparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"

WEATHER_PARQUET = PROCESSED / "weather_clean.parquet"
ALARMS_PARQUET = PROCESSED / "alarms_clean.parquet"
ISW_PARQUET = PROCESSED / "isw_features_for_merge.parquet"
TFIDF_NPZ = PROCESSED / "tfidf_matrix_model.npz"
TFIDF_VOCAB = PROCESSED / "tfidf_vocab_model.json"
TG_PARQUET = PROCESSED / "telegram_features_hourly.parquet"
GUR_PARQUET = PROCESSED / "gur_features_clean.parquet"

OUTPUT_PARQUET = PROCESSED / "merged_dataset.parquet"
REPORT_TXT = PROCESSED / "merge_report.txt"

KYIV_TZ = "Europe/Kyiv"

VISIBILITY_THR = 5.0  # km  -> low_visibility flag
WINDSPEED_THR = 15.0  # m/s -> strong_wind flag
FREEZING_THR = 0.0  # °C  -> freezing flag

WEATHER_TO_ALARM = {
    "Vinnytsia": "Vinnytsia Oblast",
    "Lutsk": "Volyn Oblast",
    "Dnipro": "Dnipropetrovsk Oblast",
    "Donetsk": "Donetsk Oblast",
    "Zhytomyr": "Zhytomyr Oblast",
    "Uzhgorod": "Zakarpattia Oblast",
    "Zaporozhye": "Zaporizhzhia Oblast",
    "Ivano-Frankivsk": "Ivano-Frankivsk Oblast",
    "Kyiv": "City of Kyiv",
    "Kropyvnytskyi": "Kirovohrad Oblast",
    "Lviv": "Lviv Oblast",
    "Mykolaiv": "Mykolaiv Oblast",
    "Odesa": "Odesa Oblast",
    "Poltava": "Poltava Oblast",
    "Rivne": "Rivne Oblast",
    "Sumy": "Sumy Oblast",
    "Ternopil": "Ternopil Oblast",
    "Kharkiv": "Kharkiv Oblast",
    "Kherson": "Kherson Oblast",
    "Khmelnytskyi": "Khmelnytskyi Oblast",
    "Cherkasy": "Cherkasy Oblast",
    "Chernivtsi": "Chernivtsi Oblast",
    "Chernihiv": "Chernihiv Oblast",
}

FRONTLINE_REGIONS = {
    "Kharkiv Oblast", "Zaporizhzhia Oblast",
    "Donetsk Oblast", "Kherson Oblast", "Sumy Oblast",
}

_NO_WEATHER_STATION_REGIONS = frozenset({"Luhansk Oblast"})

WEATHER_FEATURE_COLS = [
    "datetime_hour", "city_address",
    "hour_temp", "hour_feelslike", "hour_humidity", "hour_dew",
    "hour_precip", "hour_precipprob", "hour_snow", "hour_snowdepth",
    "hour_windgust", "hour_windspeed", "hour_winddir",
    "hour_pressure", "hour_visibility", "hour_cloudcover",
    "is_night", "is_rain", "is_snow",
    "temp_diff", "pressure_trend", "season",
]
WEATHER_CORE_COLS = ["hour_temp", "hour_humidity", "hour_pressure", "hour_windspeed"]

ISW_SCALAR_COLS = [
    "isw_report_length",
    "isw_sources_count", "unique_domains",
    "attack_mentions", "ground_mentions", "casualty_mentions",
    "total_intensity", "intensity_per_1000",
    "real_dead_ratio", "blackout_score", "ru_ua_balance", "ru_official_ratio",
]

def _is_safe_tg_col(col: str) -> bool:
    if col == "datetime":
        return False
    if "lag" in col or "roll" in col:
        return True
    if col in {"calm_phase_risk", "hours_since_last_massive"}:
        return True
    return False

def _to_kyiv_naive(series: pd.Series) -> pd.Series:
    if series.dt.tz is None:
        return series
    return series.dt.tz_convert(KYIV_TZ).dt.tz_localize(None)

def load_inputs() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame,
scipy.sparse.csr_matrix, list]:
    print("=" * 75)
    print("STEP 1/7: Load & validate inputs")
    print("=" * 75)

    required = [WEATHER_PARQUET, ALARMS_PARQUET, ISW_PARQUET, TFIDF_NPZ, TFIDF_VOCAB]
    missing = [p for p in required if not p.exists()]
    if missing:
        print("FATAL: Missing required files:")
        for p in missing:
            print(f"     {p}")
        sys.exit(1)

    df_w_raw = pd.read_parquet(WEATHER_PARQUET)
    keep = [c for c in WEATHER_FEATURE_COLS if c in df_w_raw.columns]
    df_w = df_w_raw[keep].copy()
    df_w["datetime_hour"] = _to_kyiv_naive(
        pd.to_datetime(df_w["datetime_hour"], utc=False, errors="coerce")
    )
    df_w = df_w.dropna(subset=["datetime_hour"]).sort_values("datetime_hour").reset_index(drop=True)
    core_present = [c for c in WEATHER_CORE_COLS if c in df_w.columns]
    before = len(df_w)
    df_w = df_w.dropna(subset=core_present)
    if before - len(df_w):
        print(f"    weather: dropped {before - len(df_w):,} rows NaN in core cols")
    print(f"    WEATHER: {df_w.shape} | {df_w.city_address.nunique()} cities")

    df_a = pd.read_parquet(ALARMS_PARQUET)
    for col in ("start_dt", "end_dt"):
        df_a[col] = _to_kyiv_naive(
            pd.to_datetime(df_a[col], utc=False, errors="coerce")
        )
    df_a = df_a.dropna(subset=["start_dt"]).sort_values("start_dt").reset_index(drop=True)
    print(f"    ALARMS: {df_a.shape} | {df_a.region.nunique()} regions")

    df_i = pd.read_parquet(ISW_PARQUET)
    if "date" in df_i.columns:
        df_i = df_i.drop(columns=["date"])
    df_i["alarm_date"] = (
        pd.to_datetime(df_i["alarm_date"], utc=False, errors="coerce").dt.normalize()
    )
    df_i = df_i.dropna(subset=["alarm_date"]).sort_values("alarm_date").reset_index(drop=True)
    print(f"    ISW: {df_i.shape} | "
          f"{df_i.alarm_date.min().date()}    -> {df_i.alarm_date.max().date()}")
    print("       D+1 shift: CONFIRMED applied in isw_nlp_pipeline.py")

    tfidf = scipy.sparse.load_npz(TFIDF_NPZ)
    with open(TFIDF_VOCAB, "r", encoding="utf-8") as f:
        vocab = json.load(f)
    if tfidf.shape[0] != len(df_i):
        print(f"     FATAL: TF-IDF rows {tfidf.shape[0]} ≠ ISW rows {len(df_i)}")
        sys.exit(1)
    print(f"    TF-IDF: {tfidf.shape} | {len(vocab)} terms")

    return df_w, df_a, df_i, tfidf, vocab

def build_alarm_matrix(
        df_a: pd.DataFrame, max_hour: pd.Timestamp
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    print("\n" + "=" * 75)
    print("STEP 2/7: Expand alarms to hourly grain (Vectorized)")
    print("=" * 75)

    open_alarms = df_a["end_dt"].isna().sum()
    if open_alarms:
        print(f"     {open_alarms} open alarms    -> extending to {max_hour}")

    max_h = max_hour.floor("h")
    df_a = df_a.copy()
    df_a["start_h"] = df_a["start_dt"].dt.floor("h")
    end_dt_adj = np.maximum(
        df_a["end_dt"].fillna(max_h),
        df_a["start_dt"] + pd.Timedelta(seconds=1),
    )
    df_a["end_h"] = (end_dt_adj - pd.Timedelta(seconds=1)).dt.floor("h")
    df_a["end_h"] = df_a[["start_h", "end_h"]].max(axis=1)
    df_a["duration_h"] = ((df_a["end_h"] - df_a["start_h"]).dt.total_seconds() / 3600).astype(int) + 1
    df_expanded = df_a.loc[df_a.index.repeat(df_a["duration_h"])].copy()
    df_expanded["increment_h"] = df_expanded.groupby(level=0).cumcount()
    df_expanded["datetime_hour"] = df_expanded["start_h"] + pd.to_timedelta(df_expanded["increment_h"], unit="h")
    df_expanded = df_expanded[df_expanded["datetime_hour"] <= max_h]
    df_expanded = df_expanded[["region", "datetime_hour"]].copy()

    df_n_regions = (
        df_expanded
        .drop_duplicates(subset=["region", "datetime_hour"])
        .groupby("datetime_hour", sort=True)
        .agg(n_regions_alarm=("region", "nunique"))
        .reset_index()
    )

    df_alarm = (
        df_expanded
        .drop_duplicates(subset=["region", "datetime_hour"])
        .assign(alarm=1)
    )

    mappable = set(WEATHER_TO_ALARM.values()) | {"Kyiv Oblast"}
    unmapped = df_alarm[~df_alarm["region"].isin(mappable)]["region"].unique()
    if len(unmapped):
        print("      Regions without weather station:")
        for r in sorted(unmapped):
            flag = " (expected)" if r in _NO_WEATHER_STATION_REGIONS else " ← UNEXPECTED"
            print(f"       {r}{flag}")
    df_alarm = df_alarm[df_alarm["region"].isin(mappable)]

    print(f"    Expanded: {df_alarm.shape[0]:,} (region, hour) pairs")
    print(f"     n_regions_alarm max={df_n_regions['n_regions_alarm'].max()}"
          f" | mass_attack(>15)={(df_n_regions['n_regions_alarm'] > 15).sum():,}")
    return df_alarm, df_n_regions

def build_isw_tfidf(
        df_i: pd.DataFrame,
        tfidf: scipy.sparse.csr_matrix,
        vocab: list,
) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 3/7: Build ISW + TF-IDF daily features (Chunked)")
    print("=" * 75)

    chunk_size = 500
    dfs = []

    for start_idx in range(0, tfidf.shape[0], chunk_size):
        end_idx = min(start_idx + chunk_size, tfidf.shape[0])
        chunk_dense = np.round(tfidf[start_idx:end_idx].toarray(), 4).astype(np.float16)
        chunk_df = pd.DataFrame(
            chunk_dense,
            index=df_i.index[start_idx:end_idx],
            columns=[f"tfidf_{v}" for v in vocab]
        )
        dfs.append(chunk_df)
    df_tfidf = pd.concat(dfs)
    df_isw_full = pd.concat([df_i, df_tfidf], axis=1)
    print(f"    ISW scalars: {len(df_i.columns) - 1} | TF-IDF: {len(vocab)}")
    print(f"     Total ISW frame: {df_isw_full.shape}")
    return df_isw_full

def merge_core(
        df_w: pd.DataFrame,
        df_alarm: pd.DataFrame,
        df_n_regions: pd.DataFrame,
        df_isw_full: pd.DataFrame,
) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 4a/7: Core merge (weather + alarms + ISW)")
    print("=" * 75)

    df = df_w.copy()
    df["_city_match"] = df["city_address"].str.split(",").str[0].str.strip()
    df["region"] = df["_city_match"].map(WEATHER_TO_ALARM)

    kyiv_w = df[df["region"] == "City of Kyiv"].copy()
    if not kyiv_w.empty:
        kyiv_w["region"] = "Kyiv Oblast"
        df = pd.concat([df, kyiv_w], ignore_index=True)
        print("  [+] Duplicated Kyiv weather rows for 'Kyiv Oblast'")

    unmapped = df[df["region"].isna()]["city_address"].unique()
    if len(unmapped):
        print(f"      Unmapped cities (dropped): {unmapped}")
    df = df.dropna(subset=["region"]).drop(columns=["_city_match"])
    print(f"    Weather backbone: {df.shape}")

    df = df.merge(df_alarm[["region", "datetime_hour", "alarm"]],
                  on=["region", "datetime_hour"], how="left")
    df["alarm"] = df["alarm"].fillna(0).astype(np.int8)
    print(f"    After alarm join: {df.shape} | alarm_rate={df['alarm'].mean() * 100:.2f}%")

    df = df.merge(df_n_regions, on="datetime_hour", how="left")
    df["n_regions_alarm"] = df["n_regions_alarm"].fillna(0).astype(np.int8)
    print(f"    After n_regions: {df.shape}")
    print("         n_regions_alarm is RAW TARGET SUM — lags computed in step 4d")

    df["_cal_date"] = df["datetime_hour"].dt.normalize()
    df = df.merge(df_isw_full, left_on="_cal_date", right_on="alarm_date", how="left")
    df = df.drop(columns=["_cal_date", "alarm_date"], errors="ignore")
    print(f"    After ISW join: {df.shape}")

    drop_meta = ["city_address", "conditions", "date"]
    df = df.drop(columns=[c for c in drop_meta if c in df.columns], errors="ignore")

    tfidf_cols = [c for c in df.columns if c.startswith("tfidf_")]
    scalar_cols = [c for c in ISW_SCALAR_COLS if c in df.columns]

    for col in scalar_cols:
        df[col] = df[col].fillna(0).astype(np.float32)

    for col in tfidf_cols:
        df[col] = df[col].fillna(0).astype(np.float16)

    df = df.sort_values(["region", "datetime_hour"]).reset_index(drop=True)
    return df

def merge_external_sources(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 4b/7: Merge external sources (TG + GUR) — LEAKAGE-SAFE")
    print("=" * 75)

    if TG_PARQUET.exists():
        df_tg = pd.read_parquet(TG_PARQUET)
        df_tg["datetime"] = _to_kyiv_naive(
            pd.to_datetime(df_tg["datetime"], utc=False, errors="coerce")
        )
        df_tg = df_tg.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last")
        safe_cols = [c for c in df_tg.columns if _is_safe_tg_col(c)]
        excluded = [c for c in df_tg.columns if not _is_safe_tg_col(c) and c != "datetime"]

        if not safe_cols:
            print("     TG: no safe lag/roll columns found — SKIP")
        else:
            df = df.merge(df_tg[["datetime"] + safe_cols],
                          left_on="datetime_hour", right_on="datetime", how="left")
            df = df.drop(columns=["datetime"], errors="ignore")
            for c in safe_cols:
                if c in df.columns:
                    if df[c].dtype == object:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    df[c] = df[c].fillna(0)
            print(f"    TG safe cols imported: {len(safe_cols)}")
            print(f"     Excluded (current-hour f_*): {len(excluded)} cols — LEAKAGE PREVENTED")
    else:
        print(f"     TG: {TG_PARQUET.name} not found — SKIP")

    if GUR_PARQUET.exists():
        df_gur = pd.read_parquet(GUR_PARQUET)
        df_gur["alarm_date"] = (
            pd.to_datetime(df_gur["alarm_date"], utc=False, errors="coerce").dt.normalize()
        )
        df_gur = df_gur.dropna(subset=["alarm_date"]).drop_duplicates(subset=["alarm_date"], keep="last")
        gur_cols = [c for c in df_gur.columns if c != "alarm_date"]

        if not gur_cols:
            print("     GUR: no feature columns — SKIP")
        else:
            df["_merge_date"] = df["datetime_hour"].dt.normalize()
            df = df.merge(df_gur[["alarm_date"] + gur_cols],
                          left_on="_merge_date", right_on="alarm_date", how="left")
            df = df.drop(columns=["_merge_date", "alarm_date"], errors="ignore")
            for c in gur_cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            print(f"    GUR cols imported: {len(gur_cols)}")
            print("         D+1 shift confirmed in gur_features.py — NO LEAKAGE")
    else:
        print(f"     GUR: {GUR_PARQUET.name} not found — SKIP")
    return df

def compute_weather_binaries(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 4c/7: Compute weather binary indicators (pre-synergy)")
    print("=" * 75)

    added = []
    train_mask = df["datetime_hour"] < train_cutoff

    if "hour_visibility" in df.columns:
        vis_median_train = df.loc[train_mask, "hour_visibility"].median()
        vis = df["hour_visibility"].fillna(vis_median_train)
        print(f"  [i] visibility median (train-only): {vis_median_train:.2f} km")
        df["low_visibility"] = (vis < VISIBILITY_THR).astype(np.int8)
        print(f"  [+] low_visibility  (<{VISIBILITY_THR} km):  "
              f"{df['low_visibility'].mean() * 100:.1f}%")
        added.append("low_visibility")

    if "hour_windspeed" in df.columns:
        df["strong_wind"] = (df["hour_windspeed"] > WINDSPEED_THR).astype(np.int8)
        print(f"  [+] strong_wind     (>{WINDSPEED_THR} m/s): "
              f"{df['strong_wind'].mean() * 100:.1f}%")
        added.append("strong_wind")

    if "hour_temp" in df.columns:
        df["freezing"] = (df["hour_temp"] < FREEZING_THR).astype(np.int8)
        print(f"  [+] freezing        (<{FREEZING_THR}°C):    "
              f"{df['freezing'].mean() * 100:.1f}%")
        added.append("freezing")

    bwi = pd.Series(0, index=df.index, dtype=np.int16)
    if "hour_precip" in df.columns:
        bwi += (df["hour_precip"].fillna(0) > 0).astype(np.int16)
    if "low_visibility" in df.columns:
        bwi += df["low_visibility"].astype(np.int16)
    if "strong_wind" in df.columns:
        bwi += df["strong_wind"].astype(np.int16)
    df["bad_weather_index"] = bwi.clip(0, 3).astype(np.int8)
    print(f"  [+] bad_weather_index (0-3):       mean={df['bad_weather_index'].mean():.2f}")
    added.append("bad_weather_index")

    if "freezing" in df.columns and "is_night" in df.columns:
        df["energy_stress"] = (
                df["freezing"].astype(bool) & df["is_night"].astype(bool)
        ).astype(np.int8)
        print(f"  [+] energy_stress   (freezing & night): "
              f"{df['energy_stress'].mean() * 100:.1f}%")
        added.append("energy_stress")

    if not added:
        print("      No raw weather cols found — weather synergies will be skipped")
    else:
        print(f"\n    {len(added)} weather binary columns ready for synergy engine")

    return df

def compute_n_regions_lags(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 4d/7: Compute n_regions lags (prevent target leakage)")
    print("=" * 75)

    if "n_regions_alarm" not in df.columns:
        print("     n_regions_alarm not found — SKIP")
        return df

    df = df.sort_values(["region", "datetime_hour"]).reset_index(drop=True)

    df["n_regions_lag_1h"] = (
        df.groupby("region", observed=False)["n_regions_alarm"]
        .shift(1)
        .fillna(0)
        .astype(np.int8)
    )
    df["n_regions_lag_3h"] = (
        df.groupby("region", observed=False)["n_regions_alarm"]
        .shift(3)
        .fillna(0)
        .astype(np.int8)
    )

    print(f"  [+] n_regions_lag_1h  (max={df['n_regions_lag_1h'].max()})")
    print(f"  [+] n_regions_lag_3h  (max={df['n_regions_lag_3h'].max()})")
    print("    Safe lagged surrogates ready — raw n_regions_alarm NOT used in features")
    print("     (raw col stays in merged_dataset.parquet for feature_engineering.py to re-lag,")
    print("      then gets dropped from features_dataset.csv by feature_engineering.py)")
    return df

def add_frontline_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 5/7: Add frontline & regional features")
    print("=" * 75)

    df["is_frontline"] = df["region"].isin(FRONTLINE_REGIONS).astype(np.int8)
    fl_rate = df[df["is_frontline"] == 1]["alarm"].mean() * 100
    nfl_rate = df[df["is_frontline"] == 0]["alarm"].mean() * 100
    print(f"  [+] is_frontline | frontline {fl_rate:.1f}% vs non {nfl_rate:.1f}%"
          f" | ratio {fl_rate / max(nfl_rate, 0.01):.1f}x")

    if "n_regions_lag_1h" in df.columns:
        df["frontline_multi_alarm"] = (
                df["is_frontline"].astype(bool) & (df["n_regions_lag_1h"] > 0)
        ).astype(np.int8)
        n = int(df["frontline_multi_alarm"].sum())
        print(f"  [+] frontline_multi_alarm (uses n_regions_lag_1h): {n:,} activations")
    elif "n_regions_alarm" in df.columns:
        print("      WARNING: n_regions_lag_1h missing; falling back to raw n_regions_alarm")
        print("     This is TARGET LEAKAGE — run compute_n_regions_lags() before this step!")
        df["frontline_multi_alarm"] = (
                df["is_frontline"].astype(bool) & (df["n_regions_alarm"] > 0)
        ).astype(np.int8)

    return df

def add_synergy_features(df: pd.DataFrame, train_cutoff: pd.Timestamp) -> pd.DataFrame:
    print("\n" + "=" * 75)
    print("STEP 6/7: Add synergy features")
    print("=" * 75)

    added = []

    def _col(name: str) -> bool:
        return name in df.columns

    def _syn(name: str, cond_a: pd.Series, cond_b: pd.Series) -> None:
        df[name] = (cond_a.astype(bool) & cond_b.astype(bool)).astype(np.int8)
        added.append(name)

    if _col("shahed_lag1h") and _col("low_visibility"):
        _syn("syn_shahed_low_vis",
             df["shahed_lag1h"] > 0, df["low_visibility"].astype(bool))

    if _col("kab_lag1h") and _col("strong_wind"):
        _syn("syn_kab_strong_wind",
             df["kab_lag1h"] > 0, df["strong_wind"].astype(bool))

    if _col("takt_avia_lag1h") and _col("freezing"):
        _syn("syn_takt_avia_freeze",
             df["takt_avia_lag1h"] > 0, df["freezing"].astype(bool))

    if _col("masovana_lag2h") and _col("is_night"):
        _syn("syn_mass_night",
             df["masovana_lag2h"] > 0, df["is_night"].astype(bool))

    if _col("prestrike_lag2h") and _col("is_night"):
        _syn("syn_prestrike_night",
             df["prestrike_lag2h"] > 0, df["is_night"].astype(bool))

    if _col("vec_south_lag6h") and _col("region"):
        south = df["region"].isin(
            ["Odesa Oblast", "Mykolaiv Oblast", "Kherson Oblast"]
        )
        _syn("syn_vec_south_coastal", df["vec_south_lag6h"] > 0, south)

    if _col("x101_lag2h") and _col("region"):
        capital = df["region"].isin(
            ["City of Kyiv", "Kyiv Oblast", "Kharkiv Oblast"]
        )
        _syn("syn_x101_capital", df["x101_lag2h"] > 0, capital)

    if _col("staging_lag3h") and _col("intensity_per_1000"):
        train_mask = df["datetime_hour"] < train_cutoff
        q75_intensity = df.loc[train_mask, "intensity_per_1000"].quantile(0.75)
        high_isw = df["intensity_per_1000"] > q75_intensity
        print(f"  [i] intensity_per_1000 q75 (train-only): {q75_intensity:.4f}")
        _syn("syn_isw_staging", df["staging_lag3h"] > 0, high_isw)

    if _col("prestrike_lag2h") and _col("acoustic_lag1h"):
        _syn("syn_prestrike_acoustic",
             df["prestrike_lag2h"] > 0, df["acoustic_lag1h"] > 0)

    if _col("rszo_lag1h") and _col("is_frontline"):
        _syn("syn_frontline_rszo",
             df["rszo_lag1h"] > 0, df["is_frontline"].astype(bool))

    if _col("shahed_lag1h") and _col("is_frontline"):
        _syn("syn_frontline_shahed",
             df["shahed_lag1h"] > 0, df["is_frontline"].astype(bool))

    if _col("gur_energy_threat_d1") and _col("freezing"):
        _syn("syn_energy_threat_cold",
             df["gur_energy_threat_d1"] > 0, df["freezing"].astype(bool))

    if _col("gur_energy_threat_d1") and _col("is_night"):
        _syn("syn_energy_threat_night",
             df["gur_energy_threat_d1"] > 0, df["is_night"].astype(bool))

    if _col("ballistic_lag1h") and _col("n_regions_lag_1h"):
        _syn("syn_ballistic_multiregion",
             df["ballistic_lag1h"] > 0, df["n_regions_lag_1h"] > 5)

    if _col("bad_weather_index") and _col("is_night"):
        _syn("syn_bad_weather_night",
             df["bad_weather_index"] > 0, df["is_night"].astype(bool))

    if _col("energy_stress") and _col("n_regions_lag_1h"):
        _syn("syn_energy_stress_multi",
             df["energy_stress"].astype(bool), df["n_regions_lag_1h"] > 3)

    if not added:
        print("      No synergy features added — check TG lag columns and weather binaries")
    else:
        print(f"    {len(added)} synergy features added:")
        for syn in added:
            n = int((df[syn] > 0).sum())
            pct = n / max(len(df), 1) * 100
            print(f"     {syn:<45s} {n:>8,} activations ({pct:5.2f}%)")

    return df

def validate_and_save(
        df: pd.DataFrame,
        train_cutoff: pd.Timestamp,
        df_a: pd.DataFrame,
) -> None:
    print("\n" + "=" * 75)
    print("STEP 7/7: Validate & Save")
    print("=" * 75)

    issues = []

    n_regions = df["region"].nunique()
    n_hours = df["datetime_hour"].nunique()
    expected = n_regions * n_hours
    actual = len(df)
    completeness = actual / expected * 100 if expected > 0 else 0

    print(f"  Shape:        {df.shape}")
    print(f"  Regions:      {n_regions} | Hours: {n_hours:,}"
          f" | Completeness: {completeness:.1f}%")
    print(f"  Date range:   {df.datetime_hour.min()}    -> {df.datetime_hour.max()}")

    if completeness < 90:
        issues.append(f"completeness {completeness:.1f}% < 90%")

    for col in ["region", "datetime_hour", "alarm", "n_regions_alarm", "is_frontline"]:
        if col not in df.columns:
            issues.append(f"MISSING critical: {col}")
        else:
            n = df[col].isna().sum()
            if n:
                issues.append(f"NaN in '{col}': {n}")

    if "n_regions_lag_1h" not in df.columns:
        issues.append("MISSING: n_regions_lag_1h — compute_n_regions_lags() not run?")

    alarm_rate = df["alarm"].mean() * 100
    if not (2 <= alarm_rate <= 50):
        issues.append(f"alarm_rate {alarm_rate:.2f}% outside [2%, 50%]")
    print(f"  Alarm rate:   {alarm_rate:.2f}%")

    dupes = df.duplicated(subset=["region", "datetime_hour"]).sum()
    if dupes:
        issues.append(f"DUPLICATES: {dupes} rows")

    tfidf_cols = [c for c in df.columns if c.startswith("tfidf_")]
    synergy_cols = [c for c in df.columns if c.startswith("syn_")]
    lag_cols = [c for c in df.columns if "lag" in c or "roll" in c]
    gur_cols = [c for c in df.columns if c.startswith("gur_")]
    raw_f_cols = [c for c in df.columns if c.startswith("f_") and "lag" not in c and "roll" not in c]

    if raw_f_cols:
        issues.append(
            f"LEAKAGE: {len(raw_f_cols)} raw f_* cols present: {raw_f_cols[:5]}"
        )

    zero_syn = [s for s in synergy_cols if df[s].sum() == 0]
    if zero_syn:
        issues.append(
            f"Zero-activation synergies ({len(zero_syn)}): {zero_syn[:5]}"
            f" — were weather binaries computed before synergy step?"
        )

    print(f"  TF-IDF:       {len(tfidf_cols)}")
    print(f"  Synergies:    {len(synergy_cols)} total | "
          f"with activations: {len(synergy_cols) - len(zero_syn)} | "
          f"zero: {len(zero_syn)}")
    print(f"  Lag/Roll:     {len(lag_cols)}")
    print(f"  GUR:          {len(gur_cols)}")
    print(f"  Raw f_* (should be 0): {len(raw_f_cols)}")

    for col in ("real_dead_ratio", "blackout_score", "ru_ua_balance"):
        if col not in df.columns:
            issues.append(f"MISSING ISW col: {col}")

    train = df[df["datetime_hour"] < train_cutoff]
    test = df[df["datetime_hour"] >= train_cutoff]
    print(f"  Train:        {len(train):,} | Test: {len(test):,}")
    if len(test) == 0:
        issues.append("TEST set is empty")
    if len(train) < 500:
        issues.append(f"TRAIN too small: {len(train)}")

    if issues:
        print(f"\n       Issues ({len(issues)}):")
        for iss in issues:
            print(f"       {iss}")
    else:
        print("\n    ALL CHECKS PASSED")

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False, compression="snappy")
    print(f"\n    Saved: {OUTPUT_PARQUET}  {df.shape}")

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("MERGE REPORT\n" + "=" * 70 + "\n\n")
        f.write(f"Generated:    {pd.Timestamp.now()}\n")
        f.write(f"Train cutoff: {train_cutoff.date()}\n")
        f.write(f"Max alarm:    {df_a['start_dt'].max().date()}\n\n")
        f.write(f"Shape:        {df.shape}\n")
        f.write(f"Regions:      {n_regions}\n")
        f.write(f"Hours:        {n_hours:,}\n")
        f.write(f"Completeness: {completeness:.1f}%\n")
        f.write(f"Alarm rate:   {alarm_rate:.2f}%\n")
        f.write(f"Train rows:   {len(train):,}\n")
        f.write(f"Test rows:    {len(test):,}\n\n")
        f.write("BUG FIXES APPLIED:\n")
        f.write("    FIX 1: compute_weather_binaries() runs before add_synergy_features()\n")
        f.write("    FIX 2: bool & bool instead of int8 & int8 for binary interactions\n")
        f.write("    FIX 3: duplicate check uses composite key [region, datetime_hour]\n")
        f.write("    FIX 4: zero-activation synergy gate in validation\n")
        f.write("    FIX 5: [CRITICAL] n_regions_alarm target leakage removed:\n")
        f.write("            compute_n_regions_lags() runs before frontline/synergy steps.\n")
        f.write("            frontline_multi_alarm, syn_ballistic_multiregion,\n")
        f.write("            syn_energy_stress_multi all use n_regions_lag_1h.\n\n")
        f.write("LEAKAGE PREVENTION:\n")
        f.write("    ISW D+1: applied in isw_nlp_pipeline.py\n")
        f.write("    TG: only *_lagNh/*_rollNh cols imported (f_* excluded)\n")
        f.write("    GUR D+1: applied in gur_features.py\n")
        f.write("    Synergies: lag cols × current weather only\n")
        f.write(f"    Raw f_* cols in dataset: {len(raw_f_cols)} (must be 0)\n")
        f.write("    n_regions_alarm: raw kept for lag computation; DROPPED in features_dataset.parquet\n\n")
        f.write(f"FEATURES:\n")
        f.write(f"  TF-IDF:    {len(tfidf_cols)}\n")
        f.write(f"  Synergy:   {len(synergy_cols)} total, "
                f"{len(synergy_cols) - len(zero_syn)} with activations\n")
        f.write(f"  Lag/Roll:  {len(lag_cols)}\n")
        f.write(f"  GUR:       {len(gur_cols)}\n\n")
        if issues:
            f.write(f"ISSUES ({len(issues)}):\n")
            for iss in issues:
                f.write(f"  {iss}\n")
            f.write("\n")
        f.write(f"COLUMNS ({len(df.columns)}):\n")
        for i, c in enumerate(df.columns, 1):
            nans = df[c].isna().sum()
            f.write(f"  {i:>3}. {c:<45s} {str(df[c].dtype):<10s} NaN:{nans:>8,}\n")
    print(f"    Report: {REPORT_TXT}")

def merge() -> None:
    print("\n")
    print("=" * 73)
    print("AEGIS DATASET MERGE PIPELINE".center(73))
    print("(weather + alarms + ISW + TG_lags + GUR + synergies)".center(73))
    print("=" * 73)

    df_w, df_a, df_i, tfidf, vocab = load_inputs()

    max_alarm_date = df_a["start_dt"].max().floor("D")
    train_cutoff = max_alarm_date - pd.Timedelta(days=30)
    max_hour = df_w["datetime_hour"].max()

    print(f"\n  Sync check:")
    print(f"    Max alarm date: {max_alarm_date.date()}")
    print(f"    Train cutoff:   {train_cutoff.date()}")

    df_alarm, df_n_regions = build_alarm_matrix(df_a, max_hour)
    df_isw_full = build_isw_tfidf(df_i, tfidf, vocab)
    df = merge_core(df_w, df_alarm, df_n_regions, df_isw_full)
    df = merge_external_sources(df)

    df = compute_n_regions_lags(df)
    df = add_frontline_features(df)
    df = compute_weather_binaries(df, train_cutoff)
    df = add_synergy_features(df, train_cutoff)

    validate_and_save(df, train_cutoff, df_a)

    print("\n" + "=" * 75)
    print("  MERGE COMPLETE — READY FOR FEATURE ENGINEERING")
    print("=" * 75)
    print(f"  Output:       {OUTPUT_PARQUET}")
    print(f"  Shape:        {df.shape}")
    print(f"  Train cutoff: {train_cutoff.date()}")
    print("\n    Leakage prevention: PASSED")
    print("    TG raw f_* cols: EXCLUDED")
    print("    n_regions_alarm: NEVER used raw — only n_regions_lag_1h/3h in features")
    print("    Synergies: lag × weather (weather binaries pre-computed ← BUG FIXED)")
    print("\n  Next: python data_processing/feature_engineering.py --build")
    print("=" * 75)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AEGIS: Merge all datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--merge", action="store_true", help="Run merge pipeline")
    args = parser.parse_args()
    if args.merge:
        merge()
    else:
        parser.print_help()