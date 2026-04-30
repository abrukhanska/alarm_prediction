import argparse
import json
import logging
import os
import pickle
import sys
import tempfile
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypeAlias

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings("ignore")

FeatureVector: TypeAlias = dict[str, int | float]
AlarmBuffer: TypeAlias = dict[str, list[int]]
HourForecast: TypeAlias = dict[str, bool]
HourProbas: TypeAlias = dict[str, float]

TEAM_ID = "4"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
PROCESSED = PROJECT_ROOT / "data" / "processed"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
LOGS_DIR = PROJECT_ROOT / "logs"

MODEL_PKL = MODELS_DIR / f"{TEAM_ID}__lightgbm__v1.pkl"
METADATA_PATH = MODELS_DIR / "retrain_metadata.json"
FEATURES_PARQUET = PROCESSED / "features_dataset.parquet"
OUTPUT_JSON = PREDICTIONS_DIR / "latest.json"
LOG_PATH = LOGS_DIR / "predict_24h.log"

LIVE_STATE_JSON = PROJECT_ROOT / "data" / "live" / "live_state.json"

TARGET_COL = "alarm"
_BOOTSTRAP_THRESHOLD = 0.572
FORECAST_HOURS = 48
ALARM_BUFFER_DEPTH = 30

LEAKY_COLS = {
    "region",
    "datetime_hour",
    TARGET_COL,
    "n_regions_alarm",
}

THRESHOLD_GREEN = 0.30
THRESHOLD_RED = 0.70

LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("predict_24h")

def _read_live_alarms_count() -> int:
    if not LIVE_STATE_JSON.exists():
        log.warning(f"live_state.json not found at {LIVE_STATE_JSON}. live_alarms_count=0.")
        return 0
    try:
        with open(LIVE_STATE_JSON, encoding="utf-8") as f:
            data = json.load(f)
        count = int(data.get("active_alarms_count", 0))
        log.info(f"Live alarms count from poller: {count}")
        return count
    except Exception as e:
        log.warning(f"Could not read live_state.json: {e}. live_alarms_count=0.")
        return 0

def load_model() -> tuple[lgb.LGBMClassifier, float, str]:
    if not MODEL_PKL.exists():
        log.error(f"Model not found: {MODEL_PKL}")
        log.error("Run: python models/train_models.py --train")
        sys.exit(1)

    with open(MODEL_PKL, "rb") as f:
        model = pickle.load(f)

    threshold = _BOOTSTRAP_THRESHOLD
    if METADATA_PATH.exists():
        try:
            with open(METADATA_PATH, "r", encoding="utf-8") as f:
                meta = json.load(f)
            threshold = float(meta.get("last_threshold", _BOOTSTRAP_THRESHOLD))
            log.info(f"Threshold from metadata: {threshold:.3f}")
        except (json.JSONDecodeError, KeyError) as e:
            log.warning(
                f"Could not read threshold from metadata ({e}), "
                f"using bootstrap {_BOOTSTRAP_THRESHOLD}"
            )
    else:
        log.warning(
            f"retrain_metadata.json not found, "
            f"using bootstrap threshold {_BOOTSTRAP_THRESHOLD}"
        )

    mtime = os.path.getmtime(MODEL_PKL)
    train_time = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    log.info(f"Model loaded:  {MODEL_PKL.name}")
    log.info(f"Threshold:     {threshold:.3f}")
    log.info(f"Train time:    {train_time}")
    return model, threshold, train_time

def load_feature_data(
        model: lgb.LGBMClassifier,
) -> tuple[pd.DataFrame, AlarmBuffer, list[str]]:
    if not FEATURES_PARQUET.exists():
        log.error(f"Features dataset not found: {FEATURES_PARQUET}")
        log.error("Run: python features/feature_engineering.py --build")
        sys.exit(1)

    log.info(f"Loading {FEATURES_PARQUET} (Parquet)…")
    pf = pq.ParquetFile(FEATURES_PARQUET)
    max_dt = None
    for batch in pf.iter_batches(columns=["datetime_hour"], batch_size=50_000):
        s = pd.to_datetime(batch.column("datetime_hour").to_pandas())
        if max_dt is None or s.max() > max_dt:
            max_dt = s.max()

    cutoff = max_dt - pd.Timedelta(hours=64)  # 30 buffer + 24 forecast + 10

    df = pd.read_parquet(
        FEATURES_PARQUET,
        filters=[("datetime_hour", ">=", cutoff.to_pydatetime())]
    )

    if not pd.api.types.is_datetime64_any_dtype(df["datetime_hour"]):
        df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])

    region_cols = [c for c in df.columns if c.startswith("region_")]
    if region_cols:
        df["region"] = (
            df[region_cols]
            .idxmax(axis=1)
            .str.replace("region_", "", regex=False)
            .str.replace("_", " ", regex=False)
        )
    else:
        log.error("Cannot reconstruct 'region' column — no 'region_*' columns found.")
        sys.exit(1)

    df = df.sort_values(["region", "datetime_hour"]).reset_index(drop=True)
    log.info(
        f"Loaded {len(df):,} rows | {df['region'].nunique()} regions | "
        f"{df['datetime_hour'].min().date()} – {df['datetime_hour'].max().date()}"
    )

    latest_df = df.sort_values("datetime_hour").groupby("region").last().reset_index()
    buffers: AlarmBuffer = {}
    for region in latest_df["region"]:
        region_df = df[df["region"] == region].sort_values("datetime_hour")
        alarm_series = region_df["alarm"].fillna(0).astype(int).tolist()
        buf = alarm_series[-ALARM_BUFFER_DEPTH:]
        if len(buf) < ALARM_BUFFER_DEPTH:
            buf = [0] * (ALARM_BUFFER_DEPTH - len(buf)) + buf
        buffers[region] = list(buf)

    feature_cols: list[str] = list(model.feature_name_)
    log.info(f"Feature columns: {len(feature_cols)}")
    log.info(f"Latest datetime: {latest_df['datetime_hour'].max()}")
    return latest_df, buffers, feature_cols

def _update_time_features(fvec: FeatureVector, new_dt: datetime) -> FeatureVector:
    fvec = fvec.copy()
    hour = new_dt.hour
    month = new_dt.month
    dow = new_dt.weekday()
    fvec["hour_sin"] = float(np.sin(2 * np.pi * hour / 24))
    fvec["hour_cos"] = float(np.cos(2 * np.pi * hour / 24))
    fvec["month_sin"] = float(np.sin(2 * np.pi * month / 12))
    fvec["month_cos"] = float(np.cos(2 * np.pi * month / 12))
    fvec["is_weekend"] = int(dow >= 5)
    if "is_night" in fvec:     fvec["is_night"]     = int(hour >= 22 or hour <= 5)
    if "is_evening" in fvec:   fvec["is_evening"]   = int(18 <= hour <= 21)
    if "is_morning" in fvec:   fvec["is_morning"]   = int(6 <= hour <= 9)
    if "is_afternoon" in fvec: fvec["is_afternoon"] = int(12 <= hour <= 17)
    return fvec

def _update_lag_features(fvec: FeatureVector, alarm_buffer: list[int]) -> FeatureVector:
    fvec = fvec.copy()
    buf = alarm_buffer
    if len(buf) < 24:
        buf = [0] * (24 - len(buf)) + buf

    if "alarm_lag_1h"  in fvec: fvec["alarm_lag_1h"]  = int(buf[-1])
    if "alarm_lag_2h"  in fvec: fvec["alarm_lag_2h"]  = int(buf[-2])
    if "alarm_lag_3h"  in fvec: fvec["alarm_lag_3h"]  = int(buf[-3])
    if "alarm_lag_6h"  in fvec: fvec["alarm_lag_6h"]  = int(buf[-6])
    if "alarm_lag_12h" in fvec: fvec["alarm_lag_12h"] = int(buf[-12])
    if "alarm_lag_24h" in fvec: fvec["alarm_lag_24h"] = int(buf[-24])
    if "alarms_last_6h"  in fvec: fvec["alarms_last_6h"]  = int(sum(buf[-6:]))
    if "alarms_last_12h" in fvec: fvec["alarms_last_12h"] = int(sum(buf[-12:]))
    if "alarms_last_24h" in fvec: fvec["alarms_last_24h"] = int(sum(buf[-24:]))

    return fvec

def _update_synergy_features(fvec: FeatureVector) -> FeatureVector:
    fvec = fvec.copy()
    is_night = bool(fvec.get("is_night", 0))
    n_reg_1h = fvec.get("n_regions_lag_1h", 0)

    if "energy_stress" in fvec:
        fvec["energy_stress"] = int(bool(fvec.get("freezing", 0)) and is_night)
    if "inter_bad_weather_night" in fvec:
        fvec["inter_bad_weather_night"] = int(
            (fvec.get("bad_weather_index", 0) > 0) and is_night)
    if "syn_bad_weather_night" in fvec:
        fvec["syn_bad_weather_night"] = int(
            (fvec.get("bad_weather_index", 0) > 0) and is_night)
    if "syn_energy_threat_night" in fvec:
        fvec["syn_energy_threat_night"] = int(
            (fvec.get("gur_energy_threat_d1", 0) > 0) and is_night)
    if "syn_mass_night" in fvec:
        fvec["syn_mass_night"] = int(
            (fvec.get("masovana_lag2h", 0) > 0) and is_night)
    if "syn_prestrike_night" in fvec:
        fvec["syn_prestrike_night"] = int(
            (fvec.get("prestrike_lag2h", 0) > 0) and is_night)
    if "syn_energy_stress_multi" in fvec:
        fvec["syn_energy_stress_multi"] = int(
            bool(fvec.get("energy_stress", 0)) and (n_reg_1h > 3))
    if "syn_ballistic_multiregion" in fvec:
        fvec["syn_ballistic_multiregion"] = int(
            (fvec.get("ballistic_lag1h", 0) > 0) and (n_reg_1h > 5))
    return fvec


def _clear_expired_signals(fvec: FeatureVector, step: int) -> FeatureVector:
    """
    Clears lag and rolling features if the forecast step exceeds their temporal horizon.
    Prevents the model from hallucinating eternal alarms based on past events.
    """
    fvec = fvec.copy()

    exempt = {"n_regions_lag_1h", "n_regions_lag_3h", "n_regions_momentum"}

    for key in list(fvec.keys()):
        if key in exempt:
            continue

        if "_lag" in key and key.endswith("h"):
            try:
                lag_hours = int(key.split("_lag")[-1].replace("h", ""))
                if step > lag_hours:
                    fvec[key] = 0
            except ValueError:
                pass

        elif "_roll" in key and key.endswith("h"):
            try:
                roll_hours = int(key.split("_roll")[-1].replace("h", ""))
                if step > roll_hours:
                    fvec[key] = 0
            except ValueError:
                pass

    return fvec

def predict_all_regions_24h(
        latest_df: pd.DataFrame,
        buffers: AlarmBuffer,
        model: lgb.LGBMClassifier,
        threshold: float,
        feature_cols: list[str],
        verbose: bool = False,
) -> tuple[dict[str, HourForecast], dict[str, HourProbas], pd.Timestamp]:
    states: dict[str, FeatureVector] = {}
    for _, row in latest_df.iterrows():
        region = str(row["region"])
        drop = [c for c in LEAKY_COLS if c in row.index]
        fvec = row.drop(labels=drop).to_dict()
        states[region] = {k: (0 if pd.isna(v) else v) for k, v in fvec.items()}

    base_dt = latest_df["datetime_hour"].max()
    region_order = sorted(states.keys())

    regions_forecast: dict[str, HourForecast] = {r: {} for r in region_order}
    probas_log: dict[str, HourProbas] = {r: {} for r in region_order}

    alarms_t = int(latest_df["alarm"].sum())
    alarms_t_minus_1 = int(
        latest_df["n_regions_lag_1h"].max()
        if "n_regions_lag_1h" in latest_df.columns else 0
    )
    alarms_t_minus_2 = int(
        latest_df["n_regions_lag_3h"].max()
        if "n_regions_lag_3h" in latest_df.columns else 0
    )
    n_regions_hist = [alarms_t_minus_2, alarms_t_minus_1, alarms_t]

    for step in range(1, FORECAST_HOURS + 1):
        pred_dt = base_dt + timedelta(hours=step)
        hour_label = pred_dt.strftime("%Y-%m-%dT%H:00")

        curr_n_reg_1h = n_regions_hist[-1]
        curr_n_reg_3h = int(np.mean(n_regions_hist[-3:])) if len(n_regions_hist) >= 3 else 0

        X_rows = []
        for region in region_order:
            fvec = _update_time_features(states[region], pred_dt)
            fvec = _update_lag_features(fvec, buffers[region])

            if "n_regions_lag_1h"   in fvec: fvec["n_regions_lag_1h"]   = curr_n_reg_1h
            if "n_regions_lag_3h"   in fvec: fvec["n_regions_lag_3h"]   = curr_n_reg_3h
            if "n_regions_momentum" in fvec: fvec["n_regions_momentum"] = curr_n_reg_1h - curr_n_reg_3h

            fvec = _update_synergy_features(fvec)
            states[region] = fvec
            X_rows.append([fvec.get(col, 0) for col in feature_cols])

        X = pd.DataFrame(X_rows, columns=feature_cols).fillna(0)
        probas = model.predict_proba(X)[:, 1]
        preds = (probas >= threshold).astype(int)

        n_alarms_this_step = 0
        for i, region in enumerate(region_order):
            pred  = int(preds[i])
            proba = float(probas[i])
            regions_forecast[region][hour_label] = bool(pred)
            probas_log[region][hour_label] = round(proba, 4)
            buffers[region].append(pred)
            buffers[region] = buffers[region][-ALARM_BUFFER_DEPTH:]
            if pred == 1:
                n_alarms_this_step += 1

        n_regions_hist.append(n_alarms_this_step)
        n_regions_hist.pop(0)

        if verbose:
            alarm_regions = [r for i, r in enumerate(region_order) if preds[i] == 1]
            log.info(
                f"{hour_label}  alarm={n_alarms_this_step:>2}/25  "
                f"regions: {', '.join(alarm_regions) if alarm_regions else 'none'}"
            )

    return regions_forecast, probas_log, base_dt

def build_output_json(
        regions_forecast: dict[str, HourForecast],
        probas_log: dict[str, HourProbas],
        train_time: str,
        threshold: float,
        latest_df: pd.DataFrame,
        base_dt: pd.Timestamp,
        live_alarms_count: int = 0,
) -> dict:
    now_utc = datetime.now(timezone.utc).isoformat()

    first_hour_probs = [
        list(p.values())[0] for p in probas_log.values() if p
    ]
    national_risk = int(np.mean(first_hour_probs) * 100) if first_hour_probs else 0

    forecast_start = (base_dt + timedelta(hours=1)).isoformat()
    forecast_end   = (base_dt + timedelta(hours=FORECAST_HOURS)).isoformat()

    regions_for_god_api = {}
    for region in probas_log.keys():
        probas   = probas_log[region]
        forecast = regions_forecast[region]
        max_p    = max(probas.values(), default=0.0)

        try:
            temp = float(
                latest_df[latest_df["region"] == region]["hour_temp"]
                .fillna(0.0).iloc[0]
            )
        except (IndexError, KeyError):
            temp = 0.0

        hourly_list = []
        for h_label, p_val in probas.items():
            hourly_list.append({
                "hour":        h_label,
                "probability": int(p_val * 100),
                "alarm":       forecast[h_label],
                "weather": {
                    "temp":       "+0",
                    "wind":       0,
                    "cloudcover": 0,
                    "humidity":   0,
                    "precip":     0.0,
                    "icon":       "cloud",
                },
            })

        risk_level = (
            "RED"    if max_p >= THRESHOLD_RED   else
            "YELLOW" if max_p >= THRESHOLD_GREEN else
            "GREEN"
        )

        regions_for_god_api[region] = {
            "is_live_alarm_now": False,
            "risk_level":        risk_level,
            "max_probability":   round(max_p, 4),
            "current_temp":      temp,
            "hourly_data":       hourly_list,
        }

    return {
        "last_model_update":    train_time,
        "last_prediction_time": now_utc,
        "model_name":           "LightGBM",
        "model_version":        "v1",
        "team_id":              TEAM_ID,
        "threshold":            threshold,

        "base_datetime":  str(base_dt),
        "forecast_start": forecast_start,
        "forecast_end":   forecast_end,
        "forecast_hours": FORECAST_HOURS,

        "global_metrics": {
            "national_risk_index":     national_risk,
            "last_model_update":       train_time,
            "prediction_generated_at": now_utc,

            "base_datetime":  str(base_dt),
            "forecast_start": forecast_start,
            "forecast_end":   forecast_end,
            "forecast_hours": FORECAST_HOURS,

            "total_regions_at_risk": sum(
                1 for r in regions_for_god_api.values()
                if r["max_probability"] >= threshold
            ),
            "live_alarms_count": live_alarms_count,
        },
        "regions":               regions_for_god_api,
        "regions_probabilities": probas_log,
        "regions_forecast":      regions_forecast,
    }

def save_json(output: dict) -> None:
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(
            dir=PREDICTIONS_DIR, suffix=".json", text=True
        )
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, OUTPUT_JSON)
        tmp_path = None   # replaced successfully – don't delete in finally
        size_kb = OUTPUT_JSON.stat().st_size / 1024
        log.info(f"Forecast saved: {OUTPUT_JSON}  ({size_kb:.1f} KB)")
    except Exception as e:
        log.error(f"Failed to save forecast JSON: {e}")
        raise
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

def main(args: argparse.Namespace) -> None:
    sep = "-" * 70
    log.info(sep);  log.info("24-HOUR ALARM FORECAST".center(70)); log.info(sep)
    log.info(f"Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Region filter: {args.region or 'ALL'}")
    log.info(f"Verbose:       {args.verbose}")

    log.info(sep); log.info("Step 1: Load Model".center(70)); log.info(sep)
    model, threshold, train_time = load_model()

    log.info(sep); log.info("Step 2: Load Feature Data".center(70)); log.info(sep)
    latest_df, buffers, feature_cols = load_feature_data(model)

    if args.region:
        mask = latest_df["region"].str.lower() == args.region.lower()
        if not mask.any():
            log.error(
                f"Region '{args.region}' not found. "
                f"Available: {latest_df['region'].tolist()}"
            )
            sys.exit(1)
        latest_df = latest_df[mask].copy()
        buffers = {r: b for r, b in buffers.items()
                   if r.lower() == args.region.lower()}
        log.info(f"Filtered to region: {args.region}")

    log.info(sep); log.info("Step 3: Read Live Alarm Count (alerts.in.ua)".center(70)); log.info(sep)
    live_alarms_count = _read_live_alarms_count()

    log.info(sep); log.info("Step 4: Autoregressive 24-Hour Prediction".center(70)); log.info(sep)
    t0 = datetime.now()
    regions_forecast, probas_log, base_dt = predict_all_regions_24h(
        latest_df, buffers, model, threshold, feature_cols, verbose=args.verbose
    )
    elapsed = (datetime.now() - t0).total_seconds()

    log.info(f"\n{'Region':<35s} {'Alarm hrs':>9s} {'Safe hrs':>8s} {'Max P':>6s} {'Map'}")
    log.info(f"  {'-' * 70}")
    total_alarm_hours = 0
    for region in sorted(regions_forecast):
        forecast = regions_forecast[region]
        probas   = probas_log[region]
        alarm_h  = sum(1 for v in forecast.values() if v)
        max_p    = max(probas.values(), default=0.0)
        colour   = ("RED   " if max_p >= THRESHOLD_RED else
                    "YELLOW" if max_p >= THRESHOLD_GREEN else "GREEN ")
        log.info(
            f" {region:<35s}  {alarm_h:>9d}  "
            f"{FORECAST_HOURS - alarm_h:>8d} {max_p:>6.3f} {colour}"
        )
        total_alarm_hours += alarm_h
    log.info(f" {'-' * 70}")
    log.info(
        f" {'TOTAL alarm-hours':35s} {total_alarm_hours:>9d}  "
        f"(across {len(regions_forecast)} regions)"
    )
    log.info(f" Inference time: {elapsed:.2f}s")

    log.info(sep); log.info("Step 5: Save Forecast JSON".center(70)); log.info(sep)
    output = build_output_json(
        regions_forecast, probas_log, train_time, threshold,
        latest_df, base_dt, live_alarms_count=live_alarms_count,
    )
    save_json(output)

    log.info(sep); log.info("Forecast completed!".center(70)); log.info(sep)
    log.info(
        f"Total time: {elapsed:.2f}s | Output: {OUTPUT_JSON} | Base dt: {base_dt}"
    )
    log.info(sep)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AEGIS 24-hour alarm forecast")
    parser.add_argument(
        "--region", type=str, default=None, metavar="NAME",
        help="Predict for a single region (e.g. 'Kyiv Oblast'). Default: all."
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print per-hour detail: which regions alarm at each step."
    )
    main(parser.parse_args())