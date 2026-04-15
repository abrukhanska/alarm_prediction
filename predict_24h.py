import argparse
import json
import logging
import os
import pickle
import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypeAlias
import lightgbm as lgb
import numpy as np
import pandas as pd

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
FEATURES_CSV = PROCESSED  / "features_dataset.csv"
OUTPUT_JSON = PREDICTIONS_DIR / "latest.json"
LOG_PATH = LOGS_DIR / "predict_24h.log"
TARGET_COL = "alarm"
_BOOTSTRAP_THRESHOLD = 0.612
FORECAST_HOURS = 24
ALARM_BUFFER_DEPTH = 30

LEAKY_COLS = {"region", "datetime_hour", TARGET_COL, "n_regions_alarm", "n_regions_alarm_lag_2h",
              "n_regions_alarm_lag_3h", "n_regions_alarm_momentum", "alarm_lag_1h", "alarm_lag_2h", "alarm_lag_3h" }

THRESHOLD_GREEN = 0.30
THRESHOLD_RED = 0.70
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH, encoding="utf-8"),],)
log = logging.getLogger("predict_24h")

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
            log.info(f"Threshold from metadata:  {threshold:.3f}")
        except (json.JSONDecodeError, KeyError) as e:
            log.warning(f"Could not read threshold from metadata ({e}) "
                        f"using bootstrap {_BOOTSTRAP_THRESHOLD}")
    else:
        log.warning(f"retrain_metadata.json not found"
                    f"using bootstrap threshold {_BOOTSTRAP_THRESHOLD}")

    mtime = os.path.getmtime(MODEL_PKL)
    train_time = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    log.info(f"Model loaded: {MODEL_PKL.name}")
    log.info(f"Threshold: {threshold:.3f}")
    log.info(f"Train time: {train_time}")
    return model, threshold, train_time

def load_feature_data(model: lgb.LGBMClassifier,
) -> tuple[pd.DataFrame, AlarmBuffer, list[str]]:
    if not FEATURES_CSV.exists():
        log.error(f"Features dataset not found: {FEATURES_CSV}")
        log.error("Run: python features/feature_engineering.py --build")
        sys.exit(1)

    log.info(f"Loading {FEATURES_CSV} ...")
    df = pd.read_csv(FEATURES_CSV, low_memory=False)
    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])
    region_cols = [c for c in df.columns if c.startswith("region_")]
    if region_cols:
        df["region"] = df[region_cols].idxmax(axis=1).str.replace("region_", "")
    else:
        log.error("Cannot reconstruct 'region' column. No 'region_...' columns found.")
        sys.exit(1)
    df = df.sort_values(["region", "datetime_hour"]).reset_index(drop=True)
    n_regions = df["region"].nunique()
    date_min = df["datetime_hour"].min().date()
    date_max = df["datetime_hour"].max().date()
    log.info(f"Loaded {len(df):,} rows  | {n_regions} regions |  "
             f"{date_min} - {date_max}")

    latest_df = (df.sort_values("datetime_hour").groupby("region").last().reset_index())
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
    fvec["hour_sin"] = float(np.sin(2 * np.pi * hour  / 24))
    fvec["hour_cos"] = float(np.cos(2 * np.pi * hour  / 24))
    fvec["month_sin"] = float(np.sin(2 * np.pi * month / 12))
    fvec["month_cos"] = float(np.cos(2 * np.pi * month / 12))
    fvec["is_weekend"] = int(dow >= 5)
    if "is_night" in fvec:
        fvec["is_night"] = int(hour >= 22 or hour <= 5)
    if "is_evening" in fvec:
        fvec["is_evening"] = int(18 <= hour <= 21)
    if "is_morning" in fvec:
        fvec["is_morning"] = int(6 <= hour <= 9)
    return fvec

def _update_lag_features(fvec: FeatureVector, alarm_buffer: list[int]) -> FeatureVector:
    fvec = fvec.copy()
    buf = alarm_buffer
    if len(buf) < 24:
        buf = [0] * (24 - len(buf)) + buf
    if "alarm_lag_6h" in fvec:
        fvec["alarm_lag_6h"] = int(buf[-6])
    if "alarm_lag_24h" in fvec:
        fvec["alarm_lag_24h"] = int(buf[-24])
    if "alarms_last_24h" in fvec:
        fvec["alarms_last_24h"] = int(sum(buf[-24:]))
    return fvec

def predict_all_regions_24h(latest_df:    pd.DataFrame, buffers:      AlarmBuffer, model:        lgb.LGBMClassifier,
                            threshold:    float, feature_cols: list[str], verbose:      bool = False,
) -> tuple[dict[str, HourForecast], dict[str, HourProbas], pd.Timestamp]:
    states: dict[str, FeatureVector] = {}
    for _, row in latest_df.iterrows():
        region = str(row["region"])
        drop = [c for c in LEAKY_COLS if c in row.index]
        fvec = row.drop(labels=drop + ["region", "datetime_hour"],
                          errors="ignore").to_dict()
        fvec = {k: (int(v) if isinstance(v, (np.integer,)) else
                      float(v) if isinstance(v, (np.floating,)) else v)
                  for k, v in fvec.items()}
        states[region] = fvec

    n_alarms_prev_step: int = int(latest_df["n_regions_alarm_lag_1h"].max()
        if "n_regions_alarm_lag_1h" in latest_df.columns
        else 0
    )

    base_dt: pd.Timestamp = latest_df["datetime_hour"].max()
    region_order = list(states.keys())
    regions_forecast: dict[str, HourForecast] = {r: {} for r in region_order}
    probas_log: dict[str, HourProbas] = {r: {} for r in region_order}
    log.info(f"Base datetime: {base_dt}")
    log.info(f"Regions: {len(region_order)}")
    log.info(f"Forecast steps: {FORECAST_HOURS}")
    log.info(f"Threshold: {threshold:.3f}")

    for step in range(FORECAST_HOURS):
        current_dt = base_dt + timedelta(hours=step + 1)
        hour_label = current_dt.strftime("%H:%M")
        X_rows = []
        for region in region_order:
            fvec = states[region]
            fvec = _update_time_features(fvec, current_dt)
            fvec = _update_lag_features(fvec, buffers[region])

            if "n_regions_alarm_lag_1h" in fvec:
                fvec["n_regions_alarm_lag_1h"] = n_alarms_prev_step

            states[region] = fvec
            row_values = [fvec.get(col, 0) for col in feature_cols]
            X_rows.append(row_values)

        X = pd.DataFrame(X_rows, columns=feature_cols).fillna(0)

        probas = model.predict_proba(X)[:, 1]
        preds = (probas >= threshold).astype(int)

        n_alarms_this_step = 0
        for i, region in enumerate(region_order):
            pred = int(preds[i])
            proba = float(probas[i])
            regions_forecast[region][hour_label] = bool(pred)
            probas_log[region][hour_label] = round(proba, 4)

            buffers[region].append(pred)
            buffers[region] = buffers[region][-ALARM_BUFFER_DEPTH:]

            if pred == 1:
                n_alarms_this_step += 1

        n_alarms_prev_step = n_alarms_this_step
        if verbose:
            alarm_regions = [r for i, r in enumerate(region_order) if preds[i] == 1]
            log.info(f"{hour_label}  alarm={n_alarms_this_step:>2}/25  "
                     f"regions: {', '.join(alarm_regions) if alarm_regions else 'none'}")
    return regions_forecast, probas_log, base_dt



def build_output_json(regions_forecast: dict[str, HourForecast], probas_log:       dict[str, HourProbas],
                      train_time:       str, threshold:        float,
) -> dict[str, object]:
    now_utc = datetime.now(timezone.utc).isoformat()
    region_summaries: dict[str, dict[str, int | float]] = {}
    for region, forecast in regions_forecast.items():
        alarm_hours = sum(1 for v in forecast.values() if v)
        probas = probas_log.get(region, {})
        green = sum(1 for p in probas.values() if p <  THRESHOLD_GREEN)
        yellow = sum(1 for p in probas.values()
                     if THRESHOLD_GREEN <= p < THRESHOLD_RED)
        red = sum(1 for p in probas.values() if p >= THRESHOLD_RED)
        region_summaries[region] = {"alarm_hours": alarm_hours,
                                    "safe_hours":  FORECAST_HOURS - alarm_hours,
                                    "map_green":   green,
                                    "map_yellow":  yellow,
                                    "map_red":     red,
                                    "max_proba":   round(max(probas.values(), default=0.0), 4),}

    output: dict[str, object] = {"last_model_train_time": train_time,
                                 "last_prediciotn_time":  now_utc,
                                 "last_prediction_time":  now_utc,
                                 "model_file":            MODEL_PKL.name,
                                 "threshold":             round(threshold, 4),
                                 "forecast_hours":        FORECAST_HOURS,
                                 "generated_at":          now_utc,
                                 "region_summaries":      region_summaries,
                                 "regions_probabilities": probas_log,
                                 "regions_forecast":      regions_forecast,}
    return output


def save_json(output: dict[str, object]) -> None:
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    size_kb = OUTPUT_JSON.stat().st_size / 1024
    log.info(f"Forecast saved: {OUTPUT_JSON}  ({size_kb:.1f} KB)")

def main(args: argparse.Namespace) -> None:
    sep = "-" * 70
    log.info(sep)
    log.info("24-HOUR ALARM FORECAST".center(70))
    log.info(sep)
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Region filter: {args.region or 'ALL'}")
    log.info(f"Verbose: {args.verbose}")
    log.info("")
    log.info(sep)
    log.info("Step 1: Load Model".center(70))
    log.info(sep)
    model, threshold, train_time = load_model()
    log.info("")
    log.info(sep)
    log.info("Step 2: Load Feature Data".center(70))
    log.info(sep)
    latest_df, buffers, feature_cols = load_feature_data(model)
    if args.region:
        mask = latest_df["region"].str.lower() == args.region.lower()
        if not mask.any():
            available = latest_df["region"].tolist()
            log.error(f"Region '{args.region}' not found. Available: {available}")
            sys.exit(1)
        latest_df = latest_df[mask].copy()
        buffers = {r: b for r, b in buffers.items()
                     if r.lower() == args.region.lower()}
        log.info(f"Filtered to region: {args.region}")

    log.info("")
    log.info(sep)
    log.info("Step 3 Autoregressive 24-Hour Prediction".center(70))
    log.info(sep)
    t0 = datetime.now()
    regions_forecast, probas_log, base_dt = predict_all_regions_24h(latest_df, buffers, model,
                                                                    threshold, feature_cols, verbose=args.verbose)
    elapsed = (datetime.now() - t0).total_seconds()
    log.info("")
    log.info(f"{'Region':<35s} {'Alarm hrs':>9s} {'Safe hrs':>8s} "
             f"{'Max P':>6s} {'Map'}")
    log.info(f"  {'-'*70}")
    total_alarm_hours = 0
    for region in sorted(regions_forecast):
        forecast = regions_forecast[region]
        probas = probas_log[region]
        alarm_hours = sum(1 for v in forecast.values() if v)
        max_p = max(probas.values(), default=0.0)
        colour = ("RED   " if max_p >= THRESHOLD_RED   else
                  "YELLOW" if max_p >= THRESHOLD_GREEN else
                  "GREEN ")
        log.info(f" {region:<35s}  {alarm_hours:>9d}  "
                 f"{FORECAST_HOURS - alarm_hours:>8d} {max_p:>6.3f} {colour}")
        total_alarm_hours += alarm_hours

    log.info(f" {'-'*70}")
    log.info(f" {'TOTAL alarm-hours':35s} {total_alarm_hours:>9d}  "
             f"(across {len(regions_forecast)} regions)")
    log.info(f"Inference time: {elapsed:.2f}s")

    log.info("")
    log.info(sep)
    log.info("Step 4 Save Forecast JSON".center(70))
    log.info(sep)
    output = build_output_json(regions_forecast, probas_log, train_time, threshold)
    save_json(output)
    log.info("")
    log.info(sep)
    log.info("Forecast completed!".center(70))
    log.info(sep)
    log.info(f"Total time: {elapsed:.2f}s")
    log.info(f"Output: {OUTPUT_JSON}")
    log.info(f"Base dt: {base_dt}")
    log.info(f"Next run: cron will call this script at the next hour")
    log.info(sep)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AEGIS 24-hour alarm forecast",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=__doc__,)
    parser.add_argument("--region", type=str, default=None, metavar="NAME",
                        help="Predict for a single region only (e.g. 'Kyiv'). "
                        "Default: all regions.",)
    parser.add_argument("--verbose", action="store_true",
                        help="Print per hour detail: which regions alarm at each step.",)
    args = parser.parse_args()
    main(args)