"""
Usage:
  python models/retrain.py                  # full pipeline
  python models/retrain.py --dry-run        # validate only, do NOT replace model
  python models/retrain.py --force          # replace model even if AUC is lower
  python models/retrain.py --validation-days 7   # use 7-day holdout instead of 3
"""

import argparse
import json
import logging
import pickle
import shutil
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
import lightgbm as lgb

warnings.filterwarnings("ignore")

TEAM_ID      = "4"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"
MODELS_DIR   = PROJECT_ROOT / "models"
LOGS_DIR     = PROJECT_ROOT / "logs"

FEATURES_CSV     = PROCESSED / "features_dataset.csv"
PROD_MODEL_PATH  = MODELS_DIR / f"{TEAM_ID}__lightgbm__v1.pkl"
METADATA_PATH    = MODELS_DIR / "retrain_metadata.json"
LOG_PATH         = LOGS_DIR  / "retrain.log"

TARGET_COL = "alarm"
N_CV_SPLITS = 3

LEAKY_COLS = {
    "region", "datetime_hour", TARGET_COL,
    "n_regions_alarm", "n_regions_alarm_lag_2h",
    "n_regions_alarm_lag_3h", "n_regions_alarm_momentum",
    "alarm_lag_1h", "alarm_lag_2h", "alarm_lag_3h",
}

BEST_PARAMS_LIGHTGBM = {
    "colsample_bytree": 0.3,
    "learning_rate":    0.05,
    "num_leaves":       31,
    "reg_lambda":       5.0,
    "objective":        "binary",
    "metric":           "auc",
    "n_estimators":     400,
    "min_child_samples": 50,
    "subsample":        0.8,
    "random_state":     42,
    "n_jobs":           1,
    "verbose":          -1,
}

_BOOTSTRAP_THRESHOLD = 0.612

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
log = logging.getLogger("retrain")


def _banner(title: str) -> None:
    log.info("=" * 70)
    log.info(title.center(70))
    log.info("=" * 70)

def _load_metadata() -> dict:
    if METADATA_PATH.exists():
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "last_retrain_time":      None,
        "last_retrain_result":    None,
        "last_model_version":     1,
        "last_threshold":         _BOOTSTRAP_THRESHOLD,
        "retrain_history":        [],
    }

def _load_production_threshold() -> float:
    meta = _load_metadata()
    thr  = meta.get("last_threshold", _BOOTSTRAP_THRESHOLD)
    log.info(f"  Production threshold loaded from metadata: {thr:.3f}")
    return float(thr)

def _save_metadata(meta: dict) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    log.info(f"Metadata saved -> {METADATA_PATH}")

def _load_production_model() -> lgb.LGBMClassifier | None:
    if not PROD_MODEL_PATH.exists():
        log.warning(f"No production model found at {PROD_MODEL_PATH}. "
                    "Skipping comparison — new model will be deployed unconditionally.")
        return None
    with open(PROD_MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    log.info(f"Loaded production model: {PROD_MODEL_PATH.name}")
    return model

def _save_model(model: lgb.LGBMClassifier, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)
    log.info(f"Model saved -> {path.name}")

def _print_metrics(label: str, y_true: np.ndarray, y_score: np.ndarray, threshold: float) -> dict:
    y_pred = (y_score >= threshold).astype(int)

    unique_classes = np.unique(y_true)
    if len(unique_classes) < 2:
        auc = 0.5
        log.warning(f"  [{label}] Only one class in y_true ({unique_classes}) — "
                    f"AUC undefined, set to 0.5. Consider increasing --validation-days.")
    else:
        auc = roc_auc_score(y_true, y_score)

    acc  = accuracy_score(y_true, y_pred)
    f1   = f1_score(y_true, y_pred, zero_division=0)
    prec = precision_score(y_true, y_pred, zero_division=0)
    rec  = recall_score(y_true, y_pred, zero_division=0)
    cm   = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()

    log.info(f"  [{label}]  threshold={threshold:.3f}")
    log.info(f"    ROC-AUC:   {auc:.4f}{' (single-class fallback)' if len(unique_classes) < 2 else ''}")
    log.info(f"    Accuracy:  {acc:.4f}")
    log.info(f"    F1:        {f1:.4f}")
    log.info(f"    Precision: {prec:.4f}")
    log.info(f"    Recall:    {rec:.4f}")
    log.info(f"    TN={tn:,}  FP={fp:,}  FN={fn:,}  TP={tp:,}")
    log.info(f"    Miss rate: {fn/max(fn+tp,1)*100:.1f}%   "
             f"False alarm rate: {fp/max(fp+tn,1)*100:.1f}%")

    return dict(roc_auc=auc, accuracy=acc, f1=f1, precision=prec,
                recall=rec, tn=int(tn), fp=int(fp), fn=int(fn), tp=int(tp),
                threshold=threshold)

def load_data(validation_days: int) -> tuple:
    _banner("STEP 1 / 5  —  Load & Split Data")

    if not FEATURES_CSV.exists():
        log.error(f"Features file not found: {FEATURES_CSV}")
        log.error("Run feature_engineering.py --build first.")
        sys.exit(1)

    log.info(f"Reading {FEATURES_CSV} ...")
    df = pd.read_csv(FEATURES_CSV, low_memory=False)

    for col in df.select_dtypes("float64").columns:
        df[col] = df[col].astype("float32")
    for col in df.select_dtypes("int64").columns:
        df[col] = df[col].astype("int32")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])
    df = df.sort_values("datetime_hour").reset_index(drop=True)

    val_cutoff = df["datetime_hour"].max().floor("D") - pd.Timedelta(days=validation_days)

    train_df = df[df["datetime_hour"] < val_cutoff].copy()
    val_df   = df[df["datetime_hour"] >= val_cutoff].copy()

    if len(train_df) == 0:
        log.error("Training set is empty. Check data range and --validation-days.")
        sys.exit(1)
    if len(val_df) == 0:
        log.error("Validation set is empty. Reduce --validation-days.")
        sys.exit(1)

    drop_cols = [c for c in LEAKY_COLS if c in df.columns]
    X_train   = train_df.drop(columns=drop_cols).fillna(0)
    y_train   = train_df[TARGET_COL].astype(np.int8)
    X_val     = val_df.drop(columns=drop_cols).fillna(0)
    y_val     = val_df[TARGET_COL].astype(np.int8)

    log.info(f"  Dataset range: {df.datetime_hour.min().date()} -> {df.datetime_hour.max().date()}")
    log.info(f"  Val cutoff:    {val_cutoff.date()}  ({validation_days} days holdout)")
    log.info(f"  Train rows:    {len(X_train):,}   alarm={y_train.mean()*100:.2f}%")
    log.info(f"  Val rows:      {len(X_val):,}    alarm={y_val.mean()*100:.2f}%")
    log.info(f"  Features:      {X_train.shape[1]}")

    return X_train, y_train, X_val, y_val, val_cutoff

def compute_class_weight(y_train: pd.Series) -> float:
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    w   = float(neg / max(pos, 1))
    log.info(f"  Class balance — neg={neg:,}  pos={pos:,}  scale_pos_weight={w:.2f}")
    return w

def train_new_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val:   pd.DataFrame,
    y_val:   pd.Series,
    scale_pos_weight: float,
) -> tuple:
    _banner("STEP 2 / 5  —  Train New LightGBM  (no GridSearchCV)")

    params = {
        **BEST_PARAMS_LIGHTGBM,
        "scale_pos_weight": scale_pos_weight,
    }

    log.info("  Hyperparameters (from initial GridSearchCV):")
    for k, v in BEST_PARAMS_LIGHTGBM.items():
        if k not in ("objective", "metric", "random_state", "n_jobs", "verbose"):
            log.info(f"    {k}: {v}")
    log.info(f"    scale_pos_weight: {scale_pos_weight:.2f}  (recomputed from current data)")
    log.info(f"    n_estimators:     {params['n_estimators']}  (cap; early stopping governs)")
    log.info("")

    early_stop_frac = 0.15
    split_idx = int(len(X_train) * (1 - early_stop_frac))
    X_fit,   y_fit   = X_train.iloc[:split_idx],  y_train.iloc[:split_idx]
    X_early, y_early = X_train.iloc[split_idx:],  y_train.iloc[split_idx:]

    log.info(f"  Internal split for early stopping (temporal, no shuffle):")
    log.info(f"    X_fit   : {len(X_fit):,} rows  (first {100*(1-early_stop_frac):.0f}%)")
    log.info(f"    X_early : {len(X_early):,} rows  (last {100*early_stop_frac:.0f}% — early-stop monitor only)")
    log.info(f"    X_val   : {len(X_val):,} rows   (UNTOUCHED — used only in Step 4 validation)")
    log.info("")
    log.info("  Training ... (expect 2-4 minutes on 800 k rows)")

    t0 = datetime.now()

    log.info("  Phase 1: searching best_iteration on X_fit (85% subset) ...")
    temp_model = lgb.LGBMClassifier(**params)
    temp_model.fit(
        X_fit, y_fit,
        eval_set=[(X_early, y_early)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=50),
        ],
    )
    best_iter = temp_model.best_iteration_ if temp_model.best_iteration_ > 0 else params["n_estimators"]
    log.info(f"  Phase 1 done — best_iteration={best_iter}")

    log.info(f"  Phase 2: final training on FULL X_train ({len(X_train):,} rows) ...")
    final_params = {**params, "n_estimators": best_iter}
    model = lgb.LGBMClassifier(**final_params)
    model.fit(X_train, y_train)
    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"  Phase 2 done in {elapsed:.1f}s total")

    log.info("")
    log.info("  Threshold tuning on X_early via temp_model (unseen data) ...")
    y_early_score = temp_model.predict_proba(X_early)[:, 1]
    prec, rec, thr = precision_recall_curve(y_early.values, y_early_score)
    f1_arr   = 2 * prec * rec / np.maximum(prec + rec, 1e-9)
    best_idx = int(np.argmax(f1_arr[:-1]))
    new_thr  = float(thr[best_idx])
    log.info(f"  Optimal threshold: {new_thr:.3f}  "
             f"P={prec[best_idx]:.3f}  R={rec[best_idx]:.3f}  F1={f1_arr[best_idx]:.3f}")

    return model, best_iter, new_thr

def validate_models(
    new_model:  lgb.LGBMClassifier,
    old_model,
    X_val:      pd.DataFrame,
    y_val:      pd.Series,
    new_thr:    float,
    old_thr:    float,
) -> tuple:
    _banner("STEP 3 / 5  —  Model Validation  (Task 13a)")

    y_new_score = new_model.predict_proba(X_val)[:, 1]
    log.info("  NEW model metrics on validation set:")
    new_m = _print_metrics("NEW", y_val.values, y_new_score, new_thr)

    if old_model is None:
        log.info("  No old model to compare against — deploying new model unconditionally.")
        return True, new_m, {}

    y_old_score = old_model.predict_proba(X_val)[:, 1]
    log.info("")
    log.info("  OLD (production) model metrics on validation set:")
    old_m = _print_metrics("OLD", y_val.values, y_old_score, old_thr)

    log.info("")
    delta_auc    = new_m["roc_auc"] - old_m["roc_auc"]
    delta_recall = new_m["recall"]  - old_m["recall"]

    log.info(f"  Delta ROC-AUC : {delta_auc:+.4f}  "
             f"({'improvement' if delta_auc >= 0 else 'regression'})")
    log.info(f"  Delta Recall  : {delta_recall:+.4f}  "
             f"({'improvement' if delta_recall >= 0 else 'regression'})")

    if delta_auc > 0:
        log.info("  DECISION: DEPLOY  — new model has higher AUC")
        should_deploy = True
    elif abs(delta_auc) < 1e-4 and delta_recall >= 0:
        log.info("  DECISION: DEPLOY  — AUC tied, new model has equal/better Recall")
        should_deploy = True
    else:
        log.warning("  DECISION: KEEP OLD  — new model is worse; keeping production model")
        log.warning(f"    New AUC={new_m['roc_auc']:.4f}  Old AUC={old_m['roc_auc']:.4f}"
                    f"  Delta={delta_auc:+.4f}")
        should_deploy = False

    return should_deploy, new_m, old_m

def deploy_model(
    new_model:     lgb.LGBMClassifier,
    should_deploy: bool,
    new_metrics:   dict,
    old_metrics:   dict,
    new_thr:       float,
    best_iter:     int,
    val_cutoff:    pd.Timestamp,
    dry_run:       bool,
    force:         bool,
) -> None:
    _banner("STEP 4 / 5  —  Deploy Decision")

    meta      = _load_metadata()
    now_utc   = datetime.now(timezone.utc).isoformat()
    old_ver   = meta.get("last_model_version", 1)
    new_ver   = old_ver + 1

    if force and not should_deploy:
        log.warning("  --force flag set: overriding KEEP decision -> DEPLOY anyway")
        should_deploy = True

    if dry_run:
        would = "DEPLOY" if should_deploy else "KEEP OLD"
        log.info(f"  --dry-run mode: zero files written.")
        log.info(f"  Pipeline would have decided: {would}")
        log.info(f"  New model AUC={new_metrics.get('roc_auc', 0):.4f}  "
                 f"threshold={new_thr:.3f}  best_iter={best_iter}")
        return
    elif should_deploy:
        if PROD_MODEL_PATH.exists():
            archive_path = MODELS_DIR / f"{TEAM_ID}__lightgbm__v{old_ver}__archive.pkl"
            shutil.copy2(PROD_MODEL_PATH, archive_path)
            log.info(f"  Old model archived -> {archive_path.name}")

        _save_model(new_model, PROD_MODEL_PATH)
        log.info(f"  Production model updated -> {PROD_MODEL_PATH.name}")
        log.info(f"  New threshold: {new_thr:.3f}  best_iteration: {best_iter}")
        log.info("")
        log.info("  DEPLOYMENT SUCCESSFUL")
        outcome = "deployed"
    else:
        log.info("  New model discarded — production model unchanged.")
        outcome = "rejected"

    record = {
        "timestamp":         now_utc,
        "outcome":           outcome,
        "val_cutoff":        str(val_cutoff.date()),
        "best_iteration":    best_iter,
        "new_threshold":     new_thr,
        "new_metrics":       new_metrics,
        "old_metrics":       old_metrics,
    }
    meta["last_retrain_time"]   = now_utc
    meta["last_retrain_result"] = outcome
    if should_deploy and not dry_run:
        meta["last_model_version"] = new_ver
        meta["last_threshold"]     = new_thr
        meta["last_best_iteration"] = best_iter
    meta.setdefault("retrain_history", []).append(record)
    meta["retrain_history"] = meta["retrain_history"][-52:]

    _save_metadata(meta)

def print_summary(
    new_metrics: dict,
    old_metrics: dict,
    should_deploy: bool,
    best_iter: int,
    new_thr: float,
    elapsed_total: float,
) -> None:
    _banner("STEP 5 / 5  —  Summary")

    log.info(f"  Total runtime: {elapsed_total:.1f}s  ({elapsed_total/60:.1f} min)")
    log.info("")

    header = f"  {'Metric':<14s}  {'NEW':>9s}  {'OLD':>9s}  {'Delta':>9s}"
    log.info(header)
    log.info(f"  {'-'*52}")
    for m in ["roc_auc", "recall", "f1", "precision", "accuracy"]:
        nv = new_metrics.get(m, 0.0)
        ov = old_metrics.get(m, 0.0) if old_metrics else 0.0
        delta = nv - ov
        arrow = "up" if delta > 0 else ("down" if delta < 0 else "=")
        log.info(f"  {m:<14s}  {nv:>9.4f}  {ov:>9.4f}  {delta:>+8.4f}  {arrow}")

    log.info("")
    status = "DEPLOYED" if should_deploy else "REJECTED"
    log.info(f"  Result:         {status}")
    log.info(f"  Best iteration: {best_iter}")
    log.info(f"  New threshold:  {new_thr:.3f}")
    log.info("")
    log.info("  Next step: cron will call predict_24h.py in the next hour")
    log.info("=" * 70)

def main(args: argparse.Namespace) -> None:
    t_start = datetime.now()
    _banner("AEGIS  —  WEEKLY MODEL RETRAIN  (Task 13 + 13a)")
    log.info(f"  Started:         {t_start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Validation days: {args.validation_days}")
    log.info(f"  Dry run:         {args.dry_run}")
    log.info(f"  Force deploy:    {args.force}")
    log.info(f"  Production pkl:  {PROD_MODEL_PATH}")
    log.info("")

    X_train, y_train, X_val, y_val, val_cutoff = load_data(args.validation_days)

    old_model = _load_production_model()

    old_thr = _load_production_threshold()

    scale_pos_weight = compute_class_weight(y_train)

    new_model, best_iter, new_thr = train_new_model(
        X_train, y_train, X_val, y_val, scale_pos_weight
    )

    should_deploy, new_metrics, old_metrics = validate_models(
        new_model, old_model, X_val, y_val, new_thr, old_thr
    )

    deploy_model(
        new_model, should_deploy, new_metrics, old_metrics,
        new_thr, best_iter, val_cutoff,
        dry_run=args.dry_run, force=args.force,
    )

    elapsed = (datetime.now() - t_start).total_seconds()
    print_summary(new_metrics, old_metrics, should_deploy, best_iter, new_thr, elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AEGIS weekly model retraining — Task 13 + 13a",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--validation-days", type=int, default=3, metavar="N",
        help="Number of recent days to use as the holdout validation set "
             "for old vs new model comparison. Default: 3.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run the full pipeline but do NOT overwrite any files. "
             "Useful for smoke-testing the retrain script.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Deploy the new model even if its validation metrics are worse. "
             "Use only for manual overrides (e.g. after feature engineering changes).",
    )
    args = parser.parse_args()
    main(args)