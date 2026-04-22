import pickle
import sys
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score, classification_report, f1_score,
    precision_score, recall_score, roc_auc_score, precision_recall_curve,
)
from sklearn.model_selection import TimeSeriesSplit
import lightgbm as lgb

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED  = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

FEATURES_CSV = PROCESSED / "features_dataset.csv"

TARGET_COL            = "alarm"
N_CV_SPLITS           = 5
COLS_TO_REMOVE_FROM_X = ['region', 'datetime_hour', TARGET_COL, 'n_regions_alarm']


def load_and_split() -> tuple:
    print("=" * 65)
    print("STEP 1/3: Load & temporal split")
    print("=" * 65)
    if not FEATURES_CSV.exists():
        print(f"ERROR: {FEATURES_CSV} not found")
        sys.exit(1)
    df = pd.read_csv(FEATURES_CSV)
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    int_cols = df.select_dtypes(include=['int64']).columns
    df[int_cols] = df[int_cols].astype('int32')
    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'])
    df = df.sort_values(['datetime_hour', 'region']).reset_index(drop=True)
    train_cutoff = df['datetime_hour'].max().floor('D') - pd.Timedelta(days=30)
    print(f"  shape:        {df.shape}")
    print(f"  range:        {df.datetime_hour.min().date()} -> {df.datetime_hour.max().date()}")
    print(f"  alarm rate:   {df[TARGET_COL].mean()*100:.2f}%")
    train_df = df[df['datetime_hour'] < train_cutoff].copy()
    test_df  = df[df['datetime_hour'] >= train_cutoff].copy()
    if len(test_df) == 0:
        print("  ERROR: test set is empty")
        sys.exit(1)
    print(f"  TRAIN_CUTOFF: {train_cutoff.date()}")
    print(f"  train: {len(train_df):,} rows  alarm={train_df[TARGET_COL].mean()*100:.2f}%")
    print(f"  test:  {len(test_df):,} rows  alarm={test_df[TARGET_COL].mean()*100:.2f}%")
    drop_cols = [c for c in COLS_TO_REMOVE_FROM_X if c in df.columns]
    X_train = train_df.drop(columns=drop_cols).fillna(0)
    y_train = train_df[TARGET_COL].astype(int)
    X_test  = test_df.drop(columns=drop_cols).fillna(0)
    y_test  = test_df[TARGET_COL].astype(int)
    tfidf_n = sum(1 for c in X_train.columns if c.startswith('tfidf_'))
    ohe_n   = sum(1 for c in X_train.columns if c.startswith('region_'))
    print(f"  X_train: {X_train.shape}  |  X_test: {X_test.shape}")
    print(f"  features: {X_train.shape[1]} total  "
          f"(scalar: {X_train.shape[1] - tfidf_n - ohe_n}  tfidf: {tfidf_n}  ohe: {ohe_n})")
    PROCESSED.mkdir(parents=True, exist_ok=True)
    X_train.to_csv(PROCESSED / "X_train.csv", index=False)
    y_train.to_csv(PROCESSED / "y_train.csv", index=False)
    X_test.to_csv(PROCESSED  / "X_test.csv",  index=False)
    y_test.to_csv(PROCESSED  / "y_test.csv",  index=False)
    print(f"  Saved splits -> {PROCESSED}")
    return X_train, y_train, X_test, y_test, train_cutoff


def _find_optimal_threshold(y_true: np.ndarray, y_score: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    f1 = 2 * precision * recall / np.maximum(precision + recall, 1e-9)
    best_idx = np.argmax(f1[:-1])
    thr = float(thresholds[best_idx])
    print(f"  optimal threshold (max F1 on OOF): {thr:.3f}  "
          f"(P={precision[best_idx]:.3f}  R={recall[best_idx]:.3f}  F1={f1[best_idx]:.3f})")
    return thr


def _save_feature_importance(model, feature_names: list, top_n: int = 20) -> None:
    importances = model.feature_importances_
    idx = np.argsort(importances)[::-1]
    print(f"\n  Top {top_n} features [LightGBM]:")
    tfidf_in_top = 0
    for i in range(min(top_n, len(idx))):
        fname = feature_names[idx[i]]
        imp   = importances[idx[i]]
        tag   = " [TF-IDF]" if fname.startswith("tfidf_") else ""
        if fname.startswith("tfidf_"):
            tfidf_in_top += 1
        print(f"    {i+1:>2}. {fname:40s} {imp:.4f}{tag}")
    print(f"  TF-IDF features in top-{top_n}: {tfidf_in_top}")
    if tfidf_in_top == 0:
        print("  NOTE: no TF-IDF in top features — text does not contribute signal")
    fi_df   = pd.DataFrame({'feature': [feature_names[i] for i in idx], 'importance': importances[idx]})
    fi_path = MODELS_DIR / "feature_importance_lightgbm.csv"
    fi_df.to_csv(fi_path, index=False)
    print(f"  Saved: {fi_path}")


def train_lightgbm(X_train: pd.DataFrame, y_train: pd.Series,
                   tscv: TimeSeriesSplit) -> tuple:
    print("\n" + "=" * 65)
    print("STEP 2/3: LightGBM (all features, early stopping per fold)")
    print("=" * 65)
    pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    print(f"  scale_pos_weight:  {pos_weight:.2f}")
    print(f"  colsample_bytree:  0.3  (30% features per tree)")
    print(f"  min_child_samples: 100  (no fitting on rare word patterns)")
    print(f"  early_stopping:    50 rounds  |  final fit: last fold best_iter")
    lgbm_params = {
        "objective":          "binary",
        "metric":             "auc",
        "num_leaves":         31,
        "min_child_samples":  100,
        "subsample":          0.8,
        "subsample_freq":     1,
        "colsample_bytree":   0.3,
        "reg_alpha":          0.1,
        "reg_lambda":         1.0,
        "learning_rate":      0.05,
        "n_estimators":       1000,
        "scale_pos_weight":   pos_weight,
        "random_state":       42,
        "n_jobs":             1,
        "verbose":            -1,
    }
    results = {'accuracy': [], 'f1': [], 'recall': [], 'precision': [], 'roc_auc': []}
    oof_scores, oof_labels = [], []
    best_iterations = []
    last_fold_iter  = lgbm_params['n_estimators']
    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_train), 1):
        X_tr, X_val = X_train.iloc[tr_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[tr_idx], y_train.iloc[val_idx]
        m = lgb.LGBMClassifier(**lgbm_params)
        m.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
        )
        best_iter = m.best_iteration_ if m.best_iteration_ > 0 else lgbm_params['n_estimators']
        best_iterations.append(best_iter)
        last_fold_iter = best_iter
        y_score = m.predict_proba(X_val)[:, 1]
        y_pred  = m.predict(X_val)
        oof_scores.extend(y_score.tolist())
        oof_labels.extend(y_val.tolist())
        results['accuracy'].append(accuracy_score(y_val, y_pred))
        results['f1'].append(f1_score(y_val, y_pred, zero_division=0))
        results['recall'].append(recall_score(y_val, y_pred, zero_division=0))
        results['precision'].append(precision_score(y_val, y_pred, zero_division=0))
        try:
            results['roc_auc'].append(roc_auc_score(y_val, y_score))
        except ValueError:
            results['roc_auc'].append(0.5)
        print(f"    fold {fold}: AUC={results['roc_auc'][-1]:.4f}  best_iter={best_iter}")
    iter_std = float(np.std(best_iterations))
    print(f"  best_iterations per fold: {best_iterations}")
    print(f"  std={iter_std:.1f}  -> using LAST FOLD={last_fold_iter} for final fit")
    print(f"  (last fold trains on most data, closest to deployment conditions)")
    if iter_std > 100:
        print("  WARNING: high iteration spread — time periods differ significantly")
    cv_summary = {k: (float(np.mean(v)), float(np.std(v))) for k, v in results.items()}
    print(f"\n  CV results ({N_CV_SPLITS}-fold TimeSeriesSplit):")
    print(f"  {'Metric':12s}  {'Mean':>8s}  {'Std':>8s}")
    print(f"  {'-'*32}")
    for metric, (mean, std) in cv_summary.items():
        print(f"    {metric:12s}  {mean:>8.4f}  {std:>8.4f}")
    if cv_summary['roc_auc'][1] > 0.03:
        print("  WARNING: high AUC std across folds — time periods behave very differently")
    thr = _find_optimal_threshold(np.array(oof_labels), np.array(oof_scores))
    final_params = {**lgbm_params, "n_estimators": last_fold_iter}
    final_model  = lgb.LGBMClassifier(**final_params)
    final_model.fit(X_train, y_train)
    _save_feature_importance(final_model, X_train.columns.tolist())
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    pkl_path = MODELS_DIR / "lightgbm_model.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(final_model, f)
    print(f"  Saved: {pkl_path}")
    return final_model, cv_summary, final_params, thr


def evaluate_on_test(model, X_test: pd.DataFrame, y_test: pd.Series, thr: float) -> dict:
    print("\n" + "=" * 65)
    print("STEP 3/3: Evaluate on TEST set")
    print("=" * 65)
    print(f"\n  -- LightGBM --  (features: {X_test.shape[1]}  threshold: {thr:.3f})")
    y_score = model.predict_proba(X_test)[:, 1]
    y_pred  = (y_score >= thr).astype(int)
    acc  = accuracy_score(y_test, y_pred)
    f1   = f1_score(y_test, y_pred, zero_division=0)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec  = recall_score(y_test, y_pred, zero_division=0)
    try:
        auc = roc_auc_score(y_test, y_score)
    except ValueError:
        auc = 0.5
    print(f"  Accuracy:  {acc:.4f}")
    print(f"  F1:        {f1:.4f}")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {rec:.4f}")
    print(f"  ROC-AUC:   {auc:.4f}")
    print()
    print(classification_report(y_test, y_pred, target_names=['no alarm', 'alarm'], zero_division=0))
    return {'accuracy': acc, 'f1': f1, 'precision': prec, 'recall': rec,
            'roc_auc': auc, 'threshold': thr}


def main():
    X_train, y_train, X_test, y_test, train_cutoff = load_and_split()
    tscv = TimeSeriesSplit(n_splits=N_CV_SPLITS)

    model, cv, params, thr = train_lightgbm(X_train, y_train, tscv)
    test_res = evaluate_on_test(model, X_test, y_test, thr)

    cv_auc  = cv['roc_auc'][0]
    tst_auc = test_res['roc_auc']
    gap     = cv_auc - tst_auc
    flag    = "  <- overfit!" if gap > 0.05 else ""

    print("\n" + "=" * 65)
    print("TRAINING COMPLETE")
    print("=" * 65)
    print(f"  TRAIN_CUTOFF: {train_cutoff.date()}")
    print()
    print(f"  {'Model':22s}  {'CV AUC':>10s}  {'Test AUC':>10s}  {'Test F1':>8s}  {'Overfit':>8s}  {'Thr':>6s}")
    print(f"  {'-'*72}")
    print(f"  {'LightGBM':22s}  {cv_auc:>10.4f}  {tst_auc:>10.4f}  {test_res['f1']:>8.4f}  {gap:>+8.4f}  {thr:>6.3f}{flag}")
    print()
    print(f"  Feature importance -> {MODELS_DIR}/feature_importance_lightgbm.csv")
    print(f"  Model              -> {MODELS_DIR}/lightgbm_model.pkl")
    print("=" * 65)


if __name__ == "__main__":
    main()