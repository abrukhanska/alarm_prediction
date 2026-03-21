import argparse
import pickle
import sys
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score, classification_report, f1_score,
    precision_score, recall_score, roc_auc_score, precision_recall_curve,
)
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MaxAbsScaler, StandardScaler
import lightgbm as lgb

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED  = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

FEATURES_CSV = PROCESSED / "features_dataset.csv"
REPORT_TXT   = MODELS_DIR / "training_report.txt"

TARGET_COL            = "alarm"
N_CV_SPLITS           = 5
COLS_TO_REMOVE_FROM_X = ['region', 'datetime_hour', TARGET_COL, 'n_regions_alarm',
                         'n_regions_alarm_lag_1h', 'n_regions_alarm_lag_2h', 'n_regions_alarm_lag_3h',
                         'alarm_lag_1h', 'alarm_lag_2h', 'alarm_lag_3h','n_regions_alarm_momentum'
                         ]

def load_and_split() -> tuple:
    print("=" * 65)
    print("STEP 1/6: Load & temporal split")
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

def _cv_loop(model, X_train: pd.DataFrame, y_train: pd.Series,
             tscv: TimeSeriesSplit, has_proba: bool = True) -> tuple:
    results = {'accuracy': [], 'f1': [], 'recall': [], 'precision': [], 'roc_auc': []}
    oof_scores, oof_labels = [], []
    for tr_idx, val_idx in tscv.split(X_train):
        X_tr, X_val = X_train.iloc[tr_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[tr_idx], y_train.iloc[val_idx]
        model.fit(X_tr, y_tr)
        if has_proba:
            y_score = model.predict_proba(X_val)[:, 1]
            y_pred  = model.predict(X_val)
        else:
            raw     = model.predict(X_val)
            y_score = raw
            y_pred  = (raw >= 0.5).astype(int)
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
    cv_summary = {k: (float(np.mean(v)), float(np.std(v))) for k, v in results.items()}
    return cv_summary, np.array(oof_scores), np.array(oof_labels)

def _cv_loop_lgbm(params: dict, X_train: pd.DataFrame, y_train: pd.Series,
                  tscv: TimeSeriesSplit) -> tuple:
    results = {'accuracy': [], 'f1': [], 'recall': [], 'precision': [], 'roc_auc': []}
    oof_scores, oof_labels = [], []
    best_iterations = []
    last_fold_iter  = params['n_estimators']
    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_train), 1):
        X_tr, X_val = X_train.iloc[tr_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[tr_idx], y_train.iloc[val_idx]
        m = lgb.LGBMClassifier(**params)
        m.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
        )
        best_iter = m.best_iteration_ if m.best_iteration_ > 0 else params['n_estimators']
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
    return cv_summary, np.array(oof_scores), np.array(oof_labels), last_fold_iter

def _print_cv_summary(name: str, cv: dict) -> None:
    print(f"\n  CV results ({N_CV_SPLITS}-fold TimeSeriesSplit):")
    print(f"  {'Metric':12s}  {'Mean':>8s}  {'Std':>8s}")
    print(f"  {'-'*32}")
    for metric, (mean, std) in cv.items():
        print(f"    {metric:12s}  {mean:>8.4f}  {std:>8.4f}")

def _save_feature_importance(model, feature_names: list, model_name: str, top_n: int = 20) -> None:
    importances = model.feature_importances_
    idx = np.argsort(importances)[::-1]
    print(f"\n  Top {top_n} features [{model_name}]:")
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
    slug    = model_name.lower().replace(' ', '_')
    fi_path = MODELS_DIR / f"feature_importance_{slug}.csv"
    fi_df.to_csv(fi_path, index=False)
    print(f"  Saved: {fi_path}")

def train_linear_regression(X_train: pd.DataFrame, y_train: pd.Series,
                             tscv: TimeSeriesSplit) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 1: Linear Regression  (dumb baseline only)")
    print("=" * 65)
    print("  MaxAbsScaler: preserves sparsity of TF-IDF (no dense conversion)")
    pipeline = Pipeline([
        ('scaler', MaxAbsScaler()),
        ('lr',     LinearRegression(fit_intercept=True)),
    ])
    cv_summary, oof_scores, oof_labels = _cv_loop(pipeline, X_train, y_train, tscv, has_proba=False)
    _print_cv_summary("Linear Regression", cv_summary)
    pipeline.fit(X_train, y_train)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    pkl_path = MODELS_DIR / "linear_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(pipeline, f)
    print(f"  Saved: {pkl_path}")
    return pipeline, cv_summary, 0.5

def train_logistic_regression(X_train: pd.DataFrame, y_train: pd.Series,
                               tscv: TimeSeriesSplit) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 2: Logistic Regression (scalar + OHE, L1 + L2 grid)")
    print("=" * 65)
    print("  TF-IDF excluded: ~2GB RAM to densify; scalar features carry signal")
    print("  StandardScaler: correct for scalar/OHE features (no TF-IDF here)")
    print("  L1 penalty: automatic feature selection from scalar signals")
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('logreg', LogisticRegression(
            max_iter=1000, random_state=42,
            solver='liblinear',
        )),
    ])
    param_grid = {
        'logreg__C':            [0.01, 0.1, 1],
        'logreg__class_weight': [None, 'balanced'],
        'logreg__penalty':      ['l1', 'l2'],
    }
    n_combos = (len(param_grid['logreg__C'])
                * len(param_grid['logreg__class_weight'])
                * len(param_grid['logreg__penalty']))
    print(f"  Grid: {n_combos} combos x {N_CV_SPLITS} folds = {n_combos * N_CV_SPLITS} fits")
    grid_search = GridSearchCV(pipeline, param_grid, cv=tscv, scoring='roc_auc',
                               n_jobs=2, verbose=1, refit=True, return_train_score=False)
    grid_search.fit(X_train, y_train)
    best_params = grid_search.best_params_
    best_model  = grid_search.best_estimator_
    print(f"\n  Best params: {best_params}")
    print(f"  Best CV AUC: {grid_search.best_score_:.4f}")
    if best_params.get('logreg__penalty') == 'l1':
        coef      = best_model.named_steps['logreg'].coef_[0]
        n_nonzero = (coef != 0).sum()
        print(f"  L1 selected: {n_nonzero} / {len(coef)} features have non-zero weight")
    cv_summary, oof_scores, oof_labels = _cv_loop(best_model, X_train, y_train, tscv, has_proba=True)
    _print_cv_summary("Logistic Regression", cv_summary)
    thr = _find_optimal_threshold(oof_labels, oof_scores)
    pkl_path = MODELS_DIR / "logistic_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(best_model, f)
    print(f"  Saved: {pkl_path}")
    return best_model, cv_summary, best_params, thr

def train_lightgbm(X_train: pd.DataFrame, y_train: pd.Series,
                   tscv: TimeSeriesSplit) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 3: LightGBM (all features, early stopping per fold)")
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
    cv_summary, oof_scores, oof_labels, best_n = _cv_loop_lgbm(lgbm_params, X_train, y_train, tscv)
    _print_cv_summary("LightGBM", cv_summary)
    if cv_summary['roc_auc'][1] > 0.03:
        print("  WARNING: high AUC std across folds — time periods behave very differently")
    thr = _find_optimal_threshold(oof_labels, oof_scores)
    final_params = {**lgbm_params, "n_estimators": best_n}
    final_model  = lgb.LGBMClassifier(**final_params)
    final_model.fit(X_train, y_train)
    _save_feature_importance(final_model, X_train.columns.tolist(), "LightGBM")
    pkl_path = MODELS_DIR / "lightgbm_model.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(final_model, f)
    print(f"  Saved: {pkl_path}")
    return final_model, cv_summary, final_params, thr

def evaluate_on_test(models: dict, x_tests: dict, y_test: pd.Series,
                     thresholds: dict) -> dict:
    print("\n" + "=" * 65)
    print("STEP 5/6: Evaluate on TEST set (same period for all models)")
    print("=" * 65)
    test_results = {}
    for name, model in models.items():
        X_test_model = x_tests[name]
        thr = thresholds[name]
        print(f"\n  -- {name} --  (features: {X_test_model.shape[1]}  threshold: {thr:.3f})")
        if name == "Linear Regression":
            raw     = model.predict(X_test_model)
            y_score = raw
            y_pred  = (raw >= thr).astype(int)
        else:
            y_score = model.predict_proba(X_test_model)[:, 1]
            y_pred  = (y_score >= thr).astype(int)
        acc  = accuracy_score(y_test, y_pred)
        f1   = f1_score(y_test, y_pred, zero_division=0)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec  = recall_score(y_test, y_pred, zero_division=0)
        try:
            auc = roc_auc_score(y_test, y_score)
        except ValueError:
            auc = 0.5
        test_results[name] = {
            'accuracy': acc, 'f1': f1, 'precision': prec,
            'recall': rec, 'roc_auc': auc,
            'threshold': thr, 'y_pred': y_pred, 'y_score': y_score,
        }
        print(f"  Accuracy:  {acc:.4f}")
        print(f"  F1:        {f1:.4f}")
        print(f"  Precision: {prec:.4f}")
        print(f"  Recall:    {rec:.4f}")
        print(f"  ROC-AUC:   {auc:.4f}")
        print()
        print(classification_report(y_test, y_pred,
                                    target_names=['no alarm', 'alarm'], zero_division=0))
    return test_results

def save_report(cv_results: dict, test_results: dict, best_params: dict,
                feature_names: list, logreg_n_features: int,
                train_cutoff: pd.Timestamp) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    metrics = ['accuracy', 'f1', 'recall', 'precision', 'roc_auc']
    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write("TRAINING REPORT\n" + "=" * 60 + "\n\n")
        f.write(f"TRAIN_CUTOFF:      {train_cutoff.date()}\n")
        f.write(f"CV strategy:       TimeSeriesSplit(n_splits={N_CV_SPLITS})\n")
        f.write(f"Features (full):   {len(feature_names)}\n")
        f.write(f"Features (LogReg): {logreg_n_features}  (TF-IDF excluded, memory)\n")
        f.write(f"Scaler (LinReg):   MaxAbsScaler (preserves TF-IDF sparsity)\n")
        f.write(f"Scaler (LogReg):   StandardScaler (scalar+OHE only, no TF-IDF)\n")
        f.write(f"LGBM iter choice:  last fold best_iteration (closest to future)\n")
        f.write(f"Dropped from X:    {COLS_TO_REMOVE_FROM_X}\n\n")
        f.write("=" * 60 + "\nCross-Validation Metrics\n" + "=" * 60 + "\n")
        for mname, cv in cv_results.items():
            f.write(f"\n  {mname}:\n")
            for m in metrics:
                mean, std = cv[m]
                f.write(f"    {m}: {mean:.4f} +/- {std:.4f}\n")
        f.write("\n" + "=" * 60 + "\nBest Hyperparameters\n" + "=" * 60 + "\n")
        for mname, params in best_params.items():
            f.write(f"\n  {mname}:\n")
            for k, v in params.items():
                f.write(f"    {k}: {v}\n")
        f.write("\n" + "=" * 60 + "\nTEST SET METRICS\n" + "=" * 60 + "\n")
        for mname, res in test_results.items():
            f.write(f"\n  {mname}  (threshold={res['threshold']:.3f}):\n")
            for m in ['accuracy', 'f1', 'precision', 'recall', 'roc_auc']:
                f.write(f"    {m}: {res[m]:.4f}\n")
        f.write("\n" + "=" * 60 + "\nCOMPARISON\n" + "=" * 60 + "\n")
        f.write(f"  {'Model':22s}  {'CV AUC':>10s}  {'Test AUC':>10s}  {'Overfit gap':>12s}  {'Threshold':>10s}\n")
        f.write(f"  {'-'*68}\n")
        for mname in test_results:
            cv_auc  = cv_results[mname]['roc_auc'][0]
            tst_auc = test_results[mname]['roc_auc']
            gap     = cv_auc - tst_auc
            thr     = test_results[mname]['threshold']
            f.write(f"  {mname:22s}  {cv_auc:>10.4f}  {tst_auc:>10.4f}  {gap:>+12.4f}  {thr:>10.3f}\n")
    print(f"  Saved: {REPORT_TXT}")

def train() -> None:
    X_train, y_train, X_test, y_test, train_cutoff = load_and_split()
    feature_names    = X_train.columns.tolist()
    tscv             = TimeSeriesSplit(n_splits=N_CV_SPLITS)
    tfidf_cols       = [c for c in X_train.columns if c.startswith('tfidf_')]
    X_train_no_tfidf = X_train.drop(columns=tfidf_cols)
    X_test_no_tfidf  = X_test.drop(columns=tfidf_cols)

    print("\n" + "=" * 65)
    print("STEP 2/6: Train Linear Regression")
    print("=" * 65)
    lr_model, lr_cv, lr_thr = train_linear_regression(X_train, y_train, tscv)

    print("\n" + "=" * 65)
    print("STEP 3/6: Train Logistic Regression")
    print("=" * 65)
    log_model, log_cv, log_params, log_thr = train_logistic_regression(X_train_no_tfidf, y_train, tscv)

    print("\n" + "=" * 65)
    print("STEP 4/6: Train LightGBM")
    print("=" * 65)
    lgbm_model, lgbm_cv, lgbm_params, lgbm_thr = train_lightgbm(X_train, y_train, tscv)

    models = {
        "Linear Regression":   lr_model,
        "Logistic Regression": log_model,
        "LightGBM":            lgbm_model,
    }
    x_tests = {
        "Linear Regression":   X_test,
        "Logistic Regression": X_test_no_tfidf,
        "LightGBM":            X_test,
    }
    thresholds = {
        "Linear Regression":   lr_thr,
        "Logistic Regression": log_thr,
        "LightGBM":            lgbm_thr,
    }
    cv_results = {
        "Linear Regression":   lr_cv,
        "Logistic Regression": log_cv,
        "LightGBM":            lgbm_cv,
    }
    best_params_all = {
        "Linear Regression":   {"scaler": "MaxAbsScaler", "features": f"all {len(feature_names)}"},
        "Logistic Regression": dict(log_params, **{"features": f"{len(X_train_no_tfidf.columns)} (TF-IDF excluded)"}),
        "LightGBM":            dict(lgbm_params, **{"features": f"all {len(feature_names)}"}),
    }

    test_results = evaluate_on_test(models, x_tests, y_test, thresholds)

    print("\n" + "=" * 65)
    print("STEP 6/6: Save Report")
    print("=" * 65)
    save_report(cv_results, test_results, best_params_all,
                feature_names, len(X_train_no_tfidf.columns), train_cutoff)

    print("\n" + "=" * 65)
    print("TRAINING COMPLETE")
    print("=" * 65)
    print(f"  TRAIN_CUTOFF: {train_cutoff.date()}")
    print()
    print(f"  {'Model':22s}  {'CV AUC':>10s}  {'Test AUC':>10s}  {'Test F1':>8s}  {'Overfit':>8s}  {'Thr':>6s}")
    print(f"  {'-'*72}")
    for mname in models:
        cv_auc  = cv_results[mname]['roc_auc'][0]
        tst_auc = test_results[mname]['roc_auc']
        tst_f1  = test_results[mname]['f1']
        gap     = cv_auc - tst_auc
        thr     = test_results[mname]['threshold']
        flag    = "  <- overfit!" if gap > 0.05 else ""
        print(f"  {mname:22s}  {cv_auc:>10.4f}  {tst_auc:>10.4f}  {tst_f1:>8.4f}  {gap:>+8.4f}  {thr:>6.3f}{flag}")
    print()
    print(f"  Feature importance -> {MODELS_DIR}/feature_importance_*.csv")
    print(f"  Report             -> {REPORT_TXT}")
    print(f"  Models             -> {MODELS_DIR}/")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train 3 models for alarm prediction")
    parser.add_argument("--train", action="store_true", help="Run full training pipeline")
    args = parser.parse_args()
    if args.train:
        train()
    else:
        parser.print_help()