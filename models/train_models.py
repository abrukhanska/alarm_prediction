"""
Train 3 models on features_dataset.csv (output of feature_engineering.py).

Models:
  1. Linear Regression   (sklearn LinearRegression)
  2. Logistic Regression (sklearn LogisticRegression)
  3. Random Forest       (sklearn RandomForestClassifier)

All models use TimeSeriesSplit(n_splits=5) — no data leakage from future to past.
LogReg + LinearReg use Pipeline(StandardScaler -> model) to prevent CV scaler leakage.
RF does NOT need scaling — tree splits are scale-invariant.
"""
import argparse
import pickle
import sys
import warnings
from pathlib import Path
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

FEATURES_CSV = PROCESSED / "features_dataset.csv"
REPORT_TXT = MODELS_DIR / "training_report.txt"

TARGET_COL = "alarm"
N_CV_SPLITS = 5

COLS_TO_REMOVE_FROM_X = ['region', 'datetime_hour', TARGET_COL, 'n_regions_alarm']

def load_and_split() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    print("=" * 65)
    print("STEP 1/6: Load & temporal split")
    print("=" * 65)

    if not FEATURES_CSV.exists():
        print(f"ERROR: {FEATURES_CSV} not found")
        sys.exit(1)

    df = pd.read_csv(FEATURES_CSV)
    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'])
    df = df.sort_values(['datetime_hour', 'region']).reset_index(drop=True)

    train_cutoff = df['datetime_hour'].max().floor('D') - pd.Timedelta(days=30)

    print(f"shape:      {df.shape}")
    print(f"range:      {df.datetime_hour.min().date()} → {df.datetime_hour.max().date()}")
    print(f"alarm rate: {df[TARGET_COL].mean()*100:.2f}%")

    train_df = df[df['datetime_hour'] < train_cutoff].copy()
    test_df = df[df['datetime_hour'] >= train_cutoff].copy()

    if len(test_df) == 0:
        print(f"  ERROR: test set is empty — data ends before {train_cutoff.date()}")
        sys.exit(1)

    print(f"\n  TRAIN_CUTOFF: {train_cutoff.date()}")
    print(f"train: {len(train_df):,} rows  alarm={train_df[TARGET_COL].mean()*100:.2f}%")
    print(f"test:  {len(test_df):,} rows  alarm={test_df[TARGET_COL].mean()*100:.2f}%")

    drop_cols  = [c for c in COLS_TO_REMOVE_FROM_X if c in df.columns]
    extra_drop = [c for c in COLS_TO_REMOVE_FROM_X if c not in df.columns]
    if extra_drop:
        print(f"NOTE: columns not found (already absent): {extra_drop}")

    X_train = train_df.drop(columns=drop_cols)
    y_train = train_df[TARGET_COL].astype(int)
    X_test  = test_df.drop(columns=drop_cols)
    y_test  = test_df[TARGET_COL].astype(int)

    nan_train = X_train.isnull().sum().sum()
    nan_test  = X_test.isnull().sum().sum()
    if nan_train > 0 or nan_test > 0:
        print(f"WARNING: NaN X_train={nan_train}, X_test={nan_test} — filling with 0")
        X_train = X_train.fillna(0)
        X_test  = X_test.fillna(0)

    tfidf_n = sum(1 for c in X_train.columns if c.startswith('tfidf_'))
    ohe_n   = sum(1 for c in X_train.columns if c.startswith('region_'))
    print(f"\n  X_train: {X_train.shape}  |  X_test: {X_test.shape}")
    print(f"  features: {X_train.shape[1]} total  "
          f"(scalar: {X_train.shape[1] - tfidf_n - ohe_n}  "
          f"tfidf: {tfidf_n}  ohe: {ohe_n})")

    PROCESSED.mkdir(parents=True, exist_ok=True)
    X_train.to_csv(PROCESSED / "X_train.csv", index=False)
    y_train.to_csv(PROCESSED / "y_train.csv", index=False)
    X_test.to_csv(PROCESSED  / "X_test.csv",  index=False)
    y_test.to_csv(PROCESSED  / "y_test.csv",  index=False)
    print(f"  Saved splits → {PROCESSED}")

    return X_train, y_train, X_test, y_test, train_cutoff

def _cv_loop(model, X_train: pd.DataFrame, y_train: pd.Series,
             tscv: TimeSeriesSplit, has_proba: bool = True) -> dict:
    results = {'accuracy': [], 'f1': [], 'recall': [], 'precision': [], 'roc_auc': []}

    for tr_idx, val_idx in tscv.split(X_train):
        X_tr  = X_train.iloc[tr_idx]
        X_val = X_train.iloc[val_idx]
        y_tr  = y_train.iloc[tr_idx]
        y_val = y_train.iloc[val_idx]

        model.fit(X_tr, y_tr)

        if has_proba:
            y_pred  = model.predict(X_val)
            y_score = model.predict_proba(X_val)[:, 1]
        else:
            raw    = model.predict(X_val)
            y_pred = (raw >= 0.5).astype(int)
            y_score = raw

        results['accuracy'].append(accuracy_score(y_val, y_pred))
        results['f1'].append(f1_score(y_val, y_pred, zero_division=0))
        results['recall'].append(recall_score(y_val, y_pred, zero_division=0))
        results['precision'].append(precision_score(y_val, y_pred, zero_division=0))
        try:
            results['roc_auc'].append(roc_auc_score(y_val, y_score))
        except ValueError:
            results['roc_auc'].append(0.5)

    return {k: (float(np.mean(v)), float(np.std(v))) for k, v in results.items()}

def _print_cv_summary(name: str, cv: dict) -> None:
    print(f"\nCV results ({N_CV_SPLITS}-fold TimeSeriesSplit):")
    print(f"{'Metric':12s}  {'Mean':>8s}  {'±Std':>8s}")
    print(f"{'-'*32}")
    for metric, (mean, std) in cv.items():
        print(f"  {metric:12s}  {mean:>8.4f}  {std:>8.4f}")

def train_linear_regression(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 1: Linear Regression  (threshold=0.5 for classification)")
    print("=" * 65)
    print(f"  Input features: {X_train.shape[1]}  (scalar + tfidf + ohe)")

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('lr',     LinearRegression(fit_intercept=True)),
    ])

    cv_summary = _cv_loop(pipeline, X_train, y_train, tscv, has_proba=False)
    _print_cv_summary("Linear Regression", cv_summary)

    pipeline.fit(X_train, y_train)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    pkl_path = MODELS_DIR / "linear_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(pipeline, f)
    print(f"Saved: {pkl_path}")

    return pipeline, cv_summary

def train_logistic_regression(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("  MODEL 2: Logistic Regression  (Pipeline + GridSearchCV)")
    print("=" * 65)
    print(f"Input features: {X_train.shape[1]}  (scalar + ohe, TF-IDF excluded)")
    print("Reason: liblinear on 500 sparse TF-IDF features is prohibitively slow;")
    print("scalar features carry the predictive signal for a linear model.")

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('logreg', LogisticRegression(
            max_iter=1000,
            random_state=42,
            solver='liblinear',
        )),
    ])

    param_grid = {
        'logreg__C':            [0.01, 0.1, 1],
        'logreg__class_weight': [None, 'balanced'],
        'logreg__penalty':      ['l2'],
    }

    n_combos = len(param_grid['logreg__C']) * len(param_grid['logreg__class_weight'])
    print(f"Grid: {n_combos} combos × {N_CV_SPLITS} folds = {n_combos * N_CV_SPLITS} fits")

    grid_search = GridSearchCV(
        pipeline,
        param_grid,
        cv=tscv,
        scoring='f1',
        n_jobs=2,
        verbose=1,
        refit=True,
        return_train_score=False,
    )
    grid_search.fit(X_train, y_train)

    best_params = grid_search.best_params_
    best_model  = grid_search.best_estimator_
    print(f"\n  Best params: {best_params}")
    print(f"  Best CV F1:  {grid_search.best_score_:.4f}")

    if best_params.get('logreg__class_weight') == 'balanced':
        print("NOTE: 'balanced' selected — model penalizes False Negatives more")
        print("(alarm rate ~18%, so missing an alarm costs more than a false alarm)")

    cv_summary = _cv_loop(best_model, X_train, y_train, tscv, has_proba=True)
    _print_cv_summary("Logistic Regression", cv_summary)

    pkl_path = MODELS_DIR / "logistic_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(best_model, f)
    print(f"  Saved: {pkl_path}")

    return best_model, cv_summary, best_params

def train_random_forest(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("  MODEL 3: Random Forest  (model of choice)")
    print("=" * 65)
    print(f"Input features: {X_train.shape[1]}  (scalar + tfidf + ohe)")
    print("NOTE: no StandardScaler needed — trees are scale-invariant")

    param_grid = {
        'n_estimators': [100, 200],
        'max_depth': [10, 15],
        'min_samples_split': [10, 20],
        'class_weight': [None, 'balanced'],
    }
    n_combos = (len(param_grid['n_estimators'])
                * len(param_grid['max_depth'])
                * len(param_grid['min_samples_split'])
                * len(param_grid['class_weight']))
    print(f"  Grid: {n_combos} combos × {N_CV_SPLITS} folds = {n_combos * N_CV_SPLITS} fits")

    rf = RandomForestClassifier(random_state=42, n_jobs=None)

    grid_search = GridSearchCV(
        rf,
        param_grid,
        cv=tscv,
        scoring='f1',
        n_jobs=2,
        verbose=1,
        refit=True,
        return_train_score=False,
    )
    grid_search.fit(X_train, y_train)

    best_params = grid_search.best_params_
    best_model  = grid_search.best_estimator_
    print(f"\nBest params: {best_params}")
    print(f"Best CV F1:  {grid_search.best_score_:.4f}")

    if best_params.get('class_weight') == 'balanced':
        print("NOTE: 'balanced' selected — confirms class imbalance is meaningful")

    cv_summary = _cv_loop(best_model, X_train, y_train, tscv, has_proba=True)
    _print_cv_summary("Random Forest", cv_summary)

    pkl_path = MODELS_DIR / "random_forest.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(best_model, f)
    print(f"Saved: {pkl_path}")

    return best_model, cv_summary, best_params

def evaluate_on_test(
        models:    dict,
        x_tests:   dict,
        y_test:    pd.Series,
) -> dict:
    print("\n" + "=" * 65)
    print("STEP 5/6: Evaluate on TEST set")
    print("=" * 65)

    test_results = {}

    for name, model in models.items():
        X_test_model = x_tests[name]
        print(f"\n  -- {name} --  (test features: {X_test_model.shape[1]})")

        if name == "Linear Regression":
            raw    = model.predict(X_test_model)
            y_pred = (raw >= 0.5).astype(int)
            y_score = raw
        else:
            y_pred  = model.predict(X_test_model)
            y_score = model.predict_proba(X_test_model)[:, 1]

        acc  = accuracy_score(y_test, y_pred)
        f1   = f1_score(y_test, y_pred, zero_division=0)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec  = recall_score(y_test, y_pred, zero_division=0)
        try:
            auc = roc_auc_score(y_test, y_score)
        except ValueError:
            auc = 0.5

        test_results[name] = {
            'accuracy':  acc,  'f1':        f1,
            'precision': prec, 'recall':     rec,
            'roc_auc':   auc,  'y_pred':     y_pred,
            'y_score':   y_score,
        }

        print(f"Accuracy:  {acc:.4f}")
        print(f"F1:        {f1:.4f}")
        print(f"Precision: {prec:.4f}")
        print(f"Recall:    {rec:.4f}")
        print(f"ROC-AUC:   {auc:.4f}")
        print()
        print(classification_report(y_test, y_pred,
                                    target_names=['no alarm', 'alarm'],
                                    zero_division=0))
    return test_results

def save_report(
        cv_results:    dict,
        test_results:  dict,
        best_params:   dict,
        feature_names: list,
        logreg_n_features: int,
        train_cutoff:  pd.Timestamp,
) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    metrics = ['accuracy', 'f1', 'recall', 'precision', 'roc_auc']

    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write("TRAINING REPORT — Task 4\n" + "=" * 60 + "\n\n")
        f.write(f"TRAIN_CUTOFF:        {train_cutoff.date()}\n")
        f.write(f"CV strategy:         TimeSeriesSplit(n_splits={N_CV_SPLITS})\n")
        f.write(f"Features (full):     {len(feature_names)}\n")
        f.write(f"Features (LogReg):   {logreg_n_features}  (TF-IDF excluded)\n")
        f.write(f"Dropped from X:      {COLS_TO_REMOVE_FROM_X}\n\n")

        f.write("=" * 60 + "\n")
        f.write("Cross-Validation Metrics on TRAIN\n")
        f.write("=" * 60 + "\n")
        header = f"{'Model':22s}" + "".join(f"{m:>14s}" for m in metrics)
        f.write(header + "\n")
        f.write("  " + "-" * (len(header) - 2) + "\n")
        for mname, cv in cv_results.items():
            row = f"{mname:22s}"
            for m in metrics:
                mean, std = cv[m]
                row += f"  {mean:.3f}±{std:.3f}"
            f.write(row + "\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("Best Hyperparameters\n")
        f.write("=" * 60 + "\n")
        for mname, params in best_params.items():
            f.write(f"\n  {mname}:\n")
            for k, v in params.items():
                f.write(f"    {k}: {v}\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("TEST SET METRICS (Last 30 Days)\n")
        f.write("=" * 60 + "\n")
        for mname, res in test_results.items():
            f.write(f"\n  {mname}:\n")
            for m in ['accuracy', 'f1', 'precision', 'recall', 'roc_auc']:
                f.write(f"    {m}: {res[m]:.4f}\n")

    print(f"  Saved: {REPORT_TXT}")

def train() -> None:
    X_train, y_train, X_test, y_test, train_cutoff = load_and_split()
    feature_names = X_train.columns.tolist()
    tscv = TimeSeriesSplit(n_splits=N_CV_SPLITS)

    print(f"\nCV strategy: TimeSeriesSplit(n_splits={N_CV_SPLITS})")
    print(f"Pipeline (LinearReg + LogReg): scaler re-fit per fold — no leakage")
    print(f"RF: no scaling — scale-invariant by design")

    tfidf_cols           = [c for c in X_train.columns if c.startswith('tfidf_')]
    X_train_no_tfidf     = X_train.drop(columns=tfidf_cols)
    X_test_no_tfidf      = X_test.drop(columns=tfidf_cols)
    feature_names_logreg = X_train_no_tfidf.columns.tolist()

    print(f"\n  LogReg will use {X_train_no_tfidf.shape[1]} features "
          f"({len(tfidf_cols)} TF-IDF columns excluded)")

    print("\n" + "=" * 65)
    print("STEP 2/6: Train Linear Regression  (all features)")
    print("=" * 65)
    lr_model, lr_cv = train_linear_regression(X_train, y_train, tscv)

    print("\n" + "=" * 65)
    print("STEP 3/6: Train Logistic Regression  (scalar + OHE only)")
    print("=" * 65)
    log_model, log_cv, log_params = train_logistic_regression(
        X_train_no_tfidf, y_train, tscv
    )

    print("\n" + "=" * 65)
    print("STEP 4/6: Train Random Forest  (all features)")
    print("=" * 65)
    rf_model, rf_cv, rf_params = train_random_forest(X_train, y_train, tscv)

    models = {
        "Linear Regression":   lr_model,
        "Logistic Regression": log_model,
        "Random Forest":       rf_model,
    }
    x_tests = {
        "Linear Regression":   X_test,
        "Logistic Regression": X_test_no_tfidf,
        "Random Forest":       X_test,
    }
    cv_results = {
        "Linear Regression":   lr_cv,
        "Logistic Regression": log_cv,
        "Random Forest":       rf_cv,
    }
    best_params_all = {
        "Linear Regression":   {
            "fit_intercept": True,
            "scaling":       "StandardScaler via Pipeline",
            "features":      f"all {len(feature_names)} (scalar + tfidf + ohe)",
        },
        "Logistic Regression": dict(log_params, **{
            "features": f"{len(feature_names_logreg)} (scalar + ohe, TF-IDF excluded)",
        }),
        "Random Forest":       dict(rf_params, **{
            "features": f"all {len(feature_names)} (scalar + tfidf + ohe)",
        }),
    }

    test_results = evaluate_on_test(models, x_tests, y_test)

    print("\n" + "=" * 65)
    print("STEP 6/6: Save Report")
    print("=" * 65)

    save_report(cv_results, test_results, best_params_all,
                feature_names, len(feature_names_logreg), train_cutoff)

    print("\n" + "=" * 65)
    print("TRAINING COMPLETE — Task 4")
    print("=" * 65)
    print(f"TRAIN_CUTOFF: {train_cutoff.date()}")
    print()
    print(f"  {'Model':22s}  {'CV F1':>8s}  {'Test F1':>8s}  {'Test AUC':>9s}  {'Features':>10s}")
    print(f"  {'-'*68}")
    n_feats = {
        "Linear Regression":   len(feature_names),
        "Logistic Regression": len(feature_names_logreg),
        "Random Forest":       len(feature_names),
    }
    for mname in models:
        cf1 = cv_results[mname]['f1'][0]
        tf1 = test_results[mname]['f1']
        auc = test_results[mname]['roc_auc']
        nf  = n_feats[mname]
        print(f"  {mname:22s}  {cf1:>8.4f}  {tf1:>8.4f}  {auc:>9.4f}  {nf:>10}")
    print()
    print(f"CV metrics & best params → {REPORT_TXT}")
    print(f"Models  → {MODELS_DIR}/")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train 3 models for alarm prediction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--train", action="store_true", help="Run full training pipeline")
    args = parser.parse_args()
    if args.train:
        train()
    else:
        parser.print_help()