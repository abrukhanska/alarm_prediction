"""
Train 3 models on features_dataset.csv (output of feature_engineering.py).

Models:
  1. Linear Regression   (sklearn LinearRegression)
  2. Logistic Regression (sklearn LogisticRegression)
  3. Random Forest       (sklearn RandomForestClassifier)

All models use TimeSeriesSplit(n_splits=5) — no data leakage from future to past.
LogReg + LinearReg use Pipeline(StandardScaler → model) to prevent CV scaler leakage.
RF does NOT need scaling — tree splits are scale-invariant.
"""
import argparse
import pickle
import sys
import warnings
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay,
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

PROJECT_ROOT= Path(__file__).resolve().parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"
PLOTS_DIR = PROJECT_ROOT / "analysis" / "plots" / "models"

FEATURES_CSV = PROCESSED / "features_dataset.csv"
REPORT_TXT = MODELS_DIR / "training_report.txt"

# Constants — must match feature_engineering.py
TRAIN_CUTOFF = pd.Timestamp("2025-01-01")
TARGET_COL   = "alarm"
N_CV_SPLITS  = 5

COLS_TO_REMOVE_FROM_X = ['region', 'datetime_hour', TARGET_COL, 'n_regions_alarm']

PAL = {
    'navy': '#003f5c', 'blue': '#2f4b7c', 'coral': '#f95d6a',
    'orange': '#ff7c43', 'green': '#2ecc71', 'gray': '#95a5a6',
}

def load_and_split() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    print("=" * 65)
    print("  STEP 1/6: Load & temporal split")
    print("=" * 65)

    if not FEATURES_CSV.exists():
        print(f"ERROR: {FEATURES_CSV} not found")
        print("Run: python data_processing/feature_engineering.py --build")
        sys.exit(1)

    df = pd.read_csv(FEATURES_CSV)
    df['datetime_hour'] = pd.to_datetime(df['datetime_hour'])

    df = df.sort_values(['datetime_hour', 'region']).reset_index(drop=True)

    print(f"shape:      {df.shape}")
    print(f"range:      {df.datetime_hour.min().date()} → {df.datetime_hour.max().date()}")
    print(f"alarm rate: {df[TARGET_COL].mean()*100:.2f}%")

    train_df = df[df['datetime_hour'] <  TRAIN_CUTOFF].copy()
    test_df  = df[df['datetime_hour'] >= TRAIN_CUTOFF].copy()

    if len(test_df) == 0:
        print(f"  ERROR: test set is empty — data ends before {TRAIN_CUTOFF.date()}")
        sys.exit(1)

    print(f"\n  TRAIN_CUTOFF: {TRAIN_CUTOFF.date()}")
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
        print(f"WARNING: NaN in X_train={nan_train}, X_test={nan_test} — filling with 0")
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

    return X_train, y_train, X_test, y_test

def _cv_loop(model, X_train, y_train, tscv, has_proba=True):
    results = {'accuracy': [], 'f1': [], 'recall': [], 'precision': [], 'roc_auc': []}

    for tr_idx, val_idx in tscv.split(X_train):
        X_tr, X_val = X_train.iloc[tr_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[tr_idx], y_train.iloc[val_idx]

        model.fit(X_tr, y_tr)

        if has_proba:
            y_pred  = model.predict(X_val)
            y_score = model.predict_proba(X_val)[:, 1]
        else:
            raw     = model.predict(X_val)
            y_pred  = (raw >= 0.5).astype(int)
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
    print(f"  {'Metric':12s}  {'Mean':>8s}  {'±Std':>8s}")
    print(f"  {'-'*32}")
    for metric, (mean, std) in cv.items():
        print(f"  {metric:12s}  {mean:>8.4f}  {std:>8.4f}")

def train_linear_regression(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 1: Linear Regression (threshold=0.5 for classification)")
    print("=" * 65)

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('lr', LinearRegression(fit_intercept=True)),
    ])

    cv_summary = _cv_loop(pipeline, X_train, y_train, tscv, has_proba=False)
    _print_cv_summary("Linear Regression", cv_summary)

    pipeline.fit(X_train, y_train)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    pkl_path = MODELS_DIR / "linear_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(pipeline, f)
    print(f"saved: {pkl_path}")

    return pipeline, cv_summary

def train_logistic_regression(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 2: Logistic Regression (Pipeline + GridSearchCV)")
    print("=" * 65)

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('logreg', LogisticRegression(
            max_iter=1000,
            random_state=42,
            solver='saga',
        )),
    ])

    param_grid = {
        'logreg__C':            [0.001, 0.01, 0.1, 1, 10, 100],
        'logreg__class_weight': [None, 'balanced'],
        'logreg__penalty':      ['l2'],
    }

    n_combos = 6 * 2 * 1
    print(f"  Grid: {n_combos} combos × {N_CV_SPLITS} folds = {n_combos * N_CV_SPLITS} fits")

    grid_search = GridSearchCV(
        pipeline,
        param_grid,
        cv=tscv,
        scoring='f1',
        n_jobs=-1,
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
        print("  NOTE: 'balanced' selected — model penalizes False Negatives more")
        print("        (alarm rate ~18%, so missing an alarm costs more than false alarm)")

    cv_summary = _cv_loop(best_model, X_train, y_train, tscv, has_proba=True)
    _print_cv_summary("Logistic Regression", cv_summary)

    pkl_path = MODELS_DIR / "logistic_regression.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(best_model, f)
    print(f"  saved: {pkl_path}")

    return best_model, cv_summary, best_params

def train_random_forest(
        X_train: pd.DataFrame,
        y_train: pd.Series,
        tscv: TimeSeriesSplit,
) -> tuple:
    print("\n" + "=" * 65)
    print("MODEL 3: Random Forest (model of choice)")
    print("=" * 65)
    print("NOTE: no StandardScaler needed — trees are scale-invariant")

    param_grid = {
        'n_estimators':      [100, 200],
        'max_depth':         [10, 20, None],
        'min_samples_split': [5, 10],
        'class_weight':      [None, 'balanced'],
    }
    n_combos = 2 * 3 * 2 * 2
    print(f"  Grid: {n_combos} combos × {N_CV_SPLITS} folds = {n_combos * N_CV_SPLITS} fits")

    rf = RandomForestClassifier(random_state=42, n_jobs=None)

    grid_search = GridSearchCV(
        rf,
        param_grid,
        cv=tscv,
        scoring='f1',
        n_jobs=-1,
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
        print("  NOTE: 'balanced' selected — confirms class imbalance is meaningful")

    cv_summary = _cv_loop(best_model, X_train, y_train, tscv, has_proba=True)
    _print_cv_summary("Random Forest", cv_summary)

    pkl_path = MODELS_DIR / "random_forest.pkl"
    with open(pkl_path, 'wb') as f:
        pickle.dump(best_model, f)
    print(f"saved: {pkl_path}")

    return best_model, cv_summary, best_params

def evaluate_on_test(
        models: dict,
        X_test: pd.DataFrame,
        y_test: pd.Series,
) -> dict:
    print("\n" + "=" * 65)
    print("STEP 5/6: Evaluate on TEST set")
    print("=" * 65)

    test_results = {}

    for name, model in models.items():
        print(f"\n  ── {name} ──")

        if name == "Linear Regression":
            raw     = model.predict(X_test)
            y_pred  = (raw >= 0.5).astype(int)
            y_score = raw
        else:
            y_pred  = model.predict(X_test)
            y_score = model.predict_proba(X_test)[:, 1]

        acc  = accuracy_score(y_test, y_pred)
        f1   = f1_score(y_test, y_pred, zero_division=0)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec  = recall_score(y_test, y_pred, zero_division=0)
        try:
            auc = roc_auc_score(y_test, y_score)
        except ValueError:
            auc = 0.5

        test_results[name] = {
            'accuracy': acc, 'f1': f1,
            'precision': prec, 'recall': rec, 'roc_auc': auc,
            'y_pred': y_pred, 'y_score': y_score,
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

def plot_cv_metrics(cv_results: dict) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    metrics     = ['accuracy', 'f1', 'recall', 'precision', 'roc_auc']
    model_names = list(cv_results.keys())
    colors      = [PAL['navy'], PAL['blue'], PAL['coral']]

    x     = np.arange(len(metrics))
    width = 0.25
    fig, ax = plt.subplots(figsize=(13, 6))

    for i, (mname, color) in enumerate(zip(model_names, colors)):
        means = [cv_results[mname][m][0] for m in metrics]
        stds  = [cv_results[mname][m][1] for m in metrics]
        bars  = ax.bar(x + i * width, means, width, label=mname,
                       color=color, alpha=0.85, yerr=stds,
                       capsize=4, error_kw={'linewidth': 1.2})
        for bar, mean in zip(bars, means):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f'{mean:.3f}', ha='center', va='bottom', fontsize=7.5,
                    fontweight='bold')

    ax.set_xticks(x + width)
    ax.set_xticklabels([m.replace('_', '\n') for m in metrics], fontsize=10)
    ax.set_ylim(0, 1.15)
    ax.set_ylabel('Score', fontsize=11)
    ax.set_title(f'Figure M1. Cross-Validation Metrics ({N_CV_SPLITS}-fold TimeSeriesSplit)',
                 fontsize=13, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.35)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.2f'))

    out = PLOTS_DIR / 'M1_cv_metrics.png'
    fig.savefig(out, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  Saved: {out}")


def plot_confusion_matrices(test_results: dict, y_test: pd.Series) -> None:
    model_names = list(test_results.keys())
    fig, axes   = plt.subplots(1, 3, figsize=(17, 5))

    for ax, name in zip(axes, model_names):
        cm   = confusion_matrix(y_test, test_results[name]['y_pred'])
        disp = ConfusionMatrixDisplay(cm, display_labels=['no alarm', 'alarm'])
        disp.plot(ax=ax, colorbar=False, cmap='Blues')
        f1  = test_results[name]['f1']
        auc = test_results[name]['roc_auc']
        ax.set_title(f'{name}\nF1={f1:.3f}  AUC={auc:.3f}', fontsize=11, fontweight='bold')

    plt.suptitle('Figure M2. Confusion Matrices — TEST set (>= 2025-01-01)',
                 fontsize=13, fontweight='bold', y=1.03)
    out = PLOTS_DIR / 'M2_confusion_matrices.png'
    fig.savefig(out, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Saved: {out}")

def plot_rf_feature_importance(rf_model, feature_names: list, top_n: int = 20) -> None:
    importances    = rf_model.feature_importances_
    sorted_indices = np.argsort(importances)[::-1]
    top_indices    = sorted_indices[:top_n]
    top_names      = [feature_names[i] for i in top_indices]
    top_vals       = importances[top_indices]

    def _feat_color(name):
        if 'lag' in name or 'last_24' in name or 'momentum' in name:
            return PAL['coral']
        if any(w in name for w in ['visibility', 'wind', 'temp', 'precip', 'humidity',
                                    'cloud', 'pressure', 'snow', 'rain', 'freezing',
                                    'night', 'weather', 'energy']):
            return PAL['orange']
        if any(w in name for w in ['isw', 'attack', 'ground', 'casualty',
                                    'intensity', 'sources']):
            return PAL['navy']
        if name.startswith('tfidf_'):
            return PAL['blue']
        return PAL['gray']

    colors = [_feat_color(n) for n in top_names]

    fig, ax = plt.subplots(figsize=(11, 7))
    bars = ax.barh(range(top_n), top_vals[::-1], color=colors[::-1], alpha=0.85)
    ax.set_yticks(range(top_n))
    ax.set_yticklabels(top_names[::-1], fontsize=9)
    ax.set_xlabel('Feature Importance (MDI)', fontsize=11)
    ax.set_title('Figure M5. Random Forest — Top-20 Feature Importances',
                 fontsize=13, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)

    hypothesis_features = {
        'alarm_lag_1h': 'H1: Short-term alarm persistence',
        'alarms_last_24h': 'H2: Wave fatigue / quiet-before-storm duality',
        'n_regions_alarm_momentum': 'H3: Attack spread velocity (original)',
        'isw_intensity_growth': 'H4: ISW media escalation precedes strikes',
        'energy_infra_stress': 'H5: Cold night energy grid vulnerability',
        'low_visibility': 'H6: AD blind spot (fog/low clouds)',
        'temp_drop_last_3d': 'H7: Cold snap grid stress',
        'isw_report_length': 'H8: ISW report length as activity proxy',
    }
    for bar, name in zip(reversed(list(bars)), top_names[::-1]):
        if name in hypothesis_features:
            ax.text(bar.get_width() + 0.0005, bar.get_y() + bar.get_height() / 2,
                    f'← {hypothesis_features[name]}',
                    va='center', fontsize=7.5, color=PAL['navy'], style='italic')

    legend_patches = [
        plt.Rectangle((0, 0), 1, 1, color=PAL['coral'], alpha=0.85, label='Alarm lag features'),
        plt.Rectangle((0, 0), 1, 1, color=PAL['orange'], alpha=0.85, label='Weather features'),
        plt.Rectangle((0, 0), 1, 1, color=PAL['navy'], alpha=0.85, label='ISW features'),
        plt.Rectangle((0, 0), 1, 1, color=PAL['blue'], alpha=0.85, label='TF-IDF features'),
    ]
    ax.legend(handles=legend_patches, fontsize=9, loc='lower right')

    out = PLOTS_DIR / 'M5_rf_feature_importance.png'
    fig.savefig(out, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"  Saved: {out}")

    print(f"\n  Hypothesis check (feature in top-{top_n}):")
    for feat, hyp in hypothesis_features.items():
        if feat in feature_names:
            feat_idx = feature_names.index(feat)
            rank     = int(np.where(sorted_indices == feat_idx)[0][0]) + 1
            status   = f"rank #{rank}" if rank <= top_n else f"NOT in top-{top_n} (rank #{rank})"
        else:
            status = "DROPPED BEFORE TRAIN"
        print(f"    {feat:25s}  {status:30s}  {hyp}")

def plot_linear_coefficients(model, feature_names: list, model_name: str, file_prefix: str, top_n: int = 20) -> None:
    if 'lr' in model.named_steps:
        coefs = model.named_steps['lr'].coef_
    elif 'logreg' in model.named_steps:
        coefs = model.named_steps['logreg'].coef_[0]
    else:
        return

    abs_coefs = np.abs(coefs)
    sorted_indices = np.argsort(abs_coefs)[::-1][:top_n]

    top_names = [feature_names[i] for i in sorted_indices]
    top_vals = coefs[sorted_indices]

    def _feat_color(name):
        if 'lag' in name or 'last_24' in name or 'momentum' in name: return PAL['coral']
        if any(w in name for w in
               ['visibility', 'wind', 'temp', 'precip', 'humidity', 'cloud', 'pressure', 'snow', 'rain', 'freezing',
                'night', 'weather', 'energy']): return PAL['orange']
        if any(w in name for w in ['isw', 'attack', 'ground', 'casualty', 'intensity', 'sources']): return PAL['navy']
        if name.startswith('tfidf_'): return PAL['blue']
        return PAL['gray']

    colors = [_feat_color(n) for n in top_names]

    fig, ax = plt.subplots(figsize=(11, 7))
    bars = ax.barh(range(top_n), top_vals[::-1], color=colors[::-1], alpha=0.85)

    ax.set_yticks(range(top_n))
    ax.set_yticklabels(top_names[::-1], fontsize=9)
    ax.set_xlabel('Coefficient Weight (Standardized)', fontsize=11)
    ax.set_title(f'Figure {file_prefix}. {model_name} — Top-{top_n} Feature Weights', fontsize=13, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    ax.axvline(0, color='black', linewidth=0.8)

    out = PLOTS_DIR / f'{file_prefix}_{model_name.replace(" ", "_").lower()}_features.png'
    fig.savefig(out, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Saved: {out}")

def save_report(
        cv_results:    dict,
        test_results:  dict,
        best_params:   dict,
        feature_names: list,
) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    metrics = ['accuracy', 'f1', 'recall', 'precision', 'roc_auc']

    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write("TRAINING REPORT — Task 4\n" + "=" * 60 + "\n\n")
        f.write(f"TRAIN_CUTOFF:   {TRAIN_CUTOFF.date()}\n")
        f.write(f"CV strategy:    TimeSeriesSplit(n_splits={N_CV_SPLITS})\n")
        f.write(f"Features:       {len(feature_names)}\n")
        f.write(f"Dropped from X: {COLS_TO_REMOVE_FROM_X}\n\n")

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
                row += f"{mean:.3f} ± {std:.3f}"
            f.write(row + "\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("Best Hyperparameters\n")
        f.write("=" * 60 + "\n")
        for mname, params in best_params.items():
            f.write(f"\n  {mname}:\n")
            for k, v in params.items():
                f.write(f"    {k}: {v}\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("TEST SET METRICS (>= 2025-01-01)\n")
        f.write("=" * 60 + "\n")
        for mname, res in test_results.items():
            f.write(f"\n{mname}:\n")
            for m in ['accuracy', 'f1', 'precision', 'recall', 'roc_auc']:
                f.write(f"{m}: {res[m]:.4f}\n")

    print(f"Saved: {REPORT_TXT}")

def train() -> None:
    X_train, y_train, X_test, y_test = load_and_split()
    feature_names = X_train.columns.tolist()
    tscv = TimeSeriesSplit(n_splits=N_CV_SPLITS)

    print(f"\nCross-validation: TimeSeriesSplit(n_splits={N_CV_SPLITS})")
    print(f"Pipeline used for LinearReg + LogReg: scaler re-fit per fold, no leakage")
    print(f"RF: no scaling — scale-invariant by design")

    print("\n" + "=" * 65)
    print("STEP 2/6: Train Linear Regression")
    print("=" * 65)
    lr_model, lr_cv = train_linear_regression(X_train, y_train, tscv)

    print("\n" + "=" * 65)
    print("STEP 3/6: Train Logistic Regression")
    print("=" * 65)
    log_model, log_cv, log_params = train_logistic_regression(X_train, y_train, tscv)

    print("\n" + "=" * 65)
    print("STEP 4/6: Train Random Forest")
    print("=" * 65)
    rf_model, rf_cv, rf_params = train_random_forest(X_train, y_train, tscv)

    models = {
        "Linear Regression":   lr_model,
        "Logistic Regression": log_model,
        "Random Forest":       rf_model,
    }
    cv_results = {
        "Linear Regression":   lr_cv,
        "Logistic Regression": log_cv,
        "Random Forest":       rf_cv,
    }
    best_params_all = {
        "Linear Regression":   {"fit_intercept": True, "scaling": "StandardScaler via Pipeline"},
        "Logistic Regression": log_params,
        "Random Forest":       rf_params,
    }

    test_results = evaluate_on_test(models, X_test, y_test)

    print("\n" + "=" * 65)
    print("STEP 6/6: Plots & Report")
    print("=" * 65)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plot_cv_metrics(cv_results)
    plot_confusion_matrices(test_results, y_test)
    plot_linear_coefficients(lr_model, feature_names, "Linear Regression", "M3")
    plot_linear_coefficients(log_model, feature_names, "Logistic Regression", "M4")
    plot_rf_feature_importance(rf_model, feature_names, top_n=20)  # Це буде M5
    save_report(cv_results, test_results, best_params_all, feature_names)

    print("\n" + "=" * 65)
    print("  TRAINING COMPLETE — Task 4")
    print("=" * 65)
    print(f"  TRAIN_CUTOFF: {TRAIN_CUTOFF.date()}")
    print()
    print(f"  {'Model':22s}  {'CV F1':>8s}  {'Test F1':>8s}  {'Test AUC':>9s}")
    print(f"  {'-'*55}")
    for mname in models:
        cf1 = cv_results[mname]['f1'][0]
        tf1 = test_results[mname]['f1']
        auc = test_results[mname]['roc_auc']
        print(f"  {mname:22s}  {cf1:>8.4f}  {tf1:>8.4f}  {auc:>9.4f}")
    print()
    print("CV metrics:      training_report.txt")
    print("Best params:     training_report.txt")
    print()
    print(f"Models: {MODELS_DIR}/")
    print(f"Plots:  {PLOTS_DIR}/")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train Models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--train", action="store_true", help="Run training pipeline")
    args = parser.parse_args()
    if args.train:
        train()
    else:
        parser.print_help()