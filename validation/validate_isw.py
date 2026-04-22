"""
AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА ISW ДАНИХ
Запуск: python validate_isw.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT  = Path(__file__).resolve().parent.parent
PARQUET_PATH  = PROJECT_ROOT / "data" / "processed" / "isw_clean.parquet"
TEXTS_PATH    = PROJECT_ROOT / "data" / "processed" / "isw_texts.json"
TFIDF_NPZ     = PROJECT_ROOT / "data" / "processed" / "isw_tfidf_matrix.npz"
TFIDF_VOCAB   = PROJECT_ROOT / "data" / "processed" / "isw_tfidf_vocab.json"
FEATURES_PATH = PROJECT_ROOT / "data" / "processed" / "isw_features_for_merge.csv"

WAR_START = pd.Timestamp("2022-02-24")

REQUIRED_COLUMNS = [
    "date", "isw_report_length", "word_count", "sentence_count",
    "paragraph_count", "avg_sentence_length",
    "isw_sources_count", "unique_domains",
    "attack_mentions", "ground_mentions", "casualty_mentions",
    "total_intensity", "intensity_per_1000",
]

FLOAT_COLS = [
    "real_dead_ratio", "blackout_score",
    "ru_ua_balance", "ru_official_ratio",
    "avg_sentence_length", "intensity_per_1000",
]

RATIO_BOUNDS = {
    "real_dead_ratio":   (0.0, 1.0),
    "blackout_score":    (0.0, 2.0),
    "ru_ua_balance":     (-1.0, 1.0),
    "ru_official_ratio": (0.0, 1.0),
}

# --------------------------------------------------------------------------- #

PASS = "✅"
WARN = "⚠️ "
FAIL = "❌"

errors:   list[str] = []
warnings: list[str] = []
passed:   list[str] = []


def ok(msg: str) -> None:
    passed.append(msg)
    print(f"  {PASS} {msg}")


def warn(msg: str) -> None:
    warnings.append(msg)
    print(f"  {WARN} {msg}")


def fail(msg: str) -> None:
    errors.append(msg)
    print(f"  {FAIL} {msg}")


def section(title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")

# --------------------------------------------------------------------------- #


def check_files() -> pd.DataFrame:
    section("1. ФАЙЛИ")

    for path in [PARQUET_PATH, TEXTS_PATH, TFIDF_NPZ, TFIDF_VOCAB]:
        if path.exists():
            size_mb = path.stat().st_size / 1024**2
            ok(f"{path.name}  ({size_mb:.2f} MB)")
        else:
            fail(f"ВІДСУТНІЙ: {path.name}")

    if FEATURES_PATH.exists():
        size_mb = FEATURES_PATH.stat().st_size / 1024**2
        ok(f"{FEATURES_PATH.name}  ({size_mb:.2f} MB)")
    else:
        warn(f"isw_features_for_merge.csv не знайдено — запусти isw_nlp_pipeline.py")

    if not PARQUET_PATH.exists():
        fail("Головний файл відсутній — перевірка неможлива")
        sys.exit(1)

    try:
        df = pd.read_parquet(PARQUET_PATH)
        ok(f"Parquet читається без помилок")
        return df
    except Exception as e:
        fail(f"Помилка читання parquet: {e}")
        sys.exit(1)


def check_structure(df: pd.DataFrame) -> None:
    section("2. СТРУКТУРА")

    ok(f"Рядків: {len(df):,}")
    ok(f"Колонок: {len(df.columns)}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        for c in missing:
            fail(f"Відсутня обов'язкова колонка: '{c}'")
    else:
        ok("Всі обов'язкові колонки присутні")

    print(f"\n  Колонки та dtype:")
    for col in df.columns:
        print(f"    {col:<35s} {str(df[col].dtype)}")


def check_dtypes(df: pd.DataFrame) -> None:
    section("3. ТИПИ ДАНИХ")

    for col in FLOAT_COLS:
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if actual == "float32":
            ok(f"{col}: float32")
        elif actual == "float64":
            warn(f"{col}: float64 — зайва RAM, краще float32")
        else:
            fail(f"{col}: {actual} — має бути float32")

    counter_cols = [
        "isw_report_length", "word_count", "sentence_count",
        "paragraph_count", "isw_sources_count", "unique_domains",
        "attack_mentions", "ground_mentions", "casualty_mentions",
        "total_intensity",
    ]
    for col in counter_cols:
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if actual in ("int16", "int32", "int64"):
            if actual == "int16":
                max_val = df[col].max()
                if max_val > 32000:
                    fail(f"{col}: int16 але max={max_val} — переповнення!")
                else:
                    ok(f"{col}: int16 (max={max_val})")
            else:
                ok(f"{col}: {actual}")
        else:
            warn(f"{col}: {actual} — очікувався int")

    ram_mb = df.memory_usage(deep=True).sum() / 1024**2
    ok(f"Розмір в RAM: {ram_mb:.1f} MB")


def check_nulls(df: pd.DataFrame) -> None:
    section("4. ПРОПУЩЕНІ ЗНАЧЕННЯ")

    critical = ["date", "isw_report_length", "total_intensity"]
    for col in critical:
        if col not in df.columns:
            continue
        n = df[col].isna().sum()
        if n == 0:
            ok(f"{col}: 0 NaN")
        else:
            fail(f"{col}: {n:,} NaN — критично!")

    for col in df.columns:
        if col in critical:
            continue
        n = df[col].isna().sum()
        pct = n / len(df) * 100
        if n == 0:
            ok(f"{col}: 0 NaN")
        elif pct < 10:
            warn(f"{col}: {n:,} NaN ({pct:.1f}%) — прийнятно")
        else:
            fail(f"{col}: {n:,} NaN ({pct:.1f}%) — забагато")


def check_dates(df: pd.DataFrame) -> None:
    section("5. ДАТИ")

    df["date"] = pd.to_datetime(df["date"])

    ok(f"Від: {df.date.min().date()}")
    ok(f"До:  {df.date.max().date()}")

    pre_war = (df.date < WAR_START).sum()
    if pre_war == 0:
        ok("Записів до 24.02.2022: 0")
    else:
        fail(f"Записів до 24.02.2022: {pre_war} — мають бути видалені!")

    future = (df.date > pd.Timestamp.now()).sum()
    if future == 0:
        ok("Записів з майбутньою датою: 0")
    else:
        fail(f"Записів з майбутньою датою: {future}")

    dupes = df.duplicated(subset=["date"]).sum()
    if dupes == 0:
        ok("Дублікатів дат: 0")
    else:
        fail(f"Дублікатів дат: {dupes} — кожен день має бути один раз!")

    sorted_ok = df["date"].is_monotonic_increasing
    if sorted_ok:
        ok("Дати відсортовані за зростанням")
    else:
        fail("Дати НЕ відсортовані!")

    all_dates = pd.date_range(df.date.min(), df.date.max(), freq="D")
    missing_dates = sorted(set(all_dates) - set(df.date))
    coverage = (1 - len(missing_dates) / len(all_dates)) * 100

    if coverage >= 95:
        ok(f"Покриття дат: {coverage:.1f}% ({len(df)}/{len(all_dates)} днів)")
    elif coverage >= 85:
        warn(f"Покриття дат: {coverage:.1f}% — {len(missing_dates)} пропущених")
    else:
        fail(f"Покриття дат: {coverage:.1f}% — занадто багато gaps!")

    if missing_dates:
        print(f"\n  Пропущені дати (перші 10):")
        for d in missing_dates[:10]:
            print(f"    {d.date()}")
        if len(missing_dates) > 10:
            print(f"    ... ще {len(missing_dates) - 10}")

    print(f"\n  По роках:")
    for yr, cnt in df.groupby(df.date.dt.year).size().items():
        print(f"    {yr}: {cnt:>4,} звітів")


def check_report_lengths(df: pd.DataFrame) -> None:
    section("6. ДОВЖИНА ЗВІТІВ")

    if "isw_report_length" not in df.columns:
        warn("isw_report_length відсутня")
        return

    rl = df["isw_report_length"]

    too_short = (rl < 200).sum()
    if too_short == 0:
        ok("Звітів < 200 символів: 0")
    else:
        fail(f"Звітів < 200 символів: {too_short} — мали бути відфільтровані!")

    too_long = (rl > 200_000).sum()
    if too_long == 0:
        ok("Звітів > 200k символів: 0")
    else:
        warn(f"Звітів > 200k символів: {too_long} — мали бути обрізані")

    print(f"\n  Статистика довжини (символи):")
    print(f"    min:    {rl.min():>8,}")
    print(f"    p5:     {rl.quantile(0.05):>8,.0f}")
    print(f"    median: {rl.median():>8,.0f}")
    print(f"    mean:   {rl.mean():>8,.0f}")
    print(f"    p95:    {rl.quantile(0.95):>8,.0f}")
    print(f"    max:    {rl.max():>8,}")

    if "word_count" in df.columns:
        wc = df["word_count"]
        print(f"\n  Статистика word_count:")
        print(f"    min:    {wc.min():>8,}")
        print(f"    median: {wc.median():>8,.0f}")
        print(f"    max:    {wc.max():>8,}")

        if wc.max() > 32767 and str(df["word_count"].dtype) == "int16":
            fail(f"word_count max={wc.max()} перевищує int16 ліміт (32767)!")


def check_keyword_features(df: pd.DataFrame) -> None:
    section("7. KEYWORD FEATURES")

    kw_cols = ["attack_mentions", "ground_mentions", "casualty_mentions",
               "total_intensity", "intensity_per_1000"]

    for col in kw_cols:
        if col not in df.columns:
            fail(f"Відсутня: {col}")
            continue
        neg = (df[col] < 0).sum()
        if neg > 0:
            fail(f"{col}: {neg} від'ємних значень!")
        zeros = (df[col] == 0).sum()
        pct_zero = zeros / len(df) * 100
        if pct_zero > 50:
            warn(f"{col}: {pct_zero:.1f}% нулів — підозріло")
        else:
            ok(f"{col}: mean={df[col].mean():.1f}  max={df[col].max():.0f}  zeros={pct_zero:.1f}%")

    if "total_intensity" in df.columns:
        cols_sum = []
        for c in ["attack_mentions", "ground_mentions", "casualty_mentions"]:
            if c in df.columns:
                cols_sum.append(c)
        if len(cols_sum) == 3:
            computed = df[cols_sum].sum(axis=1)
            mismatch = (df["total_intensity"] != computed).sum()
            if mismatch == 0:
                ok("total_intensity = sum(attack+ground+casualty): вірно")
            else:
                fail(f"total_intensity не збігається з сумою компонентів: {mismatch} рядків")

    if "intensity_per_1000" in df.columns and "isw_report_length" in df.columns:
        computed = (df["total_intensity"] / df["isw_report_length"] * 1000).round(2)
        diff = (df["intensity_per_1000"] - computed).abs()
        mismatch = (diff > 0.1).sum()
        if mismatch == 0:
            ok("intensity_per_1000 відповідає формулі")
        else:
            warn(f"intensity_per_1000 розходиться з формулою: {mismatch} рядків")

    print(f"\n  Тренд інтенсивності по роках:")
    df2 = df.copy()
    df2["year"] = pd.to_datetime(df2["date"]).dt.year
    for yr, grp in df2.groupby("year"):
        if "total_intensity" in grp.columns:
            print(f"    {yr}: mean={grp['total_intensity'].mean():.1f}  "
                  f"max={grp['total_intensity'].max():.0f}")


def check_source_features(df: pd.DataFrame) -> None:
    section("8. SOURCE FEATURES")

    source_cols = [
        "isw_sources_count", "sources_resolved", "sources_dead",
        "sources_blocked", "unique_domains",
    ]
    ratio_cols = ["real_dead_ratio", "blackout_score",
                  "ru_ua_balance", "ru_official_ratio"]

    for col in source_cols:
        if col not in df.columns:
            warn(f"Відсутня: {col} (джерела не були зібрані?)")
            continue
        neg = (df[col] < 0).sum()
        zeros = (df[col] == 0).sum()
        if neg > 0:
            fail(f"{col}: {neg} від'ємних значень!")
        elif zeros == len(df):
            warn(f"{col}: всі нулі — джерела не були зібрані")
        else:
            ok(f"{col}: mean={df[col].mean():.1f}")

    for col, (lo, hi) in RATIO_BOUNDS.items():
        if col not in df.columns:
            warn(f"Відсутня ratio колонка: {col}")
            continue
        out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
        if out_of_range == 0:
            ok(f"{col}: всі в [{lo}, {hi}]  mean={df[col].mean():.4f}")
        else:
            fail(f"{col}: {out_of_range} значень поза [{lo}, {hi}]!")


def check_texts_json() -> None:
    section("9. ISW TEXTS JSON")

    if not TEXTS_PATH.exists():
        fail("isw_texts.json не знайдено")
        return

    try:
        with open(TEXTS_PATH, "r", encoding="utf-8") as f:
            texts = json.load(f)
    except Exception as e:
        fail(f"Помилка читання isw_texts.json: {e}")
        return

    ok(f"Текстів: {len(texts):,}")

    empty = sum(1 for v in texts.values() if not v or not v.strip())
    if empty == 0:
        ok("Порожніх текстів: 0")
    else:
        warn(f"Порожніх текстів: {empty}")

    short = sum(1 for v in texts.values() if v and len(v) < 500)
    if short == 0:
        ok("Текстів < 500 символів: 0")
    else:
        warn(f"Текстів < 500 символів: {short}")

    bad_dates = [k for k in texts.keys()
                 if not (len(k) == 10 and k[4] == "-" and k[7] == "-")]
    if not bad_dates:
        ok("Всі ключі в форматі YYYY-MM-DD")
    else:
        fail(f"Невалідні ключі: {bad_dates[:5]}")

    lengths = [len(v) for v in texts.values() if v]
    if lengths:
        print(f"\n  Статистика довжини текстів:")
        print(f"    min:    {min(lengths):>8,}")
        print(f"    median: {sorted(lengths)[len(lengths)//2]:>8,}")
        print(f"    max:    {max(lengths):>8,}")


def check_tfidf() -> None:
    section("10. TF-IDF МАТРИЦЯ (EDA)")

    if not TFIDF_NPZ.exists():
        warn("isw_tfidf_matrix.npz не знайдено")
        return
    if not TFIDF_VOCAB.exists():
        warn("isw_tfidf_vocab.json не знайдено")
        return

    try:
        import scipy.sparse
        matrix = scipy.sparse.load_npz(TFIDF_NPZ)
        ok(f"Матриця: {matrix.shape[0]:,} × {matrix.shape[1]:,}")
    except Exception as e:
        fail(f"Помилка читання NPZ: {e}")
        return

    try:
        with open(TFIDF_VOCAB, "r", encoding="utf-8") as f:
            vocab = json.load(f)
        ok(f"Словник: {len(vocab):,} термінів")
    except Exception as e:
        fail(f"Помилка читання vocab: {e}")
        return

    if matrix.shape[1] != len(vocab):
        fail(f"Розмір матриці {matrix.shape[1]} ≠ словник {len(vocab)}")
    else:
        ok("Розмір матриці відповідає словнику")

    war_terms = ["attack", "strike", "missile", "drone", "forces",
                 "russian", "ukrainian", "artillery", "offense", "defense"]
    found   = [w for w in war_terms if w in vocab]
    missing = [w for w in war_terms if w not in vocab]
    ok(f"Військові терміни у vocab: {found}")
    if missing:
        warn(f"Відсутні терміни (можливо, відфільтровані): {missing}")

    mean_weights = np.asarray(matrix.mean(axis=0)).flatten()
    top10_idx = mean_weights.argsort()[::-1][:10]
    top10 = [(vocab[i], round(float(mean_weights[i]), 4)) for i in top10_idx]
    print(f"\n  Топ-10 термінів за середньою вагою:")
    for term, weight in top10:
        print(f"    {term:<30s} {weight:.4f}")

    sparsity = 1 - matrix.nnz / (matrix.shape[0] * matrix.shape[1])
    ok(f"Розрідженість: {sparsity*100:.1f}%")


def check_features_for_merge() -> None:
    section("11. ISW FEATURES FOR MERGE (D+1 shift)")

    if not FEATURES_PATH.exists():
        warn("isw_features_for_merge.csv не знайдено — запусти isw_nlp_pipeline.py")
        return

    try:
        df = pd.read_csv(FEATURES_PATH)
        ok(f"Читається: {df.shape[0]:,} рядків × {df.shape[1]} колонок")
    except Exception as e:
        fail(f"Помилка читання: {e}")
        return

    if "alarm_date" not in df.columns:
        fail("Відсутня 'alarm_date' — D+1 shift не застосовано!")
    else:
        ok("Колонка 'alarm_date' присутня (D+1 shift)")

    if "date" in df.columns and "alarm_date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df["alarm_date"] = pd.to_datetime(df["alarm_date"])
        shift_days = (df["alarm_date"] - df["date"]).dt.days
        if (shift_days == 1).all():
            ok("D+1 shift: alarm_date = date + 1 день для всіх рядків")
        else:
            wrong = (shift_days != 1).sum()
            fail(f"D+1 shift невірний для {wrong} рядків!")

    print(f"\n  Колонки features_for_merge:")
    for col in df.columns:
        print(f"    {col}")


def check_consistency_with_texts(df: pd.DataFrame) -> None:
    section("12. КОНСИСТЕНТНІСТЬ parquet vs texts.json")

    if not TEXTS_PATH.exists():
        warn("isw_texts.json не знайдено — пропускаємо перевірку")
        return

    try:
        with open(TEXTS_PATH, "r", encoding="utf-8") as f:
            texts = json.load(f)
    except Exception:
        warn("Не вдалось прочитати isw_texts.json")
        return

    df_dates = set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"))
    text_dates = set(texts.keys())

    in_parquet_not_texts = df_dates - text_dates
    in_texts_not_parquet = text_dates - df_dates

    if not in_parquet_not_texts:
        ok("Всі дати з parquet є в texts.json")
    else:
        fail(f"В parquet але не в texts.json: {len(in_parquet_not_texts)} дат")
        print(f"    Приклад: {list(in_parquet_not_texts)[:5]}")

    if not in_texts_not_parquet:
        ok("Всі дати з texts.json є в parquet")
    else:
        warn(f"В texts.json але не в parquet: {len(in_texts_not_parquet)} дат")

    if "isw_report_length" in df.columns:
        sample_mismatch = 0
        for _, row in df.head(20).iterrows():
            date_str = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
            text = texts.get(date_str, "")
            if text and abs(len(text) - row["isw_report_length"]) > 10:
                sample_mismatch += 1
        if sample_mismatch == 0:
            ok("isw_report_length відповідає довжині текстів (вибірка 20)")
        else:
            warn(f"isw_report_length розходиться з текстами: {sample_mismatch}/20")


def print_summary() -> None:
    section("ПІДСУМОК")

    print(f"  {PASS} Пройдено:        {len(passed)}")
    print(f"  {WARN} Попереджень:     {len(warnings)}")
    print(f"  {FAIL} Критичних:       {len(errors)}")

    if errors:
        print(f"\n  {FAIL} КРИТИЧНІ ПОМИЛКИ:")
        for e in errors:
            print(f"    → {e}")

    if warnings:
        print(f"\n  {WARN} ПОПЕРЕДЖЕННЯ:")
        for w in warnings:
            print(f"    → {w}")

    print()
    if not errors:
        print(f"  {PASS} ISW ДАНІ ВАЛІДНІ — можна запускати merge_datasets.py")
    else:
        print(f"  {FAIL} ISW ДАНІ МАЮТЬ ПРОБЛЕМИ — виправи перед мерджем!")
    print(f"{'=' * 65}\n")


# --------------------------------------------------------------------------- #

def main() -> None:
    print(f"\n{'=' * 65}")
    print(f"  AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА ISW ДАНИХ")
    print(f"{'=' * 65}")

    df = check_files()
    check_structure(df)
    check_dtypes(df)
    check_nulls(df)
    check_dates(df)
    check_report_lengths(df)
    check_keyword_features(df)
    check_source_features(df)
    check_texts_json()
    check_tfidf()
    check_features_for_merge()
    check_consistency_with_texts(df)
    print_summary()


if __name__ == "__main__":
    main()