"""
AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА ЦІЛІСНОСТІ ДАНИХ
Запуск: python validate_alarms.py
"""

import sys
from pathlib import Path
from datetime import timedelta

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "alarms_clean.parquet"

WAR_START = pd.Timestamp("2022-02-24")

EXPECTED_REGIONS: set[str] = {
    "Vinnytsia Oblast",       "Volyn Oblast",
    "Dnipropetrovsk Oblast",  "Donetsk Oblast",
    "Zhytomyr Oblast",        "Zakarpattia Oblast",
    "Zaporizhzhia Oblast",    "Ivano-Frankivsk Oblast",
    "Kyiv Oblast",            "City of Kyiv",
    "Kirovohrad Oblast",      "Lviv Oblast",
    "Mykolaiv Oblast",        "Odesa Oblast",
    "Poltava Oblast",         "Rivne Oblast",
    "Sumy Oblast",            "Ternopil Oblast",
    "Kharkiv Oblast",         "Kherson Oblast",
    "Khmelnytskyi Oblast",    "Cherkasy Oblast",
    "Chernivtsi Oblast",      "Chernihiv Oblast",
}

FRONTLINE: set[str] = {
    "Kharkiv Oblast", "Donetsk Oblast", "Sumy Oblast",
    "Zaporizhzhia Oblast", "Kherson Oblast",
}

REQUIRED_COLUMNS = [
    "region", "start_dt", "end_dt", "duration_min",
    "date", "hour", "day_of_week", "day_name",
    "month", "year", "is_weekend", "is_frontline",
    "is_night", "time_of_day",
]

EXPECTED_DTYPES = {
    "is_weekend":   "int8",
    "is_frontline": "int8",
    "is_night":     "int8",
    "hour":         "int8",
    "day_of_week":  "int8",
    "month":        "int8",
    "year":         "int16",
    "duration_min": "float32",
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

def check_file_exists() -> pd.DataFrame:
    section("1. ФАЙЛ")
    if not PARQUET_PATH.exists():
        fail(f"Файл не знайдено: {PARQUET_PATH}")
        sys.exit(1)

    size_mb = PARQUET_PATH.stat().st_size / 1024 ** 2
    ok(f"Файл існує: {PARQUET_PATH.name}")
    ok(f"Розмір на диску: {size_mb:.2f} MB")

    try:
        df = pd.read_parquet(PARQUET_PATH)
        ok(f"Читається без помилок")
        return df
    except Exception as e:
        fail(f"Помилка читання: {e}")
        sys.exit(1)


def check_structure(df: pd.DataFrame) -> None:
    section("2. СТРУКТУРА")

    ok(f"Рядків: {len(df):,}")
    ok(f"Колонок: {len(df.columns)}")

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        for c in missing_cols:
            fail(f"Відсутня обов'язкова колонка: '{c}'")
    else:
        ok("Всі обов'язкові колонки присутні")

    extra_cols = [c for c in df.columns if c not in REQUIRED_COLUMNS
                  and c not in ("alarm_source", "original_alarms")]
    if extra_cols:
        warn(f"Додаткові колонки (не критично): {extra_cols}")

    print(f"\n  Dtype колонок:")
    for col in df.columns:
        print(f"    {col:<25s} {str(df[col].dtype)}")


def check_dtypes(df: pd.DataFrame) -> None:
    section("3. ТИПИ ДАНИХ")

    for col, expected in EXPECTED_DTYPES.items():
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if actual == expected:
            ok(f"{col}: {actual}")
        else:
            warn(f"{col}: очікувалось {expected}, маємо {actual} — зайва RAM")

    ram_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
    ok(f"Розмір в RAM: {ram_mb:.1f} MB")


def check_nulls(df: pd.DataFrame) -> None:
    section("4. ПРОПУЩЕНІ ЗНАЧЕННЯ")

    critical_no_null = ["region", "start_dt", "duration_min", "hour", "year"]
    for col in critical_no_null:
        if col not in df.columns:
            continue
        n = df[col].isna().sum()
        if n == 0:
            ok(f"{col}: 0 NaN")
        else:
            fail(f"{col}: {n:,} NaN — критично!")

    nullable_cols = [c for c in df.columns if c not in critical_no_null]
    for col in nullable_cols:
        n = df[col].isna().sum()
        pct = n / len(df) * 100
        if n == 0:
            ok(f"{col}: 0 NaN")
        elif pct < 5:
            warn(f"{col}: {n:,} NaN ({pct:.1f}%) — прийнятно")
        else:
            fail(f"{col}: {n:,} NaN ({pct:.1f}%) — забагато")


def check_regions(df: pd.DataFrame) -> None:
    section("5. РЕГІОНИ")

    present = set(df["region"].astype(str).unique())
    missing = EXPECTED_REGIONS - present
    extra   = present - EXPECTED_REGIONS

    ok(f"Регіонів в даних: {len(present)}/24")

    if missing:
        for r in sorted(missing):
            fail(f"ВІДСУТНІЙ регіон: {r}")
    else:
        ok("Всі 24 регіони присутні")

    if extra:
        for r in sorted(extra):
            fail(f"ЗАЙВИЙ регіон (не очікувався): {r}")
    else:
        ok("Зайвих регіонів немає")

    print(f"\n  Кількість тривог по регіонах:")
    counts = df.groupby("region", observed=False).size().sort_values(ascending=False)
    for reg, cnt in counts.items():
        front = " ← FRONTLINE" if reg in FRONTLINE else ""
        bar   = "█" * (cnt // 200)
        print(f"    {reg:<35s} {cnt:>5,}  {bar}{front}")


def check_dates(df: pd.DataFrame) -> None:
    section("6. ДАТИ І ЧАС")

    df["start_dt"] = pd.to_datetime(df["start_dt"])
    df["end_dt"]   = pd.to_datetime(df["end_dt"])

    min_dt = df["start_dt"].min()
    max_dt = df["start_dt"].max()
    ok(f"Діапазон: {min_dt.date()} → {max_dt.date()}")

    pre_war = (df["start_dt"] < WAR_START).sum()
    if pre_war == 0:
        ok(f"Записів до 24.02.2022: 0")
    else:
        fail(f"Записів до 24.02.2022: {pre_war:,} — мають бути видалені!")

    future = (df["start_dt"] > pd.Timestamp.now()).sum()
    if future == 0:
        ok("Записів з майбутнім часом: 0")
    else:
        fail(f"Записів з майбутнім часом: {future:,}")

    end_before_start = (df["end_dt"] < df["start_dt"]).sum()
    if end_before_start == 0:
        ok("end_dt >= start_dt: завжди")
    else:
        fail(f"end_dt < start_dt: {end_before_start:,} записів — баг!")

    print(f"\n  По роках:")
    for yr, cnt in df.groupby("year").size().items():
        print(f"    {yr}: {cnt:>6,}")

    print(f"\n  По місяцях (останні 12):")
    df["ym"] = df["start_dt"].dt.to_period("M")
    monthly = df.groupby("ym").size().tail(12)
    for ym, cnt in monthly.items():
        print(f"    {ym}: {cnt:>5,}")


def check_duration(df: pd.DataFrame) -> None:
    section("7. ТРИВАЛІСТЬ ТРИВОГ")

    dur = df["duration_min"]

    neg = (dur <= 0).sum()
    if neg == 0:
        ok("Від'ємна тривалість: 0")
    else:
        fail(f"Від'ємна або нульова тривалість: {neg:,}")

    very_short = (dur < 1).sum()
    if very_short:
        warn(f"Тривалість < 1 хв: {very_short:,} — можливий сміт")
    else:
        ok("Тривалість < 1 хв: 0")

    very_long = (dur > 1440).sum()
    if very_long:
        warn(f"Тривалість > 24h: {very_long:,} — фронтові регіони, ок")
    else:
        ok("Тривалість > 24h: 0")

    print(f"\n  Статистика тривалості (хвилини):")
    print(f"    min:    {dur.min():.1f}")
    print(f"    p5:     {dur.quantile(0.05):.1f}")
    print(f"    median: {dur.median():.1f}")
    print(f"    mean:   {dur.mean():.1f}")
    print(f"    p95:    {dur.quantile(0.95):.1f}")
    print(f"    p99:    {dur.quantile(0.99):.1f}")
    print(f"    max:    {dur.max():.1f}")

    print(f"\n  Середня тривалість по регіонах (топ-5 найдовших):")
    top = df.groupby("region", observed=False)["duration_min"].mean().sort_values(ascending=False).head(5)
    for reg, val in top.items():
        print(f"    {reg:<35s} {val:.1f} хв")


def check_duplicates(df: pd.DataFrame) -> None:
    section("8. ДУБЛІКАТИ")

    dupes = df.duplicated(subset=["region", "start_dt"]).sum()
    if dupes == 0:
        ok("Дублікатів (region, start_dt): 0")
    else:
        fail(f"Дублікатів: {dupes:,} — треба прибрати!")

    full_dupes = df.duplicated().sum()
    if full_dupes == 0:
        ok("Повних дублікатів рядків: 0")
    else:
        warn(f"Повних дублікатів: {full_dupes:,}")


def check_binary_flags(df: pd.DataFrame) -> None:
    section("9. БІНАРНІ ПРАПОРЦІ")

    binary_cols = ["is_weekend", "is_frontline", "is_night"]
    for col in binary_cols:
        if col not in df.columns:
            continue
        unique_vals = set(df[col].unique())
        if unique_vals <= {0, 1}:
            pct = df[col].mean() * 100
            ok(f"{col}: тільки {{0,1}}, позитивних={pct:.1f}%")
        else:
            fail(f"{col}: неочікувані значення {unique_vals}")

    if "is_frontline" in df.columns:
        fl_regions = set(df[df["is_frontline"] == 1]["region"].unique())
        if fl_regions == FRONTLINE:
            ok(f"is_frontline правильно позначено для всіх 5 фронтових регіонів")
        else:
            wrong = fl_regions.symmetric_difference(FRONTLINE)
            fail(f"is_frontline не збігається з FRONTLINE: {wrong}")

    if "hour" in df.columns:
        bad_hours = df[~df["hour"].between(0, 23)]["hour"].unique()
        if len(bad_hours) == 0:
            ok("hour: всі значення в [0, 23]")
        else:
            fail(f"hour: неочікувані значення {bad_hours}")

    if "month" in df.columns:
        bad_months = df[~df["month"].between(1, 12)]["month"].unique()
        if len(bad_months) == 0:
            ok("month: всі значення в [1, 12]")
        else:
            fail(f"month: неочікувані значення {bad_months}")

    if "day_of_week" in df.columns:
        bad_dow = df[~df["day_of_week"].between(0, 6)]["day_of_week"].unique()
        if len(bad_dow) == 0:
            ok("day_of_week: всі значення в [0, 6]")
        else:
            fail(f"day_of_week: неочікувані значення {bad_dow}")


def check_consistency(df: pd.DataFrame) -> None:
    section("10. ВНУТРІШНЯ КОНСИСТЕНТНІСТЬ")

    df = df.copy()
    df["start_dt"] = pd.to_datetime(df["start_dt"])

    if "hour" in df.columns:
        computed_hour = df["start_dt"].dt.hour.astype("int8")
        mismatch = (df["hour"] != computed_hour).sum()
        if mismatch == 0:
            ok("hour відповідає start_dt.hour")
        else:
            fail(f"hour не відповідає start_dt.hour: {mismatch:,} рядків")

    if "month" in df.columns:
        computed_month = df["start_dt"].dt.month.astype("int8")
        mismatch = (df["month"] != computed_month).sum()
        if mismatch == 0:
            ok("month відповідає start_dt.month")
        else:
            fail(f"month не відповідає start_dt.month: {mismatch:,} рядків")

    if "year" in df.columns:
        computed_year = df["start_dt"].dt.year.astype("int16")
        mismatch = (df["year"] != computed_year).sum()
        if mismatch == 0:
            ok("year відповідає start_dt.year")
        else:
            fail(f"year не відповідає start_dt.year: {mismatch:,} рядків")

    if "is_night" in df.columns:
        computed_night = (df["start_dt"].dt.hour < 6).astype("int8")
        mismatch = (df["is_night"] != computed_night).sum()
        if mismatch == 0:
            ok("is_night відповідає hour < 6")
        else:
            fail(f"is_night не відповідає hour<6: {mismatch:,} рядків")

    if "is_weekend" in df.columns:
        computed_weekend = df["start_dt"].dt.dayofweek.isin([5, 6]).astype("int8")
        mismatch = (df["is_weekend"] != computed_weekend).sum()
        if mismatch == 0:
            ok("is_weekend відповідає dayofweek in [5,6]")
        else:
            fail(f"is_weekend не відповідає dayofweek: {mismatch:,} рядків")

    if "duration_min" in df.columns and "end_dt" in df.columns:
        computed_dur = ((pd.to_datetime(df["end_dt"]) - df["start_dt"])
                        .dt.total_seconds() / 60).astype("float32")
        diff = (df["duration_min"] - computed_dur).abs()
        big_diff = (diff > 1).sum()
        if big_diff == 0:
            ok("duration_min відповідає end_dt - start_dt")
        else:
            warn(f"duration_min розходиться з end-start більш ніж на 1хв: {big_diff:,} рядків")


def check_statistics(df: pd.DataFrame) -> None:
    section("11. СТАТИСТИЧНА ПЕРЕВІРКА")

    alarm_rate_ok = True

    if len(df) < 1000:
        fail(f"Дуже мало записів: {len(df):,} — очікується 50k+")
        alarm_rate_ok = False
    else:
        ok(f"Кількість записів: {len(df):,}")

    if "region" in df.columns:
        min_per_region = df.groupby("region", observed=False).size().min()
        if min_per_region < 50:
            warn(f"Найменше тривог по регіону: {min_per_region} — підозріло мало")
        else:
            ok(f"Мінімум тривог по регіону: {min_per_region}")

    print(f"\n  Розподіл по часу доби:")
    hour_dist = df.groupby("hour", observed=False).size()
    for h in range(24):
        cnt  = hour_dist.get(h, 0)
        bar  = "█" * (cnt // 300)
        print(f"    {h:02d}:00  {cnt:>5,}  {bar}")

    if "time_of_day" in df.columns:
        print(f"\n  Розподіл time_of_day:")
        for tod, cnt in df["time_of_day"].value_counts(dropna=False).items():
            print(f"    {str(tod):<25s} {cnt:>6,}")


def check_parquet_metadata(df: pd.DataFrame) -> None:
    section("12. PARQUET МЕТАДАНІ")

    try:
        import pyarrow.parquet as pq
        meta = pq.read_metadata(PARQUET_PATH)
        ok(f"Row groups: {meta.num_row_groups}")
        ok(f"Рядків (metadata): {meta.num_rows:,}")
        ok(f"Колонок (metadata): {meta.num_columns}")
        ok(f"Формат: {meta.format_version}")
        size_mb = PARQUET_PATH.stat().st_size / 1024**2
        compression_ratio = (df.memory_usage(deep=True).sum() / 1024**2) / size_mb
        ok(f"Стиснення: {compression_ratio:.1f}x (RAM vs диск)")
    except ImportError:
        warn("pyarrow не встановлено — метадані недоступні")
    except Exception as e:
        warn(f"Не вдалось прочитати метадані: {e}")


def print_final_summary() -> None:
    section("ПІДСУМОК")

    total = len(passed) + len(warnings) + len(errors)
    print(f"  Перевірок пройдено:  {PASS} {len(passed)}")
    print(f"  Попереджень:         {WARN} {len(warnings)}")
    print(f"  Критичних помилок:   {FAIL} {len(errors)}")

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
        print(f"  {PASS} ДАНІ ВАЛІДНІ — можна запускати merge_datasets.py")
    else:
        print(f"  {FAIL} ДАНІ МАЮТЬ ПРОБЛЕМИ — виправи перед мерджем!")

    print(f"{'=' * 65}\n")


# --------------------------------------------------------------------------- #

def main() -> None:
    print(f"\n{'=' * 65}")
    print(f"  AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА alarms_clean.parquet")
    print(f"{'=' * 65}")

    df = check_file_exists()
    check_structure(df)
    check_dtypes(df)
    check_nulls(df)
    check_regions(df)
    check_dates(df)
    check_duration(df)
    check_duplicates(df)
    check_binary_flags(df)
    check_consistency(df)
    check_statistics(df)
    check_parquet_metadata(df)
    print_final_summary()


if __name__ == "__main__":
    main()