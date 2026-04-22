"""
AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА ПОГОДНИХ ДАНИХ
Запуск: python validate_weather.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
PARQUET_PATH   = PROJECT_ROOT / "data" / "processed" / "weather_clean.parquet"
WAR_START      = pd.Timestamp("2022-02-24")

EXPECTED_REGIONS = [
    "Vinnytsia", "Lutsk",    "Dnipro",    "Donetsk",   "Zhytomyr",
    "Uzhgorod",  "Zaporozhye", "Ivano-Frankivsk", "Kyiv",
    "Kropyvnytskyi", "Lviv", "Mykolaiv",  "Odesa",
    "Poltava",   "Rivne",   "Sumy",      "Ternopil",  "Kharkiv",
    "Kherson",   "Khmelnytskyi", "Cherkasy", "Chernivtsi",
    "Chernihiv", "Luhansk",
]

RANGES = {
    "hour_temp":       (-50.0,  50.0),
    "hour_humidity":   (  0.0, 100.0),
    "hour_windspeed":  (  0.0, 200.0),
    "hour_pressure":   (870.0,1084.0),
    "hour_cloudcover": (  0.0, 100.0),
    "hour_visibility": (  0.0,  50.0),
    "hour_winddir":    (  0.0, 360.0),
    "hour_precip":     (  0.0, 200.0),
    "hour_windgust":   (  0.0, 300.0),
    "hour_feelslike":  (-60.0,  60.0),
}

REQUIRED_COLS = [
    "city_address", "datetime_hour",
    "hour_temp", "hour_humidity", "hour_pressure", "hour_windspeed",
    "hour_visibility", "hour_cloudcover",
    "is_night", "is_rain", "is_snow",
    "season", "hour", "day_of_week", "month",
]

EXPECTED_DTYPES = {
    "season":      "int8",
    "is_night":    "int8",
    "is_rain":     "int8",
    "is_snow":     "int8",
    "hour":        "int8",
    "day_of_week": "int8",
    "month":       "int8",
}

FLOAT_COLS = list(RANGES.keys()) + [
    "hour_dew", "hour_precipprob", "hour_snow", "hour_snowdepth",
    "hour_feelslike", "hour_windgust",
    "day_temp", "day_tempmax", "day_tempmin", "day_humidity",
    "day_precip", "day_windspeed", "day_cloudcover", "day_visibility",
    "temp_diff", "pressure_trend",
]

UKRAINE_LAT = (44.0, 53.0)
UKRAINE_LON = (22.0, 41.0)

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


def check_file() -> pd.DataFrame:
    section("1. ФАЙЛ")

    if not PARQUET_PATH.exists():
        fail(f"Файл не знайдено: {PARQUET_PATH}")
        sys.exit(1)

    size_mb = PARQUET_PATH.stat().st_size / 1024**2
    ok(f"Файл існує: {PARQUET_PATH.name}")
    ok(f"Розмір на диску: {size_mb:.2f} MB")

    try:
        df = pd.read_parquet(PARQUET_PATH)
        ok(f"Читається без помилок")
        ram_mb = df.memory_usage(deep=True).sum() / 1024**2
        ok(f"Розмір в RAM: {ram_mb:.1f} MB")
        ok(f"Стиснення: {ram_mb/size_mb:.1f}x (RAM/диск)")
        return df
    except Exception as e:
        fail(f"Помилка читання: {e}")
        sys.exit(1)


def check_structure(df: pd.DataFrame) -> None:
    section("2. СТРУКТУРА")

    ok(f"Рядків: {len(df):,}")
    ok(f"Колонок: {len(df.columns)}")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        for c in missing:
            fail(f"Відсутня обов'язкова колонка: '{c}'")
    else:
        ok("Всі обов'язкові колонки присутні")

    obj_cols = [c for c in df.columns
                if df[c].dtype == "object"
                and c not in ("city_address",)]
    if obj_cols:
        fail(f"Object dtype колонки (модель впаде): {obj_cols}")
    else:
        ok("Жодної object dtype колонки (крім city_address)")

    print(f"\n  Всі колонки:")
    for col in df.columns:
        nans = df[col].isna().sum()
        nan_str = f"NaN:{nans:>8,}" if nans > 0 else ""
        print(f"    {col:<35s} {str(df[col].dtype):<12s} {nan_str}")


def check_dtypes(df: pd.DataFrame) -> None:
    section("3. ТИПИ ДАНИХ")

    for col, expected in EXPECTED_DTYPES.items():
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if actual == expected:
            ok(f"{col}: {actual}")
        else:
            warn(f"{col}: маємо {actual}, краще {expected}")

    for col in FLOAT_COLS:
        if col not in df.columns:
            continue
        actual = str(df[col].dtype)
        if actual == "float32":
            ok(f"{col}: float32")
        elif actual == "float64":
            warn(f"{col}: float64 — зайва RAM, краще float32")
        else:
            warn(f"{col}: {actual}")


def check_nulls(df: pd.DataFrame) -> None:
    section("4. ПРОПУЩЕНІ ЗНАЧЕННЯ")

    critical = ["city_address", "datetime_hour", "hour_temp",
                "hour_humidity", "hour_pressure", "hour_windspeed"]
    for col in critical:
        if col not in df.columns:
            continue
        n = df[col].isna().sum()
        if n == 0:
            ok(f"{col}: 0 NaN")
        else:
            fail(f"{col}: {n:,} NaN — критично!")

    print(f"\n  Всі колонки з NaN:")
    for col in df.columns:
        n = df[col].isna().sum()
        if n == 0:
            continue
        pct = n / len(df) * 100
        if pct < 2:
            ok(f"{col}: {n:,} NaN ({pct:.2f}%) — прийнятно")
        elif pct < 10:
            warn(f"{col}: {n:,} NaN ({pct:.1f}%)")
        else:
            fail(f"{col}: {n:,} NaN ({pct:.1f}%) — забагато!")


def check_regions(df: pd.DataFrame) -> None:
    section("5. РЕГІОНИ")

    present  = set(df["city_address"].astype(str).unique())
    missing  = set(EXPECTED_REGIONS) - present
    extra    = present - set(EXPECTED_REGIONS)

    ok(f"Регіонів: {len(present)}/24")

    if missing:
        for r in sorted(missing):
            fail(f"ВІДСУТНІЙ: {r}")
    else:
        ok("Всі 24 регіони присутні")

    if extra:
        for r in sorted(extra):
            fail(f"ЗАЙВИЙ (не очікувався): {r}")
    else:
        ok("Зайвих регіонів немає")

    print(f"\n  Рядків по регіонах:")
    counts = df.groupby("city_address", observed=False).size().sort_values()
    for reg, cnt in counts.items():
        bar = "█" * (cnt // 5000)
        print(f"    {reg:<25s} {cnt:>8,}  {bar}")

    min_reg = counts.min()
    max_reg = counts.max()
    ratio   = max_reg / max(min_reg, 1)
    if ratio > 2:
        warn(f"Дисбаланс регіонів: {ratio:.1f}x (min={min_reg:,} max={max_reg:,})")
    else:
        ok(f"Баланс регіонів: {ratio:.1f}x")


def check_dates(df: pd.DataFrame) -> None:
    section("6. ДАТИ І ЧАС")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])

    ok(f"Від: {df.datetime_hour.min()}")
    ok(f"До:  {df.datetime_hour.max()}")

    pre_war = (df.datetime_hour < WAR_START).sum()
    if pre_war == 0:
        ok("Записів до 24.02.2022: 0")
    else:
        fail(f"Записів до 24.02.2022: {pre_war:,}")

    future = (df.datetime_hour > pd.Timestamp.now()).sum()
    if future == 0:
        ok("Записів з майбутнім часом: 0")
    else:
        warn(f"Записів з майбутнім часом: {future:,}")

    dupes = df.duplicated(subset=["city_address", "datetime_hour"]).sum()
    if dupes == 0:
        ok("Дублікатів (city, datetime): 0")
    else:
        fail(f"Дублікатів: {dupes:,}")

    print(f"\n  По роках:")
    for yr, cnt in df.groupby(df.datetime_hour.dt.year).size().items():
        print(f"    {yr}: {cnt:>8,}")

    print(f"\n  Перевірка повноти (24 год/день/регіон):")
    df["_date"] = df.datetime_hour.dt.date
    completeness = df.groupby(["city_address", "_date"], observed=False).size()
    full_days    = (completeness == 24).sum()
    partial_days = (completeness != 24).sum()
    if partial_days == 0:
        ok(f"Всі дні повні (24h): {full_days:,}")
    else:
        warn(f"Повних днів: {full_days:,} | Неповних: {partial_days:,}")
        bad = completeness[completeness != 24]
        print(f"    Приклади неповних:")
        for (reg, d), cnt in bad.head(5).items():
            print(f"      {reg} {d}: {cnt}h")
    df.drop(columns=["_date"], inplace=True)


def check_ranges(df: pd.DataFrame) -> None:
    section("7. ДІАПАЗОНИ ЗНАЧЕНЬ")

    for col, (lo, hi) in RANGES.items():
        if col not in df.columns:
            warn(f"Відсутня: {col}")
            continue

        series = pd.to_numeric(df[col], errors="coerce")
        out_of_range = series.notna() & ((series < lo) | (series > hi))
        n_out = out_of_range.sum()
        n_nan = series.isna().sum()

        if n_out == 0:
            ok(f"{col}: всі в [{lo}, {hi}]  "
               f"mean={series.mean():.1f}  "
               f"min={series.min():.1f}  "
               f"max={series.max():.1f}")
        else:
            fail(f"{col}: {n_out:,} поза [{lo}, {hi}]  "
                 f"min={series.min():.1f}  max={series.max():.1f}")

        if n_nan > 0:
            pct = n_nan / len(df) * 100
            if pct > 5:
                warn(f"  {col}: {n_nan:,} NaN ({pct:.1f}%)")


def check_derived_features(df: pd.DataFrame) -> None:
    section("8. ПОХІДНІ ФІЧІ")

    if "hour" in df.columns and "datetime_hour" in df.columns:
        df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])
        computed = df.datetime_hour.dt.hour.astype("int8")
        mismatch = (df["hour"] != computed).sum()
        if mismatch == 0:
            ok("hour відповідає datetime_hour.hour")
        else:
            fail(f"hour не відповідає: {mismatch:,} рядків")

    if "month" in df.columns:
        computed = df.datetime_hour.dt.month.astype("int8")
        mismatch = (df["month"] != computed).sum()
        if mismatch == 0:
            ok("month відповідає datetime_hour.month")
        else:
            fail(f"month не відповідає: {mismatch:,} рядків")

    if "is_night" in df.columns:
        computed = (df.datetime_hour.dt.hour < 6).astype("int8")
        mismatch = (df["is_night"] != computed).sum()
        if mismatch == 0:
            ok("is_night відповідає hour < 6")
        else:
            fail(f"is_night не відповідає: {mismatch:,} рядків")

    if "season" in df.columns:
        season_map = {12:1,1:1,2:1,3:2,4:2,5:2,6:3,7:3,8:3,9:4,10:4,11:4}
        computed = df.datetime_hour.dt.month.map(season_map).astype("int8")
        mismatch = (df["season"] != computed).sum()
        if mismatch == 0:
            ok("season відповідає місяцю")
        else:
            fail(f"season не відповідає: {mismatch:,} рядків")

    for col in ["is_night","is_rain","is_snow","season","hour","day_of_week","month"]:
        if col not in df.columns:
            continue
        unique = set(df[col].unique())
        expected = {0,1} if col in ("is_night","is_rain","is_snow") else None
        if expected and not unique <= expected:
            fail(f"{col}: неочікувані значення {unique - expected}")
        else:
            ok(f"{col}: значення коректні {sorted(unique)[:5]}")

    if "temp_diff" in df.columns:
        neg = (df.temp_diff < -30).sum()
        pos = (df.temp_diff > 30).sum()
        if neg == 0 and pos == 0:
            ok(f"temp_diff: в межах [-30, 30]  mean={df.temp_diff.mean():.1f}")
        else:
            warn(f"temp_diff: {neg} < -30, {pos} > 30")


def check_statistics(df: pd.DataFrame) -> None:
    section("9. СТАТИСТИКА")

    if len(df) < 100_000:
        warn(f"Рядків: {len(df):,} — очікується 500k+")
    else:
        ok(f"Рядків: {len(df):,}")

    print(f"\n  Статистика ключових колонок:")
    stat_cols = ["hour_temp", "hour_humidity", "hour_pressure",
                 "hour_windspeed", "hour_visibility"]
    for col in stat_cols:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        print(f"    {col:<20s}  "
              f"min={s.min():>7.1f}  "
              f"mean={s.mean():>7.1f}  "
              f"max={s.max():>7.1f}  "
              f"NaN={s.isna().sum():>6,}")

    print(f"\n  Розподіл по годинах (is_night):")
    if "is_night" in df.columns:
        night_pct = df.is_night.mean() * 100
        ok(f"is_night=1: {night_pct:.1f}% (очікується ~25%)")

    print(f"\n  Середня температура по сезонах:")
    if "season" in df.columns and "hour_temp" in df.columns:
        season_names = {1:"Зима", 2:"Весна", 3:"Літо", 4:"Осінь"}
        for s, name in season_names.items():
            mask = df.season == s
            mean_t = pd.to_numeric(df.loc[mask, "hour_temp"], errors="coerce").mean()
            print(f"    {name}: {mean_t:.1f}°C")


def check_geography(df: pd.DataFrame) -> None:
    section("10. ГЕОГРАФІЯ")

    lat_col = next((c for c in df.columns if "latitude" in c.lower()), None)
    lon_col = next((c for c in df.columns if "longitude" in c.lower()), None)

    if not lat_col or not lon_col:
        warn("Колонки latitude/longitude відсутні (видалені при очищенні — нормально)")
        return

    lat = pd.to_numeric(df[lat_col], errors="coerce")
    lon = pd.to_numeric(df[lon_col], errors="coerce")

    outside_lat = lat.notna() & ~lat.between(*UKRAINE_LAT)
    outside_lon = lon.notna() & ~lon.between(*UKRAINE_LON)
    outside = outside_lat | outside_lon

    if outside.sum() == 0:
        ok(f"Всі координати в межах України")
    else:
        fail(f"Координати поза Україною: {outside.sum():,} рядків")
        bad = df.loc[outside, "city_address"].value_counts().head(5)
        for city, cnt in bad.items():
            print(f"    {city}: {cnt:,}")


def check_parquet_metadata() -> None:
    section("11. PARQUET МЕТАДАНІ")

    try:
        import pyarrow.parquet as pq
        meta = pq.read_metadata(PARQUET_PATH)
        ok(f"Row groups:  {meta.num_row_groups}")
        ok(f"Рядків:      {meta.num_rows:,}")
        ok(f"Колонок:     {meta.num_columns}")
        size_mb  = PARQUET_PATH.stat().st_size / 1024**2
        df_tmp   = pd.read_parquet(PARQUET_PATH)
        ram_mb   = df_tmp.memory_usage(deep=True).sum() / 1024**2
        ok(f"Стиснення:   {ram_mb/size_mb:.1f}x")
    except ImportError:
        warn("pyarrow не встановлено")
    except Exception as e:
        warn(f"Не вдалось прочитати метадані: {e}")


def check_merge_readiness(df: pd.DataFrame) -> None:
    section("12. ГОТОВНІСТЬ ДО MERGE")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])

    merge_key_cols = ["city_address", "datetime_hour"]
    for col in merge_key_cols:
        if col in df.columns:
            ok(f"Merge key '{col}' присутній")
        else:
            fail(f"Merge key '{col}' відсутній!")

    dupes = df.duplicated(subset=merge_key_cols).sum()
    if dupes == 0:
        ok("Дублікатів по merge key: 0")
    else:
        fail(f"Дублікатів по merge key: {dupes:,} — merge буде некоректним!")

    core_weather = ["hour_temp", "hour_humidity",
                    "hour_pressure", "hour_windspeed"]
    for col in core_weather:
        if col not in df.columns:
            fail(f"Відсутня core weather col: {col}")
            continue
        nan_pct = df[col].isna().mean() * 100
        if nan_pct == 0:
            ok(f"{col}: 0% NaN — готово до merge")
        elif nan_pct < 1:
            warn(f"{col}: {nan_pct:.2f}% NaN — прийнятно")
        else:
            fail(f"{col}: {nan_pct:.1f}% NaN — fill перед merge!")

    min_dt = df.datetime_hour.min().date()
    max_dt = df.datetime_hour.max().date()
    print(f"\n  Доступний діапазон для merge:")
    print(f"    {min_dt} → {max_dt}")
    print(f"    Днів: {(df.datetime_hour.max() - df.datetime_hour.min()).days:,}")


def print_summary() -> None:
    section("ПІДСУМОК")

    print(f"  {PASS} Пройдено:    {len(passed)}")
    print(f"  {WARN} Попереджень: {len(warnings)}")
    print(f"  {FAIL} Критичних:   {len(errors)}")

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
        print(f"  {PASS} ПОГОДА ВАЛІДНА — можна запускати merge_datasets.py")
    else:
        print(f"  {FAIL} ПОГОДА МАЄ ПРОБЛЕМИ — виправи перед мерджем!")
    print(f"{'=' * 65}\n")


# --------------------------------------------------------------------------- #

def main() -> None:
    print(f"\n{'=' * 65}")
    print(f"  AEGIS — МАКСИМАЛЬНА ПЕРЕВІРКА weather_clean.parquet")
    print(f"{'=' * 65}")

    df = check_file()
    check_structure(df)
    check_dtypes(df)
    check_nulls(df)
    check_regions(df)
    check_dates(df)
    check_ranges(df)
    check_derived_features(df)
    check_statistics(df)
    check_geography(df)
    check_parquet_metadata()
    check_merge_readiness(df)
    print_summary()


if __name__ == "__main__":
    main()