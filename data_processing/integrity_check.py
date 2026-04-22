"""
AEGIS DATA INTEGRITY CHECK — MAXIMUM DETAIL
============================================
Полная проверка целостности данных на каждом этапе пайплайна.
Запуск: python integrity_check.py [--parquet PATH] [--verbose]
"""

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback

import numpy as np
import pandas as pd
import scipy.sparse

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# ─────────────────────────── PATHS ───────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = PROJECT_ROOT / "data" / "processed"

WEATHER_PARQUET  = PROCESSED / "weather_clean.parquet"
ALARMS_PARQUET   = PROCESSED / "alarms_clean.parquet"
ISW_PARQUET      = PROCESSED / "isw_features_for_merge.parquet"
TFIDF_NPZ        = PROCESSED / "tfidf_matrix_model.npz"
TFIDF_VOCAB      = PROCESSED / "tfidf_vocab_model.json"
TG_PARQUET       = PROCESSED / "telegram_features_hourly.parquet"
GUR_PARQUET      = PROCESSED / "gur_features_clean.parquet"
MERGED_PARQUET   = PROCESSED / "merged_dataset.parquet"

KYIV_TZ          = "Europe/Kyiv"

# ─────────────────────────── CONSTANTS ───────────────────────
WEATHER_TO_ALARM = {
    "Vinnytsia": "Vinnytsia Oblast", "Lutsk": "Volyn Oblast",
    "Dnipro": "Dnipropetrovsk Oblast", "Donetsk": "Donetsk Oblast",
    "Zhytomyr": "Zhytomyr Oblast", "Uzhgorod": "Zakarpattia Oblast",
    "Zaporozhye": "Zaporizhzhia Oblast", "Ivano-Frankivsk": "Ivano-Frankivsk Oblast",
    "Kyiv": "City of Kyiv", "Kropyvnytskyi": "Kirovohrad Oblast",
    "Lviv": "Lviv Oblast", "Mykolaiv": "Mykolaiv Oblast",
    "Odesa": "Odesa Oblast", "Poltava": "Poltava Oblast",
    "Rivne": "Rivne Oblast", "Sumy": "Sumy Oblast",
    "Ternopil": "Ternopil Oblast", "Kharkiv": "Kharkiv Oblast",
    "Kherson": "Kherson Oblast", "Khmelnytskyi": "Khmelnytskyi Oblast",
    "Cherkasy": "Cherkasy Oblast", "Chernivtsi": "Chernivtsi Oblast",
    "Chernihiv": "Chernihiv Oblast",
}
EXPECTED_REGIONS = set(WEATHER_TO_ALARM.values()) | {"Kyiv Oblast"}
FRONTLINE_REGIONS = {
    "Kharkiv Oblast", "Zaporizhzhia Oblast",
    "Donetsk Oblast", "Kherson Oblast", "Sumy Oblast",
}
WEATHER_CORE_COLS = ["hour_temp", "hour_humidity", "hour_pressure", "hour_windspeed"]
ISW_SCALAR_COLS   = [
    "isw_report_length", "isw_sources_count", "unique_domains",
    "attack_mentions", "ground_mentions", "casualty_mentions",
    "total_intensity", "intensity_per_1000",
    "real_dead_ratio", "blackout_score", "ru_ua_balance", "ru_official_ratio",
]

VISIBILITY_THR  = 5.0
WINDSPEED_THR   = 15.0
FREEZING_THR    = 0.0
ALARM_RATE_MIN  = 2.0
ALARM_RATE_MAX  = 50.0
COMPLETENESS_MIN = 90.0
MIN_TRAIN_ROWS  = 500
TFIDF_MIN_TERMS = 10

# ─────────────────────────── REPORTER ────────────────────────
class CheckReport:
    PASS  = "✅ PASS"
    FAIL  = "❌ FAIL"
    WARN  = "⚠️  WARN"
    INFO  = "ℹ️  INFO"
    SKIP  = "⏭  SKIP"

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.results: List[dict] = []
        self._section = ""

    def section(self, title: str) -> None:
        self._section = title
        sep = "═" * 72
        print(f"\n{sep}")
        print(f"  {title}")
        print(sep)

    def _log(self, status: str, name: str, detail: str = "", extra: str = "") -> None:
        icon = status
        line = f"  {icon}  {name}"
        if detail:
            line += f" — {detail}"
        print(line)
        if extra and self.verbose:
            for ln in extra.strip().split("\n"):
                print(f"         {ln}")
        self.results.append({"section": self._section, "status": status,
                              "name": name, "detail": detail})

    def ok(self, name: str, detail: str = "", extra: str = "") -> None:
        self._log(self.PASS, name, detail, extra)

    def fail(self, name: str, detail: str = "", extra: str = "") -> None:
        self._log(self.FAIL, name, detail, extra)

    def warn(self, name: str, detail: str = "", extra: str = "") -> None:
        self._log(self.WARN, name, detail, extra)

    def info(self, name: str, detail: str = "", extra: str = "") -> None:
        self._log(self.INFO, name, detail, extra)

    def skip(self, name: str, reason: str = "") -> None:
        self._log(self.SKIP, name, reason)

    def check(self, condition: bool, name: str, ok_detail: str = "",
              fail_detail: str = "", extra: str = "") -> bool:
        if condition:
            self.ok(name, ok_detail, extra)
        else:
            self.fail(name, fail_detail, extra)
        return condition

    def summary(self) -> Tuple[int, int, int]:
        passes = sum(1 for r in self.results if r["status"] == self.PASS)
        fails  = sum(1 for r in self.results if r["status"] == self.FAIL)
        warns  = sum(1 for r in self.results if r["status"] == self.WARN)
        sep = "═" * 72
        print(f"\n{sep}")
        print("  ИТОГОВЫЙ ОТЧЁТ".center(72))
        print(sep)
        print(f"  Всего проверок : {len(self.results)}")
        print(f"  ✅ PASS         : {passes}")
        print(f"  ❌ FAIL         : {fails}")
        print(f"  ⚠️  WARN         : {warns}")
        if fails == 0 and warns == 0:
            print("\n  🟢  ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ — ДАТАСЕТ ГОТОВ К ОБУЧЕНИЮ")
        elif fails == 0:
            print(f"\n  🟡  КРИТИЧЕСКИХ ОШИБОК НЕТ, НО ЕСТЬ {warns} ПРЕДУПРЕЖДЕНИЙ")
        else:
            print(f"\n  🔴  КРИТИЧЕСКИЕ ОШИБКИ ({fails}) — ДАТАСЕТ НЕ ГОТОВ")
        print(sep)
        return passes, fails, warns


R = CheckReport()


# ══════════════════════════════════════════════════════════════
# 1. ФАЙЛОВАЯ СИСТЕМА
# ══════════════════════════════════════════════════════════════
def check_filesystem() -> None:
    R.section("1. ФАЙЛОВАЯ СИСТЕМА — наличие и размер файлов")

    required = {
        "weather_clean.parquet":       WEATHER_PARQUET,
        "alarms_clean.parquet":        ALARMS_PARQUET,
        "isw_features_for_merge.parquet": ISW_PARQUET,
        "tfidf_matrix_model.npz":      TFIDF_NPZ,
        "tfidf_vocab_model.json":      TFIDF_VOCAB,
        "merged_dataset.parquet":      MERGED_PARQUET,
    }
    optional = {
        "telegram_features_hourly.parquet": TG_PARQUET,
        "gur_features_clean.parquet":       GUR_PARQUET,
    }

    for label, path in required.items():
        if path.exists():
            size_mb = path.stat().st_size / 1_048_576
            R.ok(f"[REQUIRED] {label}", f"{size_mb:.2f} MB")
            if size_mb < 0.001:
                R.fail(f"[SIZE] {label}", "файл пустой (< 1 KB)")
        else:
            R.fail(f"[REQUIRED] {label}", "ФАЙЛ ОТСУТСТВУЕТ")

    for label, path in optional.items():
        if path.exists():
            size_mb = path.stat().st_size / 1_048_576
            R.ok(f"[OPTIONAL] {label}", f"{size_mb:.2f} MB")
        else:
            R.warn(f"[OPTIONAL] {label}", "не найден — соответствующие признаки будут пропущены")


# ══════════════════════════════════════════════════════════════
# 2. WEATHER
# ══════════════════════════════════════════════════════════════
def check_weather() -> Optional[pd.DataFrame]:
    R.section("2. WEATHER — weather_clean.parquet")
    if not WEATHER_PARQUET.exists():
        R.skip("Weather checks", "файл не найден")
        return None

    df = pd.read_parquet(WEATHER_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # 2.1 Обязательные колонки
    required_cols = ["datetime_hour", "city_address"] + WEATHER_CORE_COLS
    missing = [c for c in required_cols if c not in df.columns]
    R.check(len(missing) == 0, "Обязательные колонки",
            f"все {len(required_cols)} присутствуют",
            f"отсутствуют: {missing}")

    # 2.2 Типы datetime
    if "datetime_hour" in df.columns:
        try:
            dt = pd.to_datetime(df["datetime_hour"], errors="coerce")
            n_bad = dt.isna().sum()
            R.check(n_bad == 0, "datetime_hour — корректные значения",
                    f"все {len(dt):,} валидны",
                    f"{n_bad:,} нераспознанных значений")

            # Проверка на часовой гранулярности
            if hasattr(dt.dt, "minute"):
                non_hour = (dt.dt.minute != 0).sum()
                R.check(non_hour == 0, "datetime_hour — точно по часу",
                        "все метки выровнены по часу",
                        f"{non_hour:,} меток с ненулевыми минутами")

            # Монотонность
            if "city_address" in df.columns:
                for city, grp in df.groupby("city_address"):
                    grp_dt = pd.to_datetime(grp["datetime_hour"], errors="coerce").sort_values()
                    diffs = grp_dt.diff().dropna()
                    gaps = (diffs > pd.Timedelta("2h")).sum()
                    if gaps > 0:
                        R.warn(f"Пропуски времени [{city}]",
                               f"{gaps} пропусков > 1 часа")
        except Exception as e:
            R.fail("datetime_hour — парсинг", str(e))

    # 2.3 Города
    if "city_address" in df.columns:
        cities = df["city_address"].str.split(",").str[0].str.strip().unique().tolist()
        mapped   = [c for c in cities if c in WEATHER_TO_ALARM]
        unmapped = [c for c in cities if c not in WEATHER_TO_ALARM]
        R.check(len(unmapped) == 0, "Все города замаплены на регионы",
                f"{len(mapped)} городов",
                f"незамаплены: {unmapped}")
        n_cities = df["city_address"].nunique()
        R.check(n_cities == len(WEATHER_TO_ALARM),
                "Количество городов",
                f"{n_cities} == {len(WEATHER_TO_ALARM)} ожидаемых",
                f"найдено {n_cities}, ожидалось {len(WEATHER_TO_ALARM)}")

    # 2.4 Пропуски в ключевых числовых колонках
    for col in WEATHER_CORE_COLS:
        if col in df.columns:
            n_nan = df[col].isna().sum()
            pct   = n_nan / len(df) * 100
            R.check(n_nan == 0, f"NaN в {col}",
                    f"0 пропусков",
                    f"{n_nan:,} ({pct:.1f}%) пропущено")

    # 2.5 Физические диапазоны
    ranges = {
        "hour_temp":        (-60,  60,  "°C"),
        "hour_humidity":    (0,   100,  "%"),
        "hour_pressure":    (870, 1080, "hPa"),
        "hour_windspeed":   (0,   120,  "m/s"),
        "hour_winddir":     (0,   360,  "°"),
        "hour_cloudcover":  (0,   100,  "%"),
        "hour_visibility":  (0,   100,  "km"),
        "hour_precip":      (0,    500, "mm"),
        "hour_precipprob":  (0,   100,  "%"),
        "hour_windgust":    (0,   150,  "m/s"),
        "hour_dew":         (-60,  40,  "°C"),
    }
    for col, (lo, hi, unit) in ranges.items():
        if col not in df.columns:
            continue
        out_of = ((df[col] < lo) | (df[col] > hi)).sum()
        R.check(out_of == 0, f"Диапазон {col} [{lo}..{hi} {unit}]",
                "все значения в норме",
                f"{out_of:,} значений вне диапазона",
                df[col].describe().to_string() if out_of > 0 else "")

    # 2.6 Дубликаты (город × час)
    if "city_address" in df.columns and "datetime_hour" in df.columns:
        dupes = df.duplicated(subset=["city_address", "datetime_hour"]).sum()
        R.check(dupes == 0, "Дубликаты [city × datetime_hour]",
                "дубликатов нет",
                f"{dupes:,} дублирующих строк")

    # 2.7 Бинарные флаги (0/1)
    for col in ["is_night", "is_rain", "is_snow"]:
        if col in df.columns:
            bad = (~df[col].isin([0, 1, True, False, np.nan])).sum()
            R.check(bad == 0, f"Бинарность {col}",
                    "только 0/1",
                    f"{bad:,} значений не 0/1")

    # 2.8 Сезон
    if "season" in df.columns:
        valid_seasons = {"winter", "spring", "summer", "autumn", 0, 1, 2, 3}
        bad_season = (~df["season"].isin(valid_seasons)).sum()
        R.check(bad_season == 0, "Значения season",
                "корректны",
                f"{bad_season:,} неизвестных значений сезона")

    R.info("Временной диапазон",
           f"{pd.to_datetime(df['datetime_hour']).min()} → "
           f"{pd.to_datetime(df['datetime_hour']).max()}"
           if "datetime_hour" in df.columns else "н/д")
    return df


# ══════════════════════════════════════════════════════════════
# 3. ALARMS
# ══════════════════════════════════════════════════════════════
def check_alarms() -> Optional[pd.DataFrame]:
    R.section("3. ALARMS — alarms_clean.parquet")
    if not ALARMS_PARQUET.exists():
        R.skip("Alarm checks", "файл не найден")
        return None

    df = pd.read_parquet(ALARMS_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # 3.1 Обязательные колонки
    for col in ["start_dt", "end_dt", "region"]:
        R.check(col in df.columns, f"Колонка {col}", "присутствует", "ОТСУТСТВУЕТ")

    # 3.2 Типы дат
    for col in ["start_dt", "end_dt"]:
        if col not in df.columns:
            continue
        dt = pd.to_datetime(df[col], errors="coerce", utc=False)
        n_bad = dt.notna().sum()
        if col == "start_dt":
            R.check(dt.isna().sum() == 0, f"{col} — без NaT после парсинга",
                    f"все {len(dt):,} валидны",
                    f"{dt.isna().sum():,} нераспознанных")

    # 3.3 Открытые тревоги (end_dt = NaT)
    if "end_dt" in df.columns:
        open_alarms = df["end_dt"].isna().sum()
        R.info("Открытые тревоги (end_dt=NaT)",
               f"{open_alarms:,} ({open_alarms/len(df)*100:.1f}%)")

    # 3.4 Хронологическая корректность
    if "start_dt" in df.columns and "end_dt" in df.columns:
        start = pd.to_datetime(df["start_dt"], errors="coerce")
        end   = pd.to_datetime(df["end_dt"],   errors="coerce")
        inverted = (end < start).sum()
        R.check(inverted == 0, "start_dt < end_dt (хронология)",
                "все тревоги хронологически корректны",
                f"{inverted:,} тревог с end < start")

        # Аномально длинные тревоги
        duration_h = ((end - start).dt.total_seconds() / 3600)
        ultra_long = (duration_h > 72).sum()
        if ultra_long > 0:
            R.warn("Тревоги длиннее 72 часов",
                   f"{ultra_long:,} штук",
                   f"max={duration_h.max():.1f}h")

        # Нулевые тревоги
        zero_dur = (duration_h == 0).sum()
        R.check(zero_dur == 0, "Нулевые тревоги (0 часов)",
                "нет",
                f"{zero_dur:,} тревог нулевой длительности")

    # 3.5 Регионы
    if "region" in df.columns:
        known = set(WEATHER_TO_ALARM.values()) | {"Kyiv Oblast", "Luhansk Oblast"}
        unknown_regions = set(df["region"].unique()) - known
        R.check(len(unknown_regions) == 0,
                "Все регионы известны",
                f"{df['region'].nunique()} регионов, все ожидаемые",
                f"неизвестные: {sorted(unknown_regions)}")

        # Покрытие регионов (есть ли тревоги во всех ожидаемых регионах)
        covered = set(df["region"].unique())
        missing_regions = EXPECTED_REGIONS - covered - {"Kyiv Oblast"}
        if missing_regions:
            R.warn("Регионы без единой тревоги", str(sorted(missing_regions)))

    # 3.6 Дубликаты
    if "start_dt" in df.columns and "region" in df.columns:
        dupes = df.duplicated(subset=["region", "start_dt"]).sum()
        R.check(dupes == 0, "Дубликаты [region × start_dt]",
                "нет", f"{dupes:,} дублирующих записей")

    # 3.7 Временной диапазон
    if "start_dt" in df.columns:
        start_ts = pd.to_datetime(df["start_dt"], errors="coerce")
        R.info("Временной диапазон тревог",
               f"{start_ts.min()} → {start_ts.max()}")

        # Будущие тревоги
        now = pd.Timestamp.now()
        future = (start_ts > now).sum()
        if future > 0:
            R.warn("Тревоги в будущем", f"{future:,} записей с start_dt > сейчас")

    return df


# ══════════════════════════════════════════════════════════════
# 4. ISW
# ══════════════════════════════════════════════════════════════
def check_isw() -> Optional[pd.DataFrame]:
    R.section("4. ISW — isw_features_for_merge.parquet")
    if not ISW_PARQUET.exists():
        R.skip("ISW checks", "файл не найден")
        return None

    df = pd.read_parquet(ISW_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # 4.1 alarm_date
    R.check("alarm_date" in df.columns, "Колонка alarm_date", "присутствует", "ОТСУТСТВУЕТ")
    if "alarm_date" in df.columns:
        ad = pd.to_datetime(df["alarm_date"], errors="coerce")
        n_bad = ad.isna().sum()
        R.check(n_bad == 0, "alarm_date без NaT", f"все {len(ad):,} валидны",
                f"{n_bad:,} нераспознанных")

        # Должен быть нормализован (только даты, без времени)
        non_norm = (ad.dt.hour != 0).sum() if not ad.isna().all() else 0
        R.check(non_norm == 0, "alarm_date нормализован (только дата)",
                "только даты", f"{non_norm:,} строк с ненулевым временем")

        # Уникальность дат
        dupes = ad.duplicated().sum()
        R.check(dupes == 0, "alarm_date — уникальность",
                "все даты уникальны",
                f"{dupes:,} дублирующих дат")

        # Нет будущих дат
        future = (ad > pd.Timestamp.now().normalize()).sum()
        R.check(future == 0, "alarm_date без будущих дат",
                "OK", f"{future:,} будущих дат в ISW")

        # Монотонность
        is_sorted = ad.is_monotonic_increasing
        R.check(is_sorted, "alarm_date отсортирован", "монотонно возрастает",
                "не монотонный — будет проблема при merge")

        R.info("Диапазон ISW", f"{ad.min().date()} → {ad.max().date()}")

    # 4.2 D+1 сдвиг — косвенная проверка
    # Если тревоги заканчиваются N, ISW должен иметь данные максимум до N-1
    R.info("D+1 сдвиг (ISW)", "подтверждён в isw_nlp_pipeline.py — проверка косвенная")

    # 4.3 Скалярные признаки
    for col in ISW_SCALAR_COLS:
        if col not in df.columns:
            R.warn(f"ISW scalar missing: {col}", "колонка отсутствует")
            continue
        n_nan = df[col].isna().sum()
        pct   = n_nan / len(df) * 100
        R.check(pct < 20, f"NaN в {col}",
                f"{n_nan:,} ({pct:.1f}%)",
                f"МНОГО пропусков: {n_nan:,} ({pct:.1f}%)")

        # Отрицательные значения там, где не ожидаются
        if col in {"isw_report_length", "isw_sources_count", "unique_domains",
                   "attack_mentions", "ground_mentions", "casualty_mentions",
                   "total_intensity", "intensity_per_1000"}:
            neg = (df[col].fillna(0) < 0).sum()
            R.check(neg == 0, f"Неотрицательность {col}",
                    "все ≥ 0",
                    f"{neg:,} отрицательных значений")

    # 4.4 Диапазоны ratio-признаков
    for col in ["real_dead_ratio", "ru_ua_balance", "ru_official_ratio"]:
        if col in df.columns:
            out = ((df[col] < -5) | (df[col] > 5)).sum()
            R.check(out < len(df) * 0.01, f"Экстремальные значения {col}",
                    f"< 1% выбросов",
                    f"{out:,} выбросов (|значение| > 5)")

    return df


# ══════════════════════════════════════════════════════════════
# 5. TF-IDF
# ══════════════════════════════════════════════════════════════
def check_tfidf(df_isw: Optional[pd.DataFrame]) -> None:
    R.section("5. TF-IDF — матрица и словарь")

    if not TFIDF_NPZ.exists() or not TFIDF_VOCAB.exists():
        R.skip("TF-IDF checks", "файлы не найдены")
        return

    # 5.1 Загрузка матрицы
    try:
        tfidf = scipy.sparse.load_npz(TFIDF_NPZ)
        R.ok("Загрузка tfidf_matrix_model.npz", f"shape={tfidf.shape}")
    except Exception as e:
        R.fail("Загрузка tfidf_matrix_model.npz", str(e))
        return

    # 5.2 Загрузка словаря
    try:
        with open(TFIDF_VOCAB, "r", encoding="utf-8") as f:
            vocab = json.load(f)
        R.ok("Загрузка tfidf_vocab_model.json", f"{len(vocab)} термов")
    except Exception as e:
        R.fail("Загрузка tfidf_vocab_model.json", str(e))
        return

    # 5.3 Согласованность размеров
    R.check(tfidf.shape[1] == len(vocab),
            "Размер матрицы == длина словаря",
            f"{tfidf.shape[1]} == {len(vocab)}",
            f"матрица: {tfidf.shape[1]} cols, словарь: {len(vocab)} термов")

    if df_isw is not None:
        R.check(tfidf.shape[0] == len(df_isw),
                "Строк TF-IDF == строк ISW",
                f"{tfidf.shape[0]} == {len(df_isw)}",
                f"TF-IDF: {tfidf.shape[0]}, ISW: {len(df_isw)}")

    # 5.4 Достаточный размер словаря
    R.check(len(vocab) >= TFIDF_MIN_TERMS,
            f"Словарь ≥ {TFIDF_MIN_TERMS} термов",
            f"{len(vocab)} термов",
            f"слишком маленький словарь: {len(vocab)}")

    # 5.5 Разреженность
    total = tfidf.shape[0] * tfidf.shape[1]
    nnz   = tfidf.nnz
    sparsity = (1 - nnz / max(total, 1)) * 100
    R.info("Разреженность матрицы", f"{sparsity:.1f}% нулей ({nnz:,} ненулевых из {total:,})")
    R.check(sparsity > 50, "Матрица достаточно разрежена (> 50%)",
            f"{sparsity:.1f}%",
            f"слишком плотная: {sparsity:.1f}%")

    # 5.6 Значения TF-IDF (должны быть ≥ 0)
    min_val = tfidf.data.min() if tfidf.nnz > 0 else 0
    max_val = tfidf.data.max() if tfidf.nnz > 0 else 0
    R.check(min_val >= 0, "Все TF-IDF ≥ 0",
            f"min={min_val:.4f}, max={max_val:.4f}",
            f"отрицательные значения: min={min_val:.4f}")

    # 5.7 Нет NaN/Inf в данных
    has_nan = np.isnan(tfidf.data).any() if tfidf.nnz > 0 else False
    has_inf = np.isinf(tfidf.data).any() if tfidf.nnz > 0 else False
    R.check(not has_nan, "TF-IDF без NaN", "OK", "NaN в данных матрицы!")
    R.check(not has_inf, "TF-IDF без Inf", "OK", "Inf в данных матрицы!")

    # 5.8 Уникальность термов в словаре
    R.check(len(set(vocab)) == len(vocab),
            "Уникальность термов в словаре",
            "все уникальны",
            f"{len(vocab) - len(set(vocab))} дублирующих термов")


# ══════════════════════════════════════════════════════════════
# 6. TELEGRAM
# ══════════════════════════════════════════════════════════════
def check_telegram() -> None:
    R.section("6. TELEGRAM — telegram_features_hourly.parquet")
    if not TG_PARQUET.exists():
        R.skip("Telegram checks", "файл не найден")
        return

    df = pd.read_parquet(TG_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # 6.1 datetime
    R.check("datetime" in df.columns, "Колонка datetime", "присутствует", "ОТСУТСТВУЕТ")
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce")
        R.check(dt.isna().sum() == 0, "datetime без NaT",
                "OK", f"{dt.isna().sum():,} нераспознанных")

        # Гранулярность — должен быть почасовым
        non_hour = (dt.dt.minute != 0).sum()
        R.check(non_hour == 0, "datetime выровнен по часу",
                "OK", f"{non_hour:,} меток с ненулевыми минутами")

        dupes = dt.duplicated().sum()
        R.check(dupes == 0, "datetime уникален",
                "OK", f"{dupes:,} дублирующих временных меток")

    # 6.2 Проверка лаговых колонок (leakage prevention)
    raw_f = [c for c in df.columns if c.startswith("f_")
             and "lag" not in c and "roll" not in c]
    lag_cols = [c for c in df.columns if "lag" in c or "roll" in c]
    R.check(len(lag_cols) > 0, "Есть лаговые/роллинг признаки",
            f"{len(lag_cols)} колонок", "нет ни одного лага — проблема!")
    R.check(len(raw_f) == 0, "Нет сырых f_* (защита от утечки)",
            "сырые f_* отсутствуют",
            f"LEAKAGE: {len(raw_f)} сырых f_* колонок: {raw_f[:5]}")

    # 6.3 Значения лаговых колонок (≥ 0, нет NaN)
    for col in lag_cols[:20]:  # первые 20 для скорости
        if col not in df.columns:
            continue
        n_nan = df[col].isna().sum()
        if n_nan > 0:
            R.warn(f"NaN в {col}", f"{n_nan:,} пропусков")
        neg = (pd.to_numeric(df[col], errors="coerce").fillna(0) < 0).sum()
        if neg > 0:
            R.warn(f"Отрицательные значения в {col}", f"{neg:,} строк")

    # 6.4 calm_phase_risk
    if "calm_phase_risk" in df.columns:
        valid_risk = {0, 1, True, False}
        bad = (~df["calm_phase_risk"].isin(valid_risk)).sum()
        R.check(bad == 0, "calm_phase_risk — бинарный",
                "только 0/1", f"{bad:,} некорректных значений")


# ══════════════════════════════════════════════════════════════
# 7. GUR
# ══════════════════════════════════════════════════════════════
def check_gur() -> None:
    R.section("7. GUR — gur_features_clean.parquet")
    if not GUR_PARQUET.exists():
        R.skip("GUR checks", "файл не найден")
        return

    df = pd.read_parquet(GUR_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # 7.1 alarm_date
    R.check("alarm_date" in df.columns, "Колонка alarm_date", "присутствует", "ОТСУТСТВУЕТ")
    if "alarm_date" in df.columns:
        ad = pd.to_datetime(df["alarm_date"], errors="coerce")
        R.check(ad.isna().sum() == 0, "alarm_date без NaT", "OK",
                f"{ad.isna().sum():,} нераспознанных")
        non_norm = (ad.dt.hour != 0).sum()
        R.check(non_norm == 0, "alarm_date нормализован",
                "только даты", f"{non_norm:,} с ненулевым временем")
        dupes = ad.duplicated().sum()
        R.check(dupes == 0, "alarm_date уникален", "OK",
                f"{dupes:,} дублирующих дат")
        R.info("Диапазон GUR", f"{ad.min().date()} → {ad.max().date()}")

    # 7.2 Признаки GUR
    gur_cols = [c for c in df.columns if c != "alarm_date"]
    R.info("Признаков GUR", str(len(gur_cols)))

    for col in gur_cols:
        n_nan = df[col].isna().sum()
        pct   = n_nan / len(df) * 100
        if pct > 30:
            R.warn(f"Много NaN в gur/{col}", f"{n_nan:,} ({pct:.1f}%)")

    # 7.3 D+1 сдвиг
    R.info("D+1 сдвиг (GUR)", "подтверждён в gur_features.py — проверка косвенная")


# ══════════════════════════════════════════════════════════════
# 8. MERGED DATASET — ОСНОВНАЯ ПРОВЕРКА
# ══════════════════════════════════════════════════════════════
def check_merged(df_alarms: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    R.section("8. MERGED DATASET — merged_dataset.parquet")
    if not MERGED_PARQUET.exists():
        R.skip("Merged checks", "файл не найден")
        return None

    df = pd.read_parquet(MERGED_PARQUET)
    R.info("Размер", f"{df.shape[0]:,} строк × {df.shape[1]} колонок")

    # ── 8.1 КРИТИЧЕСКИЕ КОЛОНКИ ──────────────────────────────
    R.section("8.1 Критические колонки")
    critical = ["region", "datetime_hour", "alarm", "n_regions_alarm",
                "is_frontline", "n_regions_lag_1h"]
    for col in critical:
        R.check(col in df.columns, f"[CRITICAL] {col}", "присутствует", "ОТСУТСТВУЕТ")

    # ── 8.2 ТИПЫ ДАННЫХ ──────────────────────────────────────
    R.section("8.2 Типы данных и форматы")
    if "datetime_hour" in df.columns:
        dt = pd.to_datetime(df["datetime_hour"], errors="coerce")
        n_bad = dt.isna().sum()
        R.check(n_bad == 0, "datetime_hour парсится без ошибок",
                "OK", f"{n_bad:,} нераспознанных")

        non_hour = (dt.dt.minute != 0).sum()
        R.check(non_hour == 0, "datetime_hour выровнен по часу",
                "OK", f"{non_hour:,} меток с минутами != 0")

        if dt.dt.tz is not None:
            R.warn("datetime_hour содержит timezone", "ожидается timezone-naive (локальное Kyiv)")
        else:
            R.ok("datetime_hour timezone-naive", "OK")

    # ── 8.3 ДУБЛИКАТЫ ────────────────────────────────────────
    R.section("8.3 Дубликаты")
    if "region" in df.columns and "datetime_hour" in df.columns:
        dupes = df.duplicated(subset=["region", "datetime_hour"]).sum()
        R.check(dupes == 0, "Дубликаты [region × datetime_hour]",
                "нет", f"{dupes:,} дублирующих строк")

    # ── 8.4 COMPLETENESS ─────────────────────────────────────
    R.section("8.4 Полнота (completeness)")
    if "region" in df.columns and "datetime_hour" in df.columns:
        n_regions = df["region"].nunique()
        n_hours   = df["datetime_hour"].nunique()
        expected  = n_regions * n_hours
        actual    = len(df)
        completeness = actual / expected * 100
        R.check(completeness >= COMPLETENESS_MIN,
                f"Completeness ≥ {COMPLETENESS_MIN}%",
                f"{completeness:.1f}% ({actual:,}/{expected:,})",
                f"НИЗКАЯ completeness: {completeness:.1f}%")
        R.info("Регионы", f"{n_regions} | ожидаем: {len(EXPECTED_REGIONS)}")
        R.check(n_regions >= len(EXPECTED_REGIONS) - 2,
                f"Количество регионов ≈ {len(EXPECTED_REGIONS)}",
                f"{n_regions}",
                f"мало регионов: {n_regions}, ожидалось ~{len(EXPECTED_REGIONS)}")

    # ── 8.5 ЦЕЛЕВАЯ ПЕРЕМЕННАЯ (alarm) ───────────────────────
    R.section("8.5 Целевая переменная (alarm)")
    if "alarm" in df.columns:
        n_nan = df["alarm"].isna().sum()
        R.check(n_nan == 0, "alarm без NaN", "OK", f"{n_nan:,} пропущено")

        bad_vals = (~df["alarm"].isin([0, 1])).sum()
        R.check(bad_vals == 0, "alarm только 0/1",
                "OK", f"{bad_vals:,} значений не 0/1")

        alarm_rate = df["alarm"].mean() * 100
        R.check(ALARM_RATE_MIN <= alarm_rate <= ALARM_RATE_MAX,
                f"alarm_rate в [{ALARM_RATE_MIN}%, {ALARM_RATE_MAX}%]",
                f"{alarm_rate:.2f}%",
                f"alarm_rate={alarm_rate:.2f}% выходит за границы")

        alarm_by_region = df.groupby("region")["alarm"].mean().sort_values()
        R.info("alarm_rate по регионам (min/max)",
               f"min={alarm_by_region.iloc[0]:.1%} [{alarm_by_region.index[0]}]  "
               f"max={alarm_by_region.iloc[-1]:.1%} [{alarm_by_region.index[-1]}]")

    # ── 8.6 TARGET LEAKAGE ───────────────────────────────────
    R.section("8.6 Защита от утечки цели (Target Leakage)")

    raw_f_cols = [c for c in df.columns
                  if c.startswith("f_") and "lag" not in c and "roll" not in c]
    R.check(len(raw_f_cols) == 0,
            "Нет сырых f_* колонок (TG leakage)",
            "OK", f"LEAKAGE: {raw_f_cols[:10]}")

    # n_regions_alarm — должен быть, но в FEATURES нельзя использовать напрямую
    if "n_regions_alarm" in df.columns and "n_regions_lag_1h" in df.columns:
        R.ok("n_regions_alarm + lag оба присутствуют",
             "raw col для лагирования, lag col для признаков")
    elif "n_regions_alarm" in df.columns:
        R.warn("n_regions_lag_1h отсутствует",
               "compute_n_regions_lags() не вызывался?")

    # Проверка: лаги должны быть действительно сдвинутыми (lag_1h != исходного в t=t)
    if "n_regions_alarm" in df.columns and "n_regions_lag_1h" in df.columns:
        perfect_match = (df["n_regions_alarm"] == df["n_regions_lag_1h"]).mean()
        R.check(perfect_match < 0.95,
                "n_regions_lag_1h действительно сдвинут",
                f"совпадение с raw: {perfect_match:.1%}",
                f"ПОДОЗРЕНИЕ: {perfect_match:.1%} совпадений — лаг не работает?")

    # ── 8.7 WEATHER BINARIES ─────────────────────────────────
    R.section("8.7 Погодные бинарные признаки")
    weather_binary_cols = ["low_visibility", "strong_wind", "freezing",
                           "bad_weather_index", "energy_stress"]
    for col in weather_binary_cols:
        if col not in df.columns:
            R.warn(f"{col}", "отсутствует — synergy features будут нулевыми!")
            continue
        n_nan = df[col].isna().sum()
        R.check(n_nan == 0, f"{col} без NaN", "OK", f"{n_nan:,} пропусков")
        rate = df[col].astype(float).mean() * 100
        R.info(f"{col} активен", f"{rate:.1f}% строк")
        if rate == 0.0:
            R.fail(f"{col} всегда 0",
                   "compute_weather_binaries() не вызывался до synergy step?")

    # ── 8.8 SYNERGY FEATURES ─────────────────────────────────
    R.section("8.8 Синергетические признаки (syn_*)")
    synergy_cols = [c for c in df.columns if c.startswith("syn_")]
    R.info("Количество syn_* колонок", str(len(synergy_cols)))

    zero_syns = []
    for syn in synergy_cols:
        n_act = int((df[syn] > 0).sum())
        pct   = n_act / len(df) * 100
        if n_act == 0:
            zero_syns.append(syn)
            R.fail(f"{syn}", "НОЛЬ активаций — проблема с порядком вычислений!")
        else:
            R.ok(f"{syn}", f"{n_act:,} активаций ({pct:.2f}%)")

    R.check(len(zero_syns) == 0,
            "Все synergy признаки имеют хоть 1 активацию",
            "OK",
            f"нулевые синергии: {zero_syns}")

    # ── 8.9 LAG / ROLL КОЛОНКИ ───────────────────────────────
    R.section("8.9 Лаговые и роллинг признаки")
    lag_cols  = [c for c in df.columns if "lag"  in c and not c.startswith("syn_")]
    roll_cols = [c for c in df.columns if "roll" in c and not c.startswith("syn_")]
    R.info("Лаговых колонок",  str(len(lag_cols)))
    R.info("Роллинг колонок",  str(len(roll_cols)))

    # Лаги не должны иметь NaN кроме самих первых строк
    for col in lag_cols[:10]:
        n_nan = df[col].isna().sum()
        if n_nan > df["region"].nunique() * 10:
            R.warn(f"Много NaN в лаге {col}", f"{n_nan:,} пропусков")

    # Проверяем lag_1h / lag_3h пары
    n_lag_1h = sum(1 for c in df.columns if "lag_1h" in c or "lag1h" in c)
    n_lag_3h = sum(1 for c in df.columns if "lag_3h" in c or "lag3h" in c)
    R.info("lag_1h признаков", str(n_lag_1h))
    R.info("lag_3h признаков", str(n_lag_3h))

    # ── 8.10 ISW ПРИЗНАКИ ────────────────────────────────────
    R.section("8.10 ISW признаки в merged датасете")
    for col in ISW_SCALAR_COLS:
        if col not in df.columns:
            R.warn(f"ISW scalar {col}", "ОТСУТСТВУЕТ в merged")
            continue
        n_nan = df[col].isna().sum()
        pct   = n_nan / len(df) * 100
        R.check(pct == 0, f"NaN в {col}",
                "0 пропусков", f"{n_nan:,} ({pct:.1f}%) — fillna(0) не применился?")

    tfidf_cols = [c for c in df.columns if c.startswith("tfidf_")]
    R.info("TF-IDF колонок", str(len(tfidf_cols)))
    R.check(len(tfidf_cols) >= TFIDF_MIN_TERMS,
            f"TF-IDF признаков ≥ {TFIDF_MIN_TERMS}",
            f"{len(tfidf_cols)}",
            f"слишком мало: {len(tfidf_cols)}")
    if tfidf_cols:
        tfidf_nan = df[tfidf_cols].isna().sum().sum()
        R.check(tfidf_nan == 0, "TF-IDF без NaN в merged",
                "OK", f"{tfidf_nan:,} NaN — fillna(0) не применился?")

    # ── 8.11 FRONTLINE ───────────────────────────────────────
    R.section("8.11 Frontline признаки")
    if "is_frontline" in df.columns:
        frontline_regions_in_df = df[df["is_frontline"] == 1]["region"].unique()
        missing_fl = FRONTLINE_REGIONS - set(frontline_regions_in_df)
        R.check(len(missing_fl) == 0,
                "Все frontline регионы помечены",
                str(sorted(frontline_regions_in_df)),
                f"не помечены: {missing_fl}")

        fl_alarm_rate  = df[df["is_frontline"] == 1]["alarm"].mean() * 100 if "alarm" in df.columns else 0
        nfl_alarm_rate = df[df["is_frontline"] == 0]["alarm"].mean() * 100 if "alarm" in df.columns else 0
        R.check(fl_alarm_rate > nfl_alarm_rate,
                "Frontline alarm_rate > Non-frontline",
                f"{fl_alarm_rate:.1f}% > {nfl_alarm_rate:.1f}%",
                f"АНОМАЛИЯ: frontline {fl_alarm_rate:.1f}% ≤ non-frontline {nfl_alarm_rate:.1f}%")

    if "frontline_multi_alarm" in df.columns:
        n_act = int((df["frontline_multi_alarm"] > 0).sum())
        R.check(n_act > 0, "frontline_multi_alarm имеет активации",
                f"{n_act:,} строк", "ноль активаций!")

    # ── 8.12 GUR ПРИЗНАКИ ────────────────────────────────────
    R.section("8.12 GUR признаки в merged датасете")
    gur_cols = [c for c in df.columns if c.startswith("gur_")]
    if gur_cols:
        R.info("GUR колонок", str(len(gur_cols)))
        for col in gur_cols:
            n_nan = df[col].isna().sum()
            if n_nan > 0:
                R.warn(f"NaN в {col}", f"{n_nan:,} пропусков")
    else:
        R.warn("GUR признаков нет", "telegram_features или gur_features не были обработаны")

    # ── 8.13 TRAIN / TEST SPLIT ──────────────────────────────
    R.section("8.13 Train/Test разбиение")
    if "datetime_hour" in df.columns:
        dt = pd.to_datetime(df["datetime_hour"])
        max_alarm_date = dt.max().floor("D") if df_alarms is None else (
            pd.to_datetime(df_alarms["start_dt"], errors="coerce").max().floor("D")
        )
        train_cutoff = max_alarm_date - pd.Timedelta(days=30)
        train = df[dt < train_cutoff]
        test  = df[dt >= train_cutoff]

        R.info("Train cutoff", str(train_cutoff.date()))
        R.check(len(train) >= MIN_TRAIN_ROWS,
                f"Train ≥ {MIN_TRAIN_ROWS} строк",
                f"{len(train):,}",
                f"слишком мало: {len(train):,}")
        R.check(len(test) > 0,
                "Test набор не пустой",
                f"{len(test):,} строк",
                "TEST ПУСТОЙ!")

        if len(train) > 0 and len(test) > 0:
            train_alarm = train["alarm"].mean() * 100 if "alarm" in train.columns else 0
            test_alarm  = test["alarm"].mean()  * 100 if "alarm" in test.columns  else 0
            R.info("Train alarm rate", f"{train_alarm:.2f}%")
            R.info("Test  alarm rate", f"{test_alarm:.2f}%")
            ratio = abs(train_alarm - test_alarm) / max(train_alarm, 0.01)
            R.check(ratio < 0.5,
                    "alarm_rate train ≈ test (±50%)",
                    f"разница {ratio:.1%}",
                    f"СИЛЬНОЕ РАСХОЖДЕНИЕ: train={train_alarm:.1f}%, test={test_alarm:.1f}%")

    # ── 8.14 ДАННЫЕ НА НАЛИЧИЕ INF ───────────────────────────
    R.section("8.14 Inf / -Inf в числовых колонках")
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    inf_counts = {}
    for col in numeric_cols:
        n_inf = np.isinf(df[col].values.astype(float)).sum()
        if n_inf > 0:
            inf_counts[col] = n_inf
    R.check(len(inf_counts) == 0,
            "Нет Inf/-Inf в числовых колонках",
            f"проверено {len(numeric_cols)} колонок",
            f"Inf в: {dict(list(inf_counts.items())[:10])}",
            "\n".join(f"  {c}: {v}" for c, v in inf_counts.items()) if inf_counts else "")

    # ── 8.15 КОНСТАНТНЫЕ ПРИЗНАКИ ─────────────────────────────
    R.section("8.15 Константные и почти-константные признаки")
    exclude_from_const = {"alarm", "region", "datetime_hour"}
    const_cols = []
    near_const_cols = []
    for col in numeric_cols:
        if col in exclude_from_const:
            continue
        try:
            std = df[col].std()
            if std == 0:
                const_cols.append(col)
            elif std < 1e-8:
                near_const_cols.append(col)
        except Exception:
            pass
    R.check(len(const_cols) == 0,
            "Нет полностью константных признаков",
            "OK",
            f"{len(const_cols)} константных: {const_cols[:5]}")
    if near_const_cols:
        R.warn("Почти константные признаки (std < 1e-8)",
               f"{len(near_const_cols)}: {near_const_cols[:5]}")

    # ── 8.16 ПРОПУСКИ (сводка) ────────────────────────────────
    R.section("8.16 Пропуски в каждой колонке")
    nan_summary = df.isna().sum()
    nan_summary = nan_summary[nan_summary > 0].sort_values(ascending=False)
    if len(nan_summary) == 0:
        R.ok("NaN сводка", "нет пропусков ни в одной колонке")
    else:
        R.warn(f"Есть пропуски в {len(nan_summary)} колонках",
               f"топ-5: {dict(nan_summary.head().items())}")
        for col, n in nan_summary.items():
            pct = n / len(df) * 100
            if pct > 5:
                R.fail(f"[МНОГО NaN] {col}", f"{n:,} ({pct:.1f}%)")
            else:
                R.warn(f"[NaN] {col}", f"{n:,} ({pct:.1f}%)")

    # ── 8.17 ВРЕМЕННОЙ РЯД — ПРОПУСКИ ЧАСОВ ──────────────────
    R.section("8.17 Непрерывность временного ряда по регионам")
    if "region" in df.columns and "datetime_hour" in df.columns:
        dt = pd.to_datetime(df["datetime_hour"])
        total_gaps = 0
        for region, grp in df.groupby("region"):
            grp_dt = pd.to_datetime(grp["datetime_hour"]).sort_values()
            diffs = grp_dt.diff().dropna()
            gaps = (diffs > pd.Timedelta("2h")).sum()
            total_gaps += gaps
        R.check(total_gaps == 0,
                "Нет пропусков часов в рядах по регионам",
                "все ряды непрерывны",
                f"{total_gaps} пропусков > 1 часа по всем регионам")

    # ── 8.18 KYIV OBLAST ДУБЛИРОВАНИЕ ────────────────────────
    R.section("8.18 Kyiv Oblast (дублирование из City of Kyiv)")
    if "region" in df.columns:
        has_kyiv_city   = "City of Kyiv"   in df["region"].values
        has_kyiv_oblast = "Kyiv Oblast"    in df["region"].values
        R.check(has_kyiv_city,   "City of Kyiv присутствует",   "OK", "ОТСУТСТВУЕТ!")
        R.check(has_kyiv_oblast, "Kyiv Oblast присутствует",    "OK", "ОТСУТСТВУЕТ!")
        if has_kyiv_city and has_kyiv_oblast:
            n_city   = (df["region"] == "City of Kyiv").sum()
            n_oblast = (df["region"] == "Kyiv Oblast").sum()
            R.check(abs(n_city - n_oblast) < 100,
                    "Kyiv City ≈ Kyiv Oblast по кол-ву строк",
                    f"city={n_city:,}, oblast={n_oblast:,}",
                    f"РАСХОЖДЕНИЕ: city={n_city:,}, oblast={n_oblast:,}")

    return df


# ══════════════════════════════════════════════════════════════
# 9. КРОСС-ФАЙЛОВЫЕ ПРОВЕРКИ
# ══════════════════════════════════════════════════════════════
def check_cross_file(
        df_w: Optional[pd.DataFrame],
        df_a: Optional[pd.DataFrame],
        df_i: Optional[pd.DataFrame],
        df_m: Optional[pd.DataFrame],
) -> None:
    R.section("9. КРОСС-ФАЙЛОВЫЕ проверки (согласованность источников)")

    # 9.1 Временной диапазон Weather vs Alarms
    if df_w is not None and df_a is not None:
        if "datetime_hour" in df_w.columns and "start_dt" in df_a.columns:
            w_min = pd.to_datetime(df_w["datetime_hour"], errors="coerce").min()
            w_max = pd.to_datetime(df_w["datetime_hour"], errors="coerce").max()
            a_min = pd.to_datetime(df_a["start_dt"], errors="coerce").min()
            a_max = pd.to_datetime(df_a["start_dt"], errors="coerce").max()

            overlap_start = max(w_min, a_min)
            overlap_end   = min(w_max, a_max)
            has_overlap   = overlap_start < overlap_end

            R.check(has_overlap,
                    "Weather и Alarms имеют временное перекрытие",
                    f"{overlap_start.date()} → {overlap_end.date()}",
                    f"НЕТ ПЕРЕКРЫТИЯ: weather [{w_min.date()}, {w_max.date()}] "
                    f"vs alarms [{a_min.date()}, {a_max.date()}]")

            # Тревоги не должны быть вне диапазона погоды
            a_outside = ((pd.to_datetime(df_a["start_dt"], errors="coerce") < w_min) |
                         (pd.to_datetime(df_a["start_dt"], errors="coerce") > w_max)).sum()
            if a_outside > 0:
                R.warn("Тревоги вне диапазона погоды",
                       f"{a_outside:,} тревог без соответствующих погодных данных")

    # 9.2 ISW vs Alarms — D+1 проверка
    if df_i is not None and df_a is not None:
        if "alarm_date" in df_i.columns and "start_dt" in df_a.columns:
            isw_max = pd.to_datetime(df_i["alarm_date"], errors="coerce").max()
            alarm_max = pd.to_datetime(df_a["start_dt"], errors="coerce").max().normalize()
            # ISW c D+1 сдвигом — последний ISW должен быть = alarm_max - 1 день
            expected_isw_max = alarm_max - pd.Timedelta(days=1)
            diff_days = abs((isw_max - expected_isw_max).days)
            R.check(diff_days <= 2,
                    "ISW alarm_date max ≈ alarms max - 1 день (D+1 shift)",
                    f"ISW max: {isw_max.date()}, alarm max: {alarm_max.date()} "
                    f"(diff={diff_days}d)",
                    f"СДВИГ НАРУШЕН: diff={diff_days} дней")

    # 9.3 Количество строк в merged vs ожидаемое
    if df_m is not None and df_w is not None and df_a is not None:
        if "datetime_hour" in df_w.columns:
            w_hours = df_w["datetime_hour"].nunique()
            expected_min = len(EXPECTED_REGIONS) * w_hours * 0.8
            R.check(len(df_m) >= expected_min,
                    "Merged имеет достаточно строк",
                    f"{len(df_m):,} >= {expected_min:,.0f}",
                    f"merged={len(df_m):,} < ожидаемый минимум {expected_min:,.0f}")

    # 9.4 Регионы в merged == ожидаемые регионы
    if df_m is not None and "region" in df_m.columns:
        actual_regions = set(df_m["region"].unique())
        missing = EXPECTED_REGIONS - actual_regions
        extra   = actual_regions - EXPECTED_REGIONS - {"Kyiv Oblast"}
        R.check(len(missing) == 0,
                "Все ожидаемые регионы в merged",
                "OK", f"отсутствуют: {sorted(missing)}")
        if extra:
            R.warn("Неожиданные регионы в merged", str(sorted(extra)))


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="AEGIS — Максимально детальная проверка целостности данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Пример: python integrity_check.py\n"
               "        python integrity_check.py --parquet data/processed/merged_dataset.parquet",
    )
    parser.add_argument("--parquet", type=str, default=None,
                        help="Путь к merged_dataset.parquet (необязательно)")
    parser.add_argument("--verbose", action="store_true",
                        help="Показывать детальные подсказки при ошибках")
    args = parser.parse_args()

    if args.parquet:
        global MERGED_PARQUET
        MERGED_PARQUET = Path(args.parquet)

    R.verbose = True  # всегда verbose в нашем случае

    print("\n")
    print("╔" + "═" * 70 + "╗")
    print("║" + "  AEGIS DATA INTEGRITY CHECK — ПОЛНАЯ ПРОВЕРКА ДАННЫХ  ".center(70) + "║")
    print("╚" + "═" * 70 + "╝")

    # Запускаем все проверки
    check_filesystem()

    df_w  = check_weather()
    df_a  = check_alarms()
    df_i  = check_isw()
    check_tfidf(df_i)
    check_telegram()
    check_gur()
    df_m  = check_merged(df_a)
    check_cross_file(df_w, df_a, df_i, df_m)

    passes, fails, warns = R.summary()
    sys.exit(0 if fails == 0 else 1)


if __name__ == "__main__":
    main()