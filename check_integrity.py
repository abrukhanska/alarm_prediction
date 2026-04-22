import pandas as pd

# Список усіх областей + Київ (стандартна структура AEGIS)
EXPECTED_REGIONS = {
    "Vinnytsia Oblast", "Volyn Oblast", "Dnipropetrovsk Oblast", "Donetsk Oblast",
    "Zhytomyr Oblast", "Zakarpattia Oblast", "Zaporizhzhia Oblast",
    "Ivano-Frankivsk Oblast", "Kyiv Oblast", "City of Kyiv", "Kirovohrad Oblast",
    "Luhansk Oblast", "Lviv Oblast", "Mykolaiv Oblast", "Odesa Oblast",
    "Poltava Oblast", "Rivne Oblast", "Sumy Oblast", "Ternopil Oblast",
    "Kharkiv Oblast", "Kherson Oblast", "Khmelnytskyi Oblast", "Cherkasy Oblast",
    "Chernivtsi Oblast", "Chernihiv Oblast"
}


def run_audit():
    try:
        df = pd.read_parquet('data/processed/alarms_clean.parquet')
        df['start_dt'] = pd.to_datetime(df['start_dt'])
    except Exception as e:
        print(f"❌ Помилка читання файлу: {e}")
        return

    print("=" * 60)
    print("🛰️  AEGIS: ГЛОБАЛЬНИЙ АУДИТ ДАНИХ ТРИВОГ")
    print("=" * 60)

    # --- 1. ПЕРЕВІРКА ГЕОГРАФІЇ ---
    actual_regions = set(df['region'].unique())
    missing_regions = EXPECTED_REGIONS - actual_regions
    extra_regions = actual_regions - EXPECTED_REGIONS

    print(f"📍 РЕГІОНИ ({len(actual_regions)}/25):")
    if not missing_regions:
        print("  ✅ Усі 25 регіонів присутні в датасеті.")
    else:
        print(f"  ❌ ВІДСУТНІ РЕГІОНИ: {', '.join(missing_regions)}")

    if extra_regions:
        print(f"  ⚠️  Знайдено невідомі регіони (треба перевірити скрапер): {', '.join(extra_regions)}")

    # --- 2. ПЕРЕВІРКА ЧАСОВИХ ДІРОК ---
    # Перевіряємо період з 10 березня по сьогодні
    start_point = '2026-03-10'
    trouble_period = df[df['start_dt'] >= start_point]

    daily = trouble_period.groupby(trouble_period['start_dt'].dt.date).size()
    full_calendar = pd.date_range(start=start_point, end=df['start_dt'].max(), freq='D').date
    missing_days = set(full_calendar) - set(daily.index)

    print(f"\n📅 КАЛЕНДАР (з {start_point}):")
    if missing_days:
        print(f"  ❌ ЗНАЙДЕНО ДІРКИ В ДАТАХ ({len(missing_days)} днів):")
        print(f"     {sorted(list(missing_days))[:10]} ...")
    else:
        print(f"  ✅ Жодного пропущеного дня. Скрапер закрив усі дірки.")

    # --- 3. ФІНАЛЬНА СТАТИСТИКА ---
    print("\n📊 СТАТИСТИКА:")
    print(f"  • Всього рядків у базі: {len(df):,}")
    print(f"  • Останній запис: {df['start_dt'].max()}")

    apr_data = df[df['start_dt'].dt.month == 4].shape[0]
    print(f"  • Тривог за Квітень 2026: {apr_data}")

    print("=" * 60)
    if not missing_regions and not missing_days:
        print("🚀 ВЕРДИКТ: Дані ідеальні. Можна запускати навчання.")
    else:
        print("⚠️  ВЕРДИКТ: Треба докачати дані або перевірити скрапер.")
    print("=" * 60)


if __name__ == "__main__":
    run_audit()