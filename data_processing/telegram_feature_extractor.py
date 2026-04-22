"""
Usage:
  python data_processing/telegram_feature_extractor.py --build
  python data_processing/telegram_feature_extractor.py --build --since 2023-01-01
  python data_processing/telegram_feature_extractor.py --build --debug
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR      = PROJECT_ROOT / "data" / "raw" / "telegram"
OUT_DIR      = PROJECT_ROOT / "data" / "processed"
LOG_FILE     = PROJECT_ROOT / "logs" / "telegram_feature_extractor.log"

MIN_TEXT_LEN = 5

CHANNEL_ROLES = {
    "monitorwarr":        "strategic_aviation",
    "vanek_nikolaev":     "tactical_south",
    "kpszsu":             "official_airforce",
    "GeneralStaff_ua":    "official_gsf",
    "povitryanatrivogaaa":"alarm_aggregator",
    "suspilne_news":      "impact_confirmation",
    "suspilnenews":       "impact_confirmation",
}

SPOILER_PATTERNS = re.compile(
    r"тривог[аи]|ТРИВОГА|Тривога|тревог[ау]|Тревог[ау]"
    r"|відбій|Відбій|відбой|отбой"
    r"|карта тривог|мапа тривог"
    r"|пішли в укрит|зайдіть в укрит|в укрыти[яе]|всі в укрит"
    r"|почервоніл|пожовтіл|позеленіл"
    r"|🟢|🔴|⚪|🚨|📢",
    re.IGNORECASE,
)

RE_TU95 = re.compile(
    r"Ту-?95|Tu-?95|ту-?95|Туполев.{0,10}95|TU-?95|Медв[еє]д[ья]?|95-?й",
    re.IGNORECASE,
)
RE_TU160 = re.compile(
    r"Ту-?160|Tu-?160|ту-?160|Туполев.{0,10}160",
    re.IGNORECASE,
)

RE_TU22 = re.compile(
    r"Ту-?22|Tu-?22|ту-?22|Туполев.{0,10}22",
    re.IGNORECASE,
)

RE_MIG31 = re.compile(
    r"МіГ-?31|миг-?31|MiG-?31|МИГ-?31|міг-?31к|miG-31K|петух[а-я]*(?:\s*-?31)?",
    re.IGNORECASE,
)

RE_SU34 = re.compile(
    r"Су-?34|су-?34|Su-?34|SU-?34"
    r"|Су-?35|су-?35|Su-?35"
    r"|Су-?57|су-?57|Su-?57"
    r"|сушк[аи]",
    re.IGNORECASE,
)

RE_VZLOT = re.compile(
    r"зліт|злетів|злет\b|вийшли на\s+(?:бойов|маршрут|чергуван)"
    r"|вийшов на\s+(?:маршрут|рубіж|курс)"
    r"|вылетел|вылет\b|вылетели|взлет|взлёт|взлетел|в небе|поднял"
    r"|зліт.{0,30}(?:Ту|МіГ|Су|борт|петух)"
    r"|(?:Ту|МіГ|Су|борт|петух).{0,30}зліт"
    r"|зафіксовано\s+(?:виліт|зліт)"
    r"|курс на пускові"
    r"|airborne|takeoff|🛫",
    re.IGNORECASE,
)

RE_AIRBASE = re.compile(
    r"Енгельс|Энгельс|Engels"
    r"|Оленья|Оленегорськ"
    r"|Шайковка|Сольці|Морозовськ|Morozovsk|Міллерово|Міллерове"
    r"|Саваслейка|Savasleyka|Дягилево|Миллерово|Приморсько-Ахтарськ|Приморско-Ахтарск|Халино|Курськ"
    r"|Ахтубінськ|авіабаз|авиабаз|аэродром|аеродром.{0,20}(?:РФ|Росі|рос\b)"
    r"|Таганрог|Taganrog|Єйськ|Єйск|Ейськ|Ейск"
    r"|спорядженн.{0,20}борт|🛬.{0,20}(?:аеродром|баз)",
    re.IGNORECASE,
)

RE_X101 = re.compile(r"Х-?101|X-?101|х-?101|Х101|крилата.{0,15}ракет", re.IGNORECASE)
RE_X55  = re.compile(r"Х-?55|х-?55|X-?55", re.IGNORECASE)

RE_X22  = re.compile(r"Х-?22|х-?22|X-?22|Х-22|Х-?32|х-?32|X-?32", re.IGNORECASE)

RE_X59      = re.compile(r"Х-?59|х-?59|X-?59|Х-?31|х-?31", re.IGNORECASE)
RE_KALIBR   = re.compile(r"Калібр|калібр|Калибр|калибр|Кaliber|Kalibr", re.IGNORECASE)
RE_ISKANDER = re.compile(r"Іскандер|іскандер|Искандер|искандер|Iskander", re.IGNORECASE)

RE_KINZHAL  = re.compile(
    r"кинджал|Кинджал|kinzhal|Кинжал|кинжал|гіперзвуков|гиперзвуков|аеробаліст|аэробаллист",
    re.IGNORECASE,
)
RE_BALLISTIC = re.compile(
    r"балістич\w+|баллистик\w+|баллистич\w+|ballistic"
    r"|ІCBM|МRBM|SRBM|Іскандер.{0,15}(?:М\b|балістич)"
    r"|KN-23|KN-25",
    re.IGNORECASE,
)
RE_ONIKS = re.compile(r"Онікс|онікс|Оникс|Oniks|П-800", re.IGNORECASE)

RE_KAB = re.compile(
    r"КАБ[иа]?|ФАБ[иа]?|УМПК"
    r"|керован\w+\s+авіаційн\w+\s+бомб"
    r"|авіаційн\w+\s+бомб"
    r"|кабами|пуск.{0,15}КАБ"
    r"|авіабомб\w*",
    re.IGNORECASE,
)

RE_RSZO = re.compile(
    r"РСЗВ|РСЗО|MLRS|Смерч\b|Торнадо\b|Ураган\b|Град\b"
    r"|реактивн\w+\s+систем.{0,10}залп",
    re.IGNORECASE,
)

RE_SHAHED = re.compile(
    r"шахед|Шахед|Shahed|мопед|герань|Герань|Geran|Гербер[аи]|Gerber"
    r"|балалайка|газонокосилк"
    r"|(?:ударн|барраж).{0,10}(?:дрон|БПЛА|БпЛА|беспилотн)"
    r"|БпЛА.{0,5}(?:фіксу|виявл|спостереж)"
    r"|🛵",
    re.IGNORECASE,
)
RE_LANCET = re.compile(r"Ланцет|ланцет|Lancet", re.IGNORECASE)

RE_PUSK = re.compile(
    r"пуск|пуски|Пуск|Пуски|пущен|пущені|запуск|запуски"
    r"|випустил|выпустил|выпущен|выходы|летят"
    r"|launch(?:ed)?|salvo"
    r"|🚀.{0,20}(?:пуск|виявл|зафікс)"
    r"|вийшли на пускові",
    re.IGNORECASE,
)

RE_SOUTH = re.compile(
    r"з півдня|з Криму|з моря|з Чорн\w+ мор|с юга|с моря|акватори|с Черного|южный курс|южное направлени|Тендровск"
    r"|Чорноморськ\w+ напрям|ЧМ\s+→|ЧМ,"
    r"|вектор.{0,20}південь|напрям.{0,20}північ"
    r"|Crimea|Крим.{0,10}(?:напрям|вектор|рух)"
    r"|→.{0,50}(?:Одес|Херсон|Миколаї)"
    r"|Азов|Чорн\w+ мор",
    re.IGNORECASE,
)

RE_EAST = re.compile(
    r"зі сходу|с востока|восточный курс|восточное направлени"
    r"|з-під\s+(?:Бєлгород|Воронеж|Брянськ|Курськ)"
    r"|Бєлгород|Белгород|Воронеж|Брянськ|Курськ|Ростов"
    r"|→.{0,50}(?:Харків|Запоріж|Дніпр)"
    r"|вектор.{0,20}(?:схід|захід\w+).{0,20}(?:Харків|Дніпр)",
    re.IGNORECASE,
)

RE_CHORNE_MORE = re.compile(
    r"Чорн\w+ мор|Черн\w+ мор|ЧМ\b|Чорноморськ|чорноморськ|акватори.{0,15}мор"
    r"|підводн\w+\s+(?:човен|крейсер|корвет)"
    r"|надводн\w+\s+(?:корабл|ціль)",
    re.IGNORECASE,
)

RE_BAVOVNA = re.compile(
    r"бавовн|хлопок.{0,15}(?:РФ|Росі|Краснодар|Воронеж|Бєлгород)"
    r"|пожеж.{0,30}(?:НПЗ|нафто|аеродром|авіабаз|склад).{0,20}(?:РФ|Росі|окупова)"
    r"|пожар.{0,30}(?:РФ|Росси)"
    r"|детонац.{0,20}(?:склад|боєприпас|арсенал)"
    r"|удар.{0,30}(?:Енгельс|Оленья|Морозовськ|Шайковка)"
    r"|знищен.{0,20}(?:НПЗ|нафтобаз|корабл|крейсер).{0,30}(?:РФ|Росі|флот)"
    r"|уничтожен.{0,20}(?:РФ|Росси)"
    r"|🔥.{0,30}(?:НПЗ|Енгельс|Оленья|склад\w+ боєприпас)",
    re.IGNORECASE,
)

RE_MASOVANA = re.compile(
    r"масован\w+|массированн\w+|масовий удар|масштабн\w+ атак"
    r"|масовий ракетн|масов\w+ обстріл|волна|волны"
    r"|large.?scale|mass(?:ive)?\s+(?:attack|strike)"
    r"|хвил\w+\s+(?:ракет|пуск|обстріл)"
    r"|серія\s+(?:пуск|ракет|вибух)",
    re.IGNORECASE,
)

RE_PPO = re.compile(
    r"ППО|ПВО|С-300|С-400|С-500|Patriot|патріот|NASAMS|Iris-T|HAWK"
    r"|перехоплен\w+|збит\w+|знищен\w+.{0,20}(?:ракет|дрон|БПЛА|шахед)"
    r"|сбит\w+|сбили|антимопедн\w+\s+работ"
    r"|збиваємо|перехоплюємо|работает\s+ПВО"
    r"|air\s+defense|intercepted",
    re.IGNORECASE,
)

RE_COMMS = re.compile(
    r"бойов\w+\s+(?:частот|мереж|чергуван)"
    r"|вийшли\s+на\s+(?:частот|зв'язок|бойов)"
    r"|частот\w+\s+(?:бойов|стратег)"
    r"|радіоефір|ефірна\s+активність"
    r"|перехоплен\w+\s+переговор"
    r"|активність\s+на\s+частот"
    r"|8131|стратегічн\w+\s+авіац.{0,20}(?:зв|ефір|частот)",
    re.IGNORECASE,
)
RE_SUBMARINE = re.compile(
    r"підводн\w+\s+(?:човен|крейсер|ракетоносій)"
    r"|підводн\w+\s+(?:зайшов|вийшов|позиц)"
    r"|submarine|подводн\w+\s+лодк"
    r"|РПК\w*|РПЛСН",
    re.IGNORECASE,
)
RE_KYIV_DIR = re.compile(
    r"(?:→|напрям|курс|рух).{0,40}(?:Київ|Киев|Kyiv)"
    r"|(?:Київ|Киев).{0,20}(?:напрям|під загроз|область)",
    re.IGNORECASE,
)
RE_KHARKIV_DIR = re.compile(
    r"(?:→|напрям|курс).{0,40}(?:Харків|Харьков)"
    r"|(?:Харків|Харьков).{0,20}(?:напрям|під загроз|напрям)",
    re.IGNORECASE,
)
RE_DNIPRO_DIR = re.compile(
    r"(?:→|напрям|курс).{0,40}(?:Дніпр|Дніпропетровськ)"
    r"|(?:Дніпр).{0,20}напрям",
    re.IGNORECASE,
)
RE_ODESA_DIR = re.compile(
    r"(?:→|напрям|курс).{0,40}(?:Одес|Odessa)"
    r"|(?:Одес).{0,20}напрям",
    re.IGNORECASE,
)
RE_TENSION_HIGH = re.compile(
    r"відплат\w+|відплату|retaliat"
    r"|удар\w+\s+відповід\w+"
    r"|підготовк\w+\s+(?:атак|ракет|удар)"
    r"|накопичен\w+\s+(?:ракет|засоб)"
    r"|отримал\w+\s+(?:нов\w+\s+партій|зброю|ракет)"
    r"|прибут\w+\s+(?:борт|партій|вантаж).{0,30}(?:Іран|КНДР|зброя)"
    r"|Iranian|DPRK.{0,20}weapon",
    re.IGNORECASE,
)
RE_ROCKET_COUNT = re.compile(
    r"(\d+)\s*(?:ракет|крилат\w+|Калібр|Х-\d+|Іскандер|балістич\w+)",
    re.IGNORECASE,
)

RE_TAKT_AVIA = re.compile(
    r"тактич\w+\s+авіац"
    r"|ворож\w+\s+тактич\w+\s+авіац"
    r"|активність\s+(?:ворож\w+\s+)?тактич\w+\s+авіац"
    r"|загроза.{0,30}авіаційних\s+засобів\s+ураження"
    r"|загроза.{0,15}(?:КАБ|авіабомб)",
    re.IGNORECASE,
)

RE_BUDE_HUCHNO = re.compile(
    r"буде\s+(?:дуже\s+|дуже\s+)?гучно"
    r"|будет\s+(?:очень\s+|продолжительно\s+)?громко"
    r"|може\s+бути\s+гучно|может\s+быть\s+громко"
    r"|незабаром\s+буде\s+гучно",
    re.IGNORECASE,
)

RE_KHVYLYA = re.compile(
    r"хвил[яі]\s+(?:з\s+\d+|ударних|БпЛА|дрон|мопед|ракет)"
    r"|наближ\w+\s+хвил"
    r"|нова\s+хвиля|чергова\s+хвиля"
    r"|нова\s+група.{0,20}(?:БпЛА|дрон|мопед)"
    r"|волна\s+(?:дрон|мопед|ракет|беспилотн)",
    re.IGNORECASE,
)

RE_POVTORNI = re.compile(
    r"повторн\w+\s+пуск"
    r"|новий\s+пуск|нові\s+пуски"
    r"|нова\s+атак|новий\s+налет"
    r"|повторный\s+пуск|новые\s+пуски"
    r"|❗️?\s*Повторн",
    re.IGNORECASE,
)

RE_EXPL_CONFIRM = re.compile(
    r"(?:чутно|пролунав|пролунала|сталися|стався|чули)\s+(?:звук\s+)?вибух"
    r"|вибух.{0,20}(?:Харків|Київ|Одес|Дніпр|Херсон|Запоріж|Суми|Чернігів|Миколаї|Кривий)"
    r"|прильот\b|приліт\b"
    r"|влучання.{0,30}(?:місто|район|буд)"
    r"|💥\s+\w+(?:щина|ськ|ськи)",
    re.IGNORECASE,
)

RE_STAGING = re.compile(
    r"накопич\w+\s+(?:в\s+акваторі|[Бб][Пп][Лл][Аа]|бортів|дрон|мопед)"
    r"|(?:БпЛА|борти|мопеди|дрони)\s+накопич"
    r"|скупчен\w+.{0,20}(?:БпЛА|дрон|ракет)"
    r"|групуються.{0,20}(?:море|акватор)",
    re.IGNORECASE,
)

RE_PRESTRIKE = re.compile(
    r"очікуємо\s+(?:нанесен|удар|ракет|пуск)"
    r"|можлив\w+\s+(?:ракетний|масован\w+)\s+удар"
    r"|ворог\s+планує.{0,30}(?:удар|атак|пуск)"
    r"|за\s+отриманою\s+інформацією.{0,30}(?:удар|атак|пуск|ракет)"
    r"|планує.{0,20}(?:ракетно|дроново|масован)",
    re.IGNORECASE,
)

RE_RECON = re.compile(
    r"дорозвідка|доразведка"
    r"|чисто\b|не\s+спостерігається|цілі\s+відсутні"
    r"|локаційно\s+(?:чисто|втрачен|не\s+зафікс)"
    r"|без\s+цілей|втрачено\s+(?:ціль|з\s+поля\s+зору)"
    r"|чисте\s+небо|відбій\s+загроз",
    re.IGNORECASE,
)

RE_ACOUSTIC = re.compile(
    r"акустич\w+|акустичн\w+\s+фіксац"
    r"|слышно\b|чутно\b"
    r"|звук\s+(?:двигун|мотор)"
    r"|чути\s+(?:вибух|дрон|мопед|літак)"
    r"|єППО",
    re.IGNORECASE,
)

RE_FALSE_TARGET = re.compile(
    r"фальш\b|імітац\w+|имитац\w+"
    r"|РЕБ\b|РЭБ\b"
    r"|локаційн\w+\s+ціль"
    r"|електронн\w+\s+(?:протидія|warfare)"
    r"|перешкод\w+.{0,20}(?:ракет|ціль|сигнал)"
    r"|без\s+повторних\s+пусків|без\s+повторных\s+пусков",
    re.IGNORECASE,
)

RE_DRONE_COUNT = re.compile(
    r"(\d{1,2})\s*(?:мопед\w*|БпЛА|бпла|шахед\w*|дрон\w*|беспи��отн\w*|герань\w*)",
    re.IGNORECASE,
)

def setup_logging(debug: bool = False) -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("tg_extractor")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    if logger.handlers:
        return logger
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(levelname)-7s | %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def _iter_jsonl_chunks(files: list[Path], chunk_size: int = 50000):
    chunk = []
    for path in files:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    chunk.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if len(chunk) >= chunk_size:
                    yield pd.DataFrame(chunk)
                    chunk = []
    if chunk:
        yield pd.DataFrame(chunk)

def load_all_jsonl(since: datetime | None, logger: logging.Logger) -> pd.DataFrame:
    files = sorted(RAW_DIR.rglob("*.jsonl"))
    files = [f for f in files if not f.name.startswith(".")]
    logger.info(f"Files found: {len(files)}")

    dfs = []
    for chunk_df in _iter_jsonl_chunks(files, chunk_size=50000):
        if "channel" in chunk_df.columns:
            chunk_df["channel"] = chunk_df["channel"].replace("suspilnenews", "suspilne_news")
        if "datetime" in chunk_df.columns:
            chunk_df["datetime"] = pd.to_datetime(chunk_df["datetime"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
            chunk_df = chunk_df.dropna(subset=["datetime"])
            if since is not None:
                chunk_df = chunk_df[chunk_df["datetime"] >= since]
        if not chunk_df.empty:
            dfs.append(chunk_df)

    if not dfs:
        logger.info("No records loaded.")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Records loaded (before dedup): {len(df):,}")
    return df

def deduplicate_and_clean(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["channel", "msg_id"], keep="first").copy()
    logger.info(f"After dedup (channel+msg_id): {len(df):,} (removed {before - len(df):,})")

    df["text"] = df["text"].fillna("").astype(str)

    def has_enough_text(text: str) -> bool:
        cleaned = re.sub(r"[\s\U0001F000-\U0001FFFF\u2600-\u27FF\u2300-\u23FF"
                         r"\u2190-\u21FF\u25A0-\u25FF\u2700-\u27BF→←↑↓▪►◄]",
                         "", text)
        return len(cleaned) >= MIN_TEXT_LEN

    mask = df["text"].apply(has_enough_text)
    logger.info(f"Removed too-short records: {(~mask).sum():,}")
    df = df[mask].copy()
    df = df.sort_values("datetime").reset_index(drop=True)
    logger.info(f"Clean records ready for processing: {len(df):,}")
    return df

def _count_emojis(text: str) -> int:
    return sum(
        1 for c in text
        if (0x1F000 <= ord(c) <= 0x1FFFF)
        or (0x2600 <= ord(c) <= 0x27FF)
        or c in "→←↑↓▪►◄🚀🛵🔴🟢⚠❗⚡💥🔥🛫🛬"
    )

def _clean_for_nlp(text: str) -> str:
    return SPOILER_PATTERNS.sub(" ", text)

def extract_features_per_message(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Extracting per-message features using vectorized regex...")

    df["text_clean"] = df["text"].apply(_clean_for_nlp)

    def _vec_match(pattern: re.Pattern) -> pd.Series:
        return df["text_clean"].str.contains(pattern).fillna(False).astype(np.int8)

    df["f_tu95"]       = _vec_match(RE_TU95)
    df["f_tu160"]      = _vec_match(RE_TU160)
    df["f_tu22"]       = _vec_match(RE_TU22)
    df["f_mig31"]      = _vec_match(RE_MIG31)
    df["f_su34"]       = _vec_match(RE_SU34)
    df["f_vzlot"]      = _vec_match(RE_VZLOT)
    df["f_airbase"]    = _vec_match(RE_AIRBASE)

    df["f_carrier_airborne"] = (
        (df["f_tu95"] | df["f_tu160"] | df["f_mig31"] | df["f_tu22"]) & df["f_vzlot"]
    ).astype(np.int8)

    df["f_x101"]       = _vec_match(RE_X101)
    df["f_x55"]        = _vec_match(RE_X55)
    df["f_x22"]        = _vec_match(RE_X22)
    df["f_x59"]        = _vec_match(RE_X59)
    df["f_kalibr"]     = _vec_match(RE_KALIBR)
    df["f_iskander"]   = _vec_match(RE_ISKANDER)
    df["f_kinzhal"]    = _vec_match(RE_KINZHAL)
    df["f_oniks"]      = _vec_match(RE_ONIKS)
    df["f_ballistic"]  = _vec_match(RE_BALLISTIC)
    df["f_kab"]        = _vec_match(RE_KAB)
    df["f_rszo"]       = _vec_match(RE_RSZO)

    df["f_any_cruise"] = df[["f_x101", "f_x55", "f_x22", "f_kalibr", "f_x59"]].max(axis=1).astype(np.int8)

    df["f_shahed"]     = _vec_match(RE_SHAHED)
    df["f_lancet"]     = _vec_match(RE_LANCET)

    df["f_pusk"]       = _vec_match(RE_PUSK)

    def get_max_match(series: pd.Series, pattern: re.Pattern) -> pd.Series:
        extracted = series.str.extractall(pattern)
        if extracted.empty:
            return pd.Series(0, index=series.index, dtype=np.int16)
        return extracted[0].astype(float).groupby(level=0).max().fillna(0).astype(np.int16)

    df["f_rocket_count"] = get_max_match(df["text"], RE_ROCKET_COUNT).reindex(df.index, fill_value=0).astype(np.int16)
    df["f_drone_count"]  = get_max_match(df["text"], RE_DRONE_COUNT).reindex(df.index, fill_value=0).astype(np.int16)

    df["f_takt_avia"]    = _vec_match(RE_TAKT_AVIA)
    df["f_bude_huchno"]  = _vec_match(RE_BUDE_HUCHNO)
    df["f_khvylya"]      = _vec_match(RE_KHVYLYA)
    df["f_povtorni"]     = _vec_match(RE_POVTORNI)
    df["f_staging"]      = _vec_match(RE_STAGING)
    df["f_prestrike"]    = _vec_match(RE_PRESTRIKE)
    df["f_expl_confirm"] = _vec_match(RE_EXPL_CONFIRM)

    df["f_recon"]        = _vec_match(RE_RECON)
    df["f_false_target"] = _vec_match(RE_FALSE_TARGET)
    df["f_acoustic"]     = _vec_match(RE_ACOUSTIC)

    df["f_vec_south"]   = _vec_match(RE_SOUTH)
    df["f_vec_east"]    = _vec_match(RE_EAST)
    df["f_chorne_more"] = _vec_match(RE_CHORNE_MORE)
    df["f_submarine"]   = _vec_match(RE_SUBMARINE)

    df["f_dir_kyiv"]    = _vec_match(RE_KYIV_DIR)
    df["f_dir_kharkiv"] = _vec_match(RE_KHARKIV_DIR)
    df["f_dir_dnipro"]  = _vec_match(RE_DNIPRO_DIR)
    df["f_dir_odesa"]   = _vec_match(RE_ODESA_DIR)
    df["f_cities_count"] = (
        df["f_dir_kyiv"] + df["f_dir_kharkiv"]
        + df["f_dir_dnipro"] + df["f_dir_odesa"]
    ).astype(np.int8)

    df["f_ppo"]        = _vec_match(RE_PPO)
    df["f_bavovna"]    = _vec_match(RE_BAVOVNA)
    df["f_masovana"]   = _vec_match(RE_MASOVANA)
    df["f_comms"]      = _vec_match(RE_COMMS)
    df["f_tension"]    = _vec_match(RE_TENSION_HIGH)

    df["f_text_len"]      = df["text"].str.len().astype(np.int32)
    df["f_emoji_cnt"]     = df["text"].apply(_count_emojis).astype(np.int16)
    df["f_emoji_density"] = (df["f_emoji_cnt"] / df["f_text_len"].clip(lower=1)).astype(np.float32)

    logger.info("Per-message features extracted.")
    return df

SUM_COLS = [
    # Aircraft
    "f_tu95", "f_tu160", "f_tu22",
    "f_mig31", "f_su34",
    "f_vzlot", "f_airbase", "f_carrier_airborne",
    # Weapons
    "f_x101", "f_x55", "f_x22",
    "f_x59",
    "f_kalibr", "f_iskander",
    "f_kinzhal", "f_oniks", "f_ballistic", "f_any_cruise",
    "f_kab",
    "f_rszo",
    # Drones
    "f_shahed", "f_lancet",
    # Launch signals
    "f_pusk",
    "f_rocket_count",
    "f_drone_count",
    # Operational signals
    "f_takt_avia",
    "f_bude_huchno",
    "f_khvylya",
    "f_povtorni",
    "f_staging",
    "f_prestrike",
    "f_expl_confirm",
    # Negative/all-clear
    "f_recon",
    "f_false_target",
    "f_acoustic",
    # Vectors
    "f_vec_south", "f_vec_east", "f_chorne_more", "f_submarine",
    # Target directions
    "f_dir_kyiv", "f_dir_kharkiv", "f_dir_dnipro", "f_dir_odesa",
    "f_cities_count",
    # Situational
    "f_ppo",
    "f_bavovna",
    "f_masovana",
    "f_comms",
    "f_tension",
    "f_emoji_cnt",
]
MEAN_COLS = ["f_text_len", "f_emoji_density"]

def aggregate_hourly(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    df["hour"] = df["datetime"].dt.floor("h")
    logger.info("Aggregating by hour...")

    agg_dict = {col: "sum" for col in SUM_COLS}
    agg_dict.update({col: "mean" for col in MEAN_COLS})
    agg_dict["msg_id"] = "count"

    hourly = df.groupby("hour", observed=False).agg(agg_dict).rename(
        columns={"msg_id": "total_msg_count"}
    )

    channel_counts = df.groupby(["hour", "channel"], observed=False).size().unstack(fill_value=0)
    channel_counts.columns = [f"msgs_{ch}" for ch in channel_counts.columns]
    hourly = hourly.join(channel_counts, how="left")

    hourly["active_channels"] = df.groupby("hour", observed=False)["channel"].nunique()

    official = df[df["channel"].isin(["GeneralStaff_ua", "kpszsu"])]
    hourly["official_msg_count"] = (
        official.groupby("hour", observed=False)["msg_id"].count().reindex(hourly.index, fill_value=0)
    )

    hourly = hourly.reset_index().rename(columns={"hour": "datetime"})
    full_range = pd.date_range(
        start=hourly["datetime"].min(),
        end=hourly["datetime"].max(),
        freq="h"
    )

    hourly = hourly.set_index("datetime").reindex(full_range)

    hourly = hourly.fillna(0).reset_index().rename(columns={"index": "datetime"})

    for col in SUM_COLS + ["active_channels", "official_msg_count"]:
        if col in hourly.columns:
            hourly[col] = hourly[col].astype(np.int16)

    if "total_msg_count" in hourly.columns:
        hourly["total_msg_count"] = hourly["total_msg_count"].astype(np.int32)

    for col in channel_counts.columns:
        if col in hourly.columns:
            hourly[col] = hourly[col].astype(np.int16)

    logger.info(f"Hourly rows: {len(hourly):,}")
    return hourly

LAG_FEATURES = [
    ("f_tu95",             3,  "tu95_lag3h"),
    ("f_tu160",            3,  "tu160_lag3h"),
    ("f_mig31",            2,  "mig31_lag2h"),
    ("f_kinzhal",          2,  "kinzhal_lag2h"),
    ("f_carrier_airborne", 2,  "carrier_airborne_lag2h"),
    ("f_shahed",           1,  "shahed_lag1h"),
    ("f_vec_south",        6,  "vec_south_lag6h"),
    ("f_chorne_more",      6,  "chorne_more_lag6h"),
    ("f_submarine",        6,  "submarine_lag6h"),
    ("f_vec_east",         4,  "vec_east_lag4h"),
    ("f_airbase",          3,  "airbase_lag3h"),
    ("f_comms",            3,  "comms_lag3h"),
    ("f_any_cruise",       2,  "any_cruise_lag2h"),
    ("f_x101",             2,  "x101_lag2h"),
    ("f_kalibr",           2,  "kalibr_lag2h"),
    ("f_iskander",         1,  "iskander_lag1h"),
    ("f_ballistic",        1,  "ballistic_lag1h"),
    ("f_masovana",         2,  "masovana_lag2h"),
    ("f_bavovna",          48, "bavovna_lag48h"),
    ("f_tension",          24, "tension_lag24h"),
    ("f_pusk",             1,  "pusk_lag1h"),
    ("f_rocket_count",     1,  "rocket_count_lag1h"),
    ("f_dir_kyiv",         1,  "dir_kyiv_lag1h"),
    ("f_dir_kharkiv",      1,  "dir_kharkiv_lag1h"),
    ("f_dir_dnipro",       1,  "dir_dnipro_lag1h"),
    ("f_dir_odesa",        1,  "dir_odesa_lag1h"),
    ("f_cities_count",     1,  "cities_count_lag1h"),

    ("f_tu22",             2,  "tu22_lag2h"),
    ("f_kab",              1,  "kab_lag1h"),
    ("f_rszo",             1,  "rszo_lag1h"),
    ("f_takt_avia",        1,  "takt_avia_lag1h"),
    ("f_bude_huchno",      1,  "bude_huchno_lag1h"),
    ("f_khvylya",          1,  "khvylya_lag1h"),
    ("f_povtorni",         1,  "povtorni_lag1h"),
    ("f_staging",          3,  "staging_lag3h"),
    ("f_prestrike",        2,  "prestrike_lag2h"),
    ("f_recon",            1,  "recon_lag1h"),
    ("f_false_target",     1,  "false_target_lag1h"),
    ("f_acoustic",         1,  "acoustic_lag1h"),
    ("f_drone_count",      1,  "drone_count_lag1h"),
    ("f_expl_confirm",    24,  "expl_confirm_lag24h"),
    ("f_x59",              1,  "x59_lag1h"),
]

def apply_lags(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Applying lags (anti-leakage)...")
    df = df.sort_values("datetime").reset_index(drop=True)

    for src_col, lag_h, new_col in LAG_FEATURES:
        if src_col in df.columns:
            df[new_col] = df[src_col].shift(lag_h)
        else:
            logger.warning(f"  Column {src_col} not found, skipping lag {new_col}")

    lag_cols = [new_col for _, _, new_col in LAG_FEATURES]
    df[lag_cols] = df[lag_cols].fillna(0).astype(np.int16)

    logger.info(f"Lagged columns added: {len(lag_cols)}")
    return df

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    hour = df["datetime"].dt.hour

    df["hour_sin"] = np.sin(2 * np.pi * hour / 24).astype(np.float32)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24).astype(np.float32)

    dow = df["datetime"].dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7).astype(np.float32)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7).astype(np.float32)

    week = df["datetime"].dt.isocalendar().week.astype(int)
    df["week_sin"] = np.sin(2 * np.pi * week / 52).astype(np.float32)
    df["week_cos"] = np.cos(2 * np.pi * week / 52).astype(np.float32)

    df["is_exhaustion_window"] = hour.isin([0, 1, 2, 3, 4]).astype(np.int8)
    df["is_weekend"] = (dow >= 5).astype(np.int8)

    md = df["datetime"].dt.month * 100 + df["datetime"].dt.day

    sacred_dates = {224, 509, 628, 824, 1001, 1231, 101}
    df["is_sacred_date"] = md.isin(sacred_dates).astype(np.int8)

    return df

def add_calm_phase(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Computing calm phase (hours since last mass strike)...")

    massive_mask = (
        (df["f_masovana"] > 0)
        | (df["f_rocket_count"] >= 10)
        | (df["f_drone_count"] >= 15)
    )

    last_massive = df["datetime"].where(massive_mask).ffill()
    delta_hours = (df["datetime"] - last_massive).dt.total_seconds() / 3600
    df["hours_since_last_massive"] = delta_hours.fillna(0).astype(np.int16)

    df["calm_phase_risk"] = np.log1p(
        df["hours_since_last_massive"].clip(0, 240)
    ).astype(np.float32)

    return df

def add_rolling_features(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Adding rolling features...")
    df = df.set_index("datetime").sort_index()

    key_cols = [
        "f_shahed", "f_tu95", "f_mig31", "f_pusk",
        "total_msg_count", "f_ppo", "f_masovana",
        "f_vec_south", "f_vec_east",
        "f_kab",
        "f_takt_avia",
        "f_bude_huchno",
        "f_expl_confirm",
        "f_drone_count",
    ]

    roll_parts = []
    for col in key_cols:
        if col not in df.columns:
            continue
        shifted = df[col].shift(1)

        dtype_to_use = np.int32 if col == "total_msg_count" else np.int16

        roll_3h = shifted.rolling("3h", min_periods=1).sum().fillna(0).rename(f"{col}_roll3h").astype(dtype_to_use)
        roll_6h = shifted.rolling("6h", min_periods=1).sum().fillna(0).rename(f"{col}_roll6h").astype(dtype_to_use)
        roll_24h = shifted.rolling("24h", min_periods=1).sum().fillna(0).rename(f"{col}_roll24h").astype(dtype_to_use)

        roll_parts.extend([roll_3h, roll_6h, roll_24h])

    if roll_parts:
        df = pd.concat([df] + roll_parts, axis=1).copy()

    df = df.reset_index()
    logger.info(f"Rolling features added for {len(key_cols)} columns (3h / 6h / 24h)")
    return df

def add_synergy_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat([
        df,
        pd.Series(0.0, index=df.index, name="synergy_shahed_visibility", dtype=np.float32),
        pd.Series(0.0, index=df.index, name="synergy_shahed_cloud", dtype=np.float32),
        pd.Series(0.0, index=df.index, name="synergy_kab_wind", dtype=np.float32),
    ], axis=1)
    return df

def build(since: datetime | None, debug: bool, logger: logging.Logger) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "telegram_features_hourly.parquet"

    logger.info("=" * 60)
    logger.info("TELEGRAM FEATURE EXTRACTOR — Silver Layer")
    logger.info("=" * 60)

    df_raw      = load_all_jsonl(since, logger)

    if df_raw.empty:
        logger.info("No records loaded. Exiting.")
        return

    df_clean    = deduplicate_and_clean(df_raw, logger)
    df_feat     = extract_features_per_message(df_clean, logger)
    df_hourly   = aggregate_hourly(df_feat, logger)
    df_lagged   = apply_lags(df_hourly, logger)
    df_temporal = add_temporal_features(df_lagged)
    df_calm     = add_calm_phase(df_temporal, logger)
    df_rolling  = add_rolling_features(df_calm, logger)
    df_final    = add_synergy_placeholders(df_rolling)

    float_cols = df_final.select_dtypes(include=["float64"]).columns
    if len(float_cols) > 0:
        df_final[float_cols] = df_final[float_cols].astype(np.float32)

    df_final.to_parquet(out_path, index=False, compression="snappy")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"DONE: {len(df_final):,} rows × {len(df_final.columns)} columns")
    logger.info(f"Range: {df_final['datetime'].min()} → {df_final['datetime'].max()}")
    logger.info(f"Saved: {out_path}")
    logger.info("=" * 60)
    logger.info("Next step: python data_processing/merge_datasets.py")

    if debug:
        logger.debug("\nColumns:")
        for col in df_final.columns:
            logger.debug(f"  {col} ({df_final[col].dtype})")
        logger.debug(f"\nFirst row:\n{df_final.iloc[0].to_dict()}")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="AEGIS Telegram Feature Extractor — Silver Layer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrapers/telegram_feature_extractor.py --build
  python scrapers/telegram_feature_extractor.py --build --since 2023-01-01
  python scrapers/telegram_feature_extractor.py --build --debug
        """,
    )
    parser.add_argument("--build", action="store_true", required=True,
                        help="Run Parquet build")
    parser.add_argument("--since", default=None,
                        help="Process only from this date (YYYY-MM-DD)")
    parser.add_argument("--debug", action="store_true",
                        help="Verbose log + column dump")
    args = parser.parse_args()

    since: datetime | None = None
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            print(f"ERROR: --since must be YYYY-MM-DD, got: {args.since}",
                  file=sys.stderr)
            sys.exit(1)

    logger = setup_logging(debug=args.debug)
    build(since=since, debug=args.debug, logger=logger)

if __name__ == "__main__":
    main()