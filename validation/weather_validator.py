import json
import os
import argparse
import csv
from pathlib import Path
from collections import defaultdict
from typing import Any
from dataclasses import dataclass, field
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FORECAST_DIR = PROJECT_ROOT / "data" / "raw" / "weather" / "forecast"
HISTORICAL_DIR = PROJECT_ROOT / "data" / "raw" / "weather" / "historical"
TEST_DIR = PROJECT_ROOT / "test_data"

EXPECTED_HOURS = 24

RANGES = {
    "temp": (-50.0, 50.0),
    "humidity": (0.0, 100.0),
    "windspeed": (0.0, 200.0),
    "pressure": (870.0, 1084.0),
    "cloudcover": (0.0, 100.0),
    "visibility": (0.0, 50.0),
}

REQUIRED_HOURLY = ["hour_temp", "hour_humidity", "hour_windspeed", "hour_pressure", "hour_cloudcover"]
UKRAINE_LAT, UKRAINE_LON = (44.0, 53.0), (22.0, 41.0)

HOURLY_FIELDS = [
    "hour_datetime", "hour_temp", "hour_feelslike", "hour_humidity",
    "hour_dew", "hour_precip", "hour_precipprob", "hour_snow",
    "hour_snowdepth", "hour_preciptype", "hour_windgust", "hour_windspeed",
    "hour_winddir", "hour_pressure", "hour_visibility", "hour_cloudcover",
    "hour_conditions"
]

DAILY_FIELDS = [
    "day_datetime", "day_tempmax", "day_tempmin", "day_temp",
    "day_humidity", "day_precip", "day_windspeed", "day_pressure",
    "day_cloudcover", "day_visibility", "day_conditions"
]


@dataclass
class Issue:
    severity: str
    category: str
    message: str
    file: str = ""


@dataclass
class ValidationReport:
    mode: str = ""
    issues: list[Issue] = field(default_factory=list)
    files_checked: int = 0
    total_errors: int = 0

    def add(self, severity: str, category: str, msg: str, file: str = ""):
        self.issues.append(Issue(severity, category, msg, file))
        if severity == "ERROR": self.total_errors += 1

def safe_float(val: Any) -> float | None:
    try:
        return float(val) if val is not None and str(val).strip() != "" else None
    except:
        return None


def detect_city(address: str) -> str | None:
    if not address: return None
    return address.split(",")[0].strip()

def validate_forecast_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    forecast = data.get("forecast", [])
    hours_count = len(forecast)
    null_count = 0
    out_of_range_count = 0

    for hour in forecast:
        for field, (min_val, max_val) in RANGES.items():
            value = hour.get(field)
            if value is None:
                null_count += 1
                continue
            if not (min_val <= value <= max_val):
                out_of_range_count += 1

    return {
        "hours": hours_count,
        "nulls": null_count,
        "out_of_range": out_of_range_count,
        "incomplete": hours_count != 24,
    }


def run_forecast_validation(directory, mode_name="FORECAST"):
    print(f"\n------- {mode_name} VALIDATION -------\n")
    if not os.path.exists(directory):
        print(f"Directory not found: {directory}")
        return

    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    valid_files = 0

    for filename in files:
        path = os.path.join(directory, filename)
        res = validate_forecast_file(path)

        if not res["incomplete"] and res["nulls"] == 0 and res["out_of_range"] == 0:
            valid_files += 1
            print(f"{filename} -> OK")
        else:
            print(f"{filename} -> ERROR (Hours: {res['hours']}, Nulls: {res['nulls']}, Out: {res['out_of_range']})")

    print(f"\nSummary: {valid_files}/{len(files)} files are valid.")



def process_csv_to_json(input_path: Path):
    if input_path is None:
        csv_files = list(HISTORICAL_DIR.glob("*.csv"))
        if not csv_files:
            print(f"Error: No CSV files found in {HISTORICAL_DIR}")
            return
        input_path = csv_files[0]
        print(f"Auto-detected file: {input_path.name}")

    if not input_path.exists():
        print(f"Error: File {input_path} does not exist.")
        return
    print(f"\nProcessing CSV: {input_path}...")
    data = defaultdict(lambda: defaultdict(lambda: {"daily": {}, "hours": []}))

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = detect_city(row.get("city_address", ""))
            date = row.get("day_datetime", "")
            if not city or not date: continue

            entry = data[city][date]
            if not entry["daily"]:
                entry["daily"] = {k: row.get(k, "") for k in DAILY_FIELDS if k in row}
                entry["daily"].update(
                    {"city": city, "latitude": row.get("city_latitude"), "longitude": row.get("city_longitude")})

            hour_data = {field: row.get(field) for field in HOURLY_FIELDS if field in row}
            if hour_data.get("hour_datetime"):
                entry["hours"].append(hour_data)

    written = 0
    for city, dates in data.items():
        city_dir = HISTORICAL_DIR / city.replace(" ", "_")
        city_dir.mkdir(parents=True, exist_ok=True)
        for date_str, day_data in dates.items():
            day_data["hours"].sort(key=lambda h: h.get("hour_datetime", ""))
            with open(city_dir / f"{date_str}.json", "w", encoding="utf-8") as f:
                json.dump(day_data, f, ensure_ascii=False, indent=2)
            written += 1
    print(f"Created {written} JSON files.")


def validate_historical_day(data: dict, file_name: str, report: ValidationReport):
    daily = data.get("daily", {})
    hours = data.get("hours", [])

    if len(hours) != EXPECTED_HOURS:
        report.add("ERROR", "CHRONOLOGY", f"Expected 24h, found {len(hours)}h", file_name)

    lat, lon = safe_float(daily.get("latitude")), safe_float(daily.get("longitude"))
    if lat and not (UKRAINE_LAT[0] <= lat <= UKRAINE_LAT[1]):
        report.add("ERROR", "GEOGRAPHY", f"Lat {lat} outside Ukraine", file_name)
    if lon and not (UKRAINE_LON[0] <= lon <= UKRAINE_LON[1]):
        report.add("ERROR", "GEOGRAPHY", f"Lon {lon} outside Ukraine", file_name)

    tmax, tmin = safe_float(daily.get("day_tempmax")), safe_float(daily.get("day_tempmin"))
    for h in hours:
        h_temp = safe_float(h.get("hour_temp"))
        if h_temp is not None:
            if tmax is not None and h_temp > tmax + 0.5:
                report.add("ERROR", "CONSISTENCY", f"Hour {h.get('hour_datetime')}: {h_temp} > Day Max {tmax}",
                           file_name)
            if tmin is not None and h_temp < tmin - 0.5:
                report.add("ERROR", "CONSISTENCY", f"Hour {h.get('hour_datetime')}: {h_temp} < Day Min {tmin}",
                           file_name)

        for field_name in REQUIRED_HOURLY:
            val = h.get(field_name)
            f_val = safe_float(val)
            if f_val is None:
                report.add("ERROR", "NULL", f"Empty {field_name}", file_name)
            else:
                base = field_name.replace("hour_", "")
                if base in RANGES:
                    lo, hi = RANGES[base]
                    if not (lo <= f_val <= hi):
                        report.add("ERROR", "RANGE", f"{field_name}={f_val} out of {lo}..{hi}", file_name)


def run_historical_validation(directory: Path):
    print("\n-------DEEP HISTORICAL VALIDATION-------")
    city_dates = defaultdict(list)
    city_issues = defaultdict(list)

    all_files = list(directory.glob("**/*.json"))
    for fp in all_files:
        city = fp.parent.name
        try:
            date_obj = datetime.strptime(fp.stem, "%Y-%m-%d")
            city_dates[city].append(date_obj)

            day_report = ValidationReport()
            with open(fp, "r", encoding="utf-8") as f:
                validate_historical_day(json.load(f), fp.name, day_report)

            if day_report.issues:
                city_issues[city].extend(day_report.issues)
        except Exception:
            continue


    print(f"-------Regional quality: Scanned {len(all_files)} files-------")

    for city in sorted(city_dates.keys()):
        dates = sorted(city_dates[city])
        start, end = dates[0], dates[-1]
        expected = (end - start).days + 1
        coverage = (len(dates) / expected) * 100
        issues = city_issues[city]

        print(f"\n{city.upper()}")
        print(f"Coverage: {coverage:>5.1f}% ({len(dates)}/{expected} days)")

        if not issues:
            print(f"STATUS: Clean")
        else:
            errors_cnt = sum(1 for i in issues if i.severity == "ERROR")
            print(f"STATUS: Found {len(issues)} issues ({errors_cnt} critical errors)")
            print(f"Full list of issues for {city}:")


            for issue in sorted(issues, key=lambda x: x.severity):
                status = issue.severity
                print(f"{status:7} | Category: {issue.category:12} | Message: {issue.message} | File: {issue.file}")

        print("-" * 91)




    print("-------GLOBAL PROBLEMS (for all regions)-------")
    print(" ")

    global_issues = []
    for city_name, issues_list in city_issues.items():
        for issue in issues_list:
            global_issues.append((city_name, issue))

    if not global_issues:
            print("No issues found across all regions")
    else:
        global_issues.sort(key=lambda x: (x[1].severity, x[0]))

        for city_name, issue in global_issues:
            status = issue.severity
            print(f"{status:7} | {city_name:12} | {issue.category:12} | {issue.message} | File: {issue.file}")

    total_all = len(global_issues)
    print(f"\n-------Final summary: {total_all} total issues detected-------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Data Quality")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--forecast", action="store_true", help="Validate forecast JSON files")
    group.add_argument("--historical", action="store_true", help="Validate historical JSON files")
    group.add_argument("--process", nargs='?', const=True, help="CSV to json")
    group.add_argument("--test", action="store_true", help="Run validation on test data")
    args = parser.parse_args()

    if args.process:
        csv_path = Path(args.process) if isinstance(args.process, str) else None
        process_csv_to_json(csv_path)

    elif args.forecast:
        run_forecast_validation(FORECAST_DIR, mode_name="FORECAST")

    elif args.historical:
        run_historical_validation(HISTORICAL_DIR)

    elif args.test:
        if TEST_DIR.exists():
            run_forecast_validation(TEST_DIR, mode_name="MOCK DATA")
        else:
            print(f"Error: Test directory not found at {TEST_DIR}")
    else:
        parser.print_help()