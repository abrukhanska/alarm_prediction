import datetime as dt
import argparse
import json
import os
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)
WEATHER_KEY = os.getenv("my_weather_key")


REGIONS = {
    'Kyiv_Oblast': 'Kyiv, Ukraine',
    'Kharkiv_Oblast': 'Kharkiv, Ukraine',
    'Odesa_Oblast': 'Odesa, Ukraine',
    'Lviv_Oblast': 'Lviv, Ukraine',
    "Vinnytsia_Oblast": "Vinnytsia, Ukraine",
    "Dnipropetrovska_Oblast": "Dnipro, Ukraine",
    "Donetsk_Oblast": "Donetsk, Ukraine",
    "Zhytomyr_Oblast": "Zhytomyr, Ukraine",
    "Zaporizhzhia_Oblast": "Zaporizhzhia, Ukraine",
    "Ivano-Frankivsk_Oblast": "Ivano-Frankivsk, Ukraine",
    "Kirovohradska_Oblast": "Kropyvnytskyi, Ukraine",
    "Luhansk_Oblast": "Luhansk, Ukraine",
    "Volyn_Oblast": "Lutsk, Ukraine",
    "Mykolaiv_Oblast": "Mykolaiv, Ukraine",
    "Poltava_Oblast": "Poltava, Ukraine",
    "Rivne_Oblast": "Rivne, Ukraine",
    "Sumy_Oblast": "Sumy, Ukraine",
    "Ternopil_Oblast": "Ternopil, Ukraine",
    "Zakarpatska_Oblast": "Uzhhorod, Ukraine",
    "Kherson_Oblast": "Kherson, Ukraine",
    "Khmelnytskyi_Oblast": "Khmelnytskyi, Ukraine",
    "Cherkasy_Oblast": "Cherkasy, Ukraine",
    "Chernivtsi_Oblast": "Chernivtsi, Ukraine",
    "Chernihiv_Oblast": "Chernihiv, Ukraine",
    "Crimea": "Simferopol, Ukraine"
}


def get_weather_forecast(location):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/next24hours?unitGroup=metric&key={WEATHER_KEY}&contentType=json"

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Limit 429 for {location}. Waiting 60s...")
                time.sleep(60)
            elif response.status_code == 500:
                print(f"Server error 500 for {location}. Waiting 10s...")
                time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying in 5s...")
        time.sleep(5)
    return None


def save_forecast(region_name, raw_data):
    now = dt.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H")
    directory = "data/raw/weather/forecast/"

    if not os.path.exists(directory):
        os.makedirs(directory)

    filename = f"{timestamp}_{region_name}.json"
    filepath = os.path.join(directory, filename)

    hours_data = raw_data['days'][0]['hours']

    result = {
        "region": region_name,
        "request_timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast": []
    }

    for hour in hours_data:
        hour_info = {
            "datetime": hour.get("datetime"),
            "temp": hour.get("temp"),
            "humidity": hour.get("humidity"),
            "windspeed": hour.get("windspeed"),
            "winddir": hour.get("winddir"),
            "visibility": hour.get("visibility"),
            "cloudcover": hour.get("cloudcover"),
            "pressure": hour.get("pressure"),
            "precip": hour.get("precip"),
            "conditions": hour.get("conditions"),
        }
        result["forecast"].append(hour_info)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=4, ensure_ascii=False)


    print(f"Saved: {region_name}, hours: {len(hours_data)}, time: {now.strftime('%H:%M:%S')}")


def run_forecast(all_regions=False, specific_region=None):
    if all_regions:
        for reg_name, location in REGIONS.items():
            data = get_weather_forecast(location)
            if data:
                save_forecast(reg_name, data)
            time.sleep(1.5)
    elif specific_region:
        location = REGIONS.get(specific_region, specific_region)
        data = get_weather_forecast(location)
        if data:
            save_forecast(specific_region, data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Forecast for Ukraine regions")

    parser.add_argument("--region", type=str, help="Specific region name from REGIONS list")
    parser.add_argument("--all", action="store_true", help="Collect weather forecast for all 25 regions")

    args = parser.parse_args()

    if args.all:
        print("Starting forecast collection for all regions...")
        run_forecast(all_regions=True)
    elif args.region:
        print(f"Starting forecast collection for region: {args.region}")
        run_forecast(specific_region=args.region)
    else:
        print("Error: Please specify either --region [Region_Name] or use --all flag.")
