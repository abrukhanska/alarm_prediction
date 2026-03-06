import requests
import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("ALERTS_API_KEY")
URL = "https://api.alerts.in.ua/v1/alerts/active.json"

def get_all_alarms():
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        response = requests.get(URL, headers = headers, timeout = 10)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": "API unavailable"}
    except Exception:
        return {"error": "API unavailable"}

def get_alarm_summary():
    data = get_all_alarms()
    if "error" in data:
        return data
    alerts = data.get("alerts", [])
    active_regions = set()
    for alert in alerts:
        oblast_name = alert.get("location_oblast")

        if oblast_name:
            active_regions.add(oblast_name)
    active_regions = list(active_regions)
    return {
        "active_count": len(active_regions),
        "regions": active_regions
    }
if __name__ == "__main__":
    summary = get_alarm_summary()
    print("CURRENT ALARMS")
    if "error" in summary:
        print(f"Помилка: {summary['error']}")
    else:
        print(f"Active: {summary['active_count']}/25 regions")
        print(f"Регіони: {', '.join(summary['regions'])}")