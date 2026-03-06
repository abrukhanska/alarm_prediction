import random

def get_stats():
    return {
        "active_alarms_count": 4,
        "total_regions": 25,
        "avg_threat_level": 4.5,
        "most_threatened_region": "kharkiv",
        "total_alarms_today": 12,
        "total_duration_today_hours": 5.5
    }

def get_weather(region: str):
    return {
        "region": region,
        "temp": 15.0,
        "humidity": 60.0,
        "windspeed": 5.5,
        "winddir": 180.0,
        "visibility": 10.0,
        "cloudcover": 50.0,
        "pressure": 1012.0,
        "conditions": "Clear",
        "precip": 0.0
    }

def get_current_alarms():
    return {
        "timestamp": "2024-03-05T12:00:00Z",
        "active_count": 4,
        "total_regions": 25,
        "regions": [
            {"id": "kharkiv", "name": "Kharkiv", "active": True, "type": "missile", "since": "2024-03-05T12:00:00Z",
             "threat_level": "critical"},
            {"id": "donetsk", "name": "Donetsk", "active": True, "type": "artillery", "since": "2024-03-05T12:00:00Z",
             "threat_level": "critical"},
            {"id": "zaporizhzhia", "name": "Zaporizhzhia", "active": True, "type": "drone",
             "since": "2024-03-05T12:00:00Z", "threat_level": "high"},
            {"id": "sumy", "name": "Sumy", "active": True, "type": "drone", "since": "2024-03-05T12:00:00Z",
             "threat_level": "high"},

            {"id": "luhansk", "name": "Luhansk", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "high"},
            {"id": "dnipropetrovsk", "name": "Dnipro", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "medium"},
            {"id": "poltava", "name": "Poltava", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "medium"},
            {"id": "mykolaiv", "name": "Mykolaiv", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "medium"},

            {"id": "kyiv_city", "name": "Kyiv", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "low"},
            {"id": "kyiv_oblast", "name": "Kyiv Oblast", "active": False, "type": "none",
             "since": "2024-03-05T12:00:00Z", "threat_level": "low"},
            {"id": "odesa", "name": "Odesa", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "low"},
            {"id": "chernihiv", "name": "Chernihiv", "active": False, "type": "none", "since": "2024-03-05T12:00:00Z",
             "threat_level": "low"}
        ]
    }

def get_prediction(region: str):
    return {
        "region": region,
        "region_name": region.capitalize().replace("_", " "),
        "threat_level": "Medium",
        "probability_1h": 0.45,
        "probability_3h": 0.35,
        "probability_6h": 0.20,
        "probability_12h": 0.10,
        "threat_types": {"missile": 0.4, "drone": 0.5, "artillery": 0.1},
        "updated_at": "2024-03-05T12:00:00Z"
    }

def get_timeline(region: str):
    hours_data = []
    for i in range(0, 24, 2):
        base_prob = random.uniform(0.1, 0.7)
        hours_data.append({
            "hour": f"{str(i).zfill(2)}:00",
            "probability": base_prob,
            "missile": base_prob * 0.4,
            "drone": base_prob * 0.5,
            "artillery": base_prob * 0.1
        })

    return {
        "region": region,
        "hours": hours_data
    }