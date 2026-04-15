# Inference script
#
# Dependencies:
# pip install pandas lightgbm scikit-learn numpy
#
# Python version: 3.10+
#
# How to run:
# 1. Place this file in the same folder as  4__lightgbm__v1.pkl
# 2. python inference_script.py

import pickle
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

TEST_DATA = {
    "hour_sin": [ 0.50,  0.76,  1.00,  0.66,  0.50,  0.00, -0.50, -0.67, -1.00, -0.86],
    "hour_cos": [ 0.87,  0.54,  0.00, -0.52, -0.81, -1.00, -0.91, -0.57,  0.00,  0.50],
    "month_sin": [ 0.50,  0.50,  0.87,  0.87,  1.00,  1.00,  0.87,  0.87,  0.50,  0.50],
    "month_cos": [ 0.87,  0.87,  0.50,  0.50,  0.00,  0.00, -0.50, -0.50, -0.87, -0.87],
    "dow_sin": [ 0.78,  0.97,  0.43,  0.00, -0.43, -0.97, -0.78,  0.43,  0.97,  0.00],
    "dow_cos": [ 0.62, -0.22, -0.90, -1.00, -0.90, -0.22,  0.62,  0.90, -0.22, -1.00],
    "is_night": [    0,     0,     0,     0,     0,     1,     1,     1,     0,     0],
    "is_morning": [    1,     1,     0,     0,     0,     0,     0,     0,     0,     0],
    "is_afternoon": [    0,     0,     1,     1,     0,     0,     0,     0,     1,     1],
    "is_evening": [    0,     0,     0,     0,     1,     0,     0,     0,     0,     0],
    "is_weekend": [    0,     0,     1,     1,     0,     0,     1,     1,     0,     0],
    "alarm_lag_6h": [    0,     0,     0,     1,     1,     1,     1,     0,     1,     0],
    "alarm_lag_24h": [    0,     1,     1,     1,     0,     1,     1,     1,     0,     0],
    "alarms_last_24h": [    0,     4,     9,    11,    13,     3,     8,     6,    17,     2],
    "n_regions_lag_1h": [    0,     1,     7,    11,    21,    14,     4,     0,     7,    17],
    "n_regions_lag_3h": [    1,     2,     2,     8,     5,    11,    23,    17,     5,     0],
    "n_regions_momentum": [    1,    -2,     3,     9,    11,    -8,   -12,    -5,     7,     1],
    "shahed_lag1h": [    1,     0,     1,     0,     0,     1,     1,     0,     1,     0],
    "pusk_lag1h": [    1,     0,     0,     1,     1,     0,     0,     0,     0,     0],
    "rocket_count_lag1h": [    0,     0,     0,     3,     5,     1,     0,     0,     4,     0],
    "drone_count_lag1h": [    0,     0,     2,     0,     1,     5,     4,     0,     6,     0],
    "ballistic_lag1h": [    0,     0,     0,     1,     1,     0,     0,     0,     1,     0],
    "kab_lag1h": [    0,     0,     0,     0,     1,     1,     0,     0,     0,     0],
    "acoustic_lag1h": [    0,     0,     1,     1,     1,     0,     1,     0,     1,     0],
    "iskander_lag1h": [    0,     0,     0,     1,     0,     0,     0,     0,     1,     0],
    "x59_lag1h": [    0,     0,     0,     0,     1,     0,     0,     0,     0,     0],
    "rszo_lag1h": [    0,     0,     0,     0,     1,     1,     0,     0,     0,     0],
    "takt_avia_lag1h": [    0,     0,     0,     0,     1,     0,     0,     0,     1,     0],
    "bude_huchno_lag1h": [    0,     0,     0,     1,     1,     1,     0,     0,     1,     0],
    "khvylya_lag1h": [    0,     0,     0,     1,     1,     0,     0,     0,     0,     0],
    "povtorni_lag1h": [    0,     0,     0,     0,     1,     1,     0,     0,     0,     0],
    "recon_lag1h": [    0,     0,     1,     1,     0,     0,     0,     0,     1,     0],
    "false_target_lag1h": [    0,     0,     0,     0,     1,     0,     0,     0,     0,     0],
    "dir_kyiv_lag1h": [    1,     0,     1,     0,     1,     0,     0,     1,     1,     0],
    "dir_kharkiv_lag1h": [    0,     0,     0,     1,     1,     1,     0,     0,     1,     0],
    "dir_dnipro_lag1h": [    0,     0,     0,     0,     1,     0,     1,     0,     0,     0],
    "dir_odesa_lag1h": [    0,     0,     0,     0,     0,     0,     1,     0,     0,     0],
    "cities_count_lag1h": [    6,    11,     3,    26,    60,    44,    13,     0,    14,    31],
    "f_shahed_roll3h": [0.40, 0.30, 0.00, 0.10, 0.20, 0.70, 0.90, 0.60, 0.20, 0.00],
    "f_shahed_roll6h": [0.33, 0.17, 0.00, 0.17, 0.33, 0.67, 0.83, 0.67, 0.33, 0.17],
    "f_shahed_roll24h": [0.08, 0.04, 0.08, 0.12, 0.17, 0.25, 0.33, 0.25, 0.17, 0.08],
    "f_tu95_roll3h": [0.00, 0.00, 0.40, 1.00, 1.00, 0.50, 0.00, 0.20, 0.00, 0.10],
    "f_tu95_roll6h": [0.00, 0.00, 0.00, 0.60, 0.50, 0.90, 0.70, 0.10, 0.10, 0.00],
    "f_tu95_roll24h": [0.00, 0.00, 0.04, 0.17, 0.21, 0.29, 0.17, 0.08, 0.04, 0.00],
    "f_mig31_roll3h": [0.00, 0.00, 0.00, 0.33, 0.67, 0.33, 0.00, 0.00, 0.33, 0.00],
    "f_mig31_roll6h": [0.00, 0.00, 0.00, 0.17, 0.33, 0.50, 0.33, 0.17, 0.17, 0.00],
    "f_pusk_roll3h": [0.33, 0.00, 0.00, 0.67, 1.00, 0.33, 0.00, 0.00, 0.33, 0.00],
    "f_pusk_roll6h": [0.17, 0.00, 0.00, 0.33, 0.67, 0.50, 0.17, 0.00, 0.17, 0.00],
    "f_pusk_roll24h": [0.04, 0.00, 0.04, 0.12, 0.21, 0.17, 0.08, 0.04, 0.08, 0.00],
    "f_kab_roll3h": [0.00, 0.00, 0.00, 0.00, 0.67, 0.67, 0.33, 0.00, 0.00, 0.00],
    "f_kab_roll6h": [0.00, 0.00, 0.00, 0.00, 0.33, 0.50, 0.33, 0.17, 0.00, 0.00],
    "f_kab_roll24h": [0.00, 0.00, 0.00, 0.04, 0.08, 0.12, 0.08, 0.04, 0.04, 0.00],
    "f_takt_avia_roll3h": [0.00, 0.00, 0.00, 0.00, 0.67, 0.33, 0.00, 0.00, 0.67, 0.00],
    "f_takt_avia_roll6h": [0.00, 0.00, 0.00, 0.00, 0.33, 0.33, 0.17, 0.00, 0.33, 0.00],
    "f_takt_avia_roll24h": [0.00, 0.00, 0.00, 0.04, 0.08, 0.12, 0.08, 0.04, 0.08, 0.00],
    "f_masovana_roll3h": [0.00, 0.00, 0.00, 0.00, 0.33, 0.67, 0.33, 0.00, 0.33, 0.00],
    "f_masovana_roll6h": [0.00, 0.00, 0.00, 0.00, 0.17, 0.50, 0.33, 0.17, 0.17, 0.00],
    "f_masovana_roll24h": [0.00, 0.00, 0.00, 0.04, 0.08, 0.17, 0.12, 0.08, 0.08, 0.00],
    "f_bude_huchno_roll3h": [0.00, 0.00, 0.00, 0.33, 1.00, 1.00, 0.33, 0.00, 0.67, 0.00],
    "f_bude_huchno_roll6h": [0.00, 0.00, 0.00, 0.17, 0.50, 0.83, 0.50, 0.17, 0.33, 0.00],
    "f_bude_huchno_roll24h": [0.00, 0.00, 0.00, 0.08, 0.17, 0.25, 0.21, 0.12, 0.12, 0.04],
    "f_vec_south_roll3h": [0.00, 0.00, 0.00, 0.00, 0.33, 0.33, 0.67, 0.33, 0.00, 0.00],
    "f_vec_south_roll6h": [0.00, 0.00, 0.00, 0.00, 0.17, 0.33, 0.50, 0.33, 0.17, 0.00],
    "f_vec_east_roll3h": [0.00, 0.00, 0.33, 0.67, 0.67, 0.33, 0.00, 0.00, 0.67, 0.33],
    "f_vec_east_roll6h": [0.00, 0.00, 0.17, 0.50, 0.50, 0.33, 0.17, 0.00, 0.33, 0.17],
    "f_drone_count_roll3h": [1.00, 0.00, 0.67, 0.00, 0.33, 1.67, 1.33, 0.00, 2.00, 0.00],
    "f_drone_count_roll6h": [0.50, 0.00, 0.33, 0.17, 0.33, 1.17, 1.00, 0.33, 1.00, 0.17],
    "f_drone_count_roll24h": [0.12, 0.04, 0.08, 0.12, 0.25, 0.54, 0.50, 0.29, 0.50, 0.12],
    "f_ppo_roll3h": [0.00, 0.00, 0.33, 0.67, 1.00, 0.67, 0.33, 0.00, 0.33, 0.00],
    "f_ppo_roll6h": [0.00, 0.00, 0.17, 0.50, 0.67, 0.67, 0.50, 0.17, 0.33, 0.00],
    "f_ppo_roll24h": [0.00, 0.00, 0.04, 0.17, 0.25, 0.29, 0.25, 0.12, 0.17, 0.08],
    "f_expl_confirm_roll3h": [0.00, 0.00, 0.33, 0.67, 1.00, 0.67, 0.33, 0.00, 1.00, 0.00],
    "f_expl_confirm_roll6h": [0.00, 0.00, 0.17, 0.33, 0.67, 0.67, 0.50, 0.17, 0.67, 0.00],
    "total_msg_count_roll3h": [  49,   32,  128,  256,  410,  159,   21,   11,  398,   80],
    "total_msg_count_roll6h": [ 110,   87,  222,  449,  708,  324,   58,   46,  551,  189],
    "total_msg_count_roll24h": [ 10,  487,  655,  858, 1210, 1100,  950,  845, 1512, 1345],
    "gur_massive_success_d1": [   0,    0,    0,    1,    1,    0,    0,    0,    1,    0],
    "gur_retaliation_72h": [   0,    0,    0,    0,    1,    1,    0,    0,    0,    0],
    "gur_logistics_14d": [   0,    1,    1,    1,    1,    1,    0,    0,    1,    0],
    "gur_energy_threat_d1": [   0,    0,    0,    1,    1,    1,    0,    0,    1,    0],
    "gur_intercepts_7d": [   0,    1,    1,    1,    1,    0,    1,    0,    1,    0],
    "isw_report_length": [1200, 2100, 1850, 3200, 4100, 2900, 1100,  900, 3800, 1700],
    "isw_sources_count": [   8,   12,   10,   18,   24,   16,    7,    5,   22,   11],
    "total_intensity": [ 0.1,  0.3,  0.5,  0.9,  1.5,  1.1,  0.4,  0.2,  1.8,  0.6],
    "intensity_per_1000": [ 0.2,  0.6,  1.0,  1.9,  3.0,  2.1,  0.8,  0.4,  3.5,  1.1],
    "isw_intensity_growth_1d":[ 0.0,  0.1,  0.3,  0.8,  1.2,  0.5, -0.2, -0.4,  0.9,  0.0],
    "isw_intensity_growth_7d":[ 0.0,  0.1,  0.2,  0.5,  0.9,  0.7,  0.2,  0.0,  1.1,  0.1],
    "attack_mentions": [   2,    5,    8,   18,   29,   15,    3,    1,   31,    7],
    "casualty_mentions": [   1,    3,    4,    9,   14,    8,    2,    1,   16,    4],
    "ground_mentions": [   3,    6,   10,   21,   33,   12,    4,    2,   28,    9],
    "hour_temp": [14.0, 12.0,  8.5,  5.0, -1.0, -4.0,  1.0,  3.5,  7.0, 10.0],
    "hour_humidity": [  65,   72,   80,   75,   60,   55,   78,   82,   70,   68],
    "hour_visibility": [  10,   10,    5,    8,   10,   10,    3,    6,   10,   10],
    "hour_windspeed": [ 8.0, 12.0,  6.5, 10.0, 18.0, 22.0,  5.0,  4.0, 14.0,  9.0],
    "hour_cloudcover": [  20,   40,   80,   60,   10,    5,   95,   85,   30,   45],
    "hour_pressure": [1015, 1012, 1008, 1005, 1018, 1022, 1003, 1000, 1010, 1014],
    "is_rain": [   0,    0,    1,    0,    0,    0,    1,    1,    0,    0],
    "is_snow": [   0,    0,    0,    0,    1,    1,    0,    0,    0,    0],
    "bad_weather_index": [ 0.0,  0.1,  0.6,  0.3,  0.5,  0.6,  0.8,  0.7,  0.2,  0.1],
    "low_visibility": [   0,    0,    1,    0,    0,    0,    1,    1,    0,    0],
    "strong_wind": [   0,    0,    0,    0,    1,    1,    0,    0,    0,    0],
    "freezing": [   0,    0,    0,    0,    1,    1,    0,    0,    0,    0],
    "energy_stress": [   0,    0,    0,    1,    1,    1,    0,    0,    1,    0],
    "syn_shahed_low_vis": [0, 0, 1, 0, 0, 0, 1, 0, 0, 0],
    "syn_kab_strong_wind": [0, 0, 0, 0, 1, 1, 0, 0, 0, 0],
    "syn_takt_avia_freeze": [0, 0, 0, 0, 1, 0, 0, 0, 1, 0],
    "syn_mass_night": [0, 0, 0, 0, 0, 1, 1, 1, 0, 0],
    "syn_prestrike_night": [0, 0, 0, 0, 0, 1, 0, 1, 0, 0],
    "syn_vec_south_coastal": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    "syn_x101_capital": [0, 0, 0, 0, 1, 0, 0, 0, 1, 0],
    "syn_isw_staging": [0, 0, 0, 1, 1, 0, 0, 0, 1, 0],
    "syn_prestrike_acoustic": [0, 0, 1, 1, 1, 0, 0, 0, 1, 0],
    "syn_frontline_rszo": [0, 0, 0, 0, 1, 1, 0, 0, 0, 0],
    "syn_frontline_shahed": [1, 0, 1, 0, 0, 1, 1, 0, 1, 0],
    "syn_energy_threat_cold": [0, 0, 0, 1, 1, 1, 0, 0, 1, 0],
    "syn_energy_threat_night": [0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
    "syn_ballistic_multiregion": [0, 0, 0, 1, 1, 0, 0, 0, 1, 0],
    "syn_bad_weather_night": [0, 0, 0, 0, 0, 0, 1, 1, 0, 0],
    "syn_energy_stress_multi": [0, 0, 0, 1, 1, 1, 0, 0, 1, 0],
    "calm_phase_risk": [0.01, 0.10, 0.30, 0.80, 0.96, 0.80, 0.50, 0.10, 0.06, 0.20],
    "hours_since_last_massive": [47.5, 49.5, 50.5,  0.0,  1.0,  3.0,  7.0,  4.0,  5.0,  8.0],
    "region_Chernihiv Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Chernivtsi Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_City of Kyiv": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Dnipropetrovsk Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Donetsk Oblast": [0, 0, 0, 1, 1, 1, 0, 0, 0, 0],
    "region_Ivano-Frankivsk Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Kharkiv Oblast": [0, 0, 0, 1, 1, 0, 0, 0, 1, 0],
    "region_Kherson Oblast": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    "region_Khmelnytskyi Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Kirovohrad Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Kyiv Oblast": [1, 0, 1, 0, 1, 0, 0, 1, 1, 0],
    "region_Lviv Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Mykolaiv Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Odesa Oblast": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    "region_Poltava Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Rivne Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Sumy Oblast": [0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
    "region_Ternopil Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Vinnytsia Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Volyn Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "region_Zakarpattia Oblast": [0, 0, 0, 0, 1, 0, 0, 0, 1, 0],
    "region_Zaporizhzhia Oblast": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    "region_Zhytomyr Oblast": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "is_frontline": [0, 0, 0, 1, 1, 1, 1, 0, 0, 0],
    "frontline_multi_alarm": [0, 0, 0, 1, 1, 0, 0, 0, 0, 0],
    "inter_alarm_spreading": [0.1, 0.2, 0.4, 0.7, 0.9, 0.3, 0.1, 0.2, 0.9, 0.3],
    "inter_high_intensity_frontline":[0, 0, 0, 1, 1, 0, 0, 0, 1, 0],
    "inter_recent_alarm_low_vis": [0, 0, 1, 0, 0, 0, 1, 0, 0, 0],
    "inter_bad_weather_night": [0, 0, 0, 0, 0, 0, 1, 1, 0, 0],
}
MODEL_PATH = Path(__file__).resolve().parent / "4__lightgbm__v1.pkl"
THRESHOLD  = 0.612
def load_model(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Model file not found: {path}\n"
                                f"Make sure 4__lightgbm__v1.pkl is in the same directory as this script.")
    with open(path, "rb") as f:
        return pickle.load(f)

def run_inference() -> None:
    print("-" * 50)
    print("Alarm Prediction System")
    print("-" * 50)
    print(f"\nStep 1: Loading model: {MODEL_PATH.name}")
    model = load_model(MODEL_PATH)
    expected_features: list[str] = list(model.feature_name_)
    print(f"Features expected by model: {len(expected_features)}")
    print(f"\nStep 2: Preparing data: {len(next(iter(TEST_DATA.values())))} rows")
    df = pd.DataFrame(TEST_DATA)
    df = df.reindex(columns=expected_features, fill_value=0)
    print(f"Threshold: {THRESHOLD}")
    print(f"\nStep 3: Running inference...\n")
    df = df.astype(np.float32)
    probabilities: np.ndarray = model.predict_proba(df)[:, 1]
    predictions: np.ndarray = (probabilities >= THRESHOLD).astype(int)
    print(f"{'Row':>4} {'Probability':>12} {'Decision':>10}")
    print("-" * 32)
    for i, (prob, pred) in enumerate(zip(probabilities, predictions)):
        verdict = "ALARM" if pred == 1 else "clear"
        print(f"{i:>2} {prob:>12.4f}  {verdict:>10}")
    print("-" * 32)
    print(f"\n Total rows: {len(predictions)}")
    print(f"ALARM: {int(predictions.sum())}")
    print(f"clear: {len(predictions) - int(predictions.sum())}")
    print(f"Threshold: {THRESHOLD}")
    print("\n" + "-" * 50)

if __name__ == "__main__":
    run_inference()