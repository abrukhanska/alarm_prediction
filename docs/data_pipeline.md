# Data Pipeline Documentation

This document describes the data flow, storage structure, and integration strategy for the **AEGIS project**.

---

## 1. Data Sources Overview

We integrate multiple sources to create a comprehensive dataset for air raid alert prediction:

- **ISW Reports**  
  HTML text, collected by `isw_scraper.py`.  
  Volume: ~1460 reports.  
  *(Mandatory)*

- **ISW Sources**  
  URLs and titles, collected by `isw_sources_scraper.py`.  
  Volume: ~15K references.  
  *(Mandatory)*

- **Weather Forecast**  
  JSON data, collected by `weather_forecast.py`.  
  Scope: 24h for all 25 regions.  
  *(Mandatory)*

- **Weather Historical**  
  JSON data, approximately 876K hourly records provided by Kurochkin.  
  *(Support)*

- **WAR Events**  
  CSV data, approximately 75K alarm records provided by Kurochkin.  
  *(Mandatory)*

- **Real-time Alarms**  
  JSON live data, collected via `alarm_client.py`.  
  *(Mandatory)*

---

## 2. Processing and Merging Strategy

- **Primary Key**  
  All datasets are joined using a composite key consisting of **region** and **datetime_hour**.

- **Weather Normalization**  
  Data is validated against physical ranges, such as:
  - humidity between **0–100%**
  - windspeed under **200 km/h**

- **War Events**  
  Historical alarms are converted into binary flags where:
  - `0` = no alarm  
  - `1` = active alarm  
  for each **region-hour block**.

- **ISW Text**  
  Daily intelligence reports are associated with **all 24 hours** of the respective day to provide context for the model.

---

## 3. Avoiding Data Leakage (Critical Rules)

To prevent the model from **"looking into the future"** during training, we implement these rules:

- **Time-Based Split**  
  Data is split **chronologically**  
  *(e.g., Train: 2022–2024, Test: 2025)* rather than randomly.

- **Prediction Constraint**  
  To predict an alarm for a specific hour, the model can only use **weather or ISW data from previous periods**.

- **Feature Lag**  
  ISW reports used for a specific day must be from the **previous day** to ensure the data was available at the time of prediction.

---

## 4. File Structure and Storage

- `data/raw/`  
  This is our **"source of truth"**.  
  Files in this directory are **never modified**.

- `data/processed/`  
  Contains **final datasets ready for machine learning**, which can be **fully regenerated from raw data at any time**.
