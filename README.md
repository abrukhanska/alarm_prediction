# AEGIS: Air Event Guardian & Intelligence System

AEGIS is a Python-based SaaS platform designed to predict war-related events, including air alarms, explosions, and artillery fire, across 25 regions of Ukraine. The system integrates open-source intelligence from the Institute for the Study of War (ISW), meteorological forecasts, and historical alarm statistics to build a comprehensive risk assessment model.

---

## Team 4

| Role | Name | Responsibilities |
| --- | --- | --- |
| **Team Lead** | **Alina Bruhanska** | Project management, code review, and Git-flow management. Development of the ISW Sources Scraper (processing 21k+ sources). NLP strategy, feature engineering, and AEGIS Dashboard architecture. |
| **System Architect** | **Anastasiia Yermak** | System infrastructure design and lead backend development using FastAPI. Responsible for technical documentation, final report assembly, and frontend UI components. |
| **Weather Engineer** | **Milena Mashchenko** | Weather data engineering and API integration via Visual Crossing. Handles meteorological data cleaning, normalization, and validation. |
| **Data Analyst** | **Viktoriia Boriak** | Exploratory Data Analysis (EDA) of war events and weather patterns. Conducts statistical hypothesis testing and anomaly detection. |

---

## Architecture and Data

The system is built on a multi-module infrastructure to ensure a complete data processing cycle:

- **Data Receiver**: Automated collection of reports from the ISW (1,467 documents processed), weather data from Visual Crossing, and live air alarm feeds.
- **NLP Pipeline**: A sequence of text preprocessing including cleaning, tokenization, and lemmatization to calculate a daily Escalation Score (1–10) based on linguistic analysis.
- **Integration Pipeline**: Synchronization of diverse data streams (textual, meteorological, and historical) using a composite key consisting of region and hourly datetime.
- **AEGIS Dashboard**: An interactive interface built with Next.js that visualizes real-time threat levels through color-coded mapping of Ukraine.

---

## Usage

### 1. ISW Scrapers

These modules collect daily battlefield assessments and extract analytical sources.

```bash
# Collect the full archive of reports from 2022-02-24 to present (Backfill mode)
python scrapers/isw_scraper.py --backfill

# Collect only the report for the current day (Daily mode)
python scrapers/isw_scraper.py --daily

# Collect a report for a specific date
python scrapers/isw_scraper.py --date 2024-06-15

# Extract and classify all external links from the collected reports
python scrapers/isw_sources_scraper.py --all
```

### 2. Weather Data

Modules for fetching forecasts and performing historical data validation.

```bash
# Fetch the 24h weather forecast for all 25 regions
python scrapers/weather_forecast.py --all
```

### 3. Weather Validator

Validation of historical and forecast weather data against physical constraints.

```bash
# Validate historical weather data
python validation/weather_validator.py --historical

# Validate forecast weather data
python validation/weather_validator.py --forecast

# Run full validation (historical + forecast)
python validation/weather_validator.py --full
```

### 4. Alarm Client

Real-time air alarm monitoring via WebSocket.

```bash
# Start the live alarm client
python scrapers/alarm_client.py
```

### 5. Backend API (FastAPI)

```bash
# Start the backend server
uvicorn backend.api.main:app --reload
```

### 6. Frontend (Next.js)

```bash
# Install dependencies
cd frontend
npm install

# Start the development server
npm run dev
```

---

## Tech Stack

| Layer | Technologies |
| --- | --- |
| **Backend** | Python, FastAPI, Uvicorn |
| **Frontend** | Next.js, React, TypeScript, Tailwind CSS |
| **Scrapers** | Python, BeautifulSoup, curl_cffi, websockets |
| **NLP** | Python (tokenization, lemmatization, scoring) |
| **Data** | JSON, CSV, REST API (Visual Crossing) |
| **Validation** | Custom validators with physical range checks |

---

## Project Structure

```
alarm_prediction/
├── backend/api/          # FastAPI backend
│   ├── main.py
|   ├── data/             # Mock data
│   ├── models/           # Pydantic schemas
│   └── routes/           # API endpoints (alarms, predict, stats, timeline, weather)
├── frontend/             # Next.js dashboard
│   ├── app/              # Pages and layout
│   ├── components/       # UI components (map, panels, timeline)
│   ├── lib/              # Utilities, types, API client
│   └── public/geo/       # GeoJSON for Ukraine map
├── scrapers/             # Data collection scripts
│   ├── alarm_client.py
│   ├── isw_scraper.py
│   ├── isw_sources_scraper.py
│   └── weather_forecast.py
├── validation/           # Data validators
│   └── weather_validator.py
├── analysis/             # EDA notebooks
├── data/                 # Raw and processed data
│   ├── raw/
│   └── processed/
├── docs/                 # Documentation and diagrams
├── nlp/                  # NLP pipeline (in development)
├── logs/                 # Log files
├── requirements.txt
└── README.md
```

---

## Project Resources
- **Project Data and Documentation**: [Google Drive](https://drive.google.com/drive/folders/1B8GYn1JL5meLz7me79VUDsR2iXmvvB1-)
