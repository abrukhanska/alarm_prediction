# AEGIS: Air Event Guardian & Intelligence System

AEGIS is a Python-based SaaS platform designed to predict war-related events, specifically air raid alarms, across 24 regions of Ukraine. The system operates an end-to-end automated machine learning pipeline that integrates open-source intelligence from the Institute for the Study of War (ISW), meteorological conditions, and historical alarm autocorrelation to build a high-recall LightGBM predictive model.

---

## Team 4

| Role | Name | Responsibilities |
|------|------|-----------------|
| **Team Lead / ML Engineer** | Alina Bruhanska | Project management, Git-flow, and AWS EC2 Cron architecture. Development of the ISW NLP Pipeline (TF-IDF, D+1 shift), Feature Engineering (lags, weather stress), and Machine Learning models (LightGBM). |
| **System Architect** | Anastasiia Yermak | System infrastructure design and lead backend development using FastAPI. Responsible for technical documentation, final report assembly, and frontend UI components. |
| **Weather Engineer** | Milena Mashchenko | Weather data engineering and API integration via Visual Crossing. Handles meteorological data cleaning, normalization, and validation. |
| **Data Analyst** | Viktoriia Boriak | Exploratory Data Analysis (EDA) of war events and weather patterns. Conducts statistical hypothesis testing and anomaly detection. |

---

## Architecture and Data Pipeline

The system is built on a highly modular, production-ready infrastructure:

- **Automated Data Collectors** — Headless scraping of ISW reports (1,480+ documents), Visual Crossing weather APIs, and live air alarm feeds.
- **NLP Pipeline** — Transforms unstructured ISW text into a 500-feature TF-IDF matrix with a strict D+1 temporal shift to prevent look-ahead bias and capture "Calm Before the Storm" tactical signals.
- **Feature Engineering** — Merges heterogeneous data into a unified hourly grid (Oblast-Hour granularity), engineered with cyclical time encodings, weather stress indicators (e.g., freezing night conditions), and alarm momentum lags.
- **Predictive Modeling** — A LightGBM classifier validated via TimeSeriesSplit and a 30-day dynamic hold-out test set, optimizing for high Recall in air raid detection.
- **AEGIS Dashboard** — An interactive Next.js interface that visualizes real-time and predicted threat levels through color-coded mapping of Ukraine.

---

## Usage

### 1. Data Collection (Scrapers)

These modules collect daily battlefield assessments and weather conditions.

```bash
# Collect the full archive of ISW reports (Backfill mode)
python scrapers/isw_scraper.py --backfill

# Collect ISW report for the current day
python scrapers/isw_scraper.py --daily

# Fetch the 24h weather forecast for all regions
python scrapers/weather_forecast_new.py --all

# Start the live alarm WebSocket client
python scrapers/alarm_client_new.py
```

### 2. NLP & Data Processing

Clean, vectorize, and merge raw data into the final machine learning matrix.

```bash
# Run NLP pipeline (Text cleaning, D+1 shift, TF-IDF vectorization)
python nlp/isw_nlp_pipeline.py --build

# Merge Weather, Alarms, and ISW into an hourly grid
python data_processing/merge_datasets.py --merge

# Generate temporal lags, weather stress indices, and regional OHE
python data_processing/feature_engineering.py --build
```

### 3. Machine Learning

Train and evaluate the predictive models (Linear Regression, Logistic Regression, LightGBM).

```bash
# Train models using TimeSeriesSplit and evaluate on 30-day hold-out
python models/train_models.py --train
```

### 4. Backend API (FastAPI)

```bash
# Start the backend server
uvicorn backend.api.main:app --reload
```

### 5. Frontend (Next.js)

```bash
# Install dependencies and start the dashboard
cd frontend
npm install
npm run dev
```

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Machine Learning** | Python, scikit-learn, LightGBM, SciPy, Pandas, NumPy |
| **Backend** | Python, FastAPI, Uvicorn |
| **Frontend** | Next.js, React, TypeScript, Tailwind CSS |
| **Data Collection** | BeautifulSoup, curl_cffi, websockets |
| **MLOps / Infra** | AWS EC2, Linux Cron, Git |

---

## Project Structure

```
alarm_prediction/
├── analysis/             # EDA notebooks (ISW, Weather, Merged Data)
├── backend/api/          # FastAPI backend & routes
├── frontend/             # Next.js interactive dashboard
├── scrapers/             # Data collection scripts (ISW, Alarms, Weather)
├── validation/           # Data cleaners and validators
├── nlp/                  # NLP text processing and TF-IDF matrix generation
├── data_processing/      # Core ETL: merging and feature engineering pipelines
├── models/               # Model training scripts, saved .pkl files, and reports
├── data/                 # Raw and processed datasets (CSV, NPZ, JSON)
├── logs/                 # Execution logs for AWS Cron automation
├── requirements.txt      # Python dependencies
└── README.md
```

---

## Project Resources
YouTube: https://youtu.be/z88J31f0_rM
Project Data & Documentation (Google Drive):
https://drive.google.com/drive/folders/1B8GYn1JL5meLz7me79VUDsR2iXmvvB1-?usp=sharing
