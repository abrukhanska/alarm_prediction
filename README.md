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


---

## Team Responsibility List

### Alina Bruhanska — Team Lead / ML Engineer

- **Project Oversight** — Team coordination, code reviews, and Git-flow management.
- **Data Integration** — Implemented the merge logic (Task 2c) joining Weather, Alarms, and ISW (D+1 shift) into a unified 190k-row matrix.
- **EDA** — Conducted Exploratory Data Analysis for ISW reports (Task 1c) and NLP features.
- **NLP Pipeline** — Developed TF-IDF vectorization (500 features), report length extraction, and the D+1 temporal shift (Task 2a).
- **Feature Engineering** — Engineered 17 key features including `temp_drop_last_3d` and `isw_sources_count` (Task 2b).
- **Model Training** — Implemented Linear Regression, Logistic Regression, and LightGBM using StandardScaler within a Pipeline to prevent leakage during cross-validation (Task 4).
- **Infrastructure** — AWS EC2 Cron architecture and ISW sources scraper development.
- **Frontend** — AEGIS Dashboard architecture (Next.js).

### Anastasiia Yermak — System Architect

- **EDA** — Partial analysis of ISW reports and NLP feature engineering.
- **Data Partitioning** — Implemented chronological TimeSeriesSplit (Task 3) preserving temporal integrity.
- **Backend** — Lead backend development using FastAPI.
- **Reporting** — Final report assembly, documentation, and frontend UI components.

### Milena Mashchenko — Weather Engineer

- **Weather EDA** — Comprehensive EDA for meteorological data (Task 1a), including the "Air Defense Blind Spot" visibility hypothesis and seasonal temperature cycles.
- **API Integration** — Weather data engineering via Visual Crossing, including data cleaning, normalization, and validation.
- **Model Interpretation** — Top-20 Feature Importance analysis for all models (Task 5b) with comparative analysis across models.

### Viktoriia Boriak — Data Analyst

- **War Events EDA** — EDA for alarm data (Task 1b), including "Sleep Deprivation" tactic analysis and anomaly detection ("Kurochkin's Easter Eggs").
- **Merged Data EDA** — EDA on the merged dataset (Task 1d), identifying the "Energy Terror" hypothesis through freezing temperature correlations.
- **Model Evaluation** — Confusion Matrix analysis (Task 5a) for all models with focus on Recall optimization.
- **Infrastructure** — AWS EC2 server configuration for model deployment and automated data collection.

---

## Project Resources

| Resource | Link |
|----------|------|
| **GitHub Repository** | https://github.com/abrukhanska/alarm_prediction |
| **Google Drive** | https://drive.google.com/drive/folders/1B8GYn1JL5meLz7me79VUDsR2iXmvvB1-?usp=sharing |**YouTube**|https://youtu.be/z88J31f0_rM|
