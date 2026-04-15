# AEGIS: Air Event Guardian & Intelligence System

AEGIS is a Python-based SaaS platform designed to predict war-related events, specifically air raid alarms, across 24 regions of Ukraine. The system operates an end-to-end automated machine learning pipeline that integrates open-source intelligence from the Institute for the Study of War (ISW), meteorological conditions, and historical alarm autocorrelation to build a high-recall LightGBM predictive model.

---

## Team 4

| Role | Name | Responsibilities |
|------|------|-----------------|
| **Team Lead / ML Engineer** | Alina Bruhanska | Project management, Git-flow, and AWS EC2 Cron architecture. Development of the ISW NLP Pipeline (TF-IDF, D+1 shift), Feature Engineering (lags, weather stress), and Machine Learning models (LightGBM).Telegram scraper (WOW feature). ML Models (LightGBM, XGBoost, HistGBM), Automated Retraining script.|
| **System Architect** | Anastasiia Yermak | System infrastructure design and lead backend development using FastAPI. Responsible for technical documentation, final report assembly, and frontend UI components. Postman testing|
| **Weather Engineer** | Milena Mashchenko | Weather data engineering and API integration via Visual Crossing. Handles meteorological data cleaning, normalization, and validation. 24-hour Predict script, Self-contained Inference script, Evaluation Plots (Confusion Matrices & Top-20 features).|
| **Data Analyst** | Viktoriia Boriak | Exploratory Data Analysis (EDA) of war events and weather patterns. Conducts statistical hypothesis testing and anomaly detection. GUR scraper (WOW feature), Cloudflare bypass, GUR NLP Processor (D+1 shift, rolling threat indices)|

---

## Architecture and Data Pipeline

The system is built on a highly modular, production-ready infrastructure:

- **Automated Data Collectors** — Headless scraping of ISW reports, Visual Crossing weather APIs, Telegram tactical signals (8 channels), GUR official intelligence reports, and live air alarms.
- **NLP Pipeline** — Transforms unstructured text into predictive features. Includes a 500-feature TF-IDF matrix for ISW, strict D+1 temporal shifts for GUR data, and precise hour-based lags for Telegram signals to prevent target leakage.
- **Feature Engineering** — Merges heterogeneous data into a unified hourly grid (Oblast-Hour granularity), engineered with cyclical time encodings, weather stress indicators (e.g., freezing night conditions), and alarm momentum lags.
- **Predictive Modeling** — A LightGBM classifier validated via TimeSeriesSplit and a 30-day dynamic hold-out test set, optimizing for high Recall in air raid detection. Ensembles of tree-based models (`LightGBM`, `XGBoost`, `HistGradientBoosting`) tuned via `GridSearchCV`, optimizing for high Recall in air raid detection.
- **AEGIS Dashboard & SaaS** — A FastAPI backend providing 24-hour rolling forecasts, integrated with an interactive Next.js interface that visualizes predicted threat levels through color-coded mapping of Ukraine.
---

## Usage

### 1. Data Collection (Scrapers)

These modules collect daily battlefield assessments, weather conditions and tactical intelligence.

```bash
# Collect the full archive of ISW reports (Backfill mode)
python scrapers/isw_scraper.py --backfill

# Collect ISW report for the current day
python scrapers/isw_scraper.py --daily

# Fetch the 24h weather forecast for all regions
python scrapers/weather_forecast_new.py --all

# Start the live alarm WebSocket client
python scrapers/alarm_client_new.py

# Collect historical GUR intelligence (Backfill mode)
python scrapers/gur_scraper.py --backfill

# Collect Telegram tactical signals (last 24 hours)
python scrapers/telegram_scraper.py --hours 24
```

### 2. NLP & Data Processing

Clean, vectorize, and merge raw data into the final machine learning matrix.

```bash
# Run NLP pipeline (Text cleaning, D+1 shift, TF-IDF vectorization)
python nlp/isw_nlp_pipeline.py --build

# Build GUR features (D+1 shift, 5 tactical indices)
python scrapers/gur_features.py --build

# Extract Telegram signals (lag application)
python scrapers/telegram_feature_extractor.py --build

# Merge Weather, Alarms, and ISW into an hourly grid
python data_processing/merge_datasets.py --merge

# Generate temporal lags, weather stress indices, and regional OHE
python data_processing/feature_engineering.py --build
```

### 3. Machine Learning & Inference

Train, evaluate, and test the predictive models.

```bash
# Train Top-3 Models (LightGBM, XGBoost, HistGBM) & Tune Hyperparameters
python models/train_models.py --train

# Generate Evaluation Plots (Confusion Matrices, Feature Importance)
jupyter notebook analysis/eda_models.ipynb

# Run Self-Contained Inference Script
python models/inference_script.py

# Generate 24h predictions (runs hourly via Cron)
python models/predict_24h.py

# Retrain model weekly with F1 validation gate
python models/retrain.py
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
| **Machine Learning** | Python, scikit-learn, LightGBM, XGBoost, SciPy, Pandas, NumPy |
| **Backend** | Python, FastAPI, Uvicorn |
| **Frontend** | Next.js, React, TypeScript, Tailwind CSS |
| **Data Collection** | BeautifulSoup, cloudscraper, curl_cffi, websockets |
| **MLOps / Infra** | AWS EC2, Linux Cron, Git |

---

## Project Structure

```
alarm_prediction/
├── analysis/             # EDA notebooks (ISW, Weather, Merged Data, Models) and Evaluation Plots
├── backend/api/          # FastAPI backend & routes for SaaS
├── data/                 # Raw and processed datasets (CSV, NPZ, JSON)
├── data_processing/      # Core ETL: merging and feature engineering pipelines
├── docs/                 # Documentation, architectural diagrams, and screenshots
├── frontend/             # Next.js interactive dashboard
├── logs/                 # Execution logs for AWS Cron automation
├── models/               # Training scripts, inference, retraining, and saved .pkl files
├── nlp/                  # NLP text processing and TF-IDF matrix generation
├── scrapers/             # Data collection scripts (ISW, Alarms, Weather, Telegram, GUR)
├── validation/           # Data cleaners and validators
├── .gitignore            # Git ignore rules
├── README.md             # Project documentation
├── postman_collection.json # API testing collection
└── requirements.txt      # Python dependencies
```

---


## Team Responsibility List

### Alina Bruhanska — Team Lead / ML Engineer

- **Project Oversight** — Team coordination, code reviews, and Git-flow management.
- **Data Integration** — Implemented the merge logic (Task 2c) joining Weather, Alarms, and ISW (D+1 shift) into a unified 190k-row matrix.
- **EDA** — Conducted Exploratory Data Analysis for ISW reports (Task 1c) and NLP features.
- **NLP Pipeline** — Developed TF-IDF vectorization (500 features), report length extraction, and the D+1 temporal shift (Task 2a).Developed Telegram tactical signals scraper/extractor (WOW feature).
- **Feature Engineering** — Engineered 17 key features including `temp_drop_last_3d` and `isw_sources_count` (Task 2b).
- **Model Training** — Implemented Linear Regression, Logistic Regression, and LightGBM using StandardScaler within a Pipeline to prevent leakage during cross-validation (Task 4). Trained and tuned LightGBM, XGBoost, and HistGradientBoosting models. Developed the automated CI/CD retraining pipeline.
- **Infrastructure** — AWS EC2 Cron architecture and ISW sources scraper development.
- **Frontend** — AEGIS Dashboard architecture (Next.js).
- **Documentation** - Wrote the initial project README.md and recorded the YouTube video.

### Anastasiia Yermak — System Architect

- **EDA** — Partial analysis of ISW reports and NLP feature engineering.
- **Data Partitioning** — Implemented chronological TimeSeriesSplit (Task 3) preserving temporal integrity.
- **Backend & SaaS** — Lead backend development using FastAPI. Created endpoints for forecast updates, lazy model loading, and 24-hour JSON forecast generation.
- **Testing & Reporting** — Engineered Postman collections for API validation. Compiled final PDF report, documentation, presentation.

### Milena Mashchenko — Weather Engineer

- **Weather EDA** — Comprehensive EDA for meteorological data (Task 1a), including the "Air Defense Blind Spot" visibility hypothesis and seasonal temperature cycles.
- **API Integration** — Weather data engineering via Visual Crossing, including data cleaning, normalization, and validation.
- **Inference & Prediction** — Developed the 24-hour rolling predict script and the self-contained inference script.
- **Model Interpretation** — Generated Confusion Matrices and Top-20 Feature Importance charts, justifying the selection of LightGBM based on its superior Recall metric.

### Viktoriia Boriak — Data Analyst

- **War Events EDA** — EDA for alarm data (Task 1b), including "Sleep Deprivation" tactic analysis and anomaly detection ("Kurochkin's Easter Eggs").
- **Merged Data EDA** — EDA on the merged dataset (Task 1d), identifying the "Energy Terror" hypothesis through freezing temperature correlations.
- **Model Evaluation** — Confusion Matrix analysis (Task 5a) for all models with focus on Recall optimization.
- **GUR Intelligence Pipeline (WOW Feature)** — Developed the GUR scraper with Cloudflare bypass capabilities and Backfill archive logic.
- **GUR NLP Processing** — Engineered the GUR text processor to extract military signals, applying D+1 temporal shifts and rolling threat indices to prevent target leakage.
- **Infrastructure** — AWS EC2 server configuration for model deployment and automated data collection.
- **Documentation** - Updated final README.md

---

## Project Resources

| Resource | Link |
|:---------|:-----|
| **GitHub Repository** | [github.com/abrukhanska/alarm_prediction](https://github.com/abrukhanska/alarm_prediction) |
| **Google Drive** | [Project Data & Docs](https://drive.google.com/drive/folders/1B8GYn1JL5meLz7me79VUDsR2iXmvvB1-?usp=sharing) |
| **Video Presentation** | [Watch on YouTube](https://youtu.be/z88J31f0_rM) |
