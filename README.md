# AEGIS — Air Event Guardian & Intelligence System

[![CI/CD Pipeline](https://github.com/abrukhanska/alarm_prediction/actions/workflows/deploy.yml/badge.svg)](https://github.com/abrukhanska/alarm_prediction/actions/workflows/deploy.yml)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-2.0-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![LightGBM](https://img.shields.io/badge/Model-LightGBM-brightgreen)](https://lightgbm.readthedocs.io/)
[![AWS EC2](https://img.shields.io/badge/Infra-AWS%20EC2-FF9900?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/ec2/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**AEGIS** is a production-grade SaaS platform that predicts air raid alarms across all 24 regions of Ukraine 24 hours in advance. It fuses open-source battlefield intelligence (ISW), live meteorological data, Telegram tactical signals, and GUR official reports into a unified hourly feature matrix, then applies a high-recall LightGBM classifier to generate actionable forecasts served through a FastAPI backend and a Next.js interactive dashboard.

<img width="1600" height="760" alt="aegis" src="https://github.com/user-attachments/assets/86cd847f-568e-45f8-aa53-dee638fbb16c" />


> **Demo video:** [Watch on YouTube](https://youtu.be/z88J31f0_rM) · **Live repo:** [github.com/abrukhanska/alarm_prediction](https://github.com/abrukhanska/alarm_prediction)

---

## Table of Contents

1. [Team](#team)
2. [System Architecture](#system-architecture)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Quick Start](#quick-start)
6. [Usage](#usage)
7. [API Reference](#api-reference)
8. [Tech Stack](#tech-stack)
9. [Project Structure](#project-structure)
10. [CI/CD](#cicd)
11. [Team Responsibilities](#team-responsibilities)
12. [Resources](#resources)

---

## Team

| Role | Name | Key Contributions |
|------|------|-------------------|
| **Team Lead / ML Engineer** | Alina Bruhanska | Git-flow, AWS EC2 Infrastructure, ISW NLP Pipeline, Feature Engineering, Multi-model training, Automated Retraining, Telegram scraper |
| **System Architect** | Anastasiia Yermak | FastAPI backend, SaaS infrastructure design, Postman test suite, final report & presentation |
| **Weather Engineer** | Milena Mashchenko | Visual Crossing API integration, meteorological feature engineering, 24h predict script, evaluation plots |
| **Data Analyst** | Viktoriia Boriak | EDA (war events, weather, merged), statistical hypothesis testing, GUR scraper & NLP processor |

---

## System Architecture

AEGIS operates as a fully automated pipeline running on AWS EC2:

```
┌───────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                           │
│  ISW Reports · Visual Crossing · Telegram (×8) · GUR · Alarms │
└───────────────────────┬───────────────────────────────────────┘
                        │  (Scrapers — hourly/daily cron)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                     NLP & ETL PIPELINE                      │
│  TF-IDF (500 feat) · D+1 shift · Lag features · OHE         │
└───────────────────────┬─────────────────────────────────────┘
                        │  190k-row Oblast-Hour feature matrix
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   ML MODEL (LightGBM)                       │
│  TimeSeriesSplit CV · 30-day hold-out · High-Recall tuning  │
│  Weekly automated retraining with F1 validation gate        │
└───────────────────────┬─────────────────────────────────────┘
                        │  predictions/latest.json (hourly)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              AEGIS API  (FastAPI v2.0)                      │
│  /api/forecast · /api/predict · /api/current-alarms         │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
          Next.js Dashboard — color-coded Ukraine map
```

Detailed architecture diagrams are available in [`docs/system_architecture.png`](docs/system_architecture.png) and [`docs/screenshots/system_diagram_detailed.png`](docs/screenshots/system_diagram_detailed.png).

---

## Prerequisites

Make sure the following tools are installed before proceeding.

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Python | 3.10+ | Required for `zoneinfo` stdlib module |
| Node.js | 18+ | Required for Next.js frontend |
| npm | 9+ | Bundled with Node.js |
| Git | any | — |

### Required API Keys

Register and obtain the following credentials before running the project:

| Service | Registration | `.env` variable |
|---------|-------------|-----------------|
| Air Alerts API | [devs.alerts.in.ua](https://devs.alerts.in.ua/) | `ALERTS_API_KEY` |
| Visual Crossing Weather | [visualcrossing.com](https://www.visualcrossing.com/) | `VISUAL_CROSSING_API_KEY` |
| Telegram MTProto | [my.telegram.org](https://my.telegram.org/) | `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` |

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/abrukhanska/alarm_prediction.git
cd alarm_prediction

# 2. Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Open .env and fill in your API keys (see Prerequisites above)

# 5. Install frontend dependencies
cd frontend && npm install && cd ..
```

### `.env` file reference

```env
# Air Alarms API (https://devs.alerts.in.ua/)
ALERTS_API_KEY=your_key_here

# Visual Crossing Weather API (https://www.visualcrossing.com/)
VISUAL_CROSSING_API_KEY=your_key_here

# Telegram MTProto (https://my.telegram.org/)
TELEGRAM_API_ID=your_id
TELEGRAM_API_HASH=your_hash
TELEGRAM_PHONE=+380XXXXXXXXX
```

---

## Quick Start

The fastest way to get AEGIS running end-to-end:

```bash
# Terminal 1 — Start the backend API
uvicorn backend.api.main:app --reload

# Terminal 2 — Start the frontend dashboard
cd frontend && npm run dev
```

Then open [http://localhost:3000](http://localhost:3000) for the dashboard and [http://localhost:8000/docs](http://localhost:8000/docs) for the interactive API docs.

To run a standalone inference without the full pipeline (no API keys needed):

```bash
python models/inference_script.py
```

---

## Usage

### 1. Data Collection (Scrapers)

Collect daily battlefield assessments, weather conditions, and tactical intelligence.

```bash
### 1. Data Collection (Scrapers)

Collect daily battlefield assessments, weather conditions, and tactical intelligence.
```bash
# ISW daily assessment (backfill full archive or run daily)
python scrapers/isw_scraper.py --backfill
python scrapers/isw_scraper.py --daily

# ISW Sources extractor (extract and resolve external links from downloaded reports)
python scrapers/isw_sources_scraper.py --all
python scrapers/isw_sources_scraper.py --stats

# 24-hour weather forecast for all 24 regions
python scrapers/weather_forecast_new.py --all

# Live alarm WebSocket listener
python scrapers/alarm_client.py

# GUR official intelligence (backfill or daily)
python scrapers/gur_scraper.py --backfill
python scrapers/gur_scraper.py --daily

# Telegram tactical signals (last 24 hours, 8 channels)
python scrapers/telegram_scraper.py --hours 24
```

### 2. NLP & Data Processing

Clean, vectorize, and merge raw data into the ML feature matrix.

```bash
# ISW NLP pipeline: text cleaning, D+1 shift, TF-IDF (500 features)
python nlp/isw_nlp_pipeline.py --build

# GUR features: D+1 shift, 5 rolling tactical indices
python scrapers/gur_features.py --build

# Telegram signals: lag application & feature extraction
python data_processing/telegram_feature_extractor.py --build

# Merge weather, alarms, and ISW into a unified hourly grid
python data_processing/merge_datasets.py --merge

# Generate temporal lags, weather stress indices, and regional OHE
python data_processing/feature_engineering.py --build
```

### 3. Machine Learning & Inference

Train, evaluate, and run predictions.

```bash
# Train LightGBM, XGBoost, HistGBM and tune hyperparameters
python models/train_models.py --train

# Open evaluation notebooks (confusion matrices, feature importance)
jupyter notebook analysis/eda_models.ipynb

# Self-contained inference (no live data required)
python models/inference_script.py

# Generate 24h rolling predictions (runs hourly via Cron on EC2)
python models/predict_24h.py

# Trigger weekly retraining with F1 validation gate
python models/retrain.py
```

### 4. Backend API
**Local Development:**
```bash
uvicorn backend.api.main:app --reload
# API docs available at http://localhost:8000/docs
```
**Production (AWS EC2):**
```
The backend runs continuously as a managed `systemd` service (`aegis.service`). It is automatically deployed and restarted via GitHub Actions CI/CD on every push to the `main` branch.
```bash
# Check production service status on EC2
sudo systemctl status aegis
```
### 5. Frontend Dashboard

**Local Development:**
```bash
cd frontend
npm run dev
# Dashboard available at http://localhost:3000
```
**Production (Vercel):**
```bash
The frontend is fully managed by Vercel's Continuous Deployment pipeline. Commits to the `main` branch trigger automatic builds, ensuring zero-downtime updates.
**Live URL:** [https://aegis-ukraine-alert-forecast.vercel.app](https://aegis-ukraine-alert-forecast.vercel.app)
```
---

## API Reference

All endpoints are documented interactively at `/docs` (Swagger UI) when the server is running.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/forecast` | 24h rolling forecast for all regions (primary UI endpoint) |
| `GET` | `/api/predict/{region}` | Per-region alarm prediction |
| `GET` | `/api/predict/all` | Predictions for all 24 regions |
| `GET` | `/api/current-alarms` | Live alarm status from WebSocket feed |
| `GET` | `/api/weather/{region}` | Current weather for a region |
| `GET` | `/api/stats` | Dashboard summary statistics |
| `GET` | `/api/health` | Health check |
| `POST` | `/api/update-forecast` | Manually trigger `predict_24h.py` |
| `POST` | `/api/admin/retrain` | Trigger model retraining (MLOps) |

A Postman collection covering all endpoints is available at [`postman_collection.json`](postman_collection.json).

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Machine Learning** | Python, scikit-learn, LightGBM, XGBoost, HistGradientBoosting, Pandas, NumPy |
| **NLP** | TF-IDF (500 features), custom D+1 temporal shift, rolling threat indices |
| **Backend** | FastAPI 2.0, Uvicorn, Pydantic |
| **Frontend** | Next.js 18, React, TypeScript, Tailwind CSS |
| **Data Collection** | BeautifulSoup, cloudscraper, curl_cffi, websockets, Telethon |
| **MLOps / Infra** | AWS EC2, Linux Cron, GitHub Actions (CI/CD), systemd |

---

## Project Structure

```
alarm_prediction/
│
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD: lint → deploy to EC2 on push to main
│
├── analysis/                   # Jupyter notebooks: EDA (ISW, Weather, Merged, Models)
│   └── plots/                  # Generated confusion matrices & feature importance charts
│
├── backend/
│   └── api/
│       ├── main.py             # FastAPI app entry point
│       ├── routes/             # Endpoint handlers (forecast, predict, alarms, weather, stats)
│       ├── models/             # Pydantic schemas
│       └── data/               # Live data helpers
│
├── data/
│   ├── raw/                    # Original scraped data (alarms, ISW, weather, war events)
│   └── processed/              # Feature matrices, TF-IDF arrays, merged datasets
│
├── data_processing/
│   ├── merge_datasets.py       # ETL: joins all sources into an hourly Oblast-Hour grid
|   ├── telegram_feature_extractor.py # NLP regex, hourly aggregation & rolling features for Telegram signals
│   └── feature_engineering.py  # Lags, cyclical encodings, weather stress indices, OHE
│
├── docs/
│   ├── system_architecture.png           # High-level architecture diagram
│   ├── data_pipeline.md                  # Data pipeline documentation
│   └── screenshots/
│       ├── system_diagram_detailed.png   # Detailed component diagram
│       └── nlp_io_diagram.png            # NLP input/output flow
│
├── frontend/                   # Next.js dashboard (color-coded Ukraine threat map)
│
├── logs/                       # Cron execution logs from AWS EC2
│
├── models/
│   ├── train_models.py         # Multi-model training with GridSearchCV
│   ├── predict_24h.py          # Hourly rolling 24h prediction (Cron entry point)
│   ├── retrain.py              # Weekly automated retraining with F1 gate
│   └── inference_script.py     # Self-contained inference (no live data required)
│
├── nlp/
│   └── isw_nlp_pipeline.py     # ISW text cleaning, TF-IDF vectorisation, D+1 shift
│
├── scrapers/                   # Data collection: ISW, Alarms, Weather, Telegram, GUR
│
├── validation/                 # Data quality checkers and schema validators
│
├── .env.example                # Environment variable template
├── .gitignore
├── postman_collection.json     # Ready-to-import Postman API test suite
├── README.md
└── requirements.txt
```

---

## CI/CD

AEGIS uses GitHub Actions for a two-stage pipeline that triggers automatically on every push to main.

push to main
    │
    ├── [build]  Set up Python 3.10 → install flake8 → lint (E9, F63, F7, F82)
    │
    └── [deploy] SSH into AWS EC2 → git pull → pip install → systemctl restart aegis

The workflow file is located at:

.github/workflows/deploy.yml

### Required GitHub Secrets

- AWS_HOST
- AWS_USER
- AWS_SSH_KEY

---

## Server Automation (Linux Cron)

The AWS EC2 instance runs a scheduled background job manager (crontab) for automated data collection, ML inference, and weekly model retraining.

### Schedule Overview

| Schedule | Cron Syntax | Script |
|---|---|---|
| Every hour | 0 * * * * | models/predict_24h.py |
| Every day at 06:00 UTC | 0 6 * * * | scrapers/isw_scraper.py --daily |
| Every day at 07:00 UTC | 0 7 * * * | scrapers/weather_forecast_new.py --all |
| Every Sunday at 03:00 UTC | 0 3 * * 0 | models/retrain.py |

---

## Production Crontab Configuration

# AEGIS Production Cron Jobs
# ---------------------------------------------------------

PROJECT_DIR=/home/ubuntu/alarm_prediction
PYTHON_ENV=venv/bin/python

# 1. Hourly 24h rolling forecast
0 * * * * cd $PROJECT_DIR && $PYTHON_ENV models/predict_24h.py >> logs/predict.log 2>&1

# 2. Daily ISW intelligence scraper (06:00 UTC)
0 6 * * * cd $PROJECT_DIR && $PYTHON_ENV scrapers/isw_scraper.py --daily >> logs/isw.log 2>&1

# 3. Daily Weather forecast update (07:00 UTC)
0 7 * * * cd $PROJECT_DIR && $PYTHON_ENV scrapers/weather_forecast_new.py --all >> logs/weather.log 2>&1

# 4. Weekly LightGBM Retraining with F1 Validation Gate (Sundays 03:00 UTC)
0 3 * * 0 cd $PROJECT_DIR && $PYTHON_ENV models/retrain.py >> logs/retrain.log 2>&1

---

## Team Responsibilities

<details>
<summary><strong>Alina Bruhanska — Team Lead / ML Engineer</strong></summary>

- **Project Oversight** — Team coordination, code reviews, and Git-flow management.
- **Data Integration** — Merge logic joining Weather, Alarms, and ISW (D+1 shift) into a matrix.
- **NLP Pipeline** — TF-IDF vectorisation (500 features), report length extraction, D+1 temporal shift.
* **Feature Engineering** — 170+ engineered features across multiple domains to capture complex war dynamics:
  * **Meteorological & Stress:** `bad_weather_index`, `energy_stress` (freezing + night), `temp_72h_change`, and `temp_feels_diff`.
  * **Spatial & Contagion:** `n_regions_momentum`, `alarm_lag_1h/3h/6h`, and `inter_alarm_spreading` to track cascading threats.
  * **Tactical & NLP:** 140+ vectorized Telegram signals (e.g., `f_tu95`, `f_shahed`), 3h/6h/24h rolling threat windows.
  * **Temporal:** Cyclical sin/cos encodings for hour/day/week to preserve temporal continuity.
* **Model Training & Selection** — Evaluated 5 different architectures using a 5-fold `TimeSeriesSplit` to preserve temporal integrity:
  * **Baseline Models:** `Linear Regression` (MaxAbsScaler baseline) and `Logistic Regression` (L1/L2 GridSearch) to establish performance floors.
  * **Advanced GBDT Ensemble:** `LightGBM` (Leaf-wise growth), `XGBoost` (Level-wise growth with L1+L2 leaf weights), and `HistGradientBoosting` (Native NaN handling).
  * **Optimization Pipeline:** All models integrated into `Scikit-learn Pipelines` with `StandardScaler` to prevent data leakage and `GridSearchCV` for automated hyperparameter tuning.
  * **Decision Logic:** Tuned for **High-Recall** (0.4xAUC + 0.4xRecall + 0.2xF1) using an optimal probability threshold to prioritize civil safety by minimizing missed alarms (False Negatives).
- **Infrastructure** — AWS EC2 Cron architecture, ISW sources scraper.
- **YouTube video**.

</details>

<details>
<summary><strong>Anastasiia Yermak — System Architect</strong></summary>

- **Data Partitioning** — Chronological TimeSeriesSplit preserving temporal integrity.
- **Backend & SaaS** — FastAPI backend: forecast endpoints, lazy model loading, 24h JSON generation.
- **Frontend** — AEGIS Dashboard architecture (Next.js).
- **Testing & Reporting** — Postman collection for API validation. Final PDF report and presentation.

</details>

<details>
<summary><strong>Milena Mashchenko — Weather Engineer</strong></summary>

- **Weather EDA** — Analysis of meteorological data including the "Air Defense Blind Spot" visibility hypothesis and seasonal temperature cycles.
- **API Integration** — Visual Crossing weather data engineering: cleaning, normalisation, validation.
- **Inference & Prediction** — 24-hour rolling predict script and self-contained inference script.
- **Model Interpretation** — Confusion Matrices and Top-20 Feature Importance charts; justified LightGBM selection based on superior Recall.
- - **Documentation** — Initial README

</details>

<details>
<summary><strong>Viktoriia Boriak — Data Analyst</strong></summary>

- **War Events EDA** — Alarm data analysis including "Sleep Deprivation" tactic analysis and anomaly detection ("Kurochkin's Easter Eggs").
- **Merged Data EDA** — Identified the "Energy Terror" hypothesis via freezing temperature correlations.
- **WOW Feature** — GUR scraper with Cloudflare bypass and backfill archive logic.
- **GUR NLP Processing** — Military signal extraction with D+1 shifts and rolling threat indices.
- **Infrastructure** — AWS EC2 server configuration for deployment.
- - **Documentation** — Initial README

</details>

---
## Resources

| Resource | Link |
|----------|------|
| **Live Dashboard** | [aegis-ukraine-alert-forecast.vercel.app](https://aegis-ukraine-alert-forecast.vercel.app) |
| **GitHub Repository**| [github.com/abrukhanska/alarm_prediction](https://github.com/abrukhanska/alarm_prediction) |
| **Video Demo** | [Watch on YouTube](https://youtu.be/z88J31f0_rM) |
| **Google Drive** | [Project Data & Docs Folder](https://drive.google.com/drive/folders/1B8GYn1JL5meLz7me79VUDsR2iXmvvB1-?usp=sharing) |
| **Production API Docs** | [Swagger UI (Live)](http://13.63.201.169/docs) |
| **Local API Docs** | `http://localhost:8000/docs` |
