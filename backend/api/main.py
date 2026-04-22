from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import predict, alarms, weather, stats, forecast

app = FastAPI(
    title="AEGIS API",
    version="2.0.0",
    description="Air alarm prediction system for Ukraine — Team 4",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router,  prefix="/api")
app.include_router(alarms.router,   prefix="/api")
app.include_router(weather.router,  prefix="/api")
app.include_router(stats.router,    prefix="/api")

app.include_router(forecast.router)

@app.get("/")
def root():
    return {
        "status":    "AEGIS API running",
        "version":   "2.0.0",
        "docs":      "/docs",
        "endpoints": [
            "GET  /api/forecast           ← God API (use this for the UI)",
            "GET  /api/predict/{region}   ← per-region prediction",
            "GET  /api/predict/all        ← all regions",
            "GET  /api/current-alarms     ← live alarm status",
            "GET  /api/weather/{region}   ← current weather",
            "GET  /api/stats              ← dashboard stats",
            "GET  /api/health             ← health check",
            "POST /api/update-forecast    ← trigger predict_24h.py",
            "POST /api/admin/retrain      ← trigger model retrain (MLOps)",
        ],
    }