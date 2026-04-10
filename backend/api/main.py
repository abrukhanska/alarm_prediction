from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import predict, alarms, weather, timeline, stats, forecast

app = FastAPI(title="AEGIS API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router,   prefix="/api")
app.include_router(alarms.router,    prefix="/api")
app.include_router(weather.router,   prefix="/api")
app.include_router(timeline.router,  prefix="/api")
app.include_router(stats.router,     prefix="/api")
app.include_router(forecast.router)  

@app.get("/")
def root():
    return {"status": "AEGIS API running", "version": "2.0.0", "docs": "/docs"}