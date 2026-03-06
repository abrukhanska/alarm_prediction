from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import predict, alarms, weather, timeline, stats

app = FastAPI(title="AEGIS API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router, prefix="/api")
app.include_router(alarms.router, prefix="/api")
app.include_router(weather.router, prefix="/api")
app.include_router(timeline.router, prefix="/api")
app.include_router(stats.router, prefix="/api")

@app.get("/")
def root():
    return {"status": "AEGIS API running", "docs": "/docs"}