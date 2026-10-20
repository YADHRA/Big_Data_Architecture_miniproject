from fastapi import FastAPI

from app.config import settings
from app.routers.anomalies import router as anomalies_router
from app.routers.clusters import router as clusters_router
from app.routers.powerbi import router as powerbi_router
from app.routers.kpis import router as kpis_router
from app.routers.usage import router as usage_router


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Backend API for Internet Usage and Data Consumption Analytics.",
)

app.include_router(kpis_router)

app.include_router(usage_router)
app.include_router(anomalies_router)
app.include_router(clusters_router)
app.include_router(powerbi_router)
@app.get("/api/health")
def health_check():
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
    }
