from fastapi import APIRouter

from app.schemas.anomaly import AnomalySession, AnomalyThreshold
from app.services.anomaly_service import AnomalyService


router = APIRouter(
    prefix="/api/anomalies",
    tags=["Anomalies"],
)

service = AnomalyService()


@router.get("", response_model=list[AnomalySession])
def get_anomalies() -> list[AnomalySession]:
    return service.get_anomalies()


@router.get("/thresholds", response_model=list[AnomalyThreshold])
def get_anomaly_thresholds() -> list[AnomalyThreshold]:
    return service.get_thresholds()
