from fastapi import APIRouter

from app.schemas.kpi import KPISummary
from app.services.kpi_service import KPIService


router = APIRouter(
    prefix="/api/kpis",
    tags=["KPIs"],
)

service = KPIService()


@router.get("", response_model=KPISummary)
def get_kpis() -> KPISummary:
    return service.get_summary()
