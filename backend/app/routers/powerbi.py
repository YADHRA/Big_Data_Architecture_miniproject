from fastapi import APIRouter

from app.schemas.powerbi import PowerBIConfig
from app.services.powerbi_service import PowerBIService


router = APIRouter(prefix="/api/powerbi", tags=["Power BI"])

service = PowerBIService()


@router.get("/config", response_model=PowerBIConfig)
def get_powerbi_config() -> PowerBIConfig:
    return service.get_config()
