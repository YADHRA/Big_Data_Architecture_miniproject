from fastapi import APIRouter

from app.schemas.usage import (
    UsageByApp,
    UsageByCategory,
    UsageByNetwork,
    UsageByOperator,
    UsageByTimeOfDay,
    UsageDaily,
    UsageHourly,
)
from app.services.usage_service import UsageService


router = APIRouter(
    prefix="/api/usage",
    tags=["Usage"],
)

service = UsageService()


@router.get("/apps", response_model=list[UsageByApp])
def get_usage_by_app() -> list[UsageByApp]:
    return service.get_apps()


@router.get("/categories", response_model=list[UsageByCategory])
def get_usage_by_category() -> list[UsageByCategory]:
    return service.get_categories()


@router.get("/operators", response_model=list[UsageByOperator])
def get_usage_by_operator() -> list[UsageByOperator]:
    return service.get_operators()


@router.get("/networks", response_model=list[UsageByNetwork])
def get_usage_by_network() -> list[UsageByNetwork]:
    return service.get_networks()


@router.get("/time-of-day", response_model=list[UsageByTimeOfDay])
def get_usage_by_time_of_day() -> list[UsageByTimeOfDay]:
    return service.get_time_of_day()


@router.get("/daily", response_model=list[UsageDaily])
def get_usage_daily() -> list[UsageDaily]:
    return service.get_daily()


@router.get("/hourly", response_model=list[UsageHourly])
def get_usage_hourly() -> list[UsageHourly]:
    return service.get_hourly()
