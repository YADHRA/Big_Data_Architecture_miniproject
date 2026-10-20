from fastapi import APIRouter

from app.schemas.cluster import (
    CustomerClusterProfile,
    SessionClusterByApp,
    SessionClusterSummary,
)
from app.services.cluster_service import ClusterService


router = APIRouter(prefix="/api/clusters", tags=["Clusters"])

service = ClusterService()


@router.get("/sessions", response_model=list[SessionClusterSummary])
def get_session_clusters() -> list[SessionClusterSummary]:
    return service.get_session_clusters()


@router.get("/sessions/by-app", response_model=list[SessionClusterByApp])
def get_session_clusters_by_app() -> list[SessionClusterByApp]:
    return service.get_session_clusters_by_app()


@router.get("/customers", response_model=list[CustomerClusterProfile])
def get_customer_clusters() -> list[CustomerClusterProfile]:
    return service.get_customer_clusters()
