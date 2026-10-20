from app.schemas.cluster import (
    CustomerClusterProfile,
    SessionClusterByApp,
    SessionClusterSummary,
)
from app.utils.data_loader import DataLoader


class ClusterService:
    """Provide session and customer cluster analytics from prepared CSV exports."""

    def __init__(self, loader: DataLoader | None = None):
        self.loader = loader or DataLoader()

    def get_session_clusters(self) -> list[SessionClusterSummary]:
        df = self.loader.load_csv("session_cluster_summary.csv")
        return [
            SessionClusterSummary(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_session_clusters_by_app(self) -> list[SessionClusterByApp]:
        df = self.loader.load_csv("session_cluster_by_app.csv")
        return [
            SessionClusterByApp(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_customer_clusters(self) -> list[CustomerClusterProfile]:
        df = self.loader.load_csv("customer_cluster_profile.csv")
        return [
            CustomerClusterProfile(**row)
            for row in df.to_dict(orient="records")
        ]
