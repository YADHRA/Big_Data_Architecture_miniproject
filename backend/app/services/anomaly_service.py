from app.schemas.anomaly import AnomalySession, AnomalyThreshold
from app.utils.data_loader import DataLoader


class AnomalyService:
    """Business logic for anomaly analytics data."""

    def __init__(self, loader: DataLoader | None = None):
        self.loader = loader or DataLoader()

    def get_anomalies(self) -> list[AnomalySession]:
        df = self.loader.load_csv("anomaly_sessions.csv")

        return [
            AnomalySession(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_thresholds(self) -> list[AnomalyThreshold]:
        df = self.loader.load_csv("anomaly_thresholds.csv")

        return [
            AnomalyThreshold(**row)
            for row in df.to_dict(orient="records")
        ]
