from app.utils.data_loader import DataLoader
from app.schemas.kpi import KPISummary


class KPIService:
    """Business logic for dashboard KPI data."""

    def __init__(self, loader: DataLoader | None = None):
        self.loader = loader or DataLoader()

    def get_summary(self) -> KPISummary:
        df = self.loader.load_csv("kpi_summary.csv")

        if len(df) != 1:
            raise ValueError(
                f"Expected exactly one KPI summary row, found {len(df)}"
            )

        record = df.iloc[0].to_dict()

        return KPISummary(**record)
