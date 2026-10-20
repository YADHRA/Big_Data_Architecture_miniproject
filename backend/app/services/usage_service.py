from app.schemas.usage import (
    UsageByApp,
    UsageByCategory,
    UsageByNetwork,
    UsageByOperator,
    UsageByTimeOfDay,
    UsageDaily,
    UsageHourly,
)
from app.utils.data_loader import DataLoader


class UsageService:
    """Business logic for usage analytics data."""

    def __init__(self, loader: DataLoader | None = None):
        self.loader = loader or DataLoader()

    def get_apps(self) -> list[UsageByApp]:
        df = self.loader.load_csv("usage_by_app.csv")
        return [UsageByApp(**row) for row in df.to_dict(orient="records")]

    def get_categories(self) -> list[UsageByCategory]:
        df = self.loader.load_csv("usage_by_category.csv")
        return [
            UsageByCategory(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_operators(self) -> list[UsageByOperator]:
        df = self.loader.load_csv("usage_by_operator.csv")
        return [
            UsageByOperator(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_networks(self) -> list[UsageByNetwork]:
        df = self.loader.load_csv("usage_by_network.csv")
        return [
            UsageByNetwork(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_time_of_day(self) -> list[UsageByTimeOfDay]:
        df = self.loader.load_csv("usage_by_time_of_day.csv")
        return [
            UsageByTimeOfDay(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_daily(self) -> list[UsageDaily]:
        df = self.loader.load_csv("usage_daily.csv")

        return [
            UsageDaily(**row)
            for row in df.to_dict(orient="records")
        ]

    def get_hourly(self) -> list[UsageHourly]:
        df = self.loader.load_csv("usage_hourly.csv")

        return [
            UsageHourly(**row)
            for row in df.to_dict(orient="records")
        ]
