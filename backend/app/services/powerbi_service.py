from app.schemas.powerbi import PowerBIConfig


class PowerBIService:
    """Provide Power BI embedding configuration without exposing credentials."""

    def get_config(self) -> PowerBIConfig:
        return PowerBIConfig(
            enabled=False,
            configured=False,
            message=(
                "Power BI embedding is not configured. "
                "Use the exported Power BI data files or configure "
                "Power BI Service embedding for live embedding."
            ),
        )
