
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[2]
POWERBI_DATA_DIR = PROJECT_ROOT / "powerbi_data"


class Settings(BaseSettings):
    app_name: str = "Internet Usage Analytics API"
    app_version: str = "1.0.0"
    environment: str = "development"

    powerbi_data_dir: Path = POWERBI_DATA_DIR

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
