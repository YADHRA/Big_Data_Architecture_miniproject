from pathlib import Path

import pandas as pd

from app.config import settings


class DataLoader:
    """Load prepared Power BI CSV exports used by the FastAPI backend."""

    def __init__(self, data_dir: Path | None = None):
        self.data_dir = Path(data_dir or settings.powerbi_data_dir)

    def _path(self, filename: str) -> Path:
        path = self.data_dir / filename

        if not path.exists():
            raise FileNotFoundError(
                f"Power BI data file not found: {path}"
            )

        return path

    def load_csv(self, filename: str) -> pd.DataFrame:
        """Load a CSV file from the configured Power BI data directory."""
        return pd.read_csv(self._path(filename))
