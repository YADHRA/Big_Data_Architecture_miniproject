from datetime import datetime

from pydantic import BaseModel


class KPISummary(BaseModel):
    total_sessions: int
    unique_customers: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    total_anomalies: int
    anomaly_pct: float
    first_timestamp: datetime
    last_timestamp: datetime
