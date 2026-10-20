from datetime import datetime

from pydantic import BaseModel


class AnomalySession(BaseModel):
    session_id: str
    timestamp: datetime
    customer_id: str
    operator: str
    app_name: str
    app_category: str
    data_consumed_mb: float
    session_duration_minutes: float
    network_type: str
    time_of_day: str
    anomaly_threshold_mb: float
    anomaly_flag: int
    anomaly_reason: str
    ratio_to_threshold: float


class AnomalyThreshold(BaseModel):
    app_category: str
    anomaly_threshold_mb: float
