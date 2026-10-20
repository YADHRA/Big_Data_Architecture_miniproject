from datetime import date

from pydantic import BaseModel


class UsageByApp(BaseModel):
    app_name: str
    app_category: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageByCategory(BaseModel):
    app_category: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageByOperator(BaseModel):
    operator: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageByNetwork(BaseModel):
    network_type: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageByTimeOfDay(BaseModel):
    time_of_day: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageDaily(BaseModel):
    usage_date: date
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float


class UsageHourly(BaseModel):
    hour_of_day: int
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float
    data_share_pct: float
