from pydantic import BaseModel


class SessionClusterSummary(BaseModel):
    session_cluster: int
    sessions: int
    total_data_mb: float
    avg_data_mb: float
    avg_duration_min: float
    anomalies: int
    anomaly_pct: float
    session_share_pct: float


class SessionClusterByApp(BaseModel):
    session_cluster: int
    app_name: str
    sessions: int
    total_data_mb: float
    avg_data_mb: float


class CustomerClusterProfile(BaseModel):
    cluster: int
    customer_count: int
    avg_total_sessions: float
    avg_total_data_mb: float
    avg_data_mb: float
    avg_session_duration_min: float
    avg_max_session_data_mb: float
    avg_unique_app_count: float
    avg_unique_network_count: float
    avg_weekend_sessions: float
    avg_night_sessions: float
    customer_share_pct: float
    cluster_name: str
