from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

checks = {
    "/api/kpis": [
        "total_sessions", "unique_customers", "total_data_mb",
        "avg_data_mb", "avg_duration_min", "total_anomalies",
        "anomaly_pct", "first_timestamp", "last_timestamp",
    ],
    "/api/usage/apps": [
        "app_name", "app_category", "sessions", "total_data_mb",
        "avg_data_mb", "avg_duration_min", "anomalies",
        "anomaly_pct", "session_share_pct", "data_share_pct",
    ],
    "/api/anomalies": [
        "session_id", "timestamp", "customer_id", "operator",
        "app_name", "app_category", "data_consumed_mb",
        "session_duration_minutes", "network_type", "time_of_day",
        "anomaly_threshold_mb", "anomaly_flag",
        "anomaly_reason", "ratio_to_threshold",
    ],
    "/api/clusters/sessions": [
        "session_cluster", "sessions", "total_data_mb",
        "avg_data_mb", "avg_duration_min", "anomalies",
        "anomaly_pct", "session_share_pct",
    ],
    "/api/clusters/sessions/by-app": [
        "session_cluster", "app_name", "sessions",
        "total_data_mb", "avg_data_mb",
    ],
    "/api/clusters/customers": [
        "cluster", "customer_count", "avg_total_sessions",
        "avg_total_data_mb", "avg_data_mb",
        "avg_session_duration_min", "avg_max_session_data_mb",
        "avg_unique_app_count", "avg_unique_network_count",
        "avg_weekend_sessions", "avg_night_sessions",
        "customer_share_pct", "cluster_name",
    ],
    "/api/powerbi/config": [
        "enabled", "configured", "message",
        "report_id", "embed_url",
    ],
}

print("=" * 70)
print("API RESPONSE SCHEMA AUDIT")
print("=" * 70)

for endpoint, expected_fields in checks.items():
    response = client.get(endpoint)

    assert response.status_code == 200

    data = response.json()

    if isinstance(data, list):
        assert len(data) > 0
        actual_fields = set(data[0].keys())
    else:
        actual_fields = set(data.keys())

    missing = set(expected_fields) - actual_fields

    assert not missing, (
        f"{endpoint}: missing fields {sorted(missing)}"
    )

    print(
        f"PASS | {endpoint:35} | "
        f"{len(expected_fields)} required fields present"
    )

print("=" * 70)
print("API RESPONSE SCHEMA AUDIT PASSED")
print("=" * 70)
