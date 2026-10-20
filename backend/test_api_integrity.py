from math import isclose

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def get_json(endpoint: str):
    response = client.get(endpoint)
    assert response.status_code == 200, (
        f"{endpoint}: HTTP {response.status_code}"
    )
    return response.json()


print("=" * 70)
print("API DATA INTEGRITY AUDIT")
print("=" * 70)


# ------------------------------------------------------------
# KPI validation
# ------------------------------------------------------------

kpis = get_json("/api/kpis")

assert kpis["total_sessions"] == 800000
assert kpis["unique_customers"] == 765245
assert isclose(kpis["total_data_mb"], 101000617.12, rel_tol=0, abs_tol=0.01)
assert isclose(kpis["avg_data_mb"], 126.2508, rel_tol=0, abs_tol=0.0001)
assert isclose(kpis["avg_duration_min"], 90.5961, rel_tol=0, abs_tol=0.0001)
assert kpis["total_anomalies"] == 11804
assert isclose(kpis["anomaly_pct"], 1.4755, rel_tol=0, abs_tol=0.0001)

print("PASS | KPI totals and metrics")


# ------------------------------------------------------------
# Session-count reconciliation
# ------------------------------------------------------------

session_count_endpoints = [
    "/api/usage/apps",
    "/api/usage/categories",
    "/api/usage/operators",
    "/api/usage/networks",
    "/api/usage/time-of-day",
    "/api/usage/daily",
    "/api/usage/hourly",
]

for endpoint in session_count_endpoints:
    rows = get_json(endpoint)
    total_sessions = sum(row["sessions"] for row in rows)

    assert total_sessions == 800000, (
        f"{endpoint}: expected 800000 sessions, "
        f"got {total_sessions}"
    )

    print(
        f"PASS | {endpoint:35} | "
        f"sessions={total_sessions}"
    )


# ------------------------------------------------------------
# Session cluster reconciliation
# ------------------------------------------------------------

session_clusters = get_json("/api/clusters/sessions")

cluster_sessions = sum(
    row["sessions"] for row in session_clusters
)

assert cluster_sessions == 800000

print(
    f"PASS | /api/clusters/sessions           | "
    f"sessions={cluster_sessions}"
)


# ------------------------------------------------------------
# Customer cluster reconciliation
# ------------------------------------------------------------

customer_clusters = get_json("/api/clusters/customers")

customer_count = sum(
    row["customer_count"] for row in customer_clusters
)

assert customer_count == 33766

print(
    f"PASS | /api/clusters/customers           | "
    f"customers={customer_count}"
)


# ------------------------------------------------------------
# Anomaly reconciliation
# ------------------------------------------------------------

anomalies = get_json("/api/anomalies")

assert len(anomalies) == 11804
assert all(row["anomaly_flag"] == 1 for row in anomalies)

print(
    f"PASS | /api/anomalies                    | "
    f"anomalies={len(anomalies)}"
)


# ------------------------------------------------------------
# Threshold validation
# ------------------------------------------------------------

thresholds = get_json("/api/anomalies/thresholds")

assert len(thresholds) == 3

categories = {
    row["app_category"]
    for row in thresholds
}

assert categories == {"other", "social", "streaming"}

print(
    "PASS | /api/anomalies/thresholds          | "
    "3 categories"
)


# ------------------------------------------------------------
# Power BI configuration validation
# ------------------------------------------------------------

powerbi = get_json("/api/powerbi/config")

assert powerbi["enabled"] is False
assert powerbi["configured"] is False
assert powerbi["report_id"] is None
assert powerbi["embed_url"] is None

print(
    "PASS | /api/powerbi/config               | "
    "safe unconfigured state"
)


print("=" * 70)
print("API DATA INTEGRITY AUDIT PASSED")
print("=" * 70)
