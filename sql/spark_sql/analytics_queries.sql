-- Stage 9: analytical queries on the cleaned dataset (temp view: usage_cleaned)
-- Each query starts with a "-- name:" line. The runner uses that name for the output folder.

-- name: overall_kpis
SELECT
  COUNT(*)                                         AS total_sessions,
  COUNT(DISTINCT customer_id)                      AS unique_customers,
  ROUND(SUM(data_consumed_mb), 2)                  AS total_data_mb,
  ROUND(SUM(data_consumed_mb) / 1024, 2)           AS total_data_gb,
  ROUND(AVG(data_consumed_mb), 2)                  AS avg_data_mb_per_session,
  ROUND(AVG(session_duration_minutes), 2)          AS avg_duration_min,
  COUNT(DISTINCT app_name)                         AS num_apps,
  COUNT(DISTINCT app_category)                     AS num_categories,
  COUNT(DISTINCT operator)                         AS num_operators,
  COUNT(DISTINCT network_type)                    AS num_network_types,
  MIN(timestamp)                                   AS first_ts,
  MAX(timestamp)                                   AS last_ts
FROM usage_cleaned;

-- name: usage_by_app
SELECT
  app_name,
  app_category,
  COUNT(*)                                                           AS sessions,
  ROUND(SUM(data_consumed_mb), 2)                                    AS total_data_mb,
  ROUND(AVG(data_consumed_mb), 2)                                    AS avg_data_mb,
  ROUND(percentile_approx(data_consumed_mb, 0.5), 2)                 AS median_data_mb_approx,
  ROUND(AVG(session_duration_minutes), 2)                            AS avg_duration_min,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_sessions,
  ROUND(100.0 * SUM(data_consumed_mb) / SUM(SUM(data_consumed_mb)) OVER (), 2) AS pct_of_total_data
FROM usage_cleaned
GROUP BY app_name, app_category
ORDER BY total_data_mb DESC;

-- name: usage_by_category
SELECT
  app_category,
  COUNT(*)                                                           AS sessions,
  ROUND(SUM(data_consumed_mb), 2)                                    AS total_data_mb,
  ROUND(AVG(data_consumed_mb), 2)                                    AS avg_data_mb,
  ROUND(percentile_approx(data_consumed_mb, 0.5), 2)                 AS median_data_mb_approx,
  ROUND(AVG(session_duration_minutes), 2)                            AS avg_duration_min,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_sessions,
  ROUND(100.0 * SUM(data_consumed_mb) / SUM(SUM(data_consumed_mb)) OVER (), 2) AS pct_of_total_data
FROM usage_cleaned
GROUP BY app_category
ORDER BY total_data_mb DESC;

-- name: usage_by_network
SELECT
  network_type,
  COUNT(*)                                                           AS sessions,
  ROUND(SUM(data_consumed_mb), 2)                                    AS total_data_mb,
  ROUND(AVG(data_consumed_mb), 2)                                    AS avg_data_mb,
  ROUND(AVG(session_duration_minutes), 2)                            AS avg_duration_min,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_sessions,
  ROUND(100.0 * SUM(data_consumed_mb) / SUM(SUM(data_consumed_mb)) OVER (), 2) AS pct_of_total_data
FROM usage_cleaned
GROUP BY network_type
ORDER BY network_type;

-- name: usage_by_operator
SELECT
  operator,
  COUNT(*)                                                           AS sessions,
  ROUND(SUM(data_consumed_mb), 2)                                    AS total_data_mb,
  ROUND(AVG(data_consumed_mb), 2)                                    AS avg_data_mb,
  ROUND(AVG(session_duration_minutes), 2)                            AS avg_duration_min,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                 AS pct_of_sessions,
  ROUND(100.0 * SUM(data_consumed_mb) / SUM(SUM(data_consumed_mb)) OVER (), 2) AS pct_of_total_data
FROM usage_cleaned
GROUP BY operator
ORDER BY total_data_mb DESC;

-- name: usage_by_date
SELECT
  to_date(timestamp)                                AS usage_date,
  COUNT(*)                                          AS sessions,
  ROUND(SUM(data_consumed_mb), 2)                   AS total_data_mb,
  ROUND(AVG(data_consumed_mb), 2)                   AS avg_data_mb,
  ROUND(AVG(session_duration_minutes), 2)           AS avg_duration_min
FROM usage_cleaned
GROUP BY to_date(timestamp)
ORDER BY usage_date;
