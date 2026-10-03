-- Stage 10: Hive metastore database and external Parquet-backed tables.
-- EXTERNAL = metadata only. DROP TABLE never deletes the Parquet files.
-- Schemas are read from the Parquet footers.

CREATE DATABASE IF NOT EXISTS internet_usage
COMMENT 'Internet Usage Analytics: synthetic telecom application usage';

DROP TABLE IF EXISTS internet_usage.raw_usage;

CREATE TABLE internet_usage.raw_usage
USING PARQUET
LOCATION 'hdfs://localhost:9000/internet_usage/raw_compat';

DROP TABLE IF EXISTS internet_usage.cleaned_usage;

CREATE TABLE internet_usage.cleaned_usage
USING PARQUET
LOCATION 'hdfs://localhost:9000/internet_usage/cleaned/usage_cleaned';
