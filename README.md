# Internet Usage and Data Consumption Analytics using Apache Spark and Power BI

A Big Data mini-project that analyses application-level internet usage, identifies usage patterns, groups sessions and repeat customers using K-Means clustering, and flags unusually high data consumption using category-specific IQR thresholds.

## Table of Contents
- [Overview](#overview)
- [Objectives](#objectives)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Key Features](#key-features)
- [Project Results](#project-results)
- [Project Structure](#project-structure)
- [Environment Setup](#environment-setup)
- [Run the Backend](#run-the-backend)
- [Run the Frontend](#run-the-frontend)
- [API Endpoints](#api-endpoints)
- [Power BI Reporting](#power-bi-reporting)
- [Limitations](#limitations)
- [Future Enhancements](#future-enhancements)

## Overview

This project demonstrates an end-to-end Big Data analytics workflow using Hadoop Distributed File System (HDFS), Apache Spark, PySpark, Spark SQL, Apache Hive, and machine-learning techniques. Processed analytics are exported for reporting and served through a FastAPI backend to a React-based web dashboard.

The system analyses consumption by application, category, operator, network type, date, hour, and time of day. It also performs session-level and repeat-customer segmentation and flags sessions with unusually high consumption.

## Objectives

- Process and analyse a large telecom usage dataset using Big Data tools.
- Store raw and processed data in HDFS using Parquet.
- Use Spark SQL and Hive for structured analytics and table access.
- Apply K-Means clustering to session and repeat-customer features.
- Identify unusual data-consumption sessions using category-specific IQR thresholds.
- Present results through interactive charts and reporting pages.

## Dataset

**Dataset:** Africa Synthetic Telecom Application Data Consumption — Nigeria  
**Source:** [Hugging Face dataset](https://huggingface.co/datasets/electricsheepafrica/africa-synth-telecom-application-data-consumption-nigeria)

- Records: 800,000 sessions
- Columns: 10
- Time range: September 1 to October 1, 2025
- Format: Parquet

The dataset is synthetic and is intended to simulate application-level telecom usage. It must not be interpreted as real customer traffic.

## Architecture

```text
Synthetic Telecom Dataset (800,000 sessions)
                    |
                    v
          HDFS + Parquet Storage
                    |
                    v
       Apache Spark / PySpark Processing
                    |
                    v
         Spark SQL + Apache Hive
                    |
                    v
          Feature Engineering
                    |
          +---------+---------+
          |                   |
          v                   v
     K-Means Clustering   IQR Anomaly Flags
          |                   |
          +---------+---------+
                    |
                    v
           CSV Reporting Exports
                    |
             +------+------+
             |             |
             v             v
         FastAPI        Power BI
             |
             v
        React Dashboard
```

The development environment uses local Spark execution and a single-machine HDFS setup. It is not a multi-node production deployment.

## Technology Stack

| Technology | Purpose |
|---|---|
| Hadoop HDFS | Stores raw and processed project data |
| Apache Spark | Data processing and transformations |
| PySpark | Python interface for Spark |
| Spark SQL | Analytical queries and aggregations |
| Apache Hive | Table metadata and structured data access |
| Parquet | Columnar data storage |
| Spark MLlib / K-Means | Session and customer segmentation |
| IQR method | Category-specific unusual-consumption flags |
| Python | Data engineering and helper scripts |
| FastAPI | Backend API for analytics |
| React + Vite | Interactive web interface |
| Recharts | Charts in the web dashboard |
| Power BI | Reporting and visual analysis |
| CSV | Prepared reporting exports |

## Key Features

### 1. Data preparation
- Dataset quality checks and cleaning.
- Processed Parquet data stored in HDFS.
- Time-based and session-level feature engineering.
- Structured analytical tables available through Hive.

### 2. Session segmentation
K-Means was evaluated for multiple values of K and random seeds. The selected session model uses three clusters based on the chosen feature set.

### 3. Repeat-customer segmentation
Customer-level features are calculated for customers with at least two sessions. The selected customer model uses three clusters.

### 4. Usage anomaly flagging
The IQR method uses a separate upper threshold for each application category:

`Upper Threshold = Q3 + 1.5 \times IQR`

A session is flagged when its data consumption exceeds its category's threshold. Flagged records are retained for analysis.

### 5. Interactive analytics
The React interface includes:
- Dashboard
- Usage Analytics
- Anomalies
- Segments
- Power BI-style reporting page

## Project Results

The following values come from the validated project outputs recorded during development.

| Metric | Result |
|---|---:|
| Total sessions | 800,000 |
| Unique customers in full dataset | 765,245 |
| Total data consumption | 101,000,617.12 MB |
| Average data per session | 126.2508 MB |
| Average session duration | 90.5961 minutes |
| Flagged anomaly sessions | 11,804 |
| Anomaly rate | 1.4755% |
| Session clusters selected | 3 |
| Repeat customers included in customer clustering | 33,766 |
| Customer clusters selected | 3 |

### Category-specific anomaly thresholds

| Application category | Upper threshold |
|---|---:|
| Other | 74.390 MB |
| Social | 191.180 MB |
| Streaming | 724.895 MB |

These thresholds were calculated from the corresponding category's usage distribution. The values are statistical flags, not confirmed incidents.

### Session cluster interpretation

- **Cluster 0:** Long Social Sessions
- **Cluster 1:** High-Data Streaming Sessions
- **Cluster 2:** Short Social Sessions

The first and third clusters have similar average data consumption and app mix, but differ mainly in session duration. Therefore, these clusters should be interpreted cautiously.

### Customer cluster interpretation

- **Cluster 0:** Moderate-Data Multi-App Customers
- **Cluster 1:** Low-Data Single-App Customers
- **Cluster 2:** High-Data Customers

Customer clustering covers repeat customers only, not all unique customers in the dataset.

## Project Structure

The main project folders include:

```text
internet-usage-analytics/
├── backend/
│   ├── app/
│   │   ├── routers/
│   │   ├── schemas/
│   │   ├── services/
│   │   └── utils/
│   ├── requirements.txt
│   └── requirements-dev.txt
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   ├── services/
│   │   └── utils/
│   └── package.json
├── data/
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── analytics/
│   ├── features/
│   └── ml/
├── sql/
│   ├── spark_sql/
│   └── hive/
├── powerbi/
├── powerbi_data/
├── configs/
├── results/
├── tests/
└── README.md
```

Some folders may contain additional files or have different contents in the current checkout.

## Environment Setup

The original development environment uses Ubuntu through WSL2, Java 17, Python 3.12, Hadoop 3.4.2, PySpark 4.1.1, and Node.js 22.

### Start HDFS

Use the HDFS startup procedure configured on your machine. Verify it with:

```bash
hdfs dfs -ls /
```

Confirm the project data exists before running the analytics pipeline.

### Backend setup

From the repository root:

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If your virtual environment already exists, activate it rather than recreating it.

### Start the backend

```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Backend health check: [http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)  
API documentation: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

The backend expects the prepared CSV files in the configured `powerbi_data` directory.

### Frontend setup

Open a separate terminal:

```bash
cd frontend
npm install
npm run dev
```

Use the local URL printed by Vite. The frontend expects the API routes to be reachable through its configured development setup. If the backend is running on a different port, update the frontend proxy or API configuration accordingly.

### Production frontend build

```bash
npm run build
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/health` | Backend health |
| `GET /api/kpis` | Overall project KPIs |
| `GET /api/usage/apps` | Usage by application |
| `GET /api/usage/categories` | Usage by category |
| `GET /api/usage/operators` | Usage by operator |
| `GET /api/usage/networks` | Usage by network type |
| `GET /api/usage/time-of-day` | Usage by time of day |
| `GET /api/usage/daily` | Daily usage |
| `GET /api/usage/hourly` | Hourly usage |
| `GET /api/anomalies` | Flagged usage sessions |
| `GET /api/anomalies/thresholds` | Category thresholds |
| `GET /api/clusters/sessions` | Session cluster summaries |
| `GET /api/clusters/sessions/by-app` | Session clusters by application |
| `GET /api/clusters/customers` | Customer cluster profiles |
| `GET /api/powerbi/config` | Power BI embedding configuration status |

## Power BI Reporting

The project exports prepared CSV summaries for reporting. The React reporting page presents analytics using the FastAPI endpoints.

A live embedded Power BI Service report is a separate integration and requires a published report and appropriate Power BI Service configuration. The application does not claim that live embedding is enabled when it is not configured.

## Limitations

- The dataset is synthetic and does not represent verified real-world telecom activity.
- The HDFS and Spark setup is local/single-machine, not a multi-node production cluster.
- No verified ground-truth anomaly labels are available; anomaly detection accuracy metrics cannot be claimed.
- An anomaly is not proof of fraud or malicious behaviour.
- Customer segmentation includes only customers with two or more sessions.
- Session clusters should be interpreted cautiously because some clusters differ primarily in session duration.
- Customer-level shuffled-session controls did not establish strong evidence of persistent individual behaviour.

## Future Enhancements

- Configure and validate live Power BI Service embedding.
- Add automated pipeline execution and scheduled refreshes.
- Evaluate anomaly methods using verified labels when available.
- Test performance on larger datasets and a distributed cluster.
- Add automated integration tests and deployment configuration.

## Acknowledgements

Dataset: [Africa Synthetic Telecom Application Data Consumption — Nigeria](https://huggingface.co/datasets/electricsheepafrica/africa-synth-telecom-application-data-consumption-nigeria)

Project repository: [YADHRA/Big_Data_Architecture_miniproject](https://github.com/YADHRA/Big_Data_Architecture_miniproject)
