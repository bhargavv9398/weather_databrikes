# Weather Data Pipeline — Databricks + S3 + Delta Lake

An end-to-end incremental data pipeline that ingests live weather data from the Open-Meteo API, lands it in Amazon S3, and processes it through a medallion (bronze → silver → gold) architecture using Databricks and Delta Lake.

## Overview

This project simulates a real-world data engineering workflow: hourly ingestion of live weather data for 5 Indian cities, incremental processing into clean structured tables, aggregation into analytics-ready summaries, and automated data quality validation.

**Cities tracked:** Hyderabad, Delhi, Mumbai, Bangalore, Chennai

## Architecture

```
Open-Meteo API (live weather data)
        │
        │  hourly, scheduled via Databricks Jobs
        ▼
   Amazon S3 — Bronze Layer
   (raw JSON, partitioned by date/hour)
        │
        │  boto3 read → flatten → Spark DataFrame
        ▼
   Delta Table — Silver Layer
   (cleaned, deduped, structured: city, timestamp,
    temperature, humidity, precipitation, wind)
        │
        │  aggregation
        ▼
   Delta Table — Gold Layer
   (daily summaries per city: avg/max/min temp,
    total precipitation, avg wind/humidity)
        │
        ▼
   Automated Data Quality Gate
   (completeness, nulls, range checks, duplicates, freshness)
```

## Tech Stack

- **Ingestion:** Python, `requests`, `boto3`
- **Storage:** Amazon S3 (raw landing zone)
- **Processing:** Databricks (Serverless compute), PySpark
- **Storage format:** Delta Lake
- **Orchestration:** Databricks Jobs (hourly schedule)
- **Data source:** [Open-Meteo API](https://open-meteo.com/) (free, no API key required)

## Pipeline Stages

### 1. Ingestion → Bronze
A Python script calls the Open-Meteo `/v1/forecast` endpoint for each city and writes the raw JSON response to S3, partitioned by ingestion date and hour:
```
s3://bucket/bronze/date=YYYY-MM-DD/hour=HH/CityName.json
```
This runs automatically every hour via a scheduled Databricks Job, so the dataset grows continuously rather than being a static snapshot.

### 2. Bronze → Silver
Raw JSON files are read with `boto3` (Databricks Serverless compute restricts direct Spark-to-S3 filesystem access, so `boto3` is used as the fetch layer instead of `s3a://` paths). Each file's nested `hourly` arrays are flattened into individual rows, then loaded into a Spark DataFrame and written as a Delta table (`weather_silver`).

Key transformations:
- Flattening nested hourly arrays into one row per `(city, timestamp)`
- Deduplication on `(city, timestamp)` to handle overlapping hourly API pulls
- Schema standardization (typed timestamp, numeric fields)

### 3. Silver → Gold
Daily aggregates are computed per city: average/max/min temperature, total precipitation, average humidity and wind speed. Saved as `weather_gold_daily_summary`, ready for dashboarding or BI tools.

### 4. Data Quality Gate
Before data is considered "trusted," an automated validation step checks:
- **Completeness** — every city has data for the expected time range
- **Null checks** — no missing values in key fields
- **Range/sanity checks** — humidity between 0–100%, temperature within plausible bounds, no negative precipitation
- **Duplicate detection** — confirms dedup logic worked
- **Freshness** — latest timestamp per city is recent, confirming the hourly job is still running

The pipeline raises an exception and halts if any check fails, rather than silently proceeding with bad data.

## Results

| Metric | Value |
|---|---|
| Bronze files ingested | 105+ (growing hourly) |
| Silver rows (post-dedup) | 960 |
| Cities tracked | 5 |
| Data quality violations | 0 |
| Duplicate records | 0 |
| Null values | 0 |

## Design Decisions

- **Medallion architecture (bronze/silver/gold):** separates raw, cleaned, and aggregated data so each layer has a clear responsibility and failures can be traced to a specific stage.
- **Partitioning by date/hour:** makes it possible to process only new data incrementally, rather than reprocessing the entire dataset on every run.
- **boto3 over native Spark S3 connector:** Databricks Serverless compute blocks direct Hadoop-level S3 credential configuration (`fs.s3a.*`) for security reasons. `boto3` is used as an intermediate fetch layer, with Spark taking over once data is in memory.
- **Open-Meteo forecast data includes future hours:** the API returns both recent and forecasted hourly data in a single response, so the bronze layer naturally contains a mix of historical and forecast records — handled explicitly rather than treated as a bug.
- **Automated quality gate over passive logging:** checks are designed to fail loudly (raise an exception) rather than just print warnings, mirroring how production pipelines should behave.

## Future Improvements

- Switch from full-table overwrite to Delta Lake `MERGE` for true incremental upserts
- Add a lightweight dashboard (Databricks SQL or Streamlit) on top of the gold layer
- Add Slack/email alerting on data quality gate failures
- Move credentials to Databricks Secrets instead of notebook variables
- Add a second data source (e.g. air quality) and join in the silver/gold layers

## Setup

1. Create an S3 bucket and an IAM user with S3 access
2. Set up a Databricks workspace (Community Edition works)
3. Create a notebook, install `requests` and `boto3` if not preinstalled
4. Configure your AWS credentials (see Databricks Secrets for production use — do not hardcode keys)
5. Run the ingestion cell manually once, then schedule it as an hourly Databricks Job
6. Run the silver/gold transformation cells to build the Delta tables
7. Run the data quality cell to validate the pipeline
