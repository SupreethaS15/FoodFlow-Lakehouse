# Food Delivery Analytics Lakehouse Platform

An end-to-end data engineering project that ingests, cleans, models, and visualizes food delivery operational data using a medallion (Bronze-Silver-Gold) lakehouse architecture on **Databricks**, with a fully automated, self-triggering ETL pipeline and an interactive AI/BI dashboard.

Built as part of a Data Engineering coursework project — designed and implemented to reflect real production data engineering practices (idempotency, atomicity, incremental loading, automated orchestration) rather than a one-off analysis script.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Pipeline Details](#pipeline-details)
- [Star Schema](#star-schema)
- [Dashboard](#dashboard)
- [Automation](#automation)
- [Results & Insights](#results--insights)
- [Author](#author)

---

## Overview

Online food delivery platforms generate continuous operational data — restaurant listings, delivery transactions, and customer feedback — from independent systems that rarely share common keys. This project builds a governed analytics platform that:

1. Extracts data from 3 real, independently-sourced datasets
2. Cleans and validates it through a documented EDA process (null checks, outlier detection, normalization)
3. Models it into a dimensional Star Schema for fast analytical querying
4. Automatically detects and processes new data with zero manual intervention
5. Surfaces the results through a live, auto-refreshing dashboard

---

## Architecture

```
 Zomato Restaurants ─┐
 Food Delivery Time  ─┼──▶  BRONZE  ──▶  SILVER  ──▶  GOLD  ──▶  AI/BI Dashboard
 Restaurant Reviews  ─┘     (raw)      (cleaned,     (Star        (auto-refresh)
                                        validated,    Schema)
                                        normalized)
```

- **Bronze** — raw ingestion from all sources, tagged with ingestion timestamp and source file path for full lineage
- **Silver** — deduplication, null handling, IQR-based outlier flagging, min-max normalization of numeric features
- **Gold** — dimensional Star Schema (fact + dimension tables) optimized for BI queries

All tables are stored as **Delta Lake** tables, giving ACID transactions, versioning, and time-travel out of the box.

---

## Tech Stack

| Category | Tools |
|---|---|
| Compute & Storage | Databricks, PySpark, Delta Lake |
| Orchestration | Databricks Workflows (Jobs), File Arrival Triggers |
| Data Modeling | Star Schema (dimensional modeling) |
| Dashboarding | Databricks AI/BI Dashboards (Genie natural-language authoring) |
| Languages | Python, PySpark, SQL |
| Data Sources | Kaggle (public CSV datasets) |

---

## Data Sources

Three real, publicly available datasets — no fully synthetic data was used for any business-relevant field.

| Dataset | Rows | Role |
|---|---|---|
| [Zomato Restaurants Data](https://www.kaggle.com/datasets/shrutimehta/zomato-restaurants-data) | 1,000 (sampled) | Restaurant dimension |
| [Food Delivery Time Prediction](https://www.kaggle.com/datasets/denkuznetz/food-delivery-time-prediction) | 1,001 | Orders/deliveries fact data |
| [Restaurant Reviews](https://www.kaggle.com/datasets/d4rklucif3r/restaurant-reviews) | 1,001 | Text review / sentiment data |

**On data linkage:** these datasets were collected independently and don't share keys (no common restaurant ID, order ID, or customer ID across them). Rather than fabricating business data, a documented linking strategy was applied:
- Each delivery record is linked to a randomly selected real restaurant
- Order dates are synthesized by distributing records across a rolling 21-day window, using the dataset's real `Time_of_Day` field to pick a plausible hour
- Reviews are linked 1:1 to orders via random shuffle

No underlying attribute values were invented — only the connections between datasets, which no public source legitimately exposes. Every adaptation is logged automatically in `data_source_manifest.json`, generated at the end of the data preparation step, for full transparency.

---

## Pipeline Details

### Bronze — Raw Ingestion
```python
bronze_orders = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{BASE}/landing/orders/*.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))
```
Data is ingested exactly as received, with lineage metadata attached — no transformation happens at this stage.

### Silver — Cleaning, EDA & Normalization
- **Null-rate report**: percentage of missing values per column
- **IQR outlier detection**: flags (not deletes) rows outside `Q1 - 1.5×IQR` to `Q3 + 1.5×IQR` for distance and delivery time
- **Deduplication**: on `order_id`
- **Standardization**: missing categorical values filled with `"Unknown"` rather than left null
- **Min-max normalization**: applied to distance, delivery time, prep time, and courier experience so features on different scales become comparable
- **Idempotent upsert**: Delta Lake `MERGE` keyed on `order_id` — reruns never create duplicate rows

### Gold — Star Schema
Cleaned data is joined and reshaped into a dimensional model optimized for dashboard queries (see below).

---

## Star Schema

```
                 ┌────────────────────┐
                 │   DIM_RESTAURANT   │
                 │  restaurant_id     │
                 │  name, city        │
                 │  cuisine, rating   │
                 └─────────▲──────────┘
                           │
┌──────────────────────────────────────────────┐
│               FACT_ORDERS                     │
│  order_id, restaurant_id, date_key            │
│  distance_km, delivery_time_min               │
│  weather, traffic_level, vehicle_type          │
│  courier_experience_yrs, liked                │
└──────────────────────────────────────────────┘
                           │
                 ┌─────────▼──────────┐
                 │     DIM_DATE       │
                 │  date_key, date    │
                 │  day_name, month   │
                 └────────────────────┘
```

A single fact table (`gold_fact_orders`) captures measurable order-level events, surrounded by descriptive dimension tables (`gold_dim_restaurant`, `gold_dim_date`). This minimizes joins needed at query time — all dashboard queries run against pre-joined, pre-aggregated Gold tables.

---

## Dashboard

Built as a **Databricks AI/BI Dashboard**, authored using natural-language prompts (Genie) rather than manual chart configuration.

**Layout:**
- KPI counters — total orders, average delivery time, average distance, % liked
- Orders-per-day trend line
- Comparison charts — orders by cuisine, delivery time by weather/traffic/vehicle type
- Detail tables — top-rated restaurants, most-ordered restaurants, outlier orders, sample reviews, recent orders

> View the published Dashboard via 'https://dbc-f1860229-946f.cloud.databricks.com/dashboardsv3/01f1963467801641b93c5b6fc906a25a/published?o=7474648018775897'

---

## Automation

```
New CSV (via UI upload or generator) 
        │
        ▼
File Arrival Trigger
        │
        ▼
Databricks Job:  Bronze (incremental append) → Silver (MERGE upsert) → Gold (refresh) → Dashboard refresh
```

A multi-task Databricks Job is configured with a **File Arrival trigger** on the landing zone. When a new order file appears — whether from the project's own data-generation utility or a manual upload through the Databricks UI — the entire pipeline runs automatically and the dashboard reflects the update without any manual notebook execution.

- **Idempotency**: Delta MERGE upserts guarantee safe reruns
- **Atomicity**: provided natively by Delta Lake's transaction log (`_delta_log`)
- **Incremental loading**: a `processed_files` tracking table ensures only new files are ingested at each run

---


## Results & Insights

- Delivery time varies measurably with weather and traffic conditions, visible directly in the dashboard's comparison panels
- Order volume is concentrated in a subset of cuisines and restaurants rather than evenly distributed
- Vehicle type shows measurable differences in average delivery time, useful for fleet allocation decisions
- Data-quality outlier rate is tracked continuously as a trust indicator for all other reported metrics


---
