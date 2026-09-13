# Server-Side E-Commerce Data Pipeline & API Activation

An end-to-end Data Engineering and Analytics architecture simulating a Shopify to Server-Side GTM (sGTM) data stream, loaded into Google Cloud BigQuery, processed via Python ETL, and routed to external APIs for conversion optimization.

---

## 🏗️ Architecture Overview

1. **Data Ingestion (Raw Layer)**: Simulates web traffic and nested JSON event data payload (`STRUCT/RECORD` schemas) originating from a Server-Side GTM container directly into BigQuery (`raw_sgtm_events`).
2. **Data Transformation & Aggregations (Data Mart Layer)**: Python ETL script extracts raw events, flattens nested schema items using BigQuery SQL, calculates daily funnel metrics (CR%), and outputs an optimized Data Mart table (`daily_funnel_metrics`).
3. **Data Quality Assertions**: Automated checking logic to flag performance anomalies (e.g., sudden drop in conversion rate per country or device category).
4. **Server-Side API Activation**: Queries qualified high-value transactions ($\ge 40$ EUR) and pushes structured payloads via HTTP POST to external conversion endpoints (Meta CAPI / GA4 Measurement Protocol simulation).

---

## 🛠️ Tech Stack

* **Cloud Infrastructure**: Google Cloud Platform (BigQuery)
* **Languages & Libraries**: Python 3, Pandas, Google Cloud BigQuery SDK, Requests
* **Visualization Ready**: Google Looker Studio
* **Authentication**: Application Default Credentials (GCP ADC via `gcloud SDK`)

---

## 🚀 Pipeline Flow & Stages

```text
Shopify / sGTM Stream ──► BigQuery (Raw Events) ──► Python ETL (Pandas/SQL)
                                                           │
              ┌────────────────────────────────────────────┴────────────────────────────────────────────┐
              ▼                                                                                         ▼
 BigQuery Data Mart (daily_funnel_metrics)                                                Server-Side API Activation (HTTP POST)
              │                                                                                         │
              ▼                                                                                         ▼
 Looker Studio Dashboards                                                                  Meta CAPI / GA4 Measurement Protocol