import json
import random
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import bigquery

# Pipeline Configuration
PROJECT_ID = "project-6d0dd4a4-de2b-428e-a3c"
DATASET_ID = "magazin_demo"
RAW_TABLE = "raw_sgtm_events"
MART_TABLE = "daily_funnel_metrics"
TARGET_API_URL = "https://httpbin.org/post"

client = bigquery.Client(project=PROJECT_ID)

def generate_synthetic_events(session_count=50):
    """Simulates realistic e-commerce traffic originating from Server-Side GTM."""
    products = [
        {"item_id": "PROD_101", "price": 29.99},
        {"item_id": "PROD_202", "price": 119.50},
        {"item_id": "PROD_303", "price": 45.00},
        {"item_id": "PROD_404", "price": 89.00},
    ]
    countries, devices = ["BE", "NL", "DE", "FR"], ["desktop", "mobile", "tablet"]
    events = []
    base_time = datetime.now() - timedelta(hours=2)

    for _ in range(session_count):
        client_id = f"GA1.1.{random.randint(100000000, 999999999)}"
        country, device = random.choice(countries), random.choice(devices)
        session_time = base_time + timedelta(minutes=random.randint(0, 120))
        
        # page_view event
        events.append({
            "event_timestamp": session_time.isoformat(),
            "event_name": "page_view",
            "client_id": client_id,
            "user_data": {"country": country, "device": device},
            "ecommerce": {"value": 0.0, "currency": "EUR"}
        })
        
        # add_to_cart (40% conversion rate)
        if random.random() < 0.4:
            product = random.choice(products)
            events.append({
                "event_timestamp": session_time.isoformat(),
                "event_name": "add_to_cart",
                "client_id": client_id,
                "user_data": {"country": country, "device": device},
                "ecommerce": {"value": product["price"], "currency": "EUR"}
            })
            # purchase (50% checkout conversion rate)
            if random.random() < 0.5:
                events.append({
                    "event_timestamp": session_time.isoformat(),
                    "event_name": "purchase",
                    "client_id": client_id,
                    "user_data": {"country": country, "device": device},
                    "ecommerce": {"value": product["price"], "currency": "EUR"}
                })

    df_stream = pd.DataFrame(events)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df_stream, table_ref, job_config=job_config).result()
    print(f"[STAGE 1] Ingested {len(df_stream)} raw sGTM events into {table_ref}.")

def build_data_mart():
    """Extracts raw data, flattens nested schemas, and updates the Data Mart layer."""
    query = f"""
        SELECT 
            DATE(event_timestamp) AS event_date,
            user_data.country AS country,
            user_data.device AS device_category,
            COUNTIF(event_name = 'page_view') AS page_views,
            COUNTIF(event_name = 'add_to_cart') AS cart_additions,
            COUNTIF(event_name = 'purchase') AS total_orders,
            SUM(IF(event_name = 'purchase', ecommerce.value, 0)) AS total_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
        GROUP BY event_date, country, device_category
    """
    df_metrics = client.query(query).to_dataframe()
    df_metrics["conversion_rate"] = (df_metrics["total_orders"] / df_metrics["page_views"] * 100).fillna(0).round(2)
    mart_ref = f"{PROJECT_ID}.{DATASET_ID}.{MART_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_metrics, mart_ref, job_config=job_config).result()
    print(f"[STAGE 2] Updated Data Mart layer in BigQuery: {mart_ref}.")
    return df_metrics

def run_data_quality_checks(df_metrics, min_conversion_rate=0.5):
    """Performs data assertions and logs performance alerts."""
    anomalies = df_metrics[df_metrics["conversion_rate"] < min_conversion_rate]
    if not anomalies.empty:
        print(f"[STAGE 3 ALERT] Low conversion rate detected in {len(anomalies)} traffic segment(s).")
    else:
        print("[STAGE 3 OK] Data Quality checks passed cleanly.")

def dispatch_server_side_events(min_order_value=40.0):
    """Dispatches qualified high-value orders to external APIs (Meta CAPI / GA4 MP)."""
    query = f"""
        SELECT 
            client_id,
            GENERATE_UUID() AS transaction_id,
            ecommerce.value AS total_value,
            ecommerce.currency AS currency,
            user_data.country AS country
        FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
        WHERE event_name = 'purchase'
          AND ecommerce.value >= {min_order_value}
    """
    df_purchases = client.query(query).to_dataframe()
    synced_count = 0

    for _, row in df_purchases.iterrows():
        payload = {
            "client_id": row["client_id"],
            "events": [{
                "name": "purchase",
                "params": {
                    "transaction_id": row["transaction_id"],
                    "value": float(row["total_value"]),
                    "currency": row["currency"],
                    "country": row["country"]
                }
            }]
        }
        res = requests.post(TARGET_API_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        if res.status_code == 200:
            synced_count += 1

    print(f"[STAGE 4] Synced {synced_count}/{len(df_purchases)} qualified orders to Server API.")

if __name__ == "__main__":
    print("--- STARTING END-TO-END DATA PIPELINE ---")
    generate_synthetic_events(session_count=30)
    metrics_df = build_data_mart()
    run_data_quality_checks(metrics_df)
    dispatch_server_side_events()
    print("--- PIPELINE EXECUTION COMPLETE ---")