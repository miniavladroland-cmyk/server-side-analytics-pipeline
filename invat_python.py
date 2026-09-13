import json
import pandas as pd
import requests
from google.cloud import bigquery

PROJECT_ID = "project-6d0dd4a4-de2b-428e-a3c"
DATASET_ID = "magazin_demo"
SOURCE_TABLE = "raw_sgtm_events"

client = bigquery.Client(project=PROJECT_ID)

# 1. Extract qualified purchases with a fallback unique transaction ID
query = f"""
    SELECT 
        event_timestamp,
        client_id,
        GENERATE_UUID() AS transaction_id,
        ecommerce.value AS total_value,
        ecommerce.currency AS currency,
        user_data.country AS country
    FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}`
    WHERE event_name = 'purchase'
      AND ecommerce.value >= 40.0
"""

df_purchases = client.query(query).to_dataframe()

print(f"--- SERVER-SIDE ACTIVATION: FOUND {len(df_purchases)} QUALIFIED TRANSACTIONS ---")

# 2. External Endpoint Simulation (httpbin.org)
TARGET_API_URL = "https://httpbin.org/post"

# 3. Dispatch HTTP payloads
successful_dispatches = 0

for index, row in df_purchases.iterrows():
    payload = {
        "client_id": row["client_id"],
        "events": [
            {
                "name": "purchase",
                "params": {
                    "transaction_id": row["transaction_id"],
                    "value": float(row["total_value"]),
                    "currency": row["currency"],
                    "country": row["country"],
                    "event_source": "server_side_pipeline"
                }
            }
        ]
    }
    
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(TARGET_API_URL, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        successful_dispatches += 1
        print(f"[API SUCCESS] Transaction {row['transaction_id'][:8]}... ({row['total_value']} {row['currency']}) sent successfully.")
    else:
        print(f"[API ERROR] Status code: {response.status_code}")

print(f"\n--- DISPATCH COMPLETED: {successful_dispatches}/{len(df_purchases)} EVENTS SYNCED ---")