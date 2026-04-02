import os
import requests
import sys
from google.cloud import storage
from datetime import datetime, timedelta

# Configuration from pipeline.env
APP_TOKEN = os.getenv("NYC_APP_TOKEN")
BUCKET_NAME = os.getenv("GCS_BUCKET")
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

if not APP_TOKEN:
    raise ValueError("Missing required environment variable: NYC_APP_TOKEN")

if not BUCKET_NAME:
    raise ValueError("Missing required environment variable: GCS_BUCKET")


def fetch_data(target_date):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    headers = {"X-App-Token": APP_TOKEN}

    where_clause = (
        f"$where=created_date >= '{target_date}T00:00:00' "
        f"AND created_date <= '{target_date}T23:59:59'"
    )
    url = f"{BASE_URL}?{where_clause}&$limit=100000"

    print(f"Fetching data for {target_date} ...")
    response = requests.get(url, headers=headers, timeout=60)

    if response.status_code == 200 and len(response.text.strip().split('\n')) > 1:
        blob_name = f"raw/full_311/311_{target_date}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(response.text.encode("utf-8"), content_type="text/csv")
        print(f"Successfully saved to: {blob_name}")
    else:
        raise RuntimeError(
            f"Fetch failed or no data available for this date. Status Code: {response.status_code}"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_to_fetch = sys.argv[1]
    else:
        date_to_fetch = (datetime.now() - timedelta(2)).strftime("%Y-%m-%d")

    fetch_data(date_to_fetch)