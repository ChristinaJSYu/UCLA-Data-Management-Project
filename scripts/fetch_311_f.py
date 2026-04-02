import os
import requests
import sys
from google.cloud import storage
from io import StringIO

# Configuration from pipeline.env
APP_TOKEN = os.getenv("NYC_APP_TOKEN")
BUCKET_NAME = os.getenv("GCS_BUCKET")
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

if not APP_TOKEN:
    raise ValueError("Missing required environment variable: NYC_APP_TOKEN")

if not BUCKET_NAME:
    raise ValueError("Missing required environment variable: GCS_BUCKET")


def fetch_yearly_data(year: int) -> str:
    """
    Fetch full-year 311 data from Socrata in paginated batches and return one CSV string.
    """
    headers = {"X-App-Token": APP_TOKEN}
    limit = 50000
    offset = 0

    start_dt = f"{year}-01-01T00:00:00"
    end_dt = f"{year}-12-31T23:59:59"

    csv_parts = []
    header_written = False
    total_rows = 0

    print(f"Fetching 311 data for full year {year} ...")

    while True:
        where_clause = (
            f"$where=created_date >= '{start_dt}' "
            f"AND created_date <= '{end_dt}'"
        )
        url = (
            f"{BASE_URL}?{where_clause}"
            f"&$limit={limit}"
            f"&$offset={offset}"
        )

        print(f"  -> Requesting batch with offset={offset}")
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()

        text = response.text.strip()
        lines = text.split("\n")

        # No data at all
        if len(lines) <= 1:
            break

        batch_header = lines[0]
        batch_rows = lines[1:]

        if not header_written:
            csv_parts.append(batch_header)
            header_written = True

        csv_parts.extend(batch_rows)

        batch_count = len(batch_rows)
        total_rows += batch_count
        print(f"  -> Retrieved {batch_count} rows (running total: {total_rows})")

        if batch_count < limit:
            break

        offset += limit

    if total_rows == 0:
        raise RuntimeError(f"No 311 data returned for year {year}")

    final_csv = "\n".join(csv_parts) + "\n"
    print(f"Finished fetching year {year}. Total rows: {total_rows}")

    return final_csv


def upload_yearly_data(year: int) -> None:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    csv_content = fetch_yearly_data(year)

    blob_name = f"raw/full_311/311_{year}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_content.encode("utf-8"), content_type="text/csv")

    print(f"Successfully saved yearly 311 data to: {blob_name}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        year_to_fetch = int(sys.argv[1])
    else:
        raise ValueError("Please provide a year, e.g. 2020")

    upload_yearly_data(year_to_fetch)