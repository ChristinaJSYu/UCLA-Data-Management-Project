import os
import requests
from google.cloud import storage

# Configuration from pipeline.env
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET")
VARIABLES = "NAME,B19013_001E,B15003_001E,B15003_022E,B15003_023E,B15003_024E,B15003_025E"
YEARS = [2020, 2021, 2022, 2023, 2024]

if not PROJECT_ID:
    raise ValueError("Missing required environment variable: GCP_PROJECT_ID")

if not BUCKET_NAME:
    raise ValueError("Missing required environment variable: GCS_BUCKET")


def fetch_and_upload_census(year):
    """
    Fetch ACS data for a specific year and upload to GCS
    if the file does not already exist.
    """
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"raw/census_income_edu_{year}.json"
    blob = bucket.blob(blob_name)

    if blob.exists():
        print(f"[SKIP] {year} Census data already exists in GCS.")
        return

    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={VARIABLES}&for=zip%20code%20tabulation%20area:*"
    )

    print(f"[FETCH] Requesting Census data for {year}...")

    try:
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            blob.upload_from_string(
                response.content,
                content_type="application/json"
            )
            print(f"[SUCCESS] {year} data uploaded to {blob_name}")

        elif response.status_code == 404:
            print(f"[NOT AVAILABLE] {year} Census data is not available from the API.")

        else:
            raise RuntimeError(
                f"[ERROR] Fetch failed for {year}. Status Code: {response.status_code}"
            )

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"[CONNECTION ERROR] Failed to connect to Census API for {year}: {e}"
        ) from e


if __name__ == "__main__":
    for year in YEARS:
        fetch_and_upload_census(year)