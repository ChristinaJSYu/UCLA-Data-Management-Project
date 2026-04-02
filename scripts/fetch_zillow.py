import os
from google.cloud import storage
import requests

# Configuration from pipeline.env
BUCKET_NAME = os.getenv("GCS_BUCKET", "jeremy-msba-pipeline-bucket")
ZILLOW_URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"


def upload_to_gcs(bucket_name, source_url, destination_blob_name):
    response = requests.get(source_url, stream=True, timeout=120)
    response.raise_for_status()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(response.content, content_type="text/csv")
    print(f"File uploaded to: {destination_blob_name}")


if __name__ == "__main__":
    upload_to_gcs(BUCKET_NAME, ZILLOW_URL, "raw/zillow_housing.csv")