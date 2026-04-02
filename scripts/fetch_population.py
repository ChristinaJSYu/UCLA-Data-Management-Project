import os
import requests
from google.cloud import storage
import json

# Configuration from pipeline.env
BUCKET_NAME = os.getenv("GCS_BUCKET")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
VARIABLES = "NAME,DP05_0001E"
YEARS = [2020, 2021, 2022, 2023, 2024]

if not PROJECT_ID:
    raise ValueError("Missing required environment variable: GCP_PROJECT_ID")

if not BUCKET_NAME:
    raise ValueError("Missing required environment variable: GCS_BUCKET")

# Specified NYC Zip Code list
NYC_ZIP_CODES = {
    "10463", "10471", "10466", "10469", "10470", "10475", "10458", "10467", "10468",
    "10461", "10462", "10464", "10465", "10472", "10473", "10453", "10457", "10460",
    "10451", "10452", "10456", "10454", "10455", "10459", "10474", "11211", "11222",
    "11201", "11205", "11215", "11217", "11231", "11213", "11212", "11216", "11233",
    "11238", "11207", "11208", "11220", "11232", "11204", "11218", "11219", "11230",
    "11203", "11210", "11225", "11226", "11234", "11236", "11239", "11209", "11214",
    "11228", "11223", "11224", "11229", "11235", "11206", "11221", "11237", "10031",
    "10032", "10033", "10034", "10040", "10026", "10027", "10030", "10037", "10039",
    "10029", "10035", "10023", "10024", "10025", "10021", "10028", "10044", "10128",
    "10001", "10011", "10018", "10019", "10020", "10036", "10010", "10016", "10017",
    "10022", "10012", "10013", "10014", "10002", "10003", "10009", "10004", "10005",
    "10006", "10007", "10038", "10280", "11101", "11102", "11103", "11104", "11105",
    "11106", "11368", "11369", "11370", "11372", "11373", "11377", "11378", "11354",
    "11355", "11356", "11357", "11358", "11359", "11360", "11361", "11362", "11363",
    "11364", "11374", "11375", "11379", "11385", "11365", "11366", "11367", "11414",
    "11415", "11416", "11417", "11418", "11419", "11420", "11421", "11412", "11423",
    "11432", "11433", "11434", "11435", "11436", "11004", "11005", "11411", "11413",
    "11422", "11426", "11427", "11428", "11429", "11691", "11692", "11693", "11694",
    "11695", "11697", "10302", "10303", "10310", "10301", "10304", "10305", "10314",
    "10306", "10307", "10308", "10309", "10312"
}


def fetch_and_upload_population(year):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5/profile"
        f"?get={VARIABLES}&for=zip%20code%20tabulation%20area:*"
    )
    print(f"Fetching {year} data from Census API (Data Profile table)...")

    try:
        response = requests.get(url, timeout=60)

        if response.status_code == 200:
            raw_data = response.json()
            header = raw_data[0]
            rows = raw_data[1:]

            zip_idx = header.index("zip code tabulation area")
            filtered_rows = [row for row in rows if row[zip_idx] in NYC_ZIP_CODES]

            output = {
                "year": year,
                "header": header,
                "data": filtered_rows
            }

            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"raw/nyc_total_pop_dp05_{year}.json")
            blob.upload_from_string(
                json.dumps(output),
                content_type="application/json"
            )

            print(f"✅ {year} completed! (Filtered {len(filtered_rows)} Zip Codes)")
        else:
            raise RuntimeError(
                f"❌ {year} failed. Status: {response.status_code}, Reason: {response.text}"
            )

    except Exception as e:
        raise RuntimeError(f"An error occurred while processing year {year}: {e}") from e


if __name__ == "__main__":
    for y in YEARS:
        fetch_and_upload_population(y)