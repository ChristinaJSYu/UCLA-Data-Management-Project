# Team 5 — NYC Housing Signals Pipeline

This project builds an end-to-end data pipeline to analyze whether high-frequency nuisance complaints (**NYC 311**) act as leading indicators of housing price growth (**Zillow**) at the ZIP-code level from **2020–2024**. 

The system integrates Zillow prices, 311 complaints, Census socioeconomic data, and population statistics into a unified **Snowflake** model, powering **Tableau** dashboards for trend exploration.

> **Key Hypothesis:** High-frequency "friction" (311 complaints) can predict real estate trends before slower, traditional Census updates reflect those changes.

**Here's our final dashboard link created on Tableau, showing our analysis: https://public.tableau.com/app/profile/jinwoo.roh/viz/405_Team5/Story1?publish=yes**
---

## 1. Pipeline Architecture

The pipeline follows a modern data stack approach, utilizing cloud storage as a data lake and Spark for heavy lifting.

### Workflow components:
* **Sources:** Zillow, NYC 311 Open Data, US Census Bureau.
* **Storage (GCS):** Acts as the raw and processed data lake.
* **Processing (Spark on Dataproc):** Handles cleaning, dimensional reduction (311), and reshaping Zillow data from wide to long format.
* **Warehouse (Snowflake):** Two-tier architecture (**STAGING** for ingestion → **PRODUCTION** for fact/dimension modeling).
* **Orchestration:** Controlled via `pipeline/pipeline.sh`.

---

## 2. Prerequisites

### 2.1 Accounts & Services
* **Google Cloud Platform:** GCS and Dataproc enabled.
* **Snowflake Account:** Access to create databases and warehouses.
* **NYC Open Data:** [API Token](https://api.census.gov/data/key_signup.html) for 311 data.

### 2.2 Environment Setup
Create a `pipeline.env` file inside the `/pipeline` directory based on the template below:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_account_role
SNOWFLAKE_DATABASE=NYC_HOUSING_DB
SNOWFLAKE_SCHEMA=STAGING
SNOWFLAKE_WAREHOUSE=your_warehouse_name
SNOWFLAKE_STAGE=your_stage_name

# GCP / GCS Configuration
GCS_BUCKET=your_bucket_name
GCP_PROJECT_ID=your_project_id

# Dataproc
DATAPROC_CLUSTER=your_cluster_name
DATAPROC_REGION=your_region

# API Tokens
NYC_APP_TOKEN=your_nyc_open_data_token
```
---

## 3. Installation & Setup

### 3.1 Google Cloud Infrastructure
This section explains how to set up the required Google Cloud resources for the NYC Housing pipeline, including:

•⁠  ⁠enabling required APIs
•⁠  ⁠creating the GCS bucket
•⁠  ⁠creating the Dataproc cluster
•⁠  ⁠authenticating locally
•⁠  ⁠verifying the setup

1.  Prerequisites
Before running the pipeline, make sure you have:
•⁠  ⁠a Google Cloud project
•⁠  ⁠billing enabled on the project
•⁠  ⁠access to Google Cloud Console
•⁠  ⁠⁠ `gcloud` ⁠ installed locally
•⁠  ⁠⁠ `gsutil` ⁠ installed locally
•⁠  ⁠Python installed locally
•⁠  ⁠SnowSQL installed locally if you are loading data into Snowflake

2. Set the active GCP project
Run this first in your terminal:

```bash
gcloud config set project your_project_name
```
3. Enable required APIs

The following APIs must be enabled before using GCS and Dataproc:

- Dataproc API
- Compute Engine API
- Cloud Storage API
- IAM API

Option A: Enable in the UI

Go to the Google Cloud Console API Library and enable:

- Dataproc API
- Compute Engine API
- Cloud Storage API
- Identity and Access Management (IAM) API

Option B: Enable using CLI

```bash
gcloud services enable \
  dataproc.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com
```

4.  **Create a GCS bucket:** `gs://your_bucket_name`.
<img width="370" height="276" alt="image" src="https://github.com/user-attachments/assets/4d74e3fd-09da-4773-8f55-ac5dcaa81564" />

Your folder structure inside GCS should be exactly the same

5.  **Create a Dataproc cluster:** Ensure the region matches your `DATAPROC_REGION` in the `.env` file.

```bash
gcloud dataproc clusters create your_cluster_name \
  --region=us-west1 \
  --zone=us-west1-a \
  --master-machine-type=e2-standard-4 \
  --worker-machine-type=e2-standard-4 \
  --num-workers=2 \
  --image-version=2.1-debian11 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --bucket=your_bucket_name
```


### 3.2 Snowflake Initialization
Run the initialization script via SnowSQL to create the database, schemas, and the external stage linked to your GCS bucket:

```bash
snowsql -a <account> -u <user> -r <role> -w <warehouse> \
-d NYC_HOUSING_DB -s STAGING \
-f sql/NYC_HOUSING_DB.sql
```

Once you have everything setup, make sure to update your `pipeline.env` file with the matching info.

### 3.3 Python Environment Setup

This project uses Python for data ingestion and orchestration scripts. Follow these steps to prepare your local environment.

### 3.4 Recommended: Use a Virtual Environment
Create and activate a virtual environment to isolate project dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
```
### 3.5 Install Dependencies
Ensure your package manager is up to date and install the required libraries:

```Bash
pip install --upgrade pip
pip install -r requirements.txt
```
### 3.6 Verify Installation
Run this one-liner to confirm that the critical libraries are correctly installed:

```Bash
python -c "import requests, pandas, google.cloud.storage"
```

If no errors appear, your Python environment is ready.

### 3.7 Additional System Dependencies
The pipeline relies on the following CLI tools for orchestration:

1. Google Cloud SDK (gcloud)
Used for submitting Spark jobs to Dataproc.

Install (macOS): ```brew install --cask google-cloud-sdk```

Initialize:

```Bash
gcloud init
gcloud auth application-default login
```

2. SnowSQL (Snowflake CLI)
Used to execute SQL scripts directly from the pipeline.

Install (macOS): ```brew install --cask snowflake-snowsql```

Verify: ```snowsql -v```

3. Java (Required for PySpark)
Spark requires a Java Runtime Environment:

Install (macOS): ```brew install openjdk```

### 3.8 Environment Notes
Spark Execution: Dataproc clusters already include Spark; local PySpark is primarily used for unit testing.
Execution: As long as Dataproc is configured, the pipeline will function even without a full local Spark installation.
Authentication: Ensure your GCP authentication session is active before triggering pipeline.sh.

---

## 4. How to Run the Pipeline

From the pipeline/ directory, run:

```bash
bash pipeline.sh
```

Pipeline Phases
- Phase 0 (Census & Population): Pulls 2020–2024 data from Census API and transforms JSON to Parquet via Spark.
- Phase 1 (Zillow): Downloads dataset and converts wide time series to long format.
- Phase 2 (311): Daily incremental pull of complaint data with filtering and column reduction.
- Phase 3 (Snowflake Loading): Refreshes external stage and loads 311, Zillow, Census, and Population data.
- Phase 4 (Modeling): Runs sql/Final_Table.sql to create Dimensions and Fact tables.
- Phase 5 (Validation): Runs sql/Quick_Checker.sql to check row counts and data sanity.
- Phase 6 (Analytics): Runs sql/Analytics.sql for summary queries and trend exploration.

If you see an error message related to cluster processing overload, please stop the cluster and restart it.

Note: The `pipeline.sh` is modded to catch a single day of 311 data, as pulling the full 5-year data from 311 will take a long time (probably days). In the same folder there is a `pipeline_full_five_years.sh` that covers the full scope of our project. 

--- 

## 5. Final Output
 Main Table: `NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY`

Grain: `zip_code` x `year`

Included Data:
- Housing prices and Year-over-Year changes
- Complaint counts and rates per population
- Income and Education metrics
- Adjusted signals for modeling

Structure(Dimensions/Facts):
<img width="1536" height="1024" alt="ChatGPT Image Mar 20, 2026, 11_39_06 PM" src="https://github.com/user-attachments/assets/d553586d-7745-4e89-bc2a-fc4be0dc5b00" />

---

## 6. Tableau

After connecting to Tableau <-> Snowflake using Tableau Desktop, we have created interactive dashboards.

Here's the link to the publicly published Tableau Dashboard.

https://public.tableau.com/app/profile/jinwoo.roh/viz/405_Team5/Story1?publish=yes
