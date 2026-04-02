#!/bin/bash

set -euo pipefail

# --- Paths ---
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$BASE_DIR/.." && pwd)"
SCRIPTS_DIR="$PROJECT_ROOT/scripts"
SQL_DIR="$PROJECT_ROOT/sql"
ENV_FILE="$BASE_DIR/pipeline.env"

# --- Load environment variables ---
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: pipeline.env not found at $ENV_FILE"
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

# --- Validate required environment variables ---
required_vars=(
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ROLE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_STAGE
    GCS_BUCKET
    GCP_PROJECT_ID
    DATAPROC_CLUSTER
    DATAPROC_REGION
    NYC_APP_TOKEN
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: Required environment variable $var is not set in pipeline.env"
        exit 1
    fi
done

# --- Runtime configuration ---
YEARS_311=(2020 2021 2022 2023 2024)

echo "=================================================="
echo "Starting NYC Housing Automated Data Pipeline"
echo "Execution Time: $(date)"
echo "311 Load Years: ${YEARS_311[*]}"
echo "=================================================="

# Spark env propagation for Dataproc jobs
SPARK_ENV_PROPS="spark.driverEnv.GCS_BUCKET=$GCS_BUCKET,spark.executorEnv.GCS_BUCKET=$GCS_BUCKET,spark.yarn.appMasterEnv.GCS_BUCKET=$GCS_BUCKET,spark.driverEnv.GCP_PROJECT_ID=$GCP_PROJECT_ID,spark.executorEnv.GCP_PROJECT_ID=$GCP_PROJECT_ID,spark.yarn.appMasterEnv.GCP_PROJECT_ID=$GCP_PROJECT_ID,spark.driverEnv.NYC_APP_TOKEN=$NYC_APP_TOKEN,spark.executorEnv.NYC_APP_TOKEN=$NYC_APP_TOKEN,spark.yarn.appMasterEnv.NYC_APP_TOKEN=$NYC_APP_TOKEN,spark.driver.memory=2g,spark.executor.memory=2g,spark.executor.cores=2,spark.executor.instances=2,spark.dynamicAllocation.enabled=false"

# --- Phase 1: Fetch Data: Census & Population & Zillow---
echo "[Phase 1] Fetching Census raw data..."
python3 "$SCRIPTS_DIR/fetch_census.py"
echo "[Phase 1-1] Fetching Population raw data..."
python3 "$SCRIPTS_DIR/fetch_population.py"
echo "[Phase 1-2] Fetching Zillow raw data..."
python3 "$SCRIPTS_DIR/fetch_zillow.py"

# --- Phase 2: Process Data: Census & Population & Zillow---
echo ">>> Submitting Spark Job: Census"
gcloud dataproc jobs submit pyspark "$SCRIPTS_DIR/spark_census.py" \
    --project="$GCP_PROJECT_ID" \
    --cluster="$DATAPROC_CLUSTER" \
    --region="$DATAPROC_REGION" \
    --properties="$SPARK_ENV_PROPS"

echo ">>> Submitting Spark Job: Population"
gcloud dataproc jobs submit pyspark "$SCRIPTS_DIR/spark_population.py" \
    --project="$GCP_PROJECT_ID" \
    --cluster="$DATAPROC_CLUSTER" \
    --region="$DATAPROC_REGION" \
    --properties="$SPARK_ENV_PROPS"

echo ">>> Submitting Spark Job: Zillow format transformation..."
gcloud dataproc jobs submit pyspark "$SCRIPTS_DIR/spark_zillow_transform.py" \
    --project="$GCP_PROJECT_ID" \
    --cluster="$DATAPROC_CLUSTER" \
    --region="$DATAPROC_REGION" \
    --properties="$SPARK_ENV_PROPS"

# --- Phase 0: 311 Data (Yearly full load for 2020-2024) ---
echo "[Phase 0] Fetching and processing 311 yearly data..."

for YEAR_311 in "${YEARS_311[@]}"; do
    echo "--------------------------------------------------"
    echo "Processing 311 year: $YEAR_311"
    echo "--------------------------------------------------"

    python3 "$SCRIPTS_DIR/fetch_311_f.py" "$YEAR_311"

    echo ">>> Submitting Spark Job: 311 yearly processing for $YEAR_311 ..."
    gcloud dataproc jobs submit pyspark "$SCRIPTS_DIR/spark_311_incremental_f.py" \
        --project="$GCP_PROJECT_ID" \
        --cluster="$DATAPROC_CLUSTER" \
        --region="$DATAPROC_REGION" \
        --properties="$SPARK_ENV_PROPS" \
        -- "$YEAR_311"
done

# --- Phase 3: Snowflake Data Loading ---
echo "[Phase 3] Loading data to Snowflake..."

# SnowSQL automatically detects this password variable
export SNOWSQL_PWD="$SNOWFLAKE_PASSWORD"

SNOW_OPTS=(
    -a "$SNOWFLAKE_ACCOUNT"
    -u "$SNOWFLAKE_USER"
    -r "$SNOWFLAKE_ROLE"
    -w "$SNOWFLAKE_WAREHOUSE"
    -d "$SNOWFLAKE_DATABASE"
    -s "$SNOWFLAKE_SCHEMA"
)

snowsql "${SNOW_OPTS[@]}" -q "
  -- 1. Refresh Stage to recognize new files
  ALTER STAGE ${SNOWFLAKE_STAGE} REFRESH;

  -- 2. Clean Staging Tables
  TRUNCATE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_311_data;
  TRUNCATE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_zillow_data;
  TRUNCATE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_census_data;
  TRUNCATE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_population_data;

  -- 3. Load all 311 yearly parquet files
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_311_data
  FROM @${SNOWFLAKE_STAGE}/311_clean/
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = CONTINUE;

  -- 4. Load Zillow data
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_zillow_data
  FROM @${SNOWFLAKE_STAGE}/zillow_clean/
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

  -- 5. Load Census data
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_census_data
  FROM @${SNOWFLAKE_STAGE}/census_clean/
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

  -- 6. Load Population data
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_population_data
  FROM @${SNOWFLAKE_STAGE}/population_clean/
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

  -- 7. Quick 311 load check
  SELECT
      YEAR(created_date) AS LOAD_YEAR,
      COUNT(*) AS YEARLY_COUNT
  FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.stg_311_data
  GROUP BY 1
  ORDER BY 1;
"

# --- Phase 4: Build Production Tables ---
echo "[Phase 4] Building production tables in Snowflake..."
snowsql "${SNOW_OPTS[@]}" -f "$SQL_DIR/Final_Table.sql"

# --- Phase 5: Quick Validation Checks ---
echo "[Phase 5] Running Snowflake quick checks..."
snowsql "${SNOW_OPTS[@]}" -f "$SQL_DIR/Quick_Checker.sql"

# --- Phase 6: Optional Analytics Queries ---
echo "[Phase 6] Running Snowflake analytics checks..."
snowsql "${SNOW_OPTS[@]}" -f "$SQL_DIR/Analytics.sql"

echo "=================================================="
echo "Pipeline completed successfully."
echo "=================================================="