import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Configuration from pipeline.env
GCS_BUCKET = os.getenv("GCS_BUCKET", "jeremy-msba-pipeline-bucket")


def process_311_data(target_date):
    spark = (
        SparkSession.builder
        .appName(f"311-Incremental-Process-{target_date}")
        .getOrCreate()
    )

    input_path = f"gs://{GCS_BUCKET}/raw/full_311/311_{target_date}.csv"
    output_path = f"gs://{GCS_BUCKET}/processed/311_clean/311_{target_date}.parquet"

    print(f"Starting Spark job for date: {target_date}")
    print(f"Reading from: {input_path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )

        cleaned_df = (
            df.withColumn("created_date", to_timestamp(col("created_date")))
              .filter(col("community_board").isNotNull())
              .select(
                  "unique_key",
                  "created_date",
                  "complaint_type",
                  "incident_zip",
                  "community_board",
                  "borough"
              )
        )

        cleaned_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully processed and saved to: {output_path}")

    except Exception as e:
        print(f"Error during Spark processing: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
        process_311_data(date_arg)
    else:
        print("No target date provided to Spark script.")
        sys.exit(1)