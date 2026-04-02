import os
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Configuration from pipeline.env
GCS_BUCKET = os.getenv("GCS_BUCKET")

GCS_BUCKET = os.getenv("GCS_BUCKET", "jeremy-msba-pipeline-bucket")

YEARS = [2020, 2021, 2022, 2023, 2024]
RAW_PATH_TEMPLATE = f"gs://{GCS_BUCKET}/raw/nyc_total_pop_dp05_{{year}}.json"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/processed/population_clean"


def process_year(spark, year):
    raw_path = RAW_PATH_TEMPLATE.format(year=year)
    print(f"Processing population data for year: {year}")
    print(f"Reading from: {raw_path}")

    try:
        raw_files = spark.sparkContext.wholeTextFiles(raw_path).collect()

        if not raw_files:
            print(f"No file found for year {year}, skipping...")
            return None

        _, raw_text = raw_files[0]
        payload = json.loads(raw_text)

        header = payload["header"]
        data_rows = payload["data"]
        data_year = payload["year"]

        df = spark.createDataFrame(data_rows, schema=header)

        df = (
            df.withColumnRenamed("DP05_0001E", "total_population")
              .withColumnRenamed("zip code tabulation area", "zip_code")
              .withColumn("total_population", col("total_population").cast("int"))
              .withColumn("data_year", lit(int(data_year)))
        )

        df = df.select("zip_code", "NAME", "total_population", "data_year")

        return df

    except Exception as e:
        print(f"Error processing year {year}: {str(e)}")
        return None


def main():
    spark = (
        SparkSession.builder
        .appName("Population_Full_Load_2020_2024")
        .getOrCreate()
    )

    try:
        dfs = []

        for year in YEARS:
            df = process_year(spark, year)
            if df is not None:
                dfs.append(df)

        if not dfs:
            raise ValueError("No population data processed. Check raw files.")

        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.unionByName(df)

        print(f"Writing final dataset to: {OUTPUT_PATH}")

        (
            df_final.repartition(1)
                    .write
                    .mode("overwrite")
                    .parquet(OUTPUT_PATH)
        )

        print("Population dataset successfully created.")

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()