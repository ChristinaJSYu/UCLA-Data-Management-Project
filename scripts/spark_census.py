import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# Configuration from pipeline.env
GCS_BUCKET = os.getenv("GCS_BUCKET", "jeremy-msba-pipeline-bucket")


#if not GCS_BUCKET:
#    raise ValueError("Missing required environment variable: GCS_BUCKET")

RAW_BUCKET_TEMPLATE = f"gs://{GCS_BUCKET}/raw/census_income_edu_{{year}}.json"
FINAL_OUTPUT_PATH = f"gs://{GCS_BUCKET}/processed/census_clean"
YEARS = [2020, 2021, 2022, 2023, 2024]


def process_year(spark, year):
    raw_path = RAW_BUCKET_TEMPLATE.format(year=year)
    print(f"Processing census data for year: {year}")
    print(f"Reading from: {raw_path}")

    try:
        # Read full file as text from GCS
        text_rdd = spark.sparkContext.wholeTextFiles(raw_path)
        file_contents = text_rdd.collect()

        if not file_contents:
            print(f"[SKIP] No census file found for {year}")
            return None

        import json
        _, raw_text = file_contents[0]
        raw_data = json.loads(raw_text)

        if not raw_data or len(raw_data) < 2:
            print(f"[SKIP] No usable census data found for {year}")
            return None

        headers = raw_data[0]
        data_rows = raw_data[1:]

        df = spark.createDataFrame(data_rows, schema=headers)
        df = df.withColumn("data_year", lit(year))

        df = (
            df.withColumnRenamed("B19013_001E", "median_income")
              .withColumnRenamed("B15003_001E", "total_pop_over_25")
              .withColumnRenamed("B15003_022E", "bachelors_degree")
              .withColumnRenamed("B15003_023E", "masters_degree")
              .withColumnRenamed("B15003_024E", "professional_degree")
              .withColumnRenamed("B15003_025E", "doctorate_degree")
              .withColumnRenamed("zip code tabulation area", "zip_code")
        )

        numeric_cols = [
            "median_income",
            "total_pop_over_25",
            "bachelors_degree",
            "masters_degree",
            "professional_degree",
            "doctorate_degree"
        ]

        for c in numeric_cols:
            df = df.withColumn(c, col(c).cast("int"))

        return df

    except Exception as e:
        print(f"[ERROR] Failed processing year {year}: {str(e)}")
        return None
    raw_path = RAW_BUCKET_TEMPLATE.format(year=year)
    print(f"Processing census data for year: {year}")
    print(f"Reading from: {raw_path}")

    try:
        raw_data = spark.read.option("multiLine", "true").json(raw_path).collect()

        if not raw_data or len(raw_data) < 2:
            print(f"[SKIP] No usable census data found for {year}")
            return None

        headers = raw_data[0]
        data_rows = raw_data[1:]

        df = spark.createDataFrame(data_rows, schema=headers)
        df = df.withColumn("data_year", lit(year))

        df = (
            df.withColumnRenamed("B19013_001E", "median_income")
              .withColumnRenamed("B15003_001E", "total_pop_over_25")
              .withColumnRenamed("B15003_022E", "bachelors_degree")
              .withColumnRenamed("B15003_023E", "masters_degree")
              .withColumnRenamed("B15003_024E", "professional_degree")
              .withColumnRenamed("B15003_025E", "doctorate_degree")
              .withColumnRenamed("zip code tabulation area", "zip_code")
        )

        numeric_cols = [
            "median_income",
            "total_pop_over_25",
            "bachelors_degree",
            "masters_degree",
            "professional_degree",
            "doctorate_degree"
        ]

        for c in numeric_cols:
            df = df.withColumn(c, col(c).cast("int"))

        return df

    except Exception as e:
        print(f"[ERROR] Failed processing year {year}: {str(e)}")
        return None


def main():
    spark = (
        SparkSession.builder
        .appName("Census_Full_Load_2020_2024")
        .getOrCreate()
    )

    try:
        dfs = []

        for year in YEARS:
            df = process_year(spark, year)
            if df is not None:
                dfs.append(df)

        if not dfs:
            raise ValueError("No census data was processed successfully.")

        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.unionByName(df)

        print(f"Writing final census dataset to: {FINAL_OUTPUT_PATH}")
        df_final.repartition(1).write.mode("overwrite").parquet(FINAL_OUTPUT_PATH)

        print("Census full load completed successfully.")

    except Exception as e:
        print(f"[FATAL ERROR] {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()