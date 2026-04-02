import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Configuration from pipeline.env
GCS_BUCKET = os.getenv("GCS_BUCKET", "msba405-nyc-housing-data")

INPUT_PATH = f"gs://{GCS_BUCKET}/raw/zillow_housing.csv"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/processed/zillow_clean"

# NYC ZIP codes
NYC_ZIP_CODES = [
    # Bronx
    10463, 10471, 10466, 10469, 10470, 10475, 10458, 10467, 10468,
    10461, 10462, 10464, 10465, 10472, 10473, 10453, 10457, 10460,
    10451, 10452, 10456, 10454, 10455, 10459, 10474,
    # Brooklyn
    11211, 11222, 11201, 11205, 11215, 11217, 11231, 11213, 11212,
    11216, 11233, 11238, 11207, 11208, 11220, 11232, 11204, 11218,
    11219, 11230, 11203, 11210, 11225, 11226, 11234, 11236, 11239,
    11209, 11214, 11228, 11223, 11224, 11229, 11235, 11206, 11221, 11237,
    # Manhattan
    10031, 10032, 10033, 10034, 10040, 10026, 10027, 10030, 10037,
    10039, 10029, 10035, 10023, 10024, 10025, 10021, 10028, 10044,
    10128, 10001, 10011, 10018, 10019, 10020, 10036, 10010, 10016,
    10017, 10022, 10012, 10013, 10014, 10002, 10003, 10009, 10004,
    10005, 10006, 10007, 10038, 10280,
    # Queens
    11101, 11102, 11103, 11104, 11105, 11106, 11368, 11369, 11370,
    11372, 11373, 11377, 11378, 11354, 11355, 11356, 11357, 11358,
    11359, 11360, 11361, 11362, 11363, 11364, 11374, 11375, 11379,
    11385, 11365, 11366, 11367, 11414, 11415, 11416, 11417, 11418,
    11419, 11420, 11421, 11412, 11423, 11432, 11433, 11434, 11435,
    11436, 11004, 11005, 11411, 11413, 11422, 11426, 11427, 11428,
    11429, 11691, 11692, 11693, 11694, 11695, 11697,
    # Staten Island
    10302, 10303, 10310, 10301, 10304, 10305, 10314, 10306, 10307,
    10308, 10309, 10312
]


def process_zillow_data():
    spark = (
        SparkSession.builder
        .appName("Zillow-Housing-Transformation")
        .getOrCreate()
    )

    print(f"Reading Zillow raw data from: {INPUT_PATH}")

    try:
        # Read as strings first so the date-column unpivot is predictable
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(INPUT_PATH)
        )

        # RegionName is the ZIP code field in Zillow
        df = df.withColumn("RegionName", col("RegionName").cast("int"))

        # Keep only NYC ZIP codes
        df = df.filter(col("RegionName").isin(NYC_ZIP_CODES))

        # Identify monthly Zillow value columns like 2020-01-31
        date_cols = [c for c in df.columns if c.startswith("20")]

        if not date_cols:
            raise ValueError("No Zillow date columns found to unpivot.")

        # Unpivot wide monthly columns into long format
        n = len(date_cols)
        stack_args = ", ".join([f"'{c}', `{c}`" for c in date_cols])
        stack_expr = f"stack({n}, {stack_args}) as (date_str, price)"

        df_long = df.selectExpr(
            "RegionID",
            "SizeRank",
            "CAST(RegionName AS STRING) as zip_code",
            "RegionType as regiontype",
            "StateName as statename",
            "State as state",
            "City as city",
            "Metro as metro",
            "CountyName as countyname",
            stack_expr
        )

        df_final = (
            df_long
            .withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("price", col("price").cast("double"))
            .drop("date_str")
            .filter(col("date").isNotNull())
            .filter(col("price").isNotNull())
        )

        # Keep the columns expected by downstream SQL, plus useful date fields
        df_final = df_final.select(
            "RegionID",
            "SizeRank",
            "zip_code",
            "regiontype",
            "statename",
            "state",
            "city",
            "metro",
            "countyname",
            "price",
            "date",
            "year",
            "month"
        )

        (
            df_final
            .repartition(1)
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        print(f"Zillow data successfully processed and saved to: {OUTPUT_PATH}")

    except Exception as e:
        print(f"Error during Zillow Spark processing: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    process_zillow_data()