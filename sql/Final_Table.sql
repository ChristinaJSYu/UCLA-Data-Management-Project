USE DATABASE NYC_HOUSING_DB;
USE SCHEMA PRODUCTION;

-- =========================================================
-- 1. DIMENSIONS
-- =========================================================

CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.DIM_ZIP AS
SELECT DISTINCT
    TRIM(zip_code) AS zip_code,
    city,
    countyname,
    metro,
    state,
    statename,
    regiontype
FROM NYC_HOUSING_DB.STAGING.stg_zillow_data
WHERE zip_code IS NOT NULL;

CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.DIM_COMPLAINT_TYPE AS
SELECT DISTINCT
    complaint_type
FROM NYC_HOUSING_DB.STAGING.stg_311_data
WHERE complaint_type IS NOT NULL;

CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.DIM_YEAR AS
SELECT DISTINCT year
FROM (
    SELECT year FROM NYC_HOUSING_DB.STAGING.stg_zillow_data
    UNION
    SELECT data_year AS year FROM NYC_HOUSING_DB.STAGING.stg_census_data
    UNION
    SELECT YEAR(created_date) AS year FROM NYC_HOUSING_DB.STAGING.stg_311_data
)
WHERE year IS NOT NULL;

-- =========================================================
-- 2. FACT TABLES
-- =========================================================

-- Zillow annual fact: one row per zip_code + year
CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.FCT_ZILLOW_YEARLY AS
SELECT
    TRIM(zip_code) AS zip_code,
    year,
    AVG(price) AS avg_home_price
FROM NYC_HOUSING_DB.STAGING.stg_zillow_data
WHERE zip_code IS NOT NULL
  AND year IS NOT NULL
  AND price IS NOT NULL
GROUP BY 1, 2;

-- Census annual fact: one row per zip_code + year
CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.FCT_CENSUS_YEARLY AS
SELECT
    TRIM(zip_code) AS zip_code,
    data_year AS year,
    MAX(name) AS area_name,
    MAX(median_income) AS median_income,
    MAX(total_pop_over_25) AS total_pop_over_25,
    MAX(bachelors_degree) AS bachelors_degree,
    MAX(masters_degree) AS masters_degree,
    MAX(professional_degree) AS professional_degree,
    MAX(doctorate_degree) AS doctorate_degree,
--    MAX(state) AS state,
    CASE
        WHEN MAX(total_pop_over_25) IS NOT NULL AND MAX(total_pop_over_25) > 0
        THEN (
            MAX(bachelors_degree)
            + MAX(masters_degree)
            + MAX(professional_degree)
            + MAX(doctorate_degree)
        ) / MAX(total_pop_over_25) * 1000
        ELSE NULL
    END AS higher_edu_count_per_1k_population
FROM NYC_HOUSING_DB.STAGING.stg_census_data
WHERE zip_code IS NOT NULL
  AND data_year IS NOT NULL
GROUP BY 1, 2;

-- 311 annual fact: total complaints, one row per zip_code + year
CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_TOTAL AS
SELECT
    TRIM(incident_zip) AS zip_code,
    YEAR(created_date) AS year,
    COUNT(DISTINCT unique_key) AS complaint_count
FROM NYC_HOUSING_DB.STAGING.stg_311_data
WHERE incident_zip IS NOT NULL
  AND created_date IS NOT NULL
GROUP BY 1, 2;

-- 311 annual fact by complaint type: one row per zip_code + year + complaint_type
CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_BY_TYPE AS
SELECT
    TRIM(incident_zip) AS zip_code,
    YEAR(created_date) AS year,
    complaint_type,
    COUNT(DISTINCT unique_key) AS complaint_count
FROM NYC_HOUSING_DB.STAGING.stg_311_data
WHERE incident_zip IS NOT NULL
  AND created_date IS NOT NULL
  AND complaint_type IS NOT NULL
GROUP BY 1, 2, 3;

-- =========================================================
-- 3. FINAL JOINED TABLE
-- Grain: one row per zip_code + year
-- =========================================================

CREATE OR REPLACE TABLE NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY AS
WITH base AS (
    SELECT
        z.zip_code,
        z.year,

        z.avg_home_price,
        c.median_income,
        c.total_pop_over_25,
        c.bachelors_degree,
        c.masters_degree,
        c.professional_degree,
        c.doctorate_degree,
        c.higher_edu_count_per_1k_population,
        COALESCE(t.complaint_count, 0) AS total_complaint_count,

        CASE
            WHEN c.total_pop_over_25 IS NOT NULL AND c.total_pop_over_25 > 0
            THEN COALESCE(t.complaint_count, 0) / c.total_pop_over_25 * 1000
            ELSE NULL
        END AS complaints_per_1k_population

    FROM NYC_HOUSING_DB.PRODUCTION.FCT_ZILLOW_YEARLY z

    JOIN NYC_HOUSING_DB.PRODUCTION.FCT_CENSUS_YEARLY c
        ON z.zip_code = c.zip_code
       AND z.year = c.year

    JOIN NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_TOTAL t
        ON z.zip_code = t.zip_code
       AND z.year = t.year
),
with_prev AS (
    SELECT
        *,
        LAG(avg_home_price) OVER (
            PARTITION BY zip_code
            ORDER BY year
        ) AS prev_avg_home_price,
        LAG(complaints_per_1k_population) OVER (
            PARTITION BY zip_code
            ORDER BY year
        ) AS prev_complaints_per_1k_population,
        LAG(median_income) OVER (
            PARTITION BY zip_code
            ORDER BY year
        ) AS prev_median_income,
        LAG(higher_edu_count_per_1k_population) OVER (
            PARTITION BY zip_code
            ORDER BY year
        ) AS prev_higher_edu_count_per_1k_population
    FROM base
),
with_delta AS (
    SELECT
        *,
        CASE
            WHEN prev_avg_home_price IS NOT NULL AND prev_avg_home_price <> 0
            THEN (avg_home_price - prev_avg_home_price) / prev_avg_home_price
            ELSE NULL
        END AS delta_p,
        CASE
            WHEN prev_complaints_per_1k_population IS NOT NULL
             AND prev_complaints_per_1k_population <> 0
            THEN (complaints_per_1k_population - prev_complaints_per_1k_population)
                 / prev_complaints_per_1k_population
            ELSE NULL
        END AS delta_c,
        CASE
            WHEN prev_median_income IS NOT NULL AND prev_median_income <> 0
            THEN (median_income - prev_median_income) / prev_median_income
            ELSE NULL
        END AS delta_income,
        CASE
            WHEN prev_higher_edu_count_per_1k_population IS NOT NULL
             AND prev_higher_edu_count_per_1k_population <> 0
            THEN (higher_edu_count_per_1k_population - prev_higher_edu_count_per_1k_population)
                 / prev_higher_edu_count_per_1k_population
            ELSE NULL
        END AS delta_edu,
        CASE
            WHEN prev_avg_home_price IS NOT NULL
             AND prev_avg_home_price <> 0
             AND prev_complaints_per_1k_population IS NOT NULL
             AND prev_complaints_per_1k_population <> 0
            THEN
                ((avg_home_price - prev_avg_home_price) / prev_avg_home_price) *
                ((complaints_per_1k_population - prev_complaints_per_1k_population) / prev_complaints_per_1k_population)
            ELSE NULL
        END AS correlation_signal
    FROM with_prev
)
SELECT
    *,
    AVG(delta_p) OVER (PARTITION BY year) AS city_avg_delta_p,
    AVG(delta_c) OVER (PARTITION BY year) AS city_avg_delta_c,
    AVG(delta_income) OVER (PARTITION BY year) AS city_avg_delta_income,
    AVG(delta_edu) OVER (PARTITION BY year) AS city_avg_delta_edu,

    delta_p - AVG(delta_p) OVER (PARTITION BY year) AS adjusted_delta_p,
    delta_c - AVG(delta_c) OVER (PARTITION BY year) AS adjusted_delta_c,
    delta_income - AVG(delta_income) OVER (PARTITION BY year) AS adjusted_delta_income,
    delta_edu - AVG(delta_edu) OVER (PARTITION BY year) AS adjusted_delta_edu,

    (delta_p - AVG(delta_p) OVER (PARTITION BY year)) *
    (delta_c - AVG(delta_c) OVER (PARTITION BY year)) AS adjusted_correlation_signal
FROM with_delta;

-- =========================================================
-- 4. OPTIONAL VIEW FOR ANALYSIS
-- Same grain: zip_code + year
-- =========================================================

CREATE OR REPLACE VIEW NYC_HOUSING_DB.PRODUCTION.VW_NYC_HOUSING_SIGNALS_YEARLY AS
SELECT *
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY;

SELECT * FROM NYC_HOUSING_DB.PRODUCTION.VW_NYC_HOUSING_SIGNALS_YEARLY LIMIT 1000;