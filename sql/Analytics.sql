USE DATABASE NYC_HOUSING_DB;
USE SCHEMA PRODUCTION;

-- Preview final yearly joined table
SELECT *
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY
LIMIT 50;

-- Check grain: should be one row per zip_code + year
SELECT
    zip_code,
    year,
    COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY row_count DESC;

-- Basic row counts for fact tables
SELECT 'FCT_ZILLOW_YEARLY' AS table_name, COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_ZILLOW_YEARLY
UNION ALL
SELECT 'FCT_CENSUS_YEARLY' AS table_name, COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_CENSUS_YEARLY
UNION ALL
SELECT 'FCT_311_YEARLY_TOTAL' AS table_name, COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_TOTAL
UNION ALL
SELECT 'FCT_311_YEARLY_BY_TYPE' AS table_name, COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_BY_TYPE
UNION ALL
SELECT 'FCT_NYC_HOUSING_SIGNALS_YEARLY' AS table_name, COUNT(*) AS row_count
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY;

-- Check missingness in final table
SELECT
    COUNT(*) AS total_rows,
    COUNT(avg_home_price) AS rows_with_price,
    COUNT(median_income) AS rows_with_income,
    COUNT(total_pop_over_25) AS rows_with_population,
    COUNT(complaints_per_1k_population) AS rows_with_complaint_rate
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY;

-- Top zip-years by complaint rate
SELECT
    zip_code,
    year,
    total_complaint_count,
    total_pop_over_25,
    complaints_per_1k_population
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY
WHERE complaints_per_1k_population IS NOT NULL
ORDER BY complaints_per_1k_population DESC
LIMIT 20;

-- Correlation-friendly extract
SELECT
    zip_code,
    year,
    avg_home_price,
    median_income,
    higher_edu_count_per_1k_population,
    total_complaint_count,
    complaints_per_1k_population
FROM NYC_HOUSING_DB.PRODUCTION.FCT_NYC_HOUSING_SIGNALS_YEARLY
WHERE avg_home_price IS NOT NULL
  AND complaints_per_1k_population IS NOT NULL
  AND median_income IS NOT NULL;

-- Annual complaint type distribution
SELECT
    year,
    complaint_type,
    SUM(complaint_count) AS total_complaints
FROM NYC_HOUSING_DB.PRODUCTION.FCT_311_YEARLY_BY_TYPE
GROUP BY 1, 2
ORDER BY year, total_complaints DESC;