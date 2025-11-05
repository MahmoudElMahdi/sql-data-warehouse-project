/*
	==================================================================================
	Quality Checks
	==================================================================================
	Script Purpose:
		This script performs various quality checks for data consistency, accuracy,
	 	and standardization across the 'silver' schema. It includes checks for:
		- Null or duplicated primary keys.
		- Unwanted spaces in string fields.
		- Data standardization and consistency.
		- Invalid date ranges and orders.
		- Data consistency between related fields.
	Usage Notes:
		- Run these checks after loading the Silver Layer.
		- Investigate and resolve any discrepancies found during the checks.
	==================================================================================
*/

-- 1- Check for duplicates for the Unique Identifier.
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL


SELECT
prd_id,
prd_key,
SUBSTRING(prd_key, 1, 5) AS cat_id, -- For future relation with erp_px_cat_g1v2
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
-- Here is a _ and in crm_prd is a -
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- For future relation with erp_px_cat_g1v2
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

-- 2- Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL
-- We found NULLs.
-- Replacing NULLs with 0.
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- For future relation with erp_px_cat_g1v2
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost , 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

-- Check the possible values in the product line column
SELECT DISTINCT prd_line FROM bronze.crm_prd_info

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- For future relation with erp_px_cat_g1v2
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost , 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

-- Check the date columns
SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt
-- We have always end date < start

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- =========================================================================
-- Apply and insert all the cleansing into the silver product table (loading script)

-- Recheck
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT DISTINCT prd_line FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt
