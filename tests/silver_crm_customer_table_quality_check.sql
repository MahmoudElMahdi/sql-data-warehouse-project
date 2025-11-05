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

-- 1- Check for duplicates for Unique Identifier.
-- Expectation: No Result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- Check the id that has 3 rows
SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466
/* 
	After reviewing this record, which contains 3 rows instead of 1, 
	We have determined that we need the most recent update to include the latest information about the customer.
*/
SELECT
*
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info) t WHERE flag_last = 1

-- 2- Check for unwanted Spaces
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
-- Have unwanted spaces
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- Have unwanted spaces
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)
-- Have no unwanted spaces

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1

-- 3- Data Standardization & Consistency
-- Check the gender column
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
-- As a role for the rest of the project, we want all fields to be friendly values, such as female, instead of just the letter f
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1
-- Check the material status column
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info
-- M for Married, S for Single

-- Apply and insert all the cleansing into the silver customer table (loading script)

-- Check again
-- Expectation: No Result
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Overview
SELECT * FROM silver.crm_cust_info


-- ============================================================================
