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
-- 1- Check white space
-- sls_ord_num
-- Expectation: No Result
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sls_info]
  WHERE sls_ord_num != TRIM(sls_ord_num) 
-- sls_prd_key
-- Expectation: No Result
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sls_info]
  WHERE sls_prd_key != TRIM(sls_prd_key)
-- 2- Check for negative or null values
-- sls_cust_id
-- Expectation: No Result
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sls_info]
  WHERE sls_cust_id < 0 OR sls_cust_id IS NULL
-- 3- Check integrity with product and customer tables
-- Product table
-- Expectation: No Result
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sls_info]
  WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
-- Customer table
-- Expectation: No Result
SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sls_info]
  WHERE [sls_cust_id] NOT IN (SELECT cst_id FROM silver.crm_cust_info)
-- 4- Check for Invalid Dates
-- Expectation: No Result
SELECT sls_order_dt
FROM bronze.crm_sls_info
WHERE sls_order_dt <= 0
-- We have 0 values -> Convert it to null
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sls_info
WHERE sls_order_dt <= 0

SELECT sls_ship_dt
FROM bronze.crm_sls_info
WHERE sls_ship_dt <= 0

SELECT sls_due_dt
FROM bronze.crm_sls_info
WHERE sls_due_dt <= 0

-- 5- Converting INT to DATE
-- Check the length of the INT
-- Check date boundries
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sls_info
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_sls_info
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero, or Negative.
SELECT sls_sales as old_s,
       sls_quantity as old_q,
       sls_price as old_p,

CASE WHEN sls_sales <= 0 OR sls_sales = NULL OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price <= 0 OR sls_price = NULL
        THEN ABS(sls_price) / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS sls_price

FROM bronze.crm_sls_info
/*WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales = NULL OR sls_quantity = NULL OR sls_price = NULL*/

-- Check Again
SELECT sls_sales as old_s,
       sls_quantity as old_q,
       sls_price as old_p,

CASE WHEN sls_sales <= 0 OR sls_sales = NULL OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price <= 0 OR sls_price = NULL
        THEN ABS(sls_price) / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS sls_price

FROM bronze.crm_sls_info
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales = NULL OR sls_quantity = NULL OR sls_price = NULL


-- Apply and insert all the cleansing into silver sales table (loading script)
-- Check after insertion
SELECT * FROM silver.crm_sls_info WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales = NULL OR sls_quantity = NULL OR sls_price = NULL

SELECT * FROM silver.crm_sls_info
