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

SELECT * FROM bronze.erp_cust_az12
-- Check the customer ID with the customer key in the customers table from CRM
SELECT * FROM [silver].[crm_cust_info]
-- Removing the extra 3 letters at the start of the customer key "NAS"
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
-- Check
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END
	NOT IN
	(SELECT DISTINCT cst_key FROM silver.crm_cust_info)
-- Check the bdate column
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()
-- There are very old dates and impossible dates that are in the future
SELECT 
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()
-- Check Gender
SELECT DISTINCT gen
FROM bronze.erp_cust_az12
-- Keep only Male & Female
SELECT
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12



