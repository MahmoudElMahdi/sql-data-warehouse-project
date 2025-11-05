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

SELECT * FROM bronze.erp_loc_a101
SELECT cst_key FROM silver.crm_cust_info
-- cid has extra '-' needs to be removed, and the city column needs to be consistent and standard with the full country name.
SELECT REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') 
NOT IN
(SELECT cst_key FROM silver.crm_cust_info)
-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry as old_cntry, 
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
