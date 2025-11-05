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

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- Check id with cat id in crm product table
SELECT id
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT cat_id
FROM silver.crm_prd_info

-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistecy
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
