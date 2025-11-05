/*
	Data Checking and Cleansing Steps:
  1- Check for unwanted Characters
  2- Check for unwanted Spaces
	3- Check Data Standardization & Consistency.
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
