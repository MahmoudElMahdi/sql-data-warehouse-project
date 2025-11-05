/*
	Data Checking and Cleansing Steps:
    1- Check for unwanted Characters
  	2- Check Data Consistency and Standardization.
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
