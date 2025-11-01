/*
======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
	None.
  This stored procedure does not accept anhy parameters or return any values.

Usage Example
	EXEC bronze.load_bronze;
======================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME ,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================';

		PRINT '------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------';
		-- Load data to table 'crm_cust_info'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';

		-- Load data to table 'crm_prd_info'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';

		-- Load data to table 'crm_sls_info'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sls_info';
		TRUNCATE TABLE bronze.crm_sls_info;
	
		PRINT '>> Inserting Data Into: crm_sls_info';
		BULK INSERT bronze.crm_sls_info
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';

		PRINT '------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------';
		-- Load data to table 'erp_cust_az12'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT '>> Inserting Data Into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';

		-- Load data to table 'erp_loc_a101'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT '>> Inserting Data Into: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';
	

		-- Load data to table 'erp_px_cat_g1v2'
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT '>> Inserting Data Into: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MahmoudElmahdy\Downloads\2025 - Projects\SQL Project\sql-data-warehouse-project\sql_data_warehouse\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';
		PRINT 'Loading Bronze Layer is Completed';
		SET @batch_end_time = GETDATE();
		PRINT '>> Batch Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'second';
		PRINT '>> ---------------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'Error Occurred During Loading Bronze Layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH
END;
