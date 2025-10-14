/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
    Actions performed :
    - Truncates the silver tables.
    - Insert transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '========================================='
		PRINT 'Loading silver layer'
		PRINT '========================================='

		PRINT '------------------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------------------------------'

		-- Loading silver.crm_cust_info
		PRINT '>> Truncating Table : silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into : silver.crm_cust_info'
		
		SET @batch_start_time = GETDATE()
		SET @start_time = GETDATE()

		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) As cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- Data Normalization & Standardization
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a' --Handled Missing values
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- Data Normalization & Standardization
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date

		FROM (				--- select most recent record (remove duplicate)
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rankx
			FROM bronze.crm_cust_info
			)t
		WHERE Rankx = 1;
		SET @end_time = GETDATE()
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))
		
		-- Loading silver.crm_prd_info
		-- As we added cat_id column we need to update table
		IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
			DROP TABLE silver.crm_prd_info	
		CREATE TABLE silver.crm_prd_info (
			prd_id				INT,
			cat_id				VARCHAR(50),
			prd_key				VARCHAR(50),
			prd_nm				VARCHAR(50),
			prd_cost				VARCHAR(50),
			prd_line				VARCHAR(50),
			prd_start_dt			DATETIME,
			prd_end_dt			DATETIME,
			dwh_create_date		DATETIME2 DEFAULT GETDATE()
		);

		PRINT '>> Truncating Table : silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into : silver.crm_prd_info'
		SET @start_time = GETDATE()
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,	--Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN prd_line = UPPER(TRIM('M')) THEN 'Mountain'
				WHEN prd_line = UPPER(TRIM('R')) THEN 'Road'
				WHEN prd_line = UPPER(TRIM('S')) THEN 'Shipping'
				WHEN prd_line = UPPER(TRIM('T')) THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,		-- Map product line code to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
				AS DATE
			) AS prd_end_dt	-- Calculate end date as one day before next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE()
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))

		-- Loading silver.crm_sales_details
		-- We updated data type oforder, ship and due from 
		-- VARCHAR To DATE hence need to update table
		IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
			DROP TABLE silver.crm_sales_details	
		CREATE TABLE silver.crm_sales_details (
			sls_ord_num			VARCHAR(50),
			sls_prd_key			VARCHAR(50),
			sls_cust_id			INT,
			sls_order_dt			DATE,
			sls_ship_dt			DATE,
			sls_due_dt			DATE,
			sls_sales				INT,
			sls_quantity			INT,
			sls_price				INT,
			dwh_create_date		DATETIME2 DEFAULT GETDATE()
		);

		PRINT '>> Truncating Table : silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into : silver.crm_sales_details'
		SET @start_time = GETDATE()
		INSERT INTO  silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,	-- recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE()
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))

		PRINT '------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------------------------------'

		-- Loading silver.erp_CUST_AZ12
		PRINT '>> Truncating Table : silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data Into : silver.erp_CUST_AZ12'
		SET @start_time = GETDATE()
		INSERT INTO silver.erp_CUST_AZ12 (
		cid,
		bdate,
		gen
		)
		SELECT
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate <= '1925-01-01' THEN 'n/a'
			WHEN bdate >= GETDATE() THEN 'n/a'
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN gen = UPPER(TRIM('F')) THEN 'Female'
			WHEN gen = UPPER(TRIM('M')) THEN 'Male'
			WHEN gen IS NULL OR gen = '' THEN 'n/a'
			ELSE gen
		END AS gen
		FROM bronze.erp_CUST_AZ12;
		
		SET @end_time = GETDATE()
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))

		-- Loading silver.erp_LOC_A101
		PRINT '>> Truncating Table : silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT '>> Inserting Data Into : silver.erp_LOC_A101'
		SET @start_time = GETDATE()
		INSERT INTO  silver.erp_LOC_A101 (
		cid,
		cntry
		)
		SELECT 
			REPLACE(cid, '-', '') AS cid,  -- handled invalid values
			CASE
				WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry  -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_LOC_A101;

		SET @end_time = GETDATE()
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))

		-- Loading silver.erp_PX_CAT_G1V2	
		PRINT '>> Truncating Table : silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data Into : silver.erp_PX_CAT_G1V2'
		SET @start_time = GETDATE()
		INSERT INTO silver.erp_PX_CAT_G1V2 (
		id,
		cat,
		subcat,
		maintenance
		)

		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_PX_CAT_G1V2;

		SET @end_time = GETDATE()
		SET @batch_end_time = GETDATE()
		
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR(50))
		PRINT '========================================='
		PRINT 'Loading Silver layer is Completed'
		PRINT 'Total Load Duration : ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR(50))
		PRINT '========================================='
	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS VARCHAR)
		PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS VARCHAR)
		PRINT '========================================='
	END CATCH
END
