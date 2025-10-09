/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from Bronze Layer. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME
        DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
        BEGIN TRY
                PRINT '======================================================='
                PRINT 'Loading Silver Layer'
                PRINT '======================================================='

                PRINT '-----------------------------------------------------------------------------------------'
                PRINT 'Loading CRM Tables'
                PRINT '-----------------------------------------------------------------------------------------'

                PRINT '>> Truncating Table :  silver.crm_cust_info'
                SET @batch_start_time = GETDATE()
                SET @start_time = GETDATE()
                TRUNCATE TABLE  silver.crm_cust_info;
                INSERT INTO silver.crm_cust_info (
                cst_id,							
                cst_key,							
                cst_firstname,	
                cst_lastname	,			
                cst_marital_status,		
                cst_gndr,							
                cst_create_date
                )

                SELECT 
		                cst_id,
		                cst_key,
		                TRIM(cst_firstname) AS cst_firstname,
		                TRIM(cst_lastname) AS cst_lastname,
		                CASE 
				                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				                ELSE 'N/A'
		                END AS cst_marital_status,
		                CASE 
				                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				                ELSE 'N/A'
		                END AS cst_gndr,
		                cst_create_date
                FROM (SELECT *,
			                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) ranks
			                FROM bronze.crm_cust_info
			                WHERE cst_id IS NOT NULL
			                )t
                WHERE ranks = 1
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'
                --=======================INSERT INTO silver.crm_prd_info===================================================================================
                PRINT 'Truncating Table :  silver.crm_prd_info'
                TRUNCATE TABLE  silver.crm_prd_info;
                INSERT INTO silver.crm_prd_info(
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
                    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
                    SUBSTRING(prd_key,7, LEN(prd_key)) prd_key,
                    prd_nm,           
                    ISNULL(prd_cost, 0) prd_cost,
                    CASE
                            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Shipping'
                            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                            ELSE 'N/A'
                    END AS prd_line,  
                    CAST(prd_start_dt AS DATE) AS prd_start_dt,
                    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
                FROM bronze.crm_prd_info;
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'
                --=======================INSERT INTO silver.crm_sales_details===================================================================================
                PRINT 'Truncating Table :  silver.crm_sales_details'
                TRUNCATE TABLE  silver.crm_sales_details;
                INSERT INTO silver.crm_sales_details(
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
                        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                        END AS sls_order_dt,

                        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                        END AS sls_ship_dt,        
        
                        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                        END AS sls_due_dt,      

                        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                                ELSE sls_sales
                        END AS sls_sales,

                        sls_quantity,
                        CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
                                ELSE sls_price
                        END AS sls_price
                FROM bronze.crm_sales_details
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'
                /*
                IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	                DROP TABLE silver.crm_sales_details
                GO

                CREATE TABLE silver.crm_sales_details (
                        sls_ord_num           NVARCHAR(50),
                        sls_prd_key             NVARCHAR(50),
                        sls_cust_id               INT,
                        sls_order_dt            DATE,
                        sls_ship_dt               DATE,
                        sls_due_dt               DATE,
                        sls_sales                   INT,
                        sls_quantity             INT,
                        sls_price                   INT,
                        dwh_create_date        DATETIME2 DEFAULT GETDATE()
                );
                */
                --=======================INSERT INTO silver.erp_cust_az12===================================================================================
                PRINT '-----------------------------------------------------------------------------------------'
                PRINT 'Loading ERP Tables'
                PRINT '-----------------------------------------------------------------------------------------'

                PRINT '>> Truncating Table :  silver.erp_cust_az12'
                TRUNCATE TABLE  silver.erp_cust_az12;
                INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
                SELECT 
                CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		                ELSE cid
                END AS cid,
                CASE WHEN bdate > GETDATE() THEN NULL
		                ELSE bdate
                END AS bdate,

                CASE 
		                WHEN UPPER(TRIM(gen)) IN ('FEMALE',  'F') THEN 'Female'
		                WHEN UPPER(TRIM(gen)) IN ('MALE', 'M') THEN 'Male'
		                ELSE 'N/A'
                END AS gen
                FROM bronze.erp_cust_az12
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'

                --=======================INSERT INTO silver.erp_loc_a101===================================================================================
                PRINT '>>Truncating Table :  silver.erp_loc_a101'
                TRUNCATE TABLE  silver.erp_loc_a101;
                INSERT INTO silver.erp_loc_a101(cid, cntry)
                SELECT 
                REPLACE(cid, '-', '') cid,
                CASE 
		                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		                WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		                WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		                ELSE cntry
                END cntry
                FROM bronze.erp_loc_a101
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'

                --=======================INSERT INTO silver.erp_px_cat_g1v2===================================================================================
                PRINT '>>Truncating Table :  silver.erp_px_cat_g1v2'
                TRUNCATE TABLE  silver.erp_px_cat_g1v2;
                INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat,maintenance)

                SELECT
                id, cat, subcat,maintenance
                FROM bronze.erp_px_cat_g1v2
                SET @end_time = GETDATE()
                PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
                PRINT '>>------------------------------------------------------------------------->>'

                SET @batch_end_time = GETDATE()
                PRINT 'Loading Silver Layer is Completed'
                PRINT '>> Total Loading duration is : ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds'

        END TRY
        BEGIN CATCH
                PRINT '=================================================='
                PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
                PRINT 'Error Message' + ERROR_MESSAGE();
                PRINT 'Error Number' + CAST(ERROR_NUMBER() AS VARCHAR);
                PRINT 'Error State' + CAST(ERROR_STATE() AS VARCHAR);
                PRINT '=================================================='
        END CATCH
END

EXEC silver.load_silver


