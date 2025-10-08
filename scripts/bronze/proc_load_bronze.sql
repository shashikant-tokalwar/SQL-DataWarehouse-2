/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME;
        DECLARE @start_batch_time DATETIME, @end_batch_time DATETIME
        BEGIN TRY
                PRINT '======================================================='
                PRINT 'Loading Bronze Layer'
                PRINT '======================================================='

                PRINT '-----------------------------------------------------------------------------------------'
                PRINT 'Loading CRM Tables'
                PRINT '-----------------------------------------------------------------------------------------'
                
                SET @start_batch_time = GETDATE();
                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.crm_cust_info'
                TRUNCATE TABLE bronze.crm_cust_info; 

                PRINT '>> Inserting Data Into Table: bronze.crm_cust_info'
                BULK INSERT bronze.crm_cust_info
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.crm_prd_info'
                TRUNCATE TABLE bronze.crm_prd_info; 

                PRINT '>> Inserting Data Into Table: bronze.crm_prd_info'
                BULK INSERT bronze.crm_prd_info
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.crm_sales_details'
                TRUNCATE TABLE bronze.crm_sales_details; 

                PRINT '>> Inserting Data Into Table: bronze.crm_sales_details'
                BULK INSERT bronze.crm_sales_details
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                PRINT '-----------------------------------------------------------------------------------------'
                PRINT 'Loading ERP Tables'
                PRINT '-----------------------------------------------------------------------------------------'
                
                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.erp_cust_az12'
                TRUNCATE TABLE bronze.erp_cust_az12; 

                PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12'
                BULK INSERT bronze.erp_cust_az12
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.erp_loc_a101'
                TRUNCATE TABLE bronze.erp_loc_a101; 

                PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101'
                BULK INSERT bronze.erp_loc_a101
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                SET @start_time = GETDATE();
                PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
                TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

                PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2'
                BULK INSERT bronze.erp_px_cat_g1v2
                FROM 'C:\Users\Shashikt\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
                WITH(
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        TABLOCK
                );
                SET @end_time = GETDATE();
                PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
                PRINT '>>------------------------------------------------------------------------------------>>'

                SET @end_batch_time = GETDATE();
                PRINT 'Loading Bronze Layer is Completed'
                PRINT 'Total Loading duration is : ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS VARCHAR) + ' seconds'

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



