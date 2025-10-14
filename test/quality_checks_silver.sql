
/*
=====================================================================
Quality Checks
=====================================================================
Script Purpose:
	This script performs various quality checks for the data consistency,
	accuracy and data standardization across the 'silver' schemas. It includes 
	checks for:
	 - Null of duplicate primary keys.
	 - Unwanted spaces in string fields.
	 - Data standardization and consistency.
	 - Invalid date ranges and orders.
	 - Data consistency between related fields.

	 Usage Notes:
		- Run these checks after data loading Silver Layer.
		- Investigate and resolve any descripancies found during the checks.
========================================================================
*/

-- ======================================================
-- bronze.crm_cust_info
-- ======================================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

SELECT *
FROM ( 
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rankx
	FROM bronze.crm_cust_info
	)t
WHERE Rankx = 1

-- Check for unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


--===================================after loading data to silver============================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

SELECT
*
FROM silver.crm_cust_info
WHERE cst_id = 29466;

SELECT *
FROM ( 
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rankx
	FROM silver.crm_cust_info
	)t
WHERE Rankx = 1

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);
 
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info


-- ======================================================
-- bronze.crm_prd_info
-- ======================================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

--- Extract ID from prd_key
SELECT
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info

-- cross check with joining table
SELECT
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_key
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  NOT IN
(SELECT DISTINCT id FROM bronze.erp_PX_CAT_G1V2)

-- cross check with joining table
SELECT
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
(SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details)

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- Check for Nulls or Negetive Numbers
-- Expectation: No Result
SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- prd_line

SELECT DISTINCT
prd_line
FROM bronze.crm_prd_info

SELECT DISTINCT
prd_line,
CASE 
	WHEN prd_line = UPPER(TRIM('M')) THEN 'Mountain'
	WHEN prd_line = UPPER(TRIM('R')) THEN 'Road'
	WHEN prd_line = UPPER(TRIM('S')) THEN 'Shipping'
	WHEN prd_line = UPPER(TRIM('T')) THEN 'Touring'
	ELSE 'n/a'
END
FROM bronze.crm_prd_info

SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.erp_PX_CAT_G1V2;

-- prd_start_dt 
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509'

SELECT
*,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509'

-- =================================after loading silver.crm_prd_info========================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;


-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- Check for Nulls or Negetive Numbers
-- Expectation: No Result
SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- prd_line

SELECT DISTINCT
prd_line
FROM silver.crm_prd_info


-- ======================================================
-- bronze.crm_sales_details
-- ======================================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM silver.crm_prd_info



SELECT
sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN
(SELECT prd_key FROM silver.crm_prd_info)

SELECT
sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN
(SELECT cst_id FROM silver.crm_cust_info)

-- Check for Invalid Dates
SELECT 
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

SELECT 
sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8

SELECT 
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8

SELECT 
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

-- Business Rule
-- Sales = Quantity * Price
-- Negetive, Zeros, Nulls are not allowed
SELECT 
sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL
OR sls_sales <= 0

SELECT 
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_price != sls_sales /sls_quantity OR sls_price IS NULL OR sls_price <= 0


SELECT
	sls_sales,
	sls_quantity,
	sls_price,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales_test,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price_test
FROM bronze.crm_sales_details
WHERE sls_price != sls_sales /sls_quantity OR sls_price IS NULL OR sls_price <= 0
OR sls_sales IS NULL OR sls_sales <= 0
OR sls_quantity IS NULL OR sls_quantity <= 0

-- ======================================================
-- bronze.erp_CUST_AZ12
-- ======================================================
SELECT * FROM bronze.erp_CUST_AZ12;
SELECT cid,
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid
FROM bronze.erp_CUST_AZ12

-- identify out of range date
SELECT bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate <= '1925-01-01'

SELECT bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate >= GETDATE()

-- data standardization & consistency
SELECT DISTINCT gen,
CASE 
	WHEN gen = UPPER(TRIM('F')) THEN 'Female'
	WHEN gen = UPPER(TRIM('M')) THEN 'Male'
	WHEN gen IS NULL OR gen = '' THEN 'n/a'
	ELSE gen
END AS gen
FROM bronze.erp_CUST_AZ12

-- ======================================================
-- bronze.erp_LOC_A101
-- ======================================================
SELECT * FROM bronze.erp_LOC_A101;

SELECT 
REPLACE(cid, '-', '') AS cid
FROM bronze.erp_LOC_A101;

SELECT DISTINCT cntry FROM bronze.erp_LOC_A101;
SELECT cntry AS old,
CASE
WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'United States'
WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_LOC_A101;

-- ======================================================
-- silver.erp_PX_CAT_G1V2
-- ======================================================
SELECT * FROM bronze.erp_PX_CAT_G1V2;

-- check unwanted space
SELECT
cat,
subcat
FROM bronze.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)

SELECT DISTINCT cat FROM bronze.erp_PX_CAT_G1V2;
SELECT DISTINCT subcat FROM bronze.erp_PX_CAT_G1V2;
SELECT DISTINCT MAINTENANCE FROM bronze.erp_PX_CAT_G1V2
