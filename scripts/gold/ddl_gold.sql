/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
	The Gold layer represent the final dimention and fact tables (Star Schema)

	Each view performs transformations and combines data from the silver layer
	to produce a clean, enriched and business ready dataset.

Usage:
	- These view can be queried directly for analytics and reporting.
===============================================================================
*/

-- ===============================================================
-- Create Dimension: gold.dim_customers
-- ===============================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
		DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
		ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
		ci.cst_id AS customer_id,							
		ci.cst_key AS customer_no,							
		ci.cst_firstname AS first_name,	
		ci.cst_lastname	 AS last_name,			
		ci.cst_marital_status AS marital_status,	
		CASE WHEN cst_gndr != 'N/A' THEN cst_gndr
				ELSE COALESCE(ca.gen, 'N/A')
		END AS gender,
		ca.bdate AS birthdate,
		cntry AS country,
		ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- ===============================================================
-- Create Dimension: gold.dim_products
-- ===============================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
		DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
		ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
		pr.prd_id AS product_id,	
		pr.prd_key AS product_no,							
		pr.prd_nm AS product_name,	
		pr.cat_id AS category_id,
		ca.cat AS category, 
		ca.subcat As subcategory,	
		ca.maintenance,
		pr.prd_cost AS product_cost,	
		pr.prd_line AS product_line,
		pr.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 ca
ON pr.cat_id = ca.id
WHERE pr.prd_end_dt IS NULL -- filter out all historical data

-- ===============================================================
-- Create Dimension: gold.fact_sales
-- ===============================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
		DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
		sls_ord_num AS order_no,           
		pr.product_key,
		cu.customer_key,
		sls_order_dt AS order_date,           
		sls_ship_dt AS ship_date,               
		sls_due_dt AS due_date,    
		sls_sales AS sales_amount,
		sls_quantity AS quantity,
		sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_no
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
